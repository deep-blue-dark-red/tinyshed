#!/usr/bin/env python3
"""tinyshed - a minimal priority-queue scheduler for long jobs: one server, N hosts, ssh in between.

One file, stdlib only (python >= 3.12 plus zstd: stdlib from 3.14, else pyzstd/zstandard or the zstd binary):

  server   ~/.tinyshed/server.db   mirror of every host's jobs + hosts + ingested events
  host     ~/.tinyshed/host.db     AUTHORITATIVE queue + event log (sqlite, WAL)

The server only ever talks to a host by running `tinyshed.py rpc` over ssh (JSON in, JSON out): one
round trip delivers new jobs/cancels, pulls events after a cursor and reads resources. Every operation
is idempotent, so a lost connection just means "try again later"; meanwhile the host's agent keeps
draining its own queue. Hosts can also phone home (`ssh server tinyshed.py ingest`) to push events.

Each job runs under a watchdog process (taskset pinning, runtime timeout, RSS limit, stdout/stderr
capture, retry with backoff) that is detached from the agent, so agents can be upgraded mid-job.

Callbacks: put functions in ~/.tinyshed/hooks.py (server and/or host), each taking one event dict:
  OnJobQueued OnJobStart OnJobCompletion OnJobError OnJobTimeout OnJobOOM OnJobLost OnJobCancel
  OnHostDown OnHostUp (server only)   OnEvent (everything)
"""
import argparse, contextlib, fcntl, importlib, json, os, shlex, shutil, signal, sqlite3, subprocess, sys, tempfile, threading, time, traceback, uuid

VERSION = "0.2"
HOME = os.path.abspath(os.path.expanduser(os.environ.get("TINYSHED_HOME") or "~/.tinyshed"))
SELF = os.path.abspath(__file__)
OUT = sys.stdout
FAILURES = ("error", "timeout", "oom", "lost")
TERMINAL = ("done", "cancelled") + FAILURES
HOOKS = {"queued": "OnJobQueued", "start": "OnJobStart", "done": "OnJobCompletion", "error": "OnJobError",
         "timeout": "OnJobTimeout", "oom": "OnJobOOM", "lost": "OnJobLost", "cancelled": "OnJobCancel",
         "host_down": "OnHostDown", "host_up": "OnHostUp"}
# job fields that travel server -> host, with defaults
SPEC = dict(cmd=None, cwd=None, env=None, prio=0, cpus=1, mem_mb=0, timeout=0, retries=0,
            retry_on="error,lost", retry_delay=30, created=None)


class Fail(Exception):
    """An error to show the user as one line, without a traceback."""


def P(*parts):
    return os.path.join(HOME, *parts)


# ───────────────────────────────────────────────────────────────── shared: sqlite, hooks

def connect(name, schema):
    os.makedirs(P("logs"), exist_ok=True)
    db = sqlite3.connect(P(name), timeout=30, isolation_level=None)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.executescript(schema)
    return db


@contextlib.contextmanager
def tx(db):
    db.execute("BEGIN IMMEDIATE")
    try:
        yield db
    except BaseException:
        db.execute("ROLLBACK")
        raise
    db.execute("COMMIT")


_hooks, _hooks_mtime, _hook_lock, ECHO = {}, None, threading.Lock(), False


def fire(ev):
    """Run the callback(s) for an event. Hook output and tracebacks go to ~/.tinyshed/hooks.log."""
    global _hooks, _hooks_mtime
    if ECHO:
        OUT.write("%s %-10s %-10s %s\n" % (time.strftime("%H:%M:%S"), ev["kind"], ev.get("host") or "-", ev.get("job") or ""))
        OUT.flush()
    path = P("hooks.py")
    with _hook_lock:
        mtime = os.path.getmtime(path) if os.path.exists(path) else None
        if mtime is None:
            return
        with open(P("hooks.log"), "a") as log, contextlib.redirect_stdout(log), contextlib.redirect_stderr(log):
            try:
                if mtime != _hooks_mtime:  # (re)load on change, so long-lived daemons pick up edits
                    _hooks, _hooks_mtime = {}, mtime
                    with open(path) as f:
                        exec(compile(f.read(), path, "exec"), _hooks)
                for name in (HOOKS.get(ev["kind"]), "OnEvent"):
                    if name in _hooks:
                        _hooks[name](ev)
            except Exception:
                traceback.print_exc()


_zmod = False


def zmod():
    """A zstd implementation: stdlib (python >= 3.14), pyzstd or zstandard; None means use the zstd binary."""
    global _zmod
    if _zmod is False:
        for name in ("compression.zstd", "pyzstd", "zstandard"):
            try:
                _zmod = importlib.import_module(name)
                break
            except ImportError:
                pass
        else:
            if not shutil.which("zstd"):
                sys.exit("tinyshed: zstd is required: python >= 3.14, or `pip install pyzstd` / `zstandard`, or the zstd binary on PATH")
            _zmod = None
    return _zmod


def pack(obj):
    """JSON -> zstd blob: how every stored job/event payload is kept."""
    raw, z = json.dumps(obj, separators=(",", ":")).encode(), zmod()
    return z.compress(raw) if z else subprocess.run(["zstd", "-qc"], input=raw, capture_output=True, check=True).stdout


def unpack(blob):
    if isinstance(blob, str):  # rows written before compression
        return json.loads(blob)
    z = zmod()
    return json.loads(z.decompress(bytes(blob)) if z else subprocess.run(["zstd", "-dqc"], input=bytes(blob), capture_output=True, check=True).stdout)


def zopen(path):
    """Readable stream of a .zst file's decompressed bytes."""
    z = zmod()
    return z.open(path, "rb") if z else subprocess.Popen(["zstd", "-dqc", path], stdout=subprocess.PIPE).stdout


def compress_log(jid):
    """A job reached a final state: its log will never be appended to again, so keep it as ID.log.zst."""
    src, dst = P("logs", jid + ".log"), P("logs", jid + ".log.zst")
    try:
        z = zmod()
        with open(src, "rb") as i:
            if z:
                with z.open(dst + ".tmp", "wb") as o:
                    shutil.copyfileobj(i, o, 1 << 20)
            else:
                with open(dst + ".tmp", "wb") as o:
                    subprocess.run(["zstd", "-qc"], stdin=i, stdout=o, check=True)
        os.replace(dst + ".tmp", dst)
        os.remove(src)
    except (OSError, subprocess.SubprocessError):
        pass  # no log, or compression failed: the plain log stays


def log_tail(jid, n=2000):
    """Last n bytes of a job's log, live (plain) or finished (zstd)."""
    path, buf = P("logs", jid + ".log"), b""
    try:
        if os.path.exists(path):
            with open(path, "rb") as f:
                f.seek(0, 2)
                f.seek(max(0, f.tell() - n))
                buf = f.read()
        else:
            with zopen(path + ".zst") as f:
                for chunk in iter(lambda: f.read(1 << 20), b""):
                    buf = (buf + chunk)[-n:]
    except (OSError, EOFError):
        pass
    return buf.decode("utf-8", "replace")


def dur(s):
    """'90', '90s', '30m', '2h', '1d' -> seconds"""
    s = str(s).strip().lower()
    mult = {"s": 1, "m": 60, "h": 3600, "d": 86400}.get(s[-1:])
    return float(s[:-1]) * mult if mult else float(s)


def size(s):
    """'500', '500M', '8G' -> MB"""
    s = str(s).strip().upper().rstrip("B")
    mult = {"K": 1 / 1024, "M": 1, "G": 1024, "T": 1024 * 1024}.get(s[-1:])
    return int(float(s[:-1]) * mult) if mult else int(float(s))


# ───────────────────────────────────────────────────────────────── host: state

HOST_SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs(
  id TEXT PRIMARY KEY, cmd TEXT, cwd TEXT, env TEXT, prio INT, cpus INT, mem_mb INT, timeout REAL,
  retries INT, retry_on TEXT, retry_delay REAL, created REAL,
  state TEXT, attempt INT DEFAULT 0, not_before REAL DEFAULT 0, cores TEXT, pid INT, rc INT,
  started REAL, ended REAL, peak_mb INT, cancel INT DEFAULT 0);
CREATE INDEX IF NOT EXISTS jobs_queue ON jobs(state, prio DESC, created);
CREATE TABLE IF NOT EXISTS events(id INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL, job TEXT, kind TEXT, data TEXT);
CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v TEXT);
"""


def hdb():
    db = connect("host.db", HOST_SCHEMA)
    if not meta(db, "epoch"):  # identifies this incarnation of the event log
        db.execute("INSERT OR IGNORE INTO meta VALUES('epoch',?)", (uuid.uuid4().hex,))
    return db


def meta(db, k, v=None):
    if v is not None:
        db.execute("INSERT OR REPLACE INTO meta VALUES(?,?)", (k, str(v)))
    r = db.execute("SELECT v FROM meta WHERE k=?", (k,)).fetchone()
    return r and r[0]


def load_cfg():
    try:
        with open(P("config.json")) as f:
            return json.load(f)
    except (OSError, ValueError):
        return {}


def parse_cores(s):
    out = []
    for part in s.split(","):
        a, _, b = part.partition("-")
        out += range(int(a), int(b or a) + 1)
    return out


def host_cores(cfg):
    if cfg.get("cores"):
        return parse_cores(cfg["cores"])
    try:
        return sorted(os.sched_getaffinity(0))
    except AttributeError:  # not linux
        return list(range(os.cpu_count() or 1))


def used_cores(db):
    used = set()
    for r in db.execute("SELECT cores FROM jobs WHERE state='running' AND cores IS NOT NULL"):
        used.update(parse_cores(r[0]))
    return used


def check_cfg(cfg):
    """Reject a config that would break the agent before it reaches disk."""
    if not isinstance(cfg, dict):
        raise Fail("config must be a JSON object")
    try:
        host_cores(cfg)
    except (AttributeError, TypeError, ValueError) as e:
        raise Fail("bad config cores: %s" % e)
    return cfg


def save_cfg(cfg):
    """Atomically replace this machine's config.json; the agent reloads it every second."""
    check_cfg(cfg)
    path, tmp = P("config.json"), P("config.json.tmp")
    with open(tmp, "w") as f:
        json.dump(cfg, f, indent=2, sort_keys=True)
        f.write("\n")
    os.replace(tmp, path)
    return cfg


def edit_cfg(cfg, label, put, run=subprocess.call):
    """Open cfg as JSON in $EDITOR; validate and put() it on save. Returns (ok, message)."""
    ed = shlex.split(os.environ.get("VISUAL") or os.environ.get("EDITOR") or "vi")
    fd, path = tempfile.mkstemp(prefix="tshed-config-", suffix=".json")
    with os.fdopen(fd, "w") as f:
        f.write("// tshed config - edit and save; // lines are ignored\n"
                "// keys: name, cores (e.g. \"0-15\"), home ({\"dest\",\"cmd\"} or null)\n")
        json.dump(cfg, f, indent=2, sort_keys=True)
        f.write("\n")
    with open(path) as f:
        before = f.read()
    rc = run(ed + [path])
    if isinstance(rc, int) and rc:
        return False, "editor exited %d; config not changed (your edit is at %s)" % (rc, path)
    with open(path) as f:
        after = f.read()
    if after == before:
        os.unlink(path)
        return True, "config unchanged (%s)" % label
    body = "\n".join(l for l in after.splitlines() if not l.lstrip().startswith("//"))
    try:
        put(check_cfg(json.loads(body)))
    except Exception as e:
        return False, "config not saved: %s; your edit is at %s" % (e, path)
    os.unlink(path)
    return True, "saved config (%s)" % label


def emit(db, jid, kind, extra=None):
    """Append an event carrying a full snapshot of the job row. Call inside tx()."""
    row = db.execute("SELECT * FROM jobs WHERE id=?", (jid,)).fetchone()
    if not row:
        return None
    data = dict(row)
    data.update(extra or {})
    ts = time.time()
    eid = db.execute("INSERT INTO events(ts,job,kind,data) VALUES(?,?,?,?)", (ts, jid, kind, pack(data))).lastrowid
    return dict(host=load_cfg().get("name"), eid=eid, ts=ts, job=jid, kind=kind, data=data)


def transition(db, jid, kind, extra=None, **cols):
    """Atomically update a job and log the event, then run host-side hooks."""
    with tx(db):
        if cols:
            db.execute("UPDATE jobs SET %s WHERE id=?" % ",".join(k + "=?" for k in cols), list(cols.values()) + [jid])
        ev = emit(db, jid, kind, extra)
    if ev:
        fire(ev)


def finish(db, jid, kind, rc=None, peak_mb=None, **extra):
    """End an attempt: requeue with backoff if the failure is retryable, else make it final."""
    j = db.execute("SELECT * FROM jobs WHERE id=?", (jid,)).fetchone()
    if j["cancel"] and kind != "done":
        kind = "cancelled"
    now = time.time()
    cols = dict(state=kind, rc=rc, ended=now, pid=None, cores=None, peak_mb=peak_mb)
    final = not (kind in FAILURES and kind in j["retry_on"].split(",") and j["attempt"] <= j["retries"])
    if not final:
        cols.update(state="queued", not_before=now + j["retry_delay"] * 2 ** (j["attempt"] - 1))
    transition(db, jid, kind, dict(extra, final=final), **cols)
    if final:
        compress_log(jid)


def events_after(db, after, limit=500):
    return [dict(eid=r["id"], ts=r["ts"], job=r["job"], kind=r["kind"], data=unpack(r["data"]))
            for r in db.execute("SELECT * FROM events WHERE id>? ORDER BY id LIMIT ?", (after, limit))]


# ───────────────────────────────────────────────────────────────── host: watchdog (one per running job)

def tree_rss_mb(sid):
    """Summed RSS of every process in the job's session (shared pages are counted per process)."""
    total = 0
    if os.path.isdir("/proc/self"):
        page = os.sysconf("SC_PAGE_SIZE")
        for pid in os.listdir("/proc"):
            if pid.isdigit():
                try:
                    with open("/proc/%s/stat" % pid) as f:
                        fields = f.read().rsplit(")", 1)[1].split()  # [0]=state [3]=session [21]=rss pages
                    if int(fields[3]) == sid:
                        total += int(fields[21]) * page
                except (OSError, IndexError, ValueError):
                    pass
        return total >> 20
    try:  # macOS / BSD: fall back to the process group
        out = subprocess.run(["ps", "-axo", "pgid=,rss="], capture_output=True, text=True, timeout=10).stdout
        return sum(int(l.split()[1]) for l in out.splitlines() if l.split()[:1] == [str(sid)]) >> 10
    except (OSError, ValueError, IndexError, subprocess.SubprocessError):
        return 0


def kill_group(p, grace=10):
    try:
        os.killpg(p.pid, signal.SIGTERM)
        p.wait(timeout=grace)
    except (OSError, subprocess.TimeoutExpired):
        pass
    with contextlib.suppress(OSError):
        os.killpg(p.pid, signal.SIGKILL)
    p.wait()


def run_job(jid):
    """The watchdog. Inherits a held flock on run/<jid>.lock from the agent: lock released == watchdog dead."""
    db = hdb()
    j = db.execute("SELECT * FROM jobs WHERE id=?", (jid,)).fetchone()
    stop = []
    signal.signal(signal.SIGTERM, lambda *a: stop.append(1))
    logp = P("logs", jid + ".log")
    log = open(logp, "ab")
    log.write(("=== tinyshed %s attempt %d cores=%s %s ===\n" % (jid, j["attempt"], j["cores"], time.strftime("%F %T"))).encode())
    log.flush()
    cmd = ["bash", "-c", j["cmd"]]
    if j["cores"] and shutil.which("taskset"):
        cmd = ["taskset", "-c", j["cores"]] + cmd
    env = dict(os.environ, **json.loads(j["env"] or "{}"))
    env.update(TINYSHED_JOB=jid, TINYSHED_ATTEMPT=str(j["attempt"]), TINYSHED_CORES=j["cores"] or "")
    try:
        p = subprocess.Popen(cmd, stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT, env=env,
                             cwd=os.path.expanduser(j["cwd"] or "~"), start_new_session=True)
    except OSError as e:
        log.write(("tinyshed: cannot start: %s\n" % e).encode())
        log.flush()
        return finish(db, jid, "error", rc=127, tail=str(e))
    transition(db, jid, "start", pid=p.pid)
    deadline = time.monotonic() + j["timeout"] if j["timeout"] else None
    kind, peak, tick = None, 0, 0
    while kind is None:
        try:
            p.wait(timeout=1)
            break
        except subprocess.TimeoutExpired:
            tick += 1
        if stop:
            kind = "cancelled"
        elif deadline and time.monotonic() > deadline:
            kind = "timeout"
        elif tick % 2 == 0:
            if j["mem_mb"] or tick % 10 == 0:
                peak = max(peak, tree_rss_mb(p.pid))
            if j["mem_mb"] and peak > j["mem_mb"]:
                kind = "oom"
            elif db.execute("SELECT cancel FROM jobs WHERE id=?", (jid,)).fetchone()[0]:
                kind = "cancelled"
    if kind:
        log.write(("=== tinyshed: killing job (%s) ===\n" % kind).encode())
        log.flush()
        kill_group(p)
    else:
        kind = "done" if p.returncode == 0 else "error"
        with contextlib.suppress(OSError):  # no stragglers outlive the job
            os.killpg(p.pid, signal.SIGKILL)
    finish(db, jid, kind, rc=p.returncode, peak_mb=peak, tail=log_tail(jid))


# ───────────────────────────────────────────────────────────────── host: agent (the local scheduler)

def detach(args, **kw):
    with open(P("agent.log"), "ab") as log:
        return subprocess.Popen([sys.executable, SELF] + args, stdin=subprocess.DEVNULL, stdout=log, stderr=log,
                                start_new_session=True, **kw)


def try_lock(path):
    """Return a locked file object, or None if another process holds the lock."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    f = open(path, "a+")
    try:
        fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return f
    except OSError:
        f.close()
        return None


def agent_alive():
    f = try_lock(P("agent.lock"))
    if f:
        f.close()
    return not f


def reconcile(db):
    """A 'running' job whose watchdog lock is free has lost its watchdog (crash, kill -9, reboot)."""
    for j in db.execute("SELECT id FROM jobs WHERE state='running'").fetchall():
        f = try_lock(P("run", j["id"] + ".lock"))
        if not f:
            continue
        with f:
            j = db.execute("SELECT * FROM jobs WHERE id=?", (j["id"],)).fetchone()
            if j["state"] != "running":  # the watchdog finished normally in the meantime
                continue
            try:  # kill the orphaned job, but only if the pid provably still is our job (linux)
                with open("/proc/%d/environ" % j["pid"], "rb") as e:
                    if ("TINYSHED_JOB=" + j["id"]).encode() in e.read().split(b"\0"):
                        os.killpg(j["pid"], signal.SIGKILL)
            except (OSError, TypeError):
                pass
            finish(db, j["id"], "lost", tail=log_tail(j["id"]))


def schedule(db, cfg, kids):
    """Strict priority order: highest prio first, FIFO within a priority; no backfilling past a job that does not fit."""
    cores = host_cores(cfg)
    used = used_cores(db)
    free = [c for c in cores if c not in used]
    for j in db.execute("SELECT id, cpus FROM jobs WHERE state='queued' AND cancel=0 AND not_before<=? "
                        "ORDER BY prio DESC, created, id", (time.time(),)).fetchall():
        need = min(max(1, j["cpus"]), len(cores))
        if need > len(free):
            break
        f = try_lock(P("run", j["id"] + ".lock"))
        if not f:  # previous attempt's watchdog is still exiting
            continue
        with f:
            mine, free = free[:need], free[need:]
            with tx(db):
                n = db.execute("UPDATE jobs SET state='running', attempt=attempt+1, cores=?, started=?, ended=NULL, rc=NULL, pid=NULL "
                               "WHERE id=? AND state='queued' AND cancel=0",
                               (",".join(map(str, mine)), time.time(), j["id"])).rowcount
            if n:  # the watchdog inherits the held lock
                kids.append(detach(["_run", j["id"]], pass_fds=[f.fileno()]))


def phone_home(db, cfg, st):
    """Push not-yet-acknowledged events to the server; back off while it is unreachable."""
    home = cfg.get("home")
    if not home or st.get("busy") or time.time() < st.get("next", 0):
        return
    cursor = int(meta(db, "home_cursor") or 0)
    if not db.execute("SELECT 1 FROM events WHERE id>? LIMIT 1", (cursor,)).fetchone():
        return
    st["busy"] = True

    def go():
        tdb = hdb()
        try:
            req = dict(host=cfg.get("name"), epoch=meta(tdb, "epoch"), events=events_after(tdb, cursor))
            argv = ["bash", "-c", home["cmd"]] if home["dest"] == "local" else SSH + [home["dest"], home["cmd"]]
            r = subprocess.run(argv, input=json.dumps(req), capture_output=True, text=True, timeout=90)
            if r.returncode:
                raise RuntimeError((r.stderr.strip().splitlines() or ["exit %d" % r.returncode])[-1])
            meta(tdb, "home_cursor", int(json.loads(r.stdout)["ack"]))
            st.update(fails=0, next=0)
        except Exception as e:
            st["fails"] = st.get("fails", 0) + 1
            st["next"] = time.time() + min(600, 10 * 2 ** st["fails"])
            print("phone home failed (%s), retry in %ds" % (e, st["next"] - time.time()), flush=True)
        finally:
            tdb.close()
            st["busy"] = False

    threading.Thread(target=go, daemon=True).start()


def agent():
    lock = try_lock(P("agent.lock"))
    if not lock:
        return  # already running
    lock.truncate(0)
    lock.write(str(os.getpid()))
    lock.flush()
    db, kids, st = hdb(), [], {}
    print("agent %s started pid=%d %s" % (VERSION, os.getpid(), time.strftime("%F %T")), flush=True)
    while True:
        try:
            cfg = load_cfg()
            reconcile(db)
            schedule(db, cfg, kids)
            phone_home(db, cfg, st)
        except Exception:
            traceback.print_exc()
        kids[:] = [k for k in kids if k.poll() is None]  # reap finished watchdogs
        time.sleep(1)


# ───────────────────────────────────────────────────────────────── host: rpc endpoint + setup


def per_core_ticks():
    """Cumulative [total, idle] CPU ticks per core id, or {} where the OS has no per-core stats.
    Linux: /proc/stat cpuN lines; macOS: host_processor_info via ctypes (mach/machine.h: IDLE=2)."""
    try:
        with open("/proc/stat") as f:
            out = {}
            for line in f:
                p = line.split()
                if p[0].startswith("cpu") and p[0][3:].isdigit():
                    v = [int(x) for x in p[1:]]
                    # guest/guest_nice are already inside user/nice; iowait is idle, not busy
                    out[int(p[0][3:])] = [sum(v[:8]), v[3] + (v[4] if len(v) > 4 else 0)]
            return out
    except OSError:
        pass
    if sys.platform != "darwin":
        return {}
    import ctypes, ctypes.util
    lib = ctypes.CDLL(ctypes.util.find_library("System") or "/usr/lib/libSystem.B.dylib")
    lib.mach_host_self.restype = ctypes.c_uint
    lib.host_processor_info.argtypes = [ctypes.c_uint, ctypes.c_int, ctypes.POINTER(ctypes.c_uint),
                                        ctypes.POINTER(ctypes.POINTER(ctypes.c_int)), ctypes.POINTER(ctypes.c_uint)]
    lib.host_processor_info.restype = ctypes.c_int
    lib.vm_deallocate.argtypes = [ctypes.c_uint, ctypes.c_void_p, ctypes.c_size_t]
    ncpu, arr, n = ctypes.c_uint(), ctypes.POINTER(ctypes.c_int)(), ctypes.c_uint()
    if lib.host_processor_info(lib.mach_host_self(), 2, ctypes.byref(ncpu), ctypes.byref(arr), ctypes.byref(n)):
        return {}
    try:
        out = {}
        for i in range(ncpu.value):
            t = [arr[i * 4 + j] for j in range(4)]  # user, system, idle, nice
            out[i] = [sum(t), t[2]]
        return out
    finally:
        with contextlib.suppress(Exception):
            task = ctypes.c_uint.in_dll(lib, "mach_task_self_")
            lib.vm_deallocate(task, ctypes.cast(arr, ctypes.c_void_p), n.value * ctypes.sizeof(ctypes.c_int))


def cpu_util(db, cores):
    """Measured busy % of tinyshed's own cores, from per-core tick deltas since the previous status()
    call. The snapshot lives in host.db meta because every rpc is a fresh process. None until two
    samples exist, or where the OS exposes no per-core counters."""
    ticks = per_core_ticks()
    ids = [c for c in cores if c in ticks]
    if not ids:
        return None
    cur = {str(c): ticks[c] for c in ids}
    now = time.time()
    try:
        prev = json.loads(meta(db, "cpu") or "null")
    except ValueError:
        prev = None
    pct = None
    if prev and prev.get("cores") == sorted(cur):
        try:
            span = now - prev["ts"]
            total = sum(cur[c][0] - prev["ticks"][c][0] for c in cur)
            idle = sum(cur[c][1] - prev["ticks"][c][1] for c in cur)
            if total > 0 and 0.5 <= span <= 300:
                pct = round(max(0.0, min(100.0, 100 * (1 - idle / total))), 1)
            elif total == 0 or span < 0.5:
                return prev.get("pct")  # not enough elapsed: keep the old baseline and reading
            # counters went backwards (reboot) or the baseline is stale: fall through to a fresh one
        except (KeyError, TypeError, ValueError):
            pass
    meta(db, "cpu", json.dumps(dict(ts=now, cores=sorted(cur), ticks=cur, pct=pct)))
    return pct


def status(db, cfg):
    cores = host_cores(cfg)
    mem_total = mem_avail = None
    try:
        mem_total = os.sysconf("SC_PHYS_PAGES") * os.sysconf("SC_PAGE_SIZE") >> 20
        with open("/proc/meminfo") as f:
            for line in f:
                if line.startswith("MemAvailable"):
                    mem_avail = int(line.split()[1]) >> 10
    except (OSError, ValueError):
        pass
    gpus = []
    if shutil.which("nvidia-smi"):
        try:
            out = subprocess.run(["nvidia-smi", "--query-gpu=utilization.gpu,memory.used,memory.total", "--format=csv,noheader,nounits"],
                                 capture_output=True, text=True, timeout=5).stdout
            gpus = [dict(zip(("util", "mem_used", "mem_total"), map(int, l.split(",")))) for l in out.splitlines() if l.strip()]
        except (OSError, ValueError, subprocess.SubprocessError):
            pass
    return dict(now=time.time(), version=VERSION, ncpu=os.cpu_count(), cores=len(cores), free=len(cores) - len(used_cores(db)),
                load=os.getloadavg()[0], cpu_pct=cpu_util(db, cores), mem_total_mb=mem_total, mem_avail_mb=mem_avail,
                disk_free_gb=round(shutil.disk_usage(HOME).free / 2 ** 30, 1), gpus=gpus, agent=agent_alive(),
                counts={r[0]: r[1] for r in db.execute("SELECT state, count(*) FROM jobs GROUP BY state")},
                queued_cpus=db.execute("SELECT coalesce(sum(cpus),0) FROM jobs WHERE state='queued'").fetchone()[0])


def rpc(req):
    """Everything the server can ask of a host, in one idempotent request."""
    db, cfg = hdb(), load_cfg()
    out = dict(ok=True, host=cfg.get("name"), epoch=meta(db, "epoch"))
    for j in req.get("submit", []):
        row = {k: j.get(k, d) for k, d in SPEC.items()}
        env = j.get("env") or {}
        row.update(id=j["id"], state="queued", created=j.get("created") or time.time(),
                   env=env if isinstance(env, str) else json.dumps(env))
        with tx(db):
            n = db.execute("INSERT OR IGNORE INTO jobs(%s) VALUES(%s)" % (",".join(row), ",".join("?" * len(row))), list(row.values())).rowcount
            ev = emit(db, j["id"], "queued") if n else None
        if ev:
            fire(ev)
    for jid in req.get("cancel", []):
        with tx(db):  # queued jobs die here; running ones are killed by their watchdog, which polls the flag
            db.execute("UPDATE jobs SET cancel=1 WHERE id=?", (jid,))
            n = db.execute("UPDATE jobs SET state='cancelled', ended=? WHERE id=? AND state='queued'", (time.time(), jid)).rowcount
            ev = emit(db, jid, "cancelled", dict(final=True)) if n else None
        if ev:
            compress_log(jid)
            fire(ev)
    for jid, prio in (req.get("prio") or {}).items():
        transition(db, jid, "update", prio=int(prio))
    if "after" in req:
        out["events"] = events_after(db, req["after"])
    if req.get("status"):
        out["status"] = status(db, cfg)
    if req.get("logs"):
        out["logs"] = log_tail(req["logs"]["id"], int(req["logs"].get("bytes", 20000)))
    if req.get("config_get"):
        out["config"] = load_cfg()
    if "config_set" in req:
        try:
            out["config"] = save_cfg(req["config_set"])
        except (Fail, OSError) as e:
            out["config_error"] = str(e)
    if not agent_alive():  # self-heal: any contact from the server revives a dead agent
        detach(["agent"])
    return out


def udp_probe():
    """Listen briefly on a port in mosh's range; report whether a datagram from the server gets through."""
    import socket
    for port in range(60990, 60900, -1):
        for family, addr in ((socket.AF_INET6, "::"), (socket.AF_INET, "0.0.0.0")):  # dual-stack where possible
            try:
                sock = socket.socket(family, socket.SOCK_DGRAM)
                sock.bind((addr, port))
            except OSError:
                continue
            print(port, flush=True)
            sock.settimeout(3)
            try:
                sock.recvfrom(64)
                return print("ok")
            except OSError:
                return print("blocked")


def follow(jid):
    """Interactive log view on the host: tail -F while the log is live, a pager over the zstd file once final."""
    path = P("logs", jid + ".log")
    if os.path.exists(path) or not os.path.exists(path + ".zst"):
        os.execvp("tail", ["tail", "-n", "200", "-F", path])
    pager = subprocess.Popen(["less", "-R", "+G"] if shutil.which("less") else ["cat"], stdin=subprocess.PIPE)
    with contextlib.suppress(OSError), zopen(path + ".zst") as f:
        shutil.copyfileobj(f, pager.stdin)
    with contextlib.suppress(OSError):
        pager.stdin.close()
    pager.wait()


def stop_agent():
    if agent_alive():
        with contextlib.suppress(OSError, ValueError), open(P("agent.lock")) as f:
            os.kill(int(f.read()), signal.SIGTERM)
        for _ in range(50):
            if not agent_alive():
                break
            time.sleep(0.1)


def boot_persist(enable):
    """Start the agent at boot, so the host works its queue even if the server never calls: crontab @reboot,
    else a systemd user unit. Returns how (for the install report), or None if neither is available."""
    start = "%s %s agent -d" % (shlex.quote(sys.executable), shlex.quote(SELF))
    try:
        if shutil.which("crontab"):
            mark = "# tinyshed " + HOME
            cur = subprocess.run(["crontab", "-l"], capture_output=True, text=True, timeout=10).stdout.splitlines()
            new = [l for l in cur if not l.endswith(mark)]
            if enable:
                new.append("@reboot TINYSHED_HOME=%s %s %s" % (shlex.quote(HOME), start, mark))
            if new != cur:
                subprocess.run(["crontab", "-"], input="\n".join(new) + "\n", text=True, timeout=10, check=True)
            return "cron" if enable else None
        if shutil.which("systemctl") and HOME == os.path.expanduser("~/.tinyshed"):
            unit = os.path.expanduser("~/.config/systemd/user/tinyshed.service")
            link = os.path.expanduser("~/.config/systemd/user/default.target.wants/tinyshed.service")
            for f in (link, unit):
                with contextlib.suppress(OSError):
                    os.remove(f)
            if not enable:
                return None
            os.makedirs(os.path.dirname(link), exist_ok=True)
            with open(unit, "w") as f:  # oneshot: just launches the detached agent; never kills jobs on stop
                f.write("[Unit]\nDescription=tinyshed agent\n\n[Service]\nType=oneshot\nRemainAfterExit=yes\nKillMode=process\n"
                        "ExecStart=%s\n\n[Install]\nWantedBy=default.target\n" % start)
            os.symlink(unit, link)
            user = os.environ.get("USER") or os.environ.get("LOGNAME") or ""
            linger = "Linger=yes" in subprocess.run(["loginctl", "show-user", user, "-p", "Linger"], capture_output=True, text=True, timeout=10).stdout \
                or subprocess.run(["loginctl", "enable-linger"], capture_output=True, timeout=10).returncode == 0
            return "systemd" if linger else "systemd@login"
    except (OSError, subprocess.SubprocessError):
        pass
    return None


def setup(a):
    """Runs on the host right after the file is copied: write config, (re)start the agent, report."""
    z = zmod()  # refuse to install where zstd is unavailable
    os.makedirs(P("logs"), exist_ok=True)
    cfg = load_cfg()
    cfg["name"] = a.name or cfg.get("name")
    if a.cores:  # unset flags keep what an earlier install configured
        cfg["cores"] = None if a.cores == "all" else a.cores
    if a.home:
        cfg["home"] = None if a.home == "none" else json.loads(a.home)
    host_cores(cfg)  # a malformed --cores fails here, before anything is changed
    with open(P("config.json"), "w") as f:
        json.dump(cfg, f)
    db = hdb()
    stop_agent()  # upgrade: replace the agent; running watchdogs are separate processes and carry on
    detach(["agent"])
    boot = boot_persist(True) if a.boot else None
    for _ in range(30):
        if agent_alive():
            break
        time.sleep(0.1)
    print(json.dumps(dict(status(db, cfg), python=sys.version.split()[0], zstd=("stdlib" if z.__name__.startswith("compression") else z.__name__) if z else "binary",
                          taskset=bool(shutil.which("taskset")), mosh=bool(shutil.which("mosh-server")), boot=boot,
                          phone_home=bool(cfg.get("home")), cores_cfg=cfg.get("cores"))))


def teardown(a):
    """Undo setup. Data is kept unless --purge; never touches a host that still has work unless --force."""
    db = hdb()
    n = db.execute("SELECT count(*) FROM jobs WHERE state IN ('queued','running')").fetchone()[0]
    running = db.execute("SELECT count(*) FROM jobs WHERE state='running'").fetchone()[0]
    if running and a.purge:
        raise Fail("%d jobs are still running here; cancel them before --purge" % running)
    if n and not a.force:
        raise Fail("%d unfinished jobs on this host; cancel them, or use --force (running jobs finish, queued ones stay stored)" % n)
    boot_persist(False)
    stop_agent()
    db.close()
    if a.purge:
        shutil.rmtree(HOME)
    print(json.dumps(dict(unfinished=n, purged=a.purge)))


# ───────────────────────────────────────────────────────────────── server: state + transport

SERVER_SCHEMA = """
CREATE TABLE IF NOT EXISTS hosts(name TEXT PRIMARY KEY, dest TEXT, epoch TEXT, cursor INT DEFAULT 0,
  seen REAL DEFAULT 0, err TEXT, status TEXT);
CREATE TABLE IF NOT EXISTS jobs(id TEXT PRIMARY KEY, host TEXT, prio INT, state TEXT, created REAL,
  sent INT DEFAULT 0, cancel INT DEFAULT 0, eid INT DEFAULT 0, j TEXT);
CREATE TABLE IF NOT EXISTS events(host TEXT, eid INT, ts REAL, job TEXT, kind TEXT, data TEXT, PRIMARY KEY(host, eid));
"""
SSH = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=8", "-o", "ServerAliveInterval=15",
       "-o", "ControlMaster=auto", "-o", "ControlPath=~/.ssh/tinyshed-%C", "-o", "ControlPersist=300"]
REMOTE_PY = "python3 .tinyshed/tinyshed.py "


def sdb():
    return connect("server.db", SERVER_SCHEMA)


def is_local(dest):
    return dest == "local" or dest.startswith("local:")


def local_home(dest):
    return os.path.abspath(os.path.expanduser(dest[6:])) if dest[6:] else HOME


_route = {}  # dest -> the alternative that last answered


def remote(dest, args, input=None, timeout=45, pre=""):
    """Run `tinyshed.py <args>` on a host. dest is an ssh destination, or local[:DIR] for this machine."""
    if is_local(dest):
        home = local_home(dest)
        r = subprocess.run([sys.executable, os.path.join(home, "tinyshed.py")] + args, input=input, capture_output=True,
                           text=True, timeout=timeout, env=dict(os.environ, TINYSHED_HOME=home))
    else:  # dest may list alternatives ("box.local,box"): try in order, the one that last worked first
        for d in sorted(dest.split(","), key=lambda d: d != _route.get(dest)):
            # sh -c: the host's login shell may be fish or anything else
            r = subprocess.run(SSH + [d, "sh -c " + shlex.quote(pre + REMOTE_PY + " ".join(map(shlex.quote, args)))], input=input,
                               capture_output=True, text=True, timeout=timeout)
            if r.returncode != 255:  # 255 = ssh itself failed (unreachable); every remote op is idempotent
                _route[dest] = d
                break
    if r.returncode:
        raise Fail((r.stderr.strip().splitlines() or ["exit %d" % r.returncode])[-1].replace("tinyshed: ", ""))
    return r.stdout


def ingest(db, host, epoch, events):
    """Record host events exactly once (pull and phone-home may overlap), mirror job state, fire server hooks."""
    new = []
    with tx(db):
        h = db.execute("SELECT epoch FROM hosts WHERE name=?", (host,)).fetchone()
        if not h:
            raise Fail("unknown host %r" % host)
        if h["epoch"] != epoch:
            if h["epoch"]:  # the host's state was wiped: forget its old event log and re-send unfinished jobs
                db.execute("DELETE FROM events WHERE host=?", (host,))
                db.execute("UPDATE jobs SET eid=0 WHERE host=?", (host,))
                db.execute("UPDATE jobs SET sent=0, state='pending' WHERE host=? AND state IN ('queued','running')", (host,))
                events = []  # these were read relative to the stale cursor
            db.execute("UPDATE hosts SET epoch=?, cursor=0 WHERE name=?", (epoch, host))
        for ev in sorted(events, key=lambda e: e["eid"]):
            d = ev["data"]
            if db.execute("INSERT OR IGNORE INTO events VALUES(?,?,?,?,?,?)",
                          (host, ev["eid"], ev["ts"], ev["job"], ev["kind"], pack(d))).rowcount:
                db.execute("INSERT INTO jobs(id,host,prio,state,created,sent,eid,j) VALUES(?,?,?,?,?,1,?,?) "
                           "ON CONFLICT(id) DO UPDATE SET host=excluded.host, prio=excluded.prio, state=excluded.state, "
                           "sent=1, eid=excluded.eid, j=excluded.j WHERE excluded.eid > jobs.eid",
                           (d["id"], host, d["prio"], d["state"], d["created"], ev["eid"], pack(d)))
                new.append(dict(ev, host=host))
        if events:
            db.execute("UPDATE hosts SET cursor=max(cursor,?) WHERE name=?", (max(e["eid"] for e in events), host))
    for ev in new:
        fire(ev)
    return db.execute("SELECT cursor FROM hosts WHERE name=?", (host,)).fetchone()[0]


def sync_host(name, extra=None):
    """One round trip: deliver pending jobs + cancels, pull new events, read resources."""
    with contextlib.closing(sdb()) as db:  # close explicitly: a long-lived loop must not wait for the gc to free fds
        return _sync_host(db, name, extra)


def _sync_host(db, name, extra):
    h = db.execute("SELECT * FROM hosts WHERE name=?", (name,)).fetchone()
    submit = [unpack(r["j"]) for r in db.execute("SELECT j FROM jobs WHERE host=? AND sent=0 AND cancel=0 AND state='pending'", (name,))]
    cancel = [r["id"] for r in db.execute("SELECT id FROM jobs WHERE host=? AND sent=1 AND cancel=1 AND state NOT IN (%s)"
                                          % ",".join("?" * len(TERMINAL)), (name,) + TERMINAL)]
    req = dict(status=True, after=h["cursor"], submit=submit, cancel=cancel, **(extra or {}))
    try:
        res = json.loads(remote(h["dest"], ["rpc"], json.dumps(req)))
    except Exception as e:
        err = str(e)[:200] or type(e).__name__
        went_down = db.execute("UPDATE hosts SET err=? WHERE name=? AND err IS NULL", (err, name)).rowcount
        db.execute("UPDATE hosts SET err=? WHERE name=?", (err, name))
        if went_down:
            fire(dict(host=name, job=None, kind="host_down", ts=time.time(), data=dict(err=err)))
        return None
    for j in submit:
        db.execute("UPDATE jobs SET sent=1, state=CASE WHEN state='pending' THEN 'queued' ELSE state END WHERE id=?", (j["id"],))
    ingest(db, name, res["epoch"], res.get("events", []))
    st = dict(res["status"], skew=res["status"]["now"] - time.time())
    db.execute("UPDATE hosts SET seen=?, err=NULL, status=? WHERE name=?", (time.time(), json.dumps(st), name))
    if h["err"]:
        fire(dict(host=name, job=None, kind="host_up", ts=time.time(), data=st))
    return res


def dispatch(db):
    """Late-bind host-less jobs, highest priority first, to the live host with the most idle cores."""
    cap, cores = {}, {}
    for h in db.execute("SELECT name, status FROM hosts WHERE err IS NULL AND status IS NOT NULL AND seen>?", (time.time() - 120,)):
        s = json.loads(h["status"])
        unsent = sum(unpack(r["j"]).get("cpus", 1) for r in db.execute("SELECT j FROM jobs WHERE host=? AND sent=0 AND state='pending'", (h["name"],)))
        cap[h["name"]], cores[h["name"]] = s["free"] - s["queued_cpus"] - unsent, s["cores"]
    touched = set()
    for r in db.execute("SELECT id, j FROM jobs WHERE host IS NULL AND state='pending' AND cancel=0 ORDER BY prio DESC, created").fetchall():
        if not cap:
            break
        best = max(cap, key=cap.get)
        need = min(max(1, unpack(r["j"]).get("cpus", 1)), cores[best])
        if cap[best] < need:
            break
        db.execute("UPDATE jobs SET host=? WHERE id=? AND host IS NULL", (best, r["id"]))
        cap[best] -= need
        touched.add(best)
    return touched


def par(fn, items):
    ts = [threading.Thread(target=fn, args=(i,)) for i in items]
    for t in ts:
        t.start()
    for t in ts:
        t.join()


def sync_all():
    with contextlib.closing(sdb()) as db:
        par(sync_host, [r[0] for r in db.execute("SELECT name FROM hosts").fetchall()])
        par(sync_host, dispatch(db))


# ───────────────────────────────────────────────────────────────── server: operations

def submit_parser(**kw):
    p = argparse.ArgumentParser(**kw)
    p.add_argument("-H", "--host", help="pin to a host (default: first live host with enough idle cores)")
    p.add_argument("-p", "--prio", type=int, default=0, help="higher runs first")
    p.add_argument("-c", "--cpus", type=int, default=1, help="cores to pin with taskset")
    p.add_argument("-m", "--mem", type=size, default=0, help="RSS limit for the whole process tree, e.g. 8G")
    p.add_argument("-t", "--timeout", type=dur, default=0, help="runtime limit per attempt, e.g. 90s 30m 12h 2d")
    p.add_argument("-r", "--retries", type=int, default=0)
    p.add_argument("--retry-on", default=SPEC["retry_on"], help="comma list of error,timeout,oom,lost (default %(default)s)")
    p.add_argument("--retry-delay", type=dur, default=SPEC["retry_delay"], help="first backoff, doubles per attempt")
    p.add_argument("-C", "--cwd", help="working directory on the host (default ~)")
    p.add_argument("-e", "--env", action="append", default=[], metavar="K=V")
    p.add_argument("cmd", nargs=argparse.REMAINDER, help="command (run with bash -c)")
    return p


def add_job(db, spec, host=None):
    if host and not db.execute("SELECT 1 FROM hosts WHERE name=?", (host,)).fetchone():
        raise Fail("unknown host %r (see: tshed hosts)" % host)
    j = {k: spec.get(k, d) for k, d in SPEC.items()}
    j.update(id=uuid.uuid4().hex[:8], created=time.time())
    db.execute("INSERT INTO jobs(id,host,prio,state,created,j) VALUES(?,?,?,'pending',?,?)", (j["id"], host, j["prio"], j["created"], pack(j)))
    return j["id"]


def submit(db, argv):
    a = submit_parser(prog="submit").parse_args(argv) if isinstance(argv, list) else argv
    cmd = a.cmd[1:] if a.cmd[:1] == ["--"] else a.cmd
    if not cmd:
        raise Fail("no command given")
    spec = dict(cmd=cmd[0] if len(cmd) == 1 else shlex.join(cmd), cwd=a.cwd, env=dict(e.split("=", 1) for e in a.env),
                prio=a.prio, cpus=a.cpus, mem_mb=a.mem, timeout=a.timeout, retries=a.retries, retry_on=a.retry_on,
                retry_delay=a.retry_delay)
    return add_job(db, spec, a.host)


def cancel(db, jid):
    with tx(db):
        db.execute("UPDATE jobs SET cancel=1 WHERE id=?", (jid,))
        n = db.execute("UPDATE jobs SET state='cancelled' WHERE id=? AND sent=0 AND state='pending'", (jid,)).rowcount
    if n:  # never left the server
        fire(dict(host=None, job=jid, kind="cancelled", ts=time.time(), data=dict(id=jid, state="cancelled", final=True)))


def set_prio(db, jid, prio):
    r = db.execute("SELECT * FROM jobs WHERE id=?", (jid,)).fetchone()
    if r["sent"]:
        if sync_host(r["host"], dict(prio={jid: prio})) is None:
            raise Fail("host %s is unreachable; priority not changed" % r["host"])
    else:
        db.execute("UPDATE jobs SET prio=?, j=? WHERE id=? AND sent=0", (prio, pack(dict(unpack(r["j"]), prio=prio)), jid))


def resolve(db, prefix):
    rows = db.execute("SELECT id FROM jobs WHERE id LIKE ?", (prefix + "%",)).fetchall()
    if len(rows) != 1:
        raise Fail("%s: %s" % (prefix, "ambiguous job id (%s)" % " ".join(r[0] for r in rows[:6]) if rows else "no such job"))
    return rows[0][0]


def config_cmd(a, known):
    """Show or, with --edit, open config.json in $EDITOR: locally, or a --host's over ssh."""
    def remote_cfg(dest, req):
        r = json.loads(remote(dest, ["rpc"], json.dumps(req)))
        if r.get("config_error"):
            raise Fail(r["config_error"])
        return r["config"]

    targets = []
    for name in a.host or [None]:
        if name is None:
            targets.append(("local %s" % P("config.json"), load_cfg, save_cfg))
        else:
            if name not in known:
                raise Fail("unknown host %r (see: tshed hosts)" % name)
            dest = known[name]
            targets.append(("host %s" % name,
                            lambda d=dest: remote_cfg(d, dict(config_get=True)),
                            lambda c, d=dest: remote_cfg(d, dict(config_set=c))))
    if not a.edit:
        for label, get, _ in targets:
            cfg = get()
            if len(targets) > 1:
                print("# %s" % label)
            print(json.dumps(cfg, indent=2, sort_keys=True))
        return 0
    rc = 0
    for label, get, put in targets:
        ok, msg = edit_cfg(get(), label, put)
        print(msg, file=sys.stdout if ok else sys.stderr)
        rc |= not ok
    return rc


PREFLIGHT = ("command -v python3 >/dev/null || { echo 'tinyshed: python3 not found on PATH' >&2; exit 1; }; "
             "python3 -c 'import sys; sys.exit(sys.version_info < (3, 12))' || "
             "{ echo \"tinyshed: python >= 3.12 required, found $(python3 -V 2>&1)\" >&2; exit 1; }; "
             "mkdir -p .tinyshed && cat > .tinyshed/tinyshed.py.new && mv .tinyshed/tinyshed.py.new .tinyshed/tinyshed.py && ")


def install(name, dest, cores, home, boot):
    """Check the host, copy this file to it, start its agent: one ssh round trip. Returns the host's report."""
    args = ["setup", "--name", name] + (["--cores", cores] if cores else []) + (["--home", home] if home else [])
    with open(SELF) as f:
        src = f.read()
    if is_local(dest):
        target = os.path.join(local_home(dest), "tinyshed.py")
        os.makedirs(os.path.dirname(target), exist_ok=True)
        if target != SELF:
            shutil.copy(SELF, target)
        return json.loads(remote(dest, args))
    return json.loads(remote(dest, args + (["--boot"] if boot else []), input=src, pre=PREFLIGHT, timeout=120))


def install_notes(r):
    notes = []
    if not r["agent"]:
        notes.append("AGENT DID NOT START (see ~/.tinyshed/agent.log on the host)")
    if not r["taskset"]:
        notes.append("no taskset: jobs run unpinned")
    if r["boot"] is None:
        notes.append("no reboot persistence: agent restarts on next server contact")
    elif r["boot"] == "systemd@login":
        notes.append("agent starts at login, not boot (loginctl enable-linger was refused)")
    if not r["mosh"]:
        notes.append("no mosh-server: attach/shell use ssh")
    return "; ".join(notes)


_mosh = {}  # ssh destination -> does mosh actually work there


def mosh_ok(d):
    """mosh needs mosh-server on the host AND inbound UDP 60000-61000 to pass every firewall in between.
    Ask the host to listen on such a port, send it a datagram, and see whether it arrived."""
    if d not in _mosh:
        _mosh[d] = False
        try:
            import socket
            cmd = "command -v mosh-server >/dev/null && " + REMOTE_PY + "udp-probe"
            with subprocess.Popen(SSH + [d, "sh -c " + shlex.quote(cmd)], stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                  stderr=subprocess.DEVNULL, text=True) as p:
                port = int(p.stdout.readline())
                conf = subprocess.run(["ssh", "-G", d], capture_output=True, text=True, timeout=10).stdout
                addr = next(l.split()[1] for l in conf.splitlines() if l.startswith("hostname "))
                for info in socket.getaddrinfo(addr, port, type=socket.SOCK_DGRAM):  # every address family the name has
                    with contextlib.suppress(OSError), socket.socket(info[0], socket.SOCK_DGRAM) as sock:
                        sock.sendto(b"tshed", info[4])
                _mosh[d] = p.stdout.readline().strip() == "ok"
        except (OSError, ValueError, StopIteration, subprocess.SubprocessError):
            pass
    return _mosh[d]


def attach_argv(dest, jid=None):
    """Interactive session on a host: follow a job's log, or a shell. mosh where it works, else ssh -t."""
    if is_local(dest):
        home = local_home(dest)
        return ["env", "TINYSHED_HOME=" + home, sys.executable, os.path.join(home, "tinyshed.py"), "follow", jid] if jid else [os.environ.get("SHELL", "bash")]
    cmd = ["python3", ".tinyshed/tinyshed.py", "follow", jid] if jid else []
    dest = _route.get(dest) or dest.split(",")[0]
    if shutil.which("mosh") and mosh_ok(dest):
        return ["mosh", dest] + (["--"] + cmd if cmd else [])
    return ["ssh", "-t", dest] + cmd


# ───────────────────────────────────────────────────────────────── server: presentation (theme, tables, tui)

RESET, DIM, BOLD = "\x1b[0m", "\x1b[2m", "\x1b[1m"
GREEN, CYAN, RED, MAGENTA, BLUE = "\x1b[32m", "\x1b[36m", "\x1b[31m", "\x1b[35m", "\x1b[34m"
BOLD_YELLOW, BOLD_CYAN, BOLD_RED = "\x1b[1;33m", "\x1b[1;36m", "\x1b[1;31m"
KEY, HILITE = "\x1b[1;33m", "\x1b[48;5;237;1m"
STATE_STYLE = dict(running=(GREEN, "▶"), queued=(BOLD_YELLOW, "◌"), pending=(CYAN, "◌"), done=(GREEN, "✓"),
                   cancelled=(MAGENTA, "■"))  # every failure: (BOLD_RED, "●")
HOST_HEAD = "%-12s  %-7s %5s %5s  %7s  %10s  %-8s %4s %4s  %6s  %s" % ("Host", "State", "Load", "CPU%", "Cores", "Mem free", "GPU", "Run", "Que", "Seen", "Notes")
JOB_HEAD = "%-11s %4s  %-12s  %-8s  %8s  %5s  %s" % ("State", "Prio", "Host", "ID", "Time", "Try", "Command")


def styled(segments, width, base=""):
    """Join (style, text) segments, clipped to width columns. With base (the selection highlight) the
    background is re-applied after every reset and the row is padded to the full width."""
    out, room = [], width
    for style, text in segments:
        if room <= 0:
            break
        text = text[:room]
        room -= len(text)
        out.append("%s%s%s%s" % (base, style, text, RESET) if style or base else text)
    if base and room > 0:
        out.append(base + " " * room + RESET)
    return "".join(out)


def colors():
    return OUT.isatty() and "NO_COLOR" not in os.environ


def paint(text, style):
    return style + text + RESET if style and colors() else text


def out(segments):
    """Print a row: themed on a terminal, plain text in a pipe."""
    print(styled(segments, 10 ** 6) if colors() else "".join(t for _, t in segments).rstrip())


def fmt_dur(s):
    s = int(max(0, s or 0))
    if s < 60:
        return "%ds" % s
    if s < 3600:
        return "%dm%02ds" % (s // 60, s % 60)
    if s < 86400:
        return "%dh%02dm" % (s // 3600, s % 3600 // 60)
    return "%dd%02dh" % (s // 86400, s % 86400 // 3600)


def clean(s):
    s = (s or "").replace("\t", "    ")
    return "".join(c if c.isprintable() else " " for c in s)


def load_hosts(db):
    return [dict(r, s=json.loads(r["status"] or "{}")) for r in db.execute("SELECT * FROM hosts ORDER BY name")]


_jcache = {}  # job id -> (stored blob, decoded payload)


def load_jobs(db, hosts):
    skew = {h["name"]: h["s"].get("skew", 0) for h in hosts}
    out = []
    for r in db.execute("SELECT * FROM jobs"):
        if _jcache.get(r["id"], (None,))[0] != r["j"]:
            _jcache[r["id"]] = (r["j"], unpack(r["j"]))
        j = dict(_jcache[r["id"]][1])
        j.update(id=r["id"], host=r["host"], state=r["state"], prio=r["prio"], sent=r["sent"], cancel=r["cancel"])
        if j["state"] == "running":
            j["t"] = time.time() + skew.get(r["host"], 0) - (j.get("started") or 0)
        elif j["state"] in TERMINAL:
            j["t"] = (j.get("ended") or 0) - (j.get("started") or j.get("ended") or 0)
        else:
            j["t"] = time.time() - j["created"]
        out.append(j)

    def rank(j):  # running, then waiting by priority, then finished most-recent-first
        if j["state"] in TERMINAL:
            return (2, -(j.get("ended") or j["created"]), 0)
        return (0 if j["state"] == "running" else 1, -j["prio"], j["created"])
    return sorted(out, key=rank)


def state_seg(j):
    style, glyph = STATE_STYLE.get(j["state"], (BOLD_RED, "●"))
    return style, ("%s %s%s" % (glyph, j["state"], "…" if j["cancel"] and j["state"] not in TERMINAL else "")).ljust(11)


def host_segs(h):
    s, c = h["s"], h["s"].get("counts", {})
    state = (BOLD_RED, "● down") if h["err"] else (GREEN, "✓ up") if h["seen"] else (DIM, "◌ new")
    mem = "%s/%dG" % ("?" if s.get("mem_avail_mb") is None else "%d" % (s["mem_avail_mb"] >> 10), s["mem_total_mb"] >> 10) if s.get("mem_total_mb") else "-"
    gpus = s.get("gpus") or []
    gpu = "%dx %d%%" % (len(gpus), sum(g["util"] for g in gpus) / len(gpus)) if gpus else "-"
    run, que = c.get("running", 0), c.get("queued", 0)
    notes = h["err"] or "; ".join(n for n in (
        "agent down" if s and not s.get("agent") else "",
        "v%s, server is v%s: run tshed install" % (s.get("version"), VERSION) if s and s.get("version") != VERSION else "") if n)
    return [(BOLD_CYAN, "%-12s" % h["name"][:12]), ("", "  "), (state[0], "%-7s" % state[1]),
            ("", " %5s" % ("%.1f" % s["load"] if s else "-")),
            ("", " %5s  " % ("%d%%" % s["cpu_pct"] if s and s.get("cpu_pct") is not None else "-")),
            (KEY, "%7s" % ("%d/%d" % (s["free"], s["cores"]) if s else "-")),
            ("", "  %10s  %-8s " % (mem, gpu)), (GREEN if run else DIM, "%4d" % run), ("", " "), (BOLD_YELLOW if que else DIM, "%4d" % que),
            (DIM, "  %6s  " % (fmt_dur(time.time() - h["seen"]) if h["seen"] else "never")), (BOLD_RED if h["err"] else BOLD_YELLOW, clean(notes))]


def job_segs(j):
    return [state_seg(j), (KEY, " %4d" % j["prio"]), ("", "  "), (BOLD_CYAN, "%-12s" % (j["host"] or "(any)")[:12]),
            ("", "  %-8s  %8s  " % (j["id"], fmt_dur(j["t"]))), (DIM, "%5s" % ("%d/%d" % (j.get("attempt") or 0, (j.get("retries") or 0) + 1))),
            (DIM if j["state"] in TERMINAL else "", "  " + clean(j["cmd"]))]


def meta_segs(j):
    """The one-line summary of a job used by `show` and the detail view."""
    lim = lambda k, f: f(j[k]) if j.get(k) else "-"
    return [("", "State: "), state_seg(j), ("", "  Prio: "), (KEY, str(j["prio"])),
            ("", "   Try: %d/%d" % (j.get("attempt") or 0, (j.get("retries") or 0) + 1)),
            ("", "   Cores: "), (KEY, "%s" % (j.get("cores") or j.get("cpus") or "-")),
            ("", "   Mem peak: %s%s" % (lim("peak_mb", lambda v: "%dM" % v), " of %dM" % j["mem_mb"] if j.get("mem_mb") else "")),
            ("", "   Timeout: %s" % lim("timeout", fmt_dur)), ("", "   rc: "),
            (GREEN if j.get("rc") == 0 else BOLD_RED if j.get("rc") else DIM, "-" if j.get("rc") is None else str(j["rc"])),
            ("", "   %s: %s" % ("Waiting" if j["state"] in ("pending", "queued") else "Ran", fmt_dur(j["t"])))]


def show(j, log):
    t = lambda k: time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(j[k])) if j.get(k) else "-"
    out([("", "Job: "), (BOLD_CYAN, j["id"]), ("", "   Host: "), (BOLD_CYAN, j["host"] or "(any)")])
    out(meta_segs(j))
    out([("", "Cmd: "), (BOLD, j["cmd"])])
    out([(DIM, "cwd %s   env %s   retry on %s   created %s   started %s   ended %s" % (
        j.get("cwd") or "~", j.get("env") if j.get("env") not in (None, "", "{}", {}) else "-", j.get("retry_on"), t("created"), t("started"), t("ended")))])
    if log:
        out([(DIM, "─" * 60)])
        sys.stdout.write(log if log.endswith("\n") else log + "\n")


ALT_ON = "\x1b[?1049h\x1b[?25l\x1b[?1000h\x1b[?1006h"   # alternate screen, hide cursor, SGR mouse
ALT_OFF = "\x1b[?1000l\x1b[?1006l\x1b[?25h\x1b[?1049l"
KEYS_UP, KEYS_DOWN = ("\x1b[A", "\x1bOA", "\x10"), ("\x1b[B", "\x1bOB", "\x0e")
KEYS_PGUP, KEYS_PGDN = ("\x1b[5~", "\x1b[1;5A", "\x1b[1;2A"), ("\x1b[6~", "\x1b[1;5B", "\x1b[1;2B")
KEYS_LEFT, KEYS_RIGHT = ("\x1b[D", "\x1bOD"), ("\x1b[C", "\x1bOC")
PAGE = 10


def tui(interval):
    """The dashboard, after gitss: alternate screen, synchronized double-buffered frames, type-to-filter,
    capital command keys, SGR mouse. It is also a full server loop (delivery, ingest, callbacks)."""
    import re, select, termios, tty
    fd = sys.stdin.fileno()
    saved = termios.tcgetattr(fd)
    db = sdb()
    st = dict(sel=0, top=0, query="", hide=False, detail=None, scroll=None, msg=("", ""), prompt=None, log=(None, ""),
              fetching=False, fetched=0, synced=0, row0=0, rows=0, syncing=False)
    wake, stop = threading.Event(), threading.Event()

    def say(text, style=""):
        st["msg"] = (style, clean(str(text)))

    def syncer():
        while not stop.is_set():
            st["syncing"] = True
            try:
                sync_all()
                st["synced"] = time.time()
                if st["msg"][1].startswith("sync: "):
                    say("")
            except Exception as e:
                say("sync: %s" % e, BOLD_RED)
            st["syncing"] = False
            wake.wait(interval)
            wake.clear()

    def screen(on):
        OUT.write(ALT_ON if on else ALT_OFF)
        OUT.flush()
        tty.setcbreak(fd) if on else termios.tcsetattr(fd, termios.TCSADRAIN, saved)

    def get_key(timeout, pending=[]):  # one key per call, even when several arrive in one read (paste, key repeat)
        if not pending:
            if not select.select([fd], [], [], timeout)[0]:
                return ""
            s = os.read(fd, 1024).decode("utf-8", "ignore")
            while s == "\x1b" and select.select([fd], [], [], 0.02)[0]:
                s += os.read(fd, 1024).decode("utf-8", "ignore")
            pending.extend(re.findall(r"\x1b(?:\[[0-9;?<]*[A-Za-z~]|O[A-Z])?|.", s, re.S))
        return pending.pop(0) if pending else ""

    def background(fn, *args):
        def go():
            try:
                with contextlib.closing(sdb()) as tdb:
                    fn(tdb, *args)
            except Exception as e:
                say(e, BOLD_RED)
            wake.set()
        threading.Thread(target=go, daemon=True).start()

    def fetch_log(j, dest):
        try:
            text = json.loads(remote(dest, ["rpc"], json.dumps(dict(logs=dict(id=j["id"], bytes=200000))), timeout=20))["logs"]
        except Exception as e:
            text = (j.get("tail") or "") + "\n[live log unavailable: %s]" % e
        st.update(log=(j["id"], text), fetching=False, fetched=time.time())

    def run_fg(argv):  # hand the terminal to mosh/ssh/tail, then take it back
        screen(False)
        try:
            subprocess.call(argv)
        except (KeyboardInterrupt, OSError) as e:
            say(e)
        screen(True)

    def help_line(pairs, width):
        segs, room, sep = [], width, "   " if width >= 105 else "  "
        for k, action in pairs:  # whole pairs only: never show a clipped key
            room -= len(k) + len(action) + 3 + (len(sep) if segs else 0)
            if room < 0:
                break
            segs += [("", sep if segs else ""), (KEY, "[%s]" % k), ("", " " + action)]
        return styled(segs, width)

    def prompt_bar(W):
        label, text, _ = st["prompt"]
        hint = "[Enter: confirm, Esc: cancel, Ctrl-U: clear]"
        if label.startswith("New job"):
            hint = "[-H host  -p prio  -c cpus  -m mem  -t timeout  -r retries, then the command]"
        text = text[-max(10, W - len(label) - 8):]
        return styled([(BOLD_YELLOW, label + " "), (BOLD_CYAN, text + "█"), ("", "   "), (DIM, hint)], W)

    def draw_main(hosts, jobs, total, counts, cur):
        W, H = shutil.get_terminal_size()
        W -= 1
        L = []
        # 1. header bar
        if st["prompt"]:
            L.append(prompt_bar(W))
        elif st["query"]:
            L.append(styled([(BOLD_CYAN, "Search Jobs: %s█" % st["query"]), ("", "   "), (BOLD, "%d/%d" % (len(jobs), total)),
                             ("", " jobs   "), (DIM, "[Esc: clear, Ctrl-U: clear]")], W))
        else:
            up = sum(1 for h in hosts if h["seen"] and not h["err"])
            failed = sum(counts.get(k, 0) for k in FAILURES)
            segs = [(BOLD_CYAN, "Tinyshed:"), ("", " "), (BOLD, str(total)), ("", " jobs   "),
                    (GREEN if counts.get("running") else DIM, "%d running" % counts.get("running", 0)), ("", "   "),
                    (BOLD_YELLOW if counts.get("queued") else DIM, "%d queued" % counts.get("queued", 0))]
            if counts.get("pending"):
                segs += [("", "   "), (CYAN, "%d pending" % counts["pending"])]
            if failed:
                segs += [("", "   "), (BOLD_RED, "%d failed" % failed)]
            segs += [("", "   "), (GREEN if up == len(hosts) and hosts else BOLD_RED, "%d/%d hosts up" % (up, len(hosts))),
                     ("", "   "), (DIM, "[Type to filter, Esc to exit]")]
            L.append(styled(segs, W))
        # 2. hosts
        L.append(BOLD + HOST_HEAD[:W] + RESET)
        L.append("─" * min(W, len(HOST_HEAD) + 12))
        shown = hosts[:max(1, H // 3 - 2)]
        L.extend(styled(host_segs(h), W) for h in shown)
        if not hosts:
            L.append(DIM + "  (no hosts yet: tshed install NAME=DEST)" + RESET)
        elif len(shown) < len(hosts):
            L.append(DIM + "  (+%d more hosts)" % (len(hosts) - len(shown)) + RESET)
        L.append("")
        # 3. jobs
        L.append(BOLD + JOB_HEAD[:W] + RESET)
        L.append("─" * min(W, len(HOST_HEAD) + 12))
        room = max(1, H - len(L) - 3)
        st["top"] = max(0, min(st["top"], st["sel"], len(jobs) - room))
        if st["sel"] >= st["top"] + room:
            st["top"] = st["sel"] - room + 1
        st.update(row0=len(L) + 1, rows=min(room, len(jobs) - st["top"]))
        for i, j in enumerate(jobs[st["top"]:st["top"] + room], st["top"]):
            L.append(styled(job_segs(j), W, HILITE if i == st["sel"] else ""))
        if not jobs:
            L.append(DIM + ("  (no matching jobs)" if st["query"] or st["hide"] else "  (no jobs yet: press N to submit one)") + RESET)
        L.extend([""] * (H - 3 - len(L)))
        # 4. status message, 5. help line, 6. footer
        L.append(styled([st["msg"]], W))
        L.append(help_line([("Enter/→", "detail"), ("N", "new"), ("K", "cancel"), ("E", "rerun"), ("P", "prio"), ("A", "attach"), ("O", "shell"),
                            ("H", "show done" if st["hide"] else "hide done"), ("C", "config"), ("R", "refresh"), ("Esc", "exit")], W))
        age = "syncing…" if st["syncing"] and not st["synced"] else "synced %s ago" % fmt_dur(time.time() - st["synced"]) if st["synced"] else "not synced yet"
        L.append(styled([(DIM, "Server: %s   (%d/%d jobs shown)   %s" % (HOME.replace(os.path.expanduser("~"), "~"), len(jobs), total, age))], W))
        return L, H

    def draw_detail(j):
        W, H = shutil.get_terminal_size()
        W -= 1
        L = [prompt_bar(W) if st["prompt"] else styled([("", "Job: "), (BOLD_CYAN, j["id"]), ("", "   Host: "), (BOLD_CYAN, j["host"] or "(any)"), ("", "   "),
                                                         (DIM, "(%s on the host)" % ("~/.tinyshed/logs/%s.log" % j["id"]))], W),
             styled(meta_segs(j), W), styled([("", "Cmd: "), (BOLD, clean(j["cmd"]))], W), "─" * W]
        text = st["log"][1] if st["log"][0] == j["id"] else (j.get("tail") or "")
        lines = [clean(l.rsplit("\r", 1)[-1]) for l in text.rstrip("\n").split("\n")] if text else []
        room = max(1, H - len(L) - 2)
        last = max(0, len(lines) - room)
        if st["scroll"] is not None and st["scroll"] >= last:
            st["scroll"] = None  # back at the bottom: follow the log again
        top = last if st["scroll"] is None else max(0, st["scroll"])
        st["rows"] = last  # for scrolling keys
        for l in lines[top:top + room]:
            L.append((DIM + l[:W] + RESET) if l.startswith("=== tinyshed") else l[:W])
        L.extend([""] * (H - 2 - len(L)))
        L.append(styled([st["msg"]], W) if st["msg"][1] else styled([(DIM, "Scroll: %d-%d/%d%s" % (
            top + 1, min(len(lines), top + room), len(lines), "   (following)" if st["scroll"] is None and j["state"] == "running" else "")
            if lines else "No output yet" if j["sent"] else "Not on a host yet")], W))
        L.append(help_line([("Esc/←", "back"), ("↑/↓", "scroll"), ("K", "cancel"), ("E", "rerun"), ("P", "prio"), ("A", "attach"), ("O", "shell"), ("C", "config"), ("R", "refresh")], W))
        return L, H

    threading.Thread(target=syncer, daemon=True).start()
    try:
        screen(True)
        while True:
            hosts = load_hosts(db)
            dests = {h["name"]: h["dest"] for h in hosts}
            everything = load_jobs(db, hosts)
            counts = {}
            for j in everything:
                counts[j["state"]] = counts.get(j["state"], 0) + 1
            words = st["query"].lower().split()
            jobs = [j for j in everything if not (st["hide"] and j["state"] in TERMINAL)
                    and all(w in ("%s %s %s %s" % (j["state"], j["host"], j["id"], j["cmd"])).lower() for w in words)]
            if st["detail"]:  # stay on the same job even as the list reorders
                st["sel"] = next((i for i, j in enumerate(jobs) if j["id"] == st["detail"]), st["sel"])
            st["sel"] = max(0, min(st["sel"], len(jobs) - 1))
            cur = jobs[st["sel"]] if jobs else None
            if st["detail"] and not (cur and cur["id"] == st["detail"]):
                st["detail"] = None  # filtered away
            if st["detail"] and cur["sent"] and cur["host"] in dests and not st["fetching"] and time.time() - st["fetched"] > 2 \
                    and (cur["state"] == "running" or st["log"][0] != cur["id"]):
                st["fetching"] = True
                threading.Thread(target=fetch_log, args=(cur, dests[cur["host"]]), daemon=True).start()
            L, H = draw_detail(cur) if st["detail"] else draw_main(hosts, jobs, len(everything), counts, cur)
            OUT.write("\x1b[?2026h\x1b[H" + "\x1b[K\n".join(L[:H]) + "\x1b[K\x1b[J\x1b[?2026l")  # one atomic frame
            OUT.flush()
            key = get_key(0.5)
            if not key:
                continue
            if st["prompt"]:
                label, text, fn = st["prompt"]
                if key in ("\r", "\n"):
                    st["prompt"] = None
                    fn(text)
                elif key in ("\x1b", "\x03"):
                    st["prompt"] = None
                elif key == "\x15":
                    st["prompt"] = (label, "", fn)
                elif key in ("\x7f", "\x08"):
                    st["prompt"] = (label, text[:-1], fn)
                elif key.isprintable():
                    st["prompt"] = (label, text + key, fn)
                continue
            say("")
            step, click = 0, None
            m = re.match(r"\x1b\[<(\d+);(\d+);(\d+)([Mm])", key)
            if m:  # SGR mouse: wheel moves, click selects, click on the selection opens it
                button, row = int(m.group(1)), int(m.group(3))
                if button in (64, 65):
                    step = -1 if button == 64 else 1
                elif button == 0 and m.group(4) == "M" and not st["detail"] and st["row0"] <= row < st["row0"] + st["rows"]:
                    click = st["top"] + row - st["row0"]
            elif key in KEYS_UP + KEYS_DOWN:
                step = -1 if key in KEYS_UP else 1
            elif key in KEYS_PGUP + KEYS_PGDN:
                step = -PAGE if key in KEYS_PGUP else PAGE
            if step and st["detail"]:
                st["scroll"] = max(0, (st["rows"] if st["scroll"] is None else st["scroll"]) + step)
            elif step:
                st["sel"] += step
            elif click is not None:
                if click == st["sel"]:
                    st.update(detail=jobs[click]["id"], fetched=0, scroll=None)
                st["sel"] = click
            elif m:
                pass
            elif key in ("Q", "\x03") or (key == "\x1b" and not st["detail"] and not st["query"]):
                break
            elif key == "\x1b" or (key in KEYS_LEFT and st["detail"]):
                if st["detail"]:
                    st["detail"] = None
                else:
                    st["query"] = ""
            elif key in ("\r", "\n") + KEYS_RIGHT and cur and not st["detail"]:
                st.update(detail=cur["id"], fetched=0, scroll=None)
            elif key == "\x15":
                st["query"] = ""
            elif key == "R":
                st["fetched"] = 0
                say("Refreshing…", DIM)
                wake.set()
            elif key == "H":
                st["hide"] = not st["hide"]
            elif key == "C":
                ok, msg = edit_cfg(load_cfg(), "local %s" % P("config.json"), save_cfg, run_fg)
                say(msg, GREEN if ok else BOLD_RED)
            elif key == "N":
                def new(text):
                    try:
                        say("Submitted " + submit(db, shlex.split(text)), GREEN)
                    except (SystemExit, Exception) as e:
                        say("Submit failed: %s" % (e if str(e) not in ("", "2") else "bad arguments"), BOLD_RED)
                    wake.set()
                st["prompt"] = ("New job:", "", new)
            elif key == "K" and cur and cur["state"] not in TERMINAL:
                def kill(text, jid=cur["id"]):
                    if text.lower() in ("y", "yes"):
                        cancel(db, jid)
                        say("Cancelling %s…" % jid, BOLD_YELLOW)
                        wake.set()
                st["prompt"] = ("Cancel %s (%s)? [y/N]:" % (cur["id"], clean(cur["cmd"])[:40]), "", kill)
            elif key == "E" and cur:
                say("Resubmitted %s as %s" % (cur["id"], add_job(db, cur, cur["host"] if cur["host"] in dests else None)), GREEN)
                wake.set()
            elif key == "P" and cur and cur["state"] not in TERMINAL:
                def prio(text, jid=cur["id"]):
                    try:
                        background(set_prio, jid, int(text))
                    except ValueError:
                        say("Priority must be an integer", BOLD_RED)
                st["prompt"] = ("Priority for %s (now %d):" % (cur["id"], cur["prio"]), "", prio)
            elif key in ("A", "O") and cur and cur["host"] in dests and (key == "O" or cur["sent"]):
                argv = attach_argv(dests[cur["host"]], cur["id"] if key == "A" else None)
                run_fg(argv)
                if argv[0] == "ssh":
                    say("Used ssh: mosh is not usable on %s (no mosh-server, or UDP 60000-61000 is blocked)" % cur["host"], DIM)
            elif key in ("\x7f", "\x08"):
                st["query"] = st["query"][:-1]
            elif len(key) == 1 and key.isprintable() and not key.isupper() and not st["detail"]:
                st["query"] += key
                st["sel"] = 0
    except KeyboardInterrupt:
        pass
    finally:
        stop.set()
        screen(False)


# ───────────────────────────────────────────────────────────────── cli

EXAMPLES = """examples (tshed = this file, symlinked into PATH):
  tshed install gpu1=gpu1.local,gpu1 gpu2=me@gpu2    install or upgrade hosts (no hosts given: upgrade all)
  tshed hosts                                         ping hosts: load, idle cores, memory, GPUs
  tshed submit -H gpu1 -p 5 -c 8 -m 32G -t 12h -r 2 -- python train.py
  tshed ls -a;  tshed show ID;  tshed logs ID;  tshed wait ID;  tshed cancel ID
  tshed overview                                      hosts + active jobs, one screen
  tshed config --edit                                 edit this machine's config in $EDITOR
  tshed config --host gpu1 --edit                     edit gpu1's config over ssh
  tshed                                                  dashboard
  tshed completion fish > ~/.config/fish/completions/tshed.fish

callbacks: ~/.tinyshed/hooks.py     docs: README.md, HUMANS.md"""


def overview(db, n=30, show_all=False):
    """One-shot snapshot: hosts | head -N; ---LS; ls | head -N, with a single sync_all()."""
    n = max(1, n)
    sync_all()
    hosts = load_hosts(db)
    jobs = [j for j in load_jobs(db, hosts) if show_all or j["state"] not in TERMINAL]
    width = shutil.get_terminal_size().columns - 1 if colors() else 10 ** 6
    if hosts:
        print(paint(HOST_HEAD, BOLD))
        for h in hosts[:n - 1]:
            out(host_segs(h))
    else:
        print("no hosts yet: tshed install NAME=DEST ...")
    print(paint("---LS", DIM))
    if jobs:
        print(paint(JOB_HEAD, BOLD))
        for j in jobs[:n - 1]:
            print(styled(job_segs(j), width)) if colors() else out(job_segs(j))
    else:
        print("no jobs" if show_all else "no active jobs (ls -a shows finished ones)")


def build_parser():
    p = argparse.ArgumentParser(prog="tshed", description=__doc__.split("\n\n")[0], epilog=EXAMPLES,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("-V", "--version", action="version", version="tshed " + VERSION)
    sub = p.add_subparsers(dest="op", metavar="COMMAND", title="commands")
    s = sub.add_parser("install", help="install or upgrade hosts over ssh and register them",
                       description="Check each host, copy tinyshed to ~/.tinyshed, (re)start its agent and register it. Safe to "
                                   "re-run at any time: running jobs are untouched and options you leave out keep their installed value.")
    s.add_argument("hosts", nargs="*", metavar="[NAME=]DEST", help="ssh destination, alternatives tried in order "
                   "(box.local,box), local[:DIR], or the NAME of a registered host; none = every registered host")
    s.add_argument("--cores", help="cores jobs may use, e.g. 0-15,32-47; 'all' to reset")
    s.add_argument("--home", metavar="DEST", help="ssh destination of THIS server as seen from the hosts (enables phone-home); 'none' to disable")
    s.add_argument("--no-boot", "--no-cron", action="store_true", help="do not start the agent at boot (crontab @reboot or systemd user unit)")
    s = sub.add_parser("uninstall", help="stop a host's agent, remove its boot entry, forget it")
    s.add_argument("names", nargs="+", metavar="NAME")
    s.add_argument("--purge", action="store_true", help="also delete ~/.tinyshed on the host (queue, history, logs)")
    s.add_argument("--force", action="store_true", help="proceed although the host has unfinished jobs")
    s = sub.add_parser("hosts", help="ping every host and show resources")
    s.add_argument("--json", action="store_true", help="one JSON object per line")
    s.add_argument("--rm", metavar="NAME", help="forget a host without contacting it")
    s = sub.add_parser("config", help="show or edit config.json in $EDITOR",
                       description="Show, or with --edit open in $VISUAL/$EDITOR, this machine's config.json. "
                                   "With --host, read/write that registered host's config over ssh. Edits are "
                                   "validated (JSON and cores) before saving; the agent reloads config every "
                                   "second, so changes apply without restarting it.")
    s.add_argument("--edit", action="store_true", help="open in $VISUAL/$EDITOR instead of printing")
    s.add_argument("--host", action="append", metavar="HOST", help="a registered host (repeatable); default: this machine")
    sub.add_parser("submit", help="queue a job", parents=[submit_parser(add_help=False)])
    s = sub.add_parser("ls", help="list jobs")
    s.add_argument("-a", "--all", action="store_true", help="include finished jobs")
    s.add_argument("--json", action="store_true", help="one JSON object per line")
    s = sub.add_parser("overview", help="one-screen snapshot of hosts and active jobs",
                       description="Non-interactive digest of `tshed hosts | head -N; echo ---LS; tshed ls | head -N`: "
                                   "ping once, then print the hosts table and the active-jobs table, each capped at -n lines.")
    s.add_argument("-n", "--lines", type=int, default=30, help="max lines per table, header included (default 30)")
    s.add_argument("-a", "--all", action="store_true", help="include finished jobs")
    s = sub.add_parser("show", help="one job in detail, with the end of its log")
    s.add_argument("id")
    s.add_argument("-n", "--bytes", type=int, default=2000, help="how much log to show")
    s = sub.add_parser("logs", help="a job's captured stdout/stderr")
    s.add_argument("id")
    s.add_argument("-n", "--bytes", type=int, default=20000, help="tail size in bytes")
    s.add_argument("-f", "--follow", action="store_true", help="follow live on the host (mosh)")
    s = sub.add_parser("wait", help="block until jobs finish (exit 0 all done, 1 any failed, 2 timeout)")
    s.add_argument("ids", nargs="+", metavar="ID")
    s.add_argument("-t", "--timeout", type=dur, default=0, help="give up after e.g. 30m")
    s.add_argument("-i", "--interval", type=float, default=5, help="poll seconds")
    s = sub.add_parser("cancel", help="cancel queued or running jobs")
    s.add_argument("ids", nargs="+", metavar="ID")
    s = sub.add_parser("prio", help="change a waiting job's priority")
    s.add_argument("id")
    s.add_argument("prio", type=int)
    s = sub.add_parser("serve", help="headless server loop: deliver jobs, ingest events, run callbacks")
    s.add_argument("-i", "--interval", type=float, default=10, help="poll seconds")
    s = sub.add_parser("tui", help="interactive dashboard (the default); also the server loop while open")
    s.add_argument("-i", "--interval", type=float, default=5, help="poll seconds")
    s = sub.add_parser("completion", help="print a shell completion script")
    s.add_argument("shell", choices=("fish", "zsh", "bash"))
    public = [c.dest for c in sub._choices_actions]
    # internal: run on hosts (setup/teardown/agent/rpc/follow/_run), by hosts on the server (ingest), or by completions
    s = sub.add_parser("setup")
    s.add_argument("--name")
    s.add_argument("--cores")
    s.add_argument("--home")
    s.add_argument("--boot", action="store_true")
    s = sub.add_parser("teardown")
    s.add_argument("--purge", action="store_true")
    s.add_argument("--force", action="store_true")
    s = sub.add_parser("agent")
    s.add_argument("-d", "--daemon", action="store_true")
    sub.add_parser("rpc")
    sub.add_parser("udp-probe")
    s = sub.add_parser("follow")
    s.add_argument("id")
    sub.add_parser("ingest")
    s = sub.add_parser("_run")
    s.add_argument("id")
    s = sub.add_parser("_complete")
    s.add_argument("kind")
    return p, sub, public


def completion(shell, sub, public):
    """Generate the completion script from the argparse definition, so it cannot drift from the cli."""
    helps = {c.dest: c.help for c in sub._choices_actions}
    q = lambda t: "'%s'" % (t or "").split(";")[0].split(":")[0].split(" (")[0].replace("'", "")[:70]
    spec = []  # (command, [(option strings, takes a value, help)], what its positional completes to)
    for name in public:
        flags, kind = [], ""
        for act in sub.choices[name]._actions:
            if act.option_strings and act.dest != "help":
                flags.append((act.option_strings, act.nargs != 0, act.help))
            elif act.dest in ("id", "ids"):
                kind = "active" if name in ("wait", "cancel", "prio") else "jobs"
            elif act.dest in ("hosts", "names"):
                kind = "hosts"
            elif act.dest == "shell":
                kind = "shells"
        spec.append((name, flags, kind))
    if shell == "fish":
        o = ["# tshed completions: tshed completion fish > ~/.config/fish/completions/tshed.fish", "complete -c tshed -f"]
        for name, flags, kind in spec:
            o.append("complete -c tshed -n __fish_use_subcommand -a %s -d %s" % (name, q(helps[name])))
            seen = "'__fish_seen_subcommand_from %s'" % name
            for opts, arg, h in flags:
                names = " ".join("-s " + x[1:] if len(x) == 2 else "-l " + x[2:] for x in opts)
                value = " -x -a '(tshed _complete hosts)'" if set(opts) & {"--host", "--rm"} else " -x" if arg else ""
                o.append("complete -c tshed -n %s %s%s -d %s" % (seen, names, value, q(h)))
            if kind:
                o.append("complete -c tshed -n %s -a '(tshed _complete %s)'" % (seen, kind))
        return "\n".join(o)
    cases = []
    for name, flags, kind in spec:
        words = " ".join(x for opts, _, _ in flags for x in opts)
        dyn = "$(tshed _complete %s 2>/dev/null | cut -f1)" % kind if kind else ""
        cases.append((name, words, dyn))
    if shell == "zsh":
        body = "\n".join("    %s) if [[ $words[CURRENT] == -* ]]; then compadd -- %s; else compadd -- ${(f)\"%s\"}; fi ;;" % c for c in cases)
        return ("#compdef tshed\n# tshed completions: eval \"$(tshed completion zsh)\"  (after compinit)\n_tshed() {\n"
                "  local -a cmds; cmds=(%s)\n  if (( CURRENT == 2 )); then _describe command cmds; return; fi\n"
                "  if [[ $words[CURRENT-1] == (-H|--host|--rm) ]]; then compadd -- ${(f)\"$(tshed _complete hosts 2>/dev/null | cut -f1)\"}; return; fi\n"
                "  case $words[2] in\n%s\n  esac\n}\ncompdef _tshed tshed"
                % (" ".join("'%s:%s'" % (n, q(helps[n])[1:-1]) for n in public), body))
    body = "\n".join("      %s) if [[ $cur == -* ]]; then words=\"%s\"; else words=\"%s\"; fi ;;" % c for c in cases)
    return ("# tshed completions: eval \"$(tshed completion bash)\"\n_tshed() {\n"
            "  local cur=${COMP_WORDS[COMP_CWORD]} prev=${COMP_WORDS[COMP_CWORD-1]} words=\n"
            "  if (( COMP_CWORD == 1 )); then words=\"%s\"\n"
            "  elif [[ $prev == -H || $prev == --host || $prev == --rm ]]; then words=$(tshed _complete hosts 2>/dev/null | cut -f1)\n"
            "  else\n    case ${COMP_WORDS[1]} in\n%s\n    esac\n  fi\n"
            "  COMPREPLY=($(compgen -W \"$words\" -- \"$cur\"))\n}\ncomplete -F _tshed tshed" % (" ".join(public), body))


def complete(db, kind):
    """Candidates for the completion scripts, as `value<TAB>description`. Reads the mirror only: instant."""
    if kind == "hosts":
        for r in db.execute("SELECT name, dest FROM hosts ORDER BY name"):
            print("%s\t%s" % (r["name"], r["dest"]))
    elif kind == "shells":
        print("fish\nzsh\nbash")
    else:
        where = "WHERE state NOT IN (%s)" % ",".join("'%s'" % t for t in TERMINAL) if kind == "active" else ""
        for r in db.execute("SELECT id, host, state, j FROM jobs %s ORDER BY created DESC LIMIT 40" % where):
            print("%s\t%s %s: %s" % (r["id"], r["state"], r["host"] or "(any)", clean(unpack(r["j"]).get("cmd"))[:50]))


def main():
    global ECHO
    p, sub, public = build_parser()
    a = p.parse_args()

    if a.op == "setup":
        return setup(a)
    if a.op == "teardown":
        return teardown(a)
    if a.op == "agent":
        return detach(["agent"]) and None if a.daemon else agent()
    if a.op == "follow":
        return follow(a.id)
    if a.op == "udp-probe":
        return udp_probe()
    if a.op == "rpc":
        return print(json.dumps(rpc(json.load(sys.stdin))))
    if a.op == "_run":
        return run_job(a.id)
    if a.op == "completion":
        return print(completion(a.shell, sub, public))

    zmod()  # fail early, with a clear message, if this machine has no zstd
    db = sdb()
    known = {r["name"]: r["dest"] for r in db.execute("SELECT name, dest FROM hosts")}
    if a.op == "config":
        return config_cmd(a, known)
    if a.op == "_complete":
        return complete(db, a.kind)
    if a.op == "ingest":
        req = json.load(sys.stdin)
        return print(json.dumps(dict(ack=ingest(db, req["host"], req["epoch"], req["events"]))))
    if a.op in (None, "tui"):
        if not sys.stdin.isatty() or not OUT.isatty():
            raise Fail("the dashboard needs a terminal; see tshed --help for commands")
        return tui(getattr(a, "interval", 5))
    if a.op == "serve":
        ECHO = True
        print("tshed %s serving %d hosts every %gs" % (VERSION, len(known), a.interval), flush=True)
        while True:
            sync_all()
            time.sleep(a.interval)
    if a.op == "install":
        pairs = []
        for h in a.hosts or sorted(known):
            pairs.append(tuple(h.split("=", 1)) if "=" in h else (h, known[h]) if h in known
                         else (h.split(",")[0].split("@")[-1].split(":")[0].split(".")[0], h))
        if not pairs:
            raise Fail("no hosts registered yet: tshed install NAME=DEST ...")
        home = a.home
        if home and home != "none":  # hosts will run this exact command on the server to push events
            if P("tinyshed.py") != SELF:
                shutil.copy(SELF, P("tinyshed.py"))
            home = json.dumps(dict(dest=a.home, cmd="env TINYSHED_HOME=%s %s %s ingest" % (shlex.quote(HOME), shlex.quote(sys.executable), shlex.quote(P("tinyshed.py")))))
        results = {}

        def one(pair):
            try:
                results[pair[0]] = install(pair[0], pair[1], a.cores, home, not a.no_boot)
            except Exception as e:
                results[pair[0]] = e
        par(one, pairs)
        print(paint("%-12s  %-7s %-5s %-8s %-10s %5s  %-13s %s" % ("Host", "Result", "Ver", "Python", "Zstd", "Cores", "Boot", "Notes"), BOLD))
        for name, dest in pairs:
            r = results[name]
            if isinstance(r, Exception):
                out([(BOLD_CYAN, "%-12s" % name), ("", "  "), (BOLD_RED, "● FAILED "), ("", str(r) or type(r).__name__)])
                continue
            db.execute("INSERT INTO hosts(name,dest) VALUES(?,?) ON CONFLICT(name) DO UPDATE SET dest=excluded.dest", (name, dest))
            out([(BOLD_CYAN, "%-12s" % name), ("", "  "), (GREEN, "✓ ok    ") if r["agent"] else (BOLD_RED, "● BROKEN"),
                 ("", "%-5s %-8s %-10s " % (r["version"], r["python"], r["zstd"])), (KEY, "%5s" % r["cores"]),
                 ("", "  %-13s " % ((r["boot"] or "-") + ("+home" if r["phone_home"] else ""))), (BOLD_YELLOW, install_notes(r))])
        par(sync_host, [n for n, _ in pairs if not isinstance(results[n], Exception)])
        return 1 if any(isinstance(r, Exception) or not r["agent"] for r in results.values()) else 0
    if a.op == "uninstall":
        rc = 0
        for name in a.names:
            try:
                if name not in known:
                    raise Fail("unknown host")
                r = json.loads(remote(known[name], ["teardown"] + ["--purge"] * a.purge + ["--force"] * a.force))
            except Exception as e:
                out([(BOLD_CYAN, "%-12s" % name), ("", "  "), (BOLD_RED, "● FAILED "), ("", "%s" % e)])
                rc = 1
                continue
            db.execute("DELETE FROM hosts WHERE name=?", (name,))
            out([(BOLD_CYAN, "%-12s" % name), ("", "  "), (GREEN, "✓ removed"), ("", "  %s%s" % (
                "data purged" if r["purged"] else "data kept in ~/.tinyshed on the host",
                "; %d unfinished jobs left behind" % r["unfinished"] if r["unfinished"] else ""))])
        return rc
    if a.op == "hosts":
        if a.rm and not db.execute("DELETE FROM hosts WHERE name=?", (a.rm,)).rowcount:
            raise Fail("unknown host %r" % a.rm)
        sync_all()
        hosts = load_hosts(db)
        if a.json:
            for h in hosts:
                print(json.dumps(dict(h["s"], name=h["name"], dest=h["dest"], up=bool(h["seen"] and not h["err"]), err=h["err"])))
        elif not hosts:
            print("no hosts yet: tshed install NAME=DEST ...", file=sys.stderr)
        else:
            print(paint(HOST_HEAD, BOLD))
            for h in hosts:
                out(host_segs(h))
    elif a.op == "overview":
        overview(db, a.lines, a.all)
    elif a.op == "submit":
        if not known:
            raise Fail("no hosts yet: tshed install NAME=DEST ...")
        jid = submit(db, a)
        sync_all()
        r = db.execute("SELECT host, state FROM jobs WHERE id=?", (jid,)).fetchone()
        print(jid, r["state"], r["host"] or "(any)")
        if r["state"] == "pending":
            print("note: " + ("host %s is unreachable; the job is delivered on reconnect" % r["host"] if r["host"] else
                              "waiting for a host with enough idle cores; `tshed serve` or the dashboard must be running to place it"), file=sys.stderr)
    elif a.op == "ls":
        sync_all()
        jobs = [j for j in load_jobs(db, load_hosts(db)) if a.all or j["state"] not in TERMINAL]
        if a.json:
            keys = ("id", "host", "state", "prio", "attempt", "rc", "cmd", "created", "started", "ended", "peak_mb")
            for j in jobs:
                print(json.dumps({k: j.get(k) for k in keys}))
        elif not jobs:
            print("no jobs" if a.all else "no active jobs (ls -a shows finished ones)", file=sys.stderr)
        else:
            width = shutil.get_terminal_size().columns - 1 if colors() else 10 ** 6
            print(paint(JOB_HEAD, BOLD))
            for j in jobs:
                print(styled(job_segs(j), width)) if colors() else out(job_segs(j))
    elif a.op == "show":
        jid = resolve(db, a.id)
        sync_all()
        j = next(j for j in load_jobs(db, load_hosts(db)) if j["id"] == jid)
        log = j.get("tail") or ""
        if j["sent"] and a.bytes and j["host"] in known:
            with contextlib.suppress(Exception):
                log = json.loads(remote(known[j["host"]], ["rpc"], json.dumps(dict(logs=dict(id=jid, bytes=a.bytes)))))["logs"]
        log = log[-a.bytes:] if a.bytes else ""
        show(j, log.split("\n", 1)[-1] if len(log) >= a.bytes else log)  # drop the partial first line
    elif a.op == "wait":
        ids = [resolve(db, i) for i in a.ids]
        deadline = time.time() + a.timeout if a.timeout else None
        while True:
            sync_all()
            states = {i: db.execute("SELECT state FROM jobs WHERE id=?", (i,)).fetchone()[0] for i in ids}
            if all(s in TERMINAL for s in states.values()) or (deadline and time.time() > deadline):
                break
            time.sleep(a.interval)
        for i, s in states.items():
            print(i, s)
        return 2 if any(s not in TERMINAL for s in states.values()) else 0 if all(s == "done" for s in states.values()) else 1
    elif a.op == "cancel":
        ids = [resolve(db, i) for i in a.ids]
        for jid in ids:
            cancel(db, jid)
        sync_all()
        for jid in ids:
            s = db.execute("SELECT state FROM jobs WHERE id=?", (jid,)).fetchone()[0]
            print(jid, s if s in TERMINAL else "cancelling")
    elif a.op == "prio":
        jid = resolve(db, a.id)
        set_prio(db, jid, a.prio)
        print(jid, "prio", a.prio)
    elif a.op == "logs":
        r = db.execute("SELECT * FROM jobs WHERE id=?", (resolve(db, a.id),)).fetchone()
        if not r["sent"]:
            raise Fail("job %s has not reached a host yet, so it has no log" % r["id"])
        if r["host"] not in known:
            raise Fail("host %s is no longer registered" % r["host"])
        if a.follow:
            argv = attach_argv(known[r["host"]], r["id"])
            os.execvp(argv[0], argv)
        try:
            sys.stdout.write(json.loads(remote(known[r["host"]], ["rpc"], json.dumps(dict(logs=dict(id=r["id"], bytes=a.bytes)))))["logs"])
        except Exception as e:  # host unreachable: fall back to the tail that came with the last event
            sys.stdout.write(unpack(r["j"]).get("tail") or "")
            raise Fail("host unreachable (%s); that was the last tail the server has" % e)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Fail as e:
        sys.exit("tinyshed: %s" % e)
    except KeyboardInterrupt:
        sys.exit(130)
    except BrokenPipeError:  # e.g. `tshed ls -a | head`
        os.dup2(os.open(os.devnull, os.O_WRONLY), sys.stdout.fileno())
        sys.exit(141)

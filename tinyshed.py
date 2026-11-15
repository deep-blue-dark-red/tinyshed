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

Progress checkpoints: a job writes `DONE/TOTAL message` to $TINYSHED_PROGRESS (or runs `tshed progress`);
--stall kills a job that stops checkpointing, --expect sets the timeout to twice the expected runtime.

Callbacks: put functions in ~/.tinyshed/hooks.py (server and/or host), each taking one event dict:
  OnJobQueued OnJobStart OnJobCompletion OnJobError OnJobTimeout OnJobOOM OnJobStalled OnJobLost OnJobCancel
  OnJobProgress OnJobPaused OnJobResumed
  OnHostDown OnHostUp (server only)   OnEvent (everything)
"""
import argparse, contextlib, fcntl, importlib, json, os, re, shlex, shutil, signal, sqlite3, subprocess, sys, tempfile, threading, time, traceback, uuid

VERSION = "0.2"
HOME = os.path.abspath(os.path.expanduser(os.environ.get("TINYSHED_HOME") or "~/.tinyshed"))
SELF = os.path.abspath(__file__)
OUT = sys.stdout
FAILURES = ("error", "timeout", "oom", "stalled", "lost")
TERMINAL = ("done", "cancelled") + FAILURES
HOOKS = {"queued": "OnJobQueued", "start": "OnJobStart", "done": "OnJobCompletion", "error": "OnJobError",
         "timeout": "OnJobTimeout", "oom": "OnJobOOM", "stalled": "OnJobStalled", "lost": "OnJobLost", "cancelled": "OnJobCancel",
         "progress": "OnJobProgress", "paused": "OnJobPaused", "resumed": "OnJobResumed", "host_down": "OnHostDown", "host_up": "OnHostUp"}
# job fields that travel server -> host, with defaults
# stall: None = derived from expect once the job reports a total, 0 = off
SPEC = dict(name=None, cmd=None, cwd=None, env=None, prio=0, cpus=1, mem_mb=0, timeout=0, retries=0,
            retry_on="error,lost", retry_delay=30, created=None, expect=0, stall=None, stall_on_log=0, trace_lines=0, work=0)
PROGRESS_EVERY = 30  # seconds: a changed checkpoint becomes a `progress` event at most this often


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


_STEP = re.compile(r"(\d+(?:\.\d+)?)(?:/(\d+(?:\.\d+)?)|(%))$")


def parse_progress(s):
    """A checkpoint line: 'DONE/TOTAL message', 'PCT% message' or just a message -> (done, total, message).
    done/total are None when the first word is not a step."""
    head, _, rest = (s or "").strip().partition(" ")
    m = _STEP.match(head)
    if not m or (m[2] and float(m[2]) <= 0):
        return None, None, (s or "").strip()
    return float(m[1]), float(m[2]) if m[2] else 100.0, rest.strip()


# ───────────────────────────────────────────────────────────────── host: state

HOST_SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs(
  id TEXT PRIMARY KEY, name TEXT, cmd TEXT, cwd TEXT, env TEXT, prio INT, cpus INT, mem_mb INT, timeout REAL,
  retries INT, retry_on TEXT, retry_delay REAL, created REAL,
  state TEXT, attempt INT DEFAULT 0, not_before REAL DEFAULT 0, cores TEXT, pid INT, rc INT,
  started REAL, ended REAL, peak_mb INT, cancel INT DEFAULT 0, force INT DEFAULT 0,
  expect REAL DEFAULT 0, stall REAL, stall_on_log INT DEFAULT 0, progress TEXT, beat REAL, trace_lines INT DEFAULT 0,
  pause INT DEFAULT 0, paused_at REAL, paused_s REAL DEFAULT 0, work INT DEFAULT 0);
CREATE INDEX IF NOT EXISTS jobs_queue ON jobs(state, prio DESC, created);
CREATE TABLE IF NOT EXISTS events(id INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL, job TEXT, kind TEXT, data TEXT);
CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v TEXT);
"""


def hdb():
    db = connect("host.db", HOST_SCHEMA)
    have = {r[1] for r in db.execute("PRAGMA table_info(jobs)")}
    for col, decl in (("force", "INT DEFAULT 0"), ("name", "TEXT"),  # v0.2 queues predate these
                      ("expect", "REAL DEFAULT 0"), ("stall", "REAL"), ("stall_on_log", "INT DEFAULT 0"),  # and these
                      ("progress", "TEXT"), ("beat", "REAL"), ("trace_lines", "INT DEFAULT 0"),
                      ("pause", "INT DEFAULT 0"), ("paused_at", "REAL"), ("paused_s", "REAL DEFAULT 0"), ("work", "INT DEFAULT 0")):
        if col not in have:
            try:
                db.execute("ALTER TABLE jobs ADD COLUMN %s %s" % (col, decl))
            except sqlite3.OperationalError as e:  # another process migrated first
                if "duplicate" not in str(e):
                    raise
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
    aff = getattr(os, "sched_getaffinity", None)  # linux only
    return sorted(aff(0)) if aff else list(range(os.cpu_count() or 1))


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

def session_usage(sids):
    """{sid: [RSS MB, CPU seconds]} summed over every process whose session id is in sids (shared
    pages counted per process). Linux CPU includes reaped children (cutime/cstime); macOS sees only
    live processes and groups by process group (== session leader pid for our jobs)."""
    sids = {str(s) for s in sids}
    out = {s: [0, 0.0] for s in sids}
    if not sids:
        return out
    if os.path.isdir("/proc/self"):
        page, hz = os.sysconf("SC_PAGE_SIZE"), os.sysconf("SC_CLK_TCK")
        for pid in os.listdir("/proc"):
            if pid.isdigit():
                try:
                    with open("/proc/%s/stat" % pid) as f:
                        # after comm: [0]=state [3]=session [11..14]=utime stime cutime cstime [21]=rss pages
                        fields = f.read().rsplit(")", 1)[1].split()
                    if fields[3] in sids:
                        u = out[fields[3]]
                        u[0] += int(fields[21]) * page
                        u[1] += sum(int(x) for x in fields[11:15]) / hz
                except (OSError, IndexError, ValueError):
                    pass
        return {k: [int(v[0]) >> 20, v[1]] for k, v in out.items()}
    try:  # macOS / BSD: fall back to the process group
        ps = subprocess.run(["ps", "-axo", "pgid=,rss=,time="], capture_output=True, text=True, timeout=10).stdout
        for line in ps.splitlines():
            p = line.split()
            if len(p) >= 3 and p[0] in sids:
                u = out[p[0]]
                u[0] += int(p[1])
                d, _, hms = p[2].rpartition("-")  # [[dd-]hh:]mm:ss.ss
                secs = 0.0
                for part in hms.split(":"):
                    secs = secs * 60 + float(part)
                u[1] += secs + int(d or 0) * 86400
    except (OSError, ValueError, IndexError, subprocess.SubprocessError):
        pass
    return {k: [int(v[0]) >> 10, v[1]] for k, v in out.items()}


def session_rss_mb(sids):
    return sum(v[0] for v in session_usage(sids).values())


def tree_rss_mb(sid):
    """Summed RSS of every process in the job's session (shared pages are counted per process)."""
    return session_rss_mb({sid})


def job_usage(db):
    """Live usage of running jobs: ({id: [CPU% of its assigned cores or None, RSS MB]}, summed RSS MB,
    sum of their -m limits MB or None if any running job set no -m). CPU% = CPU-seconds delta since
    the previous status() call / (elapsed * cores assigned); the baseline lives in host.db meta."""
    rows = db.execute("SELECT id, pid, cores, cpus, mem_mb FROM jobs WHERE state IN ('running','paused')").fetchall()  # paused: RAM still held
    reserved = None if any(not r["mem_mb"] for r in rows) else sum(r["mem_mb"] for r in rows)
    use = session_usage({r["pid"] for r in rows if r["pid"]})
    now = time.time()
    try:
        prev = json.loads(meta(db, "jobcpu") or "{}")
    except ValueError:
        prev = {}
    jobs, base = {}, {}
    for r in rows:
        rss, cpu = use.get(str(r["pid"]), [0, 0.0]) if r["pid"] else [0, 0.0]
        ncores = len(parse_cores(r["cores"])) if r["cores"] else max(1, r["cpus"] or 1)
        pct, b = None, prev.get(r["id"])
        if b and b[2] == r["pid"]:
            span, dcpu = now - b[0], cpu - b[1]
            if span < 0.5:  # too soon for a reading: keep the old baseline and value
                pct, b2 = b[3], b
            elif span <= 300:
                pct, b2 = round(max(0.0, 100 * dcpu / (span * ncores)), 1), None
            else:
                b2 = None
        else:
            b2 = None
        base[r["id"]] = b2 or [now, cpu, r["pid"], pct]
        jobs[r["id"]] = [pct, rss]
    meta(db, "jobcpu", json.dumps(base))
    return jobs, sum(v[1] for v in jobs.values()), reserved


def kill_group(p, grace=10):
    try:
        os.killpg(p.pid, signal.SIGTERM)
        os.killpg(p.pid, signal.SIGCONT)  # a paused job handles TERM only once it runs again
        p.wait(timeout=grace)
    except (OSError, subprocess.TimeoutExpired):
        pass
    with contextlib.suppress(OSError):
        os.killpg(p.pid, signal.SIGKILL)
    p.wait()


# --trace-lines: every bash process of the job (BASH_ENV is read by each non-interactive bash) records the
# command it is about to run in run/ID.where/PID. Subshells skip it (BASHPID != $$); $? survives a DEBUG trap
# by itself, $_ is restored by hand; a failed write (or set -e) never affects the script.
TRACE_BASH = r"""# tinyshed --trace-lines (see tshed where); sources the job's own BASH_ENV first
[ -n "$TINYSHED_BASH_ENV" ] && . "$TINYSHED_BASH_ENV"
set -T
trap '_tshed_u=$_; [ "$BASHPID" = "$$" ] && { printf "%s:%s\t%s\t%.300s\n" "${BASH_SOURCE[0]:-bash -c}" "$LINENO" "${FUNCNAME[*]}" "$BASH_COMMAND" > "$TINYSHED_WHERE/$$"; } 2>/dev/null || :; : "$_tshed_u"' DEBUG
"""


def host_bin():
    """~/.tinyshed/bin on the host: a `tshed` shim (so jobs can call `tshed progress` whatever their PATH holds)
    and the --trace-lines BASH_ENV file. Rewritten only when they differ, atomically."""
    files = {"tshed": "#!/bin/sh\nexec %s %s \"$@\"\n" % (shlex.quote(sys.executable), shlex.quote(P("tinyshed.py"))),
             "trace.bash": TRACE_BASH}
    os.makedirs(P("bin"), exist_ok=True)
    for name, body in files.items():
        path = P("bin", name)
        with contextlib.suppress(OSError):
            with open(path) as f:
                if f.read() == body:
                    continue
        with open(path + ".%d" % os.getpid(), "w") as f:
            f.write(body)
        os.chmod(f.name, 0o755)
        os.replace(f.name, path)
    return P("bin")


def job_cwd(j):
    """A job's working directory on this host: -C (default ~), or inside its staged work dir (--git/--push)."""
    if j["work"]:
        return os.path.normpath(os.path.join(P("work", j["id"]), os.path.expanduser(j["cwd"] or ".")))
    return os.path.expanduser(j["cwd"] or "~")


def repin(sid, cores):
    """Move every thread of a job's session to new cores (a resumed job gets whatever cores are free). Linux only."""
    setaff = getattr(os, "sched_setaffinity", None)
    if not setaff or not cores:
        return
    want = set(parse_cores(cores))
    for pid in os.listdir("/proc"):
        if pid.isdigit():
            with contextlib.suppress(OSError, IndexError, ValueError):
                with open("/proc/%s/stat" % pid) as f:
                    if f.read().rsplit(")", 1)[1].split()[3] != str(sid):
                        continue
                for tid in os.listdir("/proc/%s/task" % pid):
                    with contextlib.suppress(OSError):
                        setaff(int(tid), want)


STALL_FLOOR = 10  # seconds: the shortest derived stall limit (the watchdog looks once a second)


def stall_limit(j, step):
    """Seconds without a checkpoint before the job counts as stalled; 0 = no limit. Unset --stall with --expect:
    twice the time the expected rate allows for the progress the last checkpoint made (step = its share of TOTAL),
    so a job reporting every item, every 1000th item or every 10% is judged by its own stride."""
    if j["stall"] is not None:
        return j["stall"]
    return max(STALL_FLOOR, 2 * j["expect"] * step) if j["expect"] and step else 0


def run_job(jid):
    """The watchdog. Inherits a held flock on run/<jid>.lock from the agent: lock released == watchdog dead."""
    db = hdb()
    j = db.execute("SELECT * FROM jobs WHERE id=?", (jid,)).fetchone()
    stop = []
    signal.signal(signal.SIGTERM, lambda *a: stop.append(1))
    logp, prog, trace = P("logs", jid + ".log"), P("run", jid + ".progress"), P("run", jid + ".where")
    log = open(logp, "ab")
    log.write(("=== tinyshed %s attempt %d cores=%s %s ===\n" % (jid, j["attempt"], j["cores"], time.strftime("%F %T"))).encode())
    log.flush()
    with contextlib.suppress(OSError):  # a new attempt starts without checkpoints or traces
        os.remove(prog)
    shutil.rmtree(trace, ignore_errors=True)
    cmd = ["bash", "-c", j["cmd"]]
    if j["cores"] and shutil.which("taskset"):
        cmd = ["taskset", "-c", j["cores"]] + cmd
    env = dict(os.environ, **json.loads(j["env"] or "{}"))
    env.update(TINYSHED_JOB=jid, TINYSHED_ATTEMPT=str(j["attempt"]), TINYSHED_CORES=j["cores"] or "", TINYSHED_PROGRESS=prog)
    with contextlib.suppress(OSError):
        env["PATH"] = host_bin() + os.pathsep + env.get("PATH", os.defpath)
        if j["trace_lines"]:
            os.makedirs(trace, exist_ok=True)
            env.update(TINYSHED_WHERE=trace, TINYSHED_BASH_ENV=env.get("BASH_ENV", ""), BASH_ENV=P("bin", "trace.bash"))
    try:
        p = subprocess.Popen(cmd, stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT, env=env,
                             cwd=job_cwd(j), start_new_session=True)
    except OSError as e:
        log.write(("tinyshed: cannot start: %s\n" % e).encode())
        log.flush()
        return finish(db, jid, "error", rc=127, tail=str(e))
    transition(db, jid, "start", pid=p.pid, progress=None, beat=None)
    deadline = time.monotonic() + j["timeout"] if j["timeout"] else None
    kind, why, peak, tick, paused_at = None, "", 0, 0, None
    beat, seen, logsize = time.monotonic(), None, os.fstat(log.fileno()).st_size  # last checkpoint, progress file identity
    line, saved, saved_at = None, None, 0.0  # checkpoint text, last recorded one
    last_done, step = 0.0, None  # DONE of the last checkpoint, and the share of TOTAL its step advanced

    def record():
        nonlocal saved, saved_at
        saved, saved_at = line, time.monotonic()
        transition(db, jid, "progress", progress=line, beat=time.time())

    def poll():  # a checkpoint = any write to the progress file
        nonlocal seen, beat, line, last_done, step
        with contextlib.suppress(OSError):
            st = os.stat(prog)
            if (st.st_mtime_ns, st.st_ino, st.st_size) != seen:
                seen, beat = (st.st_mtime_ns, st.st_ino, st.st_size), time.monotonic()
                with open(prog, errors="replace") as f:
                    text = " ".join(f.read(4096).split())[:200]
                if text:  # empty: a torn write or a bare `touch`, still a heartbeat
                    line = text
                    done, total, _ = parse_progress(text)
                    if done is not None and total and done > last_done:  # same DONE again: keep the stride
                        step, last_done = (done - last_done) / total, done

    while kind is None:
        try:
            p.wait(timeout=1)
            break
        except subprocess.TimeoutExpired:
            tick += 1
        poll()
        if j["stall_on_log"]:
            n = os.fstat(log.fileno()).st_size
            if n != logsize:
                logsize, beat = n, time.monotonic()
        if line != saved and time.monotonic() - saved_at >= PROGRESS_EVERY:
            record()
        limit = stall_limit(j, step)
        if stop:
            kind = "cancelled"
        elif paused_at is not None:  # stopped: no clocks run; wait for cancel or for the agent to hand back cores
            if tick % 2 == 0:
                r = db.execute("SELECT cancel, state, cores FROM jobs WHERE id=?", (jid,)).fetchone()
                if r["cancel"]:
                    kind = "cancelled"
                elif r["state"] == "running":  # resumed: the agent assigned cores
                    repin(p.pid, r["cores"])
                    with contextlib.suppress(OSError):
                        os.killpg(p.pid, signal.SIGCONT)
                    d = time.monotonic() - paused_at
                    deadline, beat, paused_at = deadline and deadline + d, beat + d, None
                    log.write(("=== tinyshed: resumed after %.0fs, cores=%s %s ===\n" % (d, r["cores"], time.strftime("%F %T"))).encode())
                    log.flush()
        elif deadline and time.monotonic() > deadline:
            kind, why = "timeout", ": ran %.0fs" % j["timeout"]
        elif limit and time.monotonic() - beat > limit:
            kind, why = "stalled", ": no checkpoint%s for %s" % (" or output" if j["stall_on_log"] else "", "%.0fs" % (time.monotonic() - beat))
        elif tick % 2 == 0:
            if j["mem_mb"] or tick % 10 == 0:
                peak = max(peak, tree_rss_mb(p.pid))
            r = db.execute("SELECT cancel, pause FROM jobs WHERE id=?", (jid,)).fetchone()
            if j["mem_mb"] and peak > j["mem_mb"]:
                kind = "oom"
            elif r["cancel"]:
                kind = "cancelled"
            elif r["pause"]:  # stop the whole group and give the cores back to the scheduler
                with contextlib.suppress(OSError):
                    os.killpg(p.pid, signal.SIGSTOP)
                paused_at = time.monotonic()
                transition(db, jid, "paused", state="paused", cores=None, paused_at=time.time())
                log.write(("=== tinyshed: paused %s ===\n" % time.strftime("%F %T")).encode())
                log.flush()
    if kind:
        log.write(("=== tinyshed: killing job (%s%s) ===\n" % (kind, why)).encode())
        log.flush()
        kill_group(p)
    else:
        kind = "done" if p.returncode == 0 else "error"
        with contextlib.suppress(OSError):  # no stragglers outlive the job
            os.killpg(p.pid, signal.SIGKILL)
    poll()
    if line != saved:  # the final event carries the last checkpoint
        record()
    finish(db, jid, kind, rc=p.returncode, peak_mb=peak, tail=log_tail(jid))
    shutil.rmtree(trace, ignore_errors=True)  # only meaningful while the job runs


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
    for j in db.execute("SELECT id FROM jobs WHERE state IN ('running','paused')").fetchall():
        f = try_lock(P("run", j["id"] + ".lock"))
        if not f:
            continue
        with f:
            j = db.execute("SELECT * FROM jobs WHERE id=?", (j["id"],)).fetchone()
            if j["state"] not in ("running", "paused"):  # the watchdog finished normally in the meantime
                continue
            try:  # kill the orphaned job, but only if the pid provably still is our job (linux)
                with open("/proc/%d/environ" % j["pid"], "rb") as e:
                    if ("TINYSHED_JOB=" + j["id"]).encode() in e.read().split(b"\0"):
                        os.killpg(j["pid"], signal.SIGKILL)
            except (OSError, TypeError):
                pass
            finish(db, j["id"], "lost", tail=log_tail(j["id"]))


def schedule(db, cfg, kids):
    """Strict priority order: highest prio first, FIFO within a priority; no backfilling past a job that does not fit.
    Forced jobs (`tshed edit --force`) go first and start at once: idle cores first, then they share busy ones."""
    cores = host_cores(cfg)
    used = used_cores(db)
    free = [c for c in cores if c not in used]
    for j in db.execute("SELECT id, cpus, force, state FROM jobs WHERE cancel=0 AND ((state='queued' AND (force=1 OR not_before<=?)) "
                        "OR (state='paused' AND pause=0)) ORDER BY force DESC, prio DESC, created, id", (time.time(),)).fetchall():
        need = min(max(1, j["cpus"]), len(cores))
        if need > len(free) and not j["force"]:
            break
        if j["state"] == "paused":  # resume: its watchdog is alive (holds the lock); it re-pins and continues the job
            mine = free[:need]
            free = free[need:]
            with tx(db):
                n = db.execute("UPDATE jobs SET state='running', cores=?, paused_s=coalesce(paused_s,0)+?-paused_at, paused_at=NULL "
                               "WHERE id=? AND state='paused' AND pause=0 AND cancel=0",
                               (",".join(map(str, mine)), time.time(), j["id"])).rowcount
                ev = emit(db, j["id"], "resumed") if n else None
            if ev:
                fire(ev)
            continue
        f = try_lock(P("run", j["id"] + ".lock"))
        if not f:  # previous attempt's watchdog is still exiting
            continue
        with f:
            mine = (free + [c for c in cores if c not in free])[:need]
            free = [c for c in free if c not in mine]
            with tx(db):
                n = db.execute("UPDATE jobs SET state='running', attempt=attempt+1, cores=?, started=?, ended=NULL, rc=NULL, pid=NULL, "
                               "force=0, pause=0, paused_at=NULL, paused_s=0 WHERE id=? AND state='queued' AND cancel=0",
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
                pct = round(max(0.0, 100 * (1 - idle / total)), 1)  # >100% is possible only with bad counters
            elif total == 0 or span < 0.5:
                return prev.get("pct")  # not enough elapsed: keep the old baseline and reading
            # counters went backwards (reboot) or the baseline is stale: fall through to a fresh one
        except (KeyError, TypeError, ValueError):
            pass
    meta(db, "cpu", json.dumps(dict(ts=now, cores=sorted(cur), ticks=cur, pct=pct)))
    return pct


def cpu_freq_ghz(cores):
    """Mean current clock (GHz) of the given cores; None where the OS does not report a clock."""
    try:  # linux: sysfs cpufreq, kHz per core
        return sum(int(open("/sys/devices/system/cpu/cpu%d/cpufreq/scaling_cur_freq" % c).read())
                   for c in cores) / len(cores) / 1e6
    except (OSError, ValueError):
        pass
    try:  # linux: /proc/cpuinfo, MHz per processor (no sysfs cpufreq on some boards)
        freqs, cur = {}, None
        with open("/proc/cpuinfo") as f:
            for line in f:
                k, _, v = line.partition(":")
                k = k.strip().lower()
                if k == "processor":
                    with contextlib.suppress(ValueError):
                        cur = int(v)
                elif cur is not None and k in ("cpu mhz", "mhz"):
                    freqs[cur] = float(v.split()[0])
        vals = [freqs[c] for c in cores if c in freqs]
        if vals:
            return sum(vals) / len(vals) / 1e3
    except (OSError, ValueError):
        pass
    if sys.platform == "darwin":  # intel macs; apple silicon hides the clock
        with contextlib.suppress(OSError, ValueError, subprocess.SubprocessError):
            return int(subprocess.run(["sysctl", "-n", "hw.cpufrequency"], capture_output=True, text=True,
                                      timeout=5).stdout) / 1e9
    return None


def power_w(db):
    """Whole-host power draw in watts, best effort; None where the OS exposes no power counter.
    RAPL package energy is a delta between polls, so the last reading lives in host.db meta."""
    uj = {}
    with contextlib.suppress(OSError):
        for n in sorted(os.listdir("/sys/class/powercap")):
            if n.startswith("intel-rapl") and n.count(":") == 1:  # packages, not subdomains
                with contextlib.suppress(OSError, ValueError):
                    uj[n] = int(open("/sys/class/powercap/%s/energy_uj" % n).read())
    if uj:
        try:
            prev = json.loads(meta(db, "power") or "null")
        except ValueError:
            prev = None
        meta(db, "power", json.dumps(dict(ts=time.time(), uj=uj)))
        span = time.time() - prev["ts"] if prev else 0
        if prev and 0.2 <= span <= 300:
            joules = 0
            for n, e in uj.items():
                d = e - prev["uj"].get(n, e)
                if d < 0:  # counters wrap at max_energy_range_uj
                    with contextlib.suppress(OSError, ValueError):
                        d += int(open("/sys/class/powercap/%s/max_energy_range_uj" % n).read())
                if d > 0:
                    joules += d
            if joules:
                return joules / span / 1e6
    try:  # battery discharge rate
        for bat in sorted(os.listdir("/sys/class/power_supply")):
            if bat.startswith("BAT"):
                with open("/sys/class/power_supply/%s/power_now" % bat) as f:
                    uw = int(f.read())
                if uw > 0:
                    return uw / 1e6
    except (OSError, ValueError):
        pass
    if sys.platform == "darwin":  # ioreg needs no root: battery draw, else the charger's wattage
        try:
            out = subprocess.run(["ioreg", "-rc", "AppleSmartBattery"], capture_output=True, text=True,
                                 timeout=10).stdout
            m = re.search(r'"Amperage" = (-?\d+)', out)  # mA, negative when discharging
            if m and int(m.group(1)) < 0:  # discharging: battery current is real consumption
                v = re.search(r'"(?:AppleRaw)?BatteryVoltage" = (\d+)', out)
                if v:
                    return -int(m.group(1)) * int(v.group(1)) / 1e6
            w = re.search(r'"Watts"=(\d+)', out)  # plugged in: charger wattage, an upper bound not a reading
            return int(w.group(1)) if w else None
        except (OSError, ValueError, AttributeError, subprocess.SubprocessError):
            pass
    return None


def status(db, cfg):
    cores = host_cores(cfg)
    pct = cpu_util(db, cores)
    job_live, mem_used_mb, mem_reserved_mb = job_usage(db)
    ghz = cpu_freq_ghz(cores)
    watts = power_w(db)
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
    warn = []
    if pct is not None and pct > 100:  # impossible with sound counters: jobs are pinned to their cores
        warn.append("CPU% >100%: limit breach")
    if mem_reserved_mb is not None and mem_used_mb > mem_reserved_mb:  # watchdog should have killed it
        warn.append("RAM over reserved")
    return dict(now=time.time(), version=VERSION, ncpu=os.cpu_count(), cores=len(cores), free=len(cores) - len(used_cores(db)),
                load=os.getloadavg()[0], cpu_pct=pct, cpu_ghz=round(ghz, 2) if ghz is not None else None,
                mem_total_mb=mem_total, mem_avail_mb=mem_avail,
                mem_used_mb=mem_used_mb, mem_reserved_mb=mem_reserved_mb, jobs=job_live,
                power_w=round(watts, 1) if watts is not None else None, warnings=warn,
                disk_free_gb=round(shutil.disk_usage(HOME).free / 2 ** 30, 1), gpus=gpus, agent=agent_alive(),
                counts={r[0]: r[1] for r in db.execute("SELECT state, count(*) FROM jobs GROUP BY state")},
                queued_cpus=db.execute("SELECT coalesce(sum(cpus),0) FROM jobs WHERE state='queued'").fetchone()[0])


def state_of(db, jid):
    r = db.execute("SELECT state FROM jobs WHERE id=?", (jid,)).fetchone()
    return r and r[0]


def script_pos(pids):
    """{pid: (script path, byte offset)} for bash processes running a script file. bash reads a script through fd
    255 and, before running a command, seeks it back to just past that command: the offset says how far it got."""
    out = {}
    if os.path.isdir("/proc/self"):
        for pid in pids:
            with contextlib.suppress(OSError, ValueError, IndexError):
                path = os.readlink("/proc/%d/fd/255" % pid)
                with open("/proc/%d/fdinfo/255" % pid) as f:
                    out[pid] = (path, int(f.read().split("pos:")[1].split()[0]))
        return out
    if not pids or not shutil.which("lsof"):
        return out
    with contextlib.suppress(OSError, subprocess.SubprocessError):
        r = subprocess.run(["lsof", "-a", "-p", ",".join(map(str, pids)), "-d", "255", "-o", "-Fpon"],
                           capture_output=True, text=True, timeout=10).stdout
        pid = off = None
        for line in r.splitlines():
            k, v = line[:1], line[1:]
            if k == "p":
                pid, off = int(v), None
            elif k == "o":
                off = int(v[2:]) if v.startswith("0t") else int(v, 16)
            elif k == "n" and pid and off is not None:
                out[pid] = (v, off)
    return out


def script_line(path, off):
    """(line number, source text) of the command that ends just before byte off."""
    try:
        with open(path, "rb") as f:
            head = f.read(min(off, 64 << 20))
    except OSError:
        return None, None
    if not head or not os.path.isfile(path):
        return None, None
    n = head.count(b"\n") + (0 if head.endswith(b"\n") else 1)
    return n, head.rstrip(b"\n").rsplit(b"\n", 1)[-1].decode("utf-8", "replace").strip()


def where_info(db, jid):
    """Where a running job is: its process tree, each bash script's position (fd 255 offset), the exact
    --trace-lines record per bash process, and a py-spy dump of python processes when py-spy is installed."""
    j = db.execute("SELECT * FROM jobs WHERE id=?", (jid,)).fetchone()
    if not j:
        raise Fail("no job %s on this host" % jid)
    out: dict = dict(id=jid, state=j["state"], trace_lines=bool(j["trace_lines"]), procs=[], pyspy={})
    if j["state"] != "running" or not j["pid"]:
        return out
    linux = os.path.isdir("/proc/self")
    cols = ["pid", "ppid", "pgid"] + (["sid"] if linux else []) + ["stat", "etime", "command"]
    try:
        ps = subprocess.run(["ps", "-axo", ",".join(c + "=" for c in cols)], capture_output=True, text=True, timeout=10).stdout
    except (OSError, subprocess.SubprocessError) as e:
        raise Fail("ps failed: %s" % e)
    procs: dict[int, dict] = {}
    for line in ps.splitlines():
        f = line.split(None, len(cols) - 1)
        if len(f) == len(cols):
            r = dict(zip(cols, f))
            procs[int(r["pid"])] = dict(r, pid=int(r["pid"]), ppid=int(r["ppid"]))
    root = j["pid"]
    kids: dict[int, list[int]] = {}
    for p in procs.values():
        kids.setdefault(p["ppid"], []).append(p["pid"])
    mine, todo = set(), [root]
    while todo:  # descendants, plus anything still in the job's session/process group (reparented)
        pid = todo.pop()
        if pid in procs and pid not in mine:
            mine.add(pid)
            todo += kids.get(pid, [])
    mine |= {p["pid"] for p in procs.values() if str(root) in (p["pgid"], p.get("sid"))}
    pos = script_pos(sorted(mine))
    order = []

    def walk(pid, depth):
        order.append((pid, depth))
        for k in sorted(kids.get(pid, [])):
            if k in mine:
                walk(k, depth + 1)
    for pid in sorted(p for p in mine if procs[p]["ppid"] not in mine):
        walk(pid, 0)
    for pid, depth in order:
        p = procs[pid]
        rec = dict(pid=pid, depth=depth, stat=p["stat"], etime=p["etime"], cmd=p["command"][:300])
        if pid in pos:
            rec["script"] = pos[pid][0]
            rec["line"], rec["text"] = script_line(*pos[pid])
        with contextlib.suppress(OSError):
            tp = P("run", jid + ".where", str(pid))
            with open(tp, errors="replace") as f:
                loc, _, rest = f.read().partition("\t")
            funcs, _, cmd = rest.partition("\t")
            fs = funcs.split()
            rec["trace"] = dict(at=loc.strip(), funcs=fs[:-1] if fs[-1:] == ["main"] else fs,  # main = the script body
                                cmd=" ".join(cmd.split()), age=time.time() - os.path.getmtime(tp))
        out["procs"].append(rec)
    spy = shutil.which("py-spy")
    pys = [r["pid"] for r in out["procs"] if re.search(r"(?i)python[\d.]*$", os.path.basename(r["cmd"].split()[0]))]
    for pid in pys[:4]:
        if not spy:
            break
        try:
            r = subprocess.run([spy, "dump", "--pid", str(pid)], capture_output=True, text=True, timeout=20)
            out["pyspy"][pid] = r.stdout.strip() if r.returncode == 0 else "py-spy: " + ((r.stderr.strip().splitlines() or ["exit %d" % r.returncode])[-1])
        except (OSError, subprocess.SubprocessError) as e:
            out["pyspy"][pid] = "py-spy: %s" % e
    if spy and len(pys) > 4:
        out["pyspy_skipped"] = len(pys) - 4
    out["pyspy_missing"] = bool(pys) and not spy
    return out


def git_run(*args, cwd=None, env=None):
    r = subprocess.run(["git", *args], cwd=cwd, env=env, capture_output=True, text=True, timeout=600)
    if r.returncode:
        raise Fail("git %s: %s" % (args[0], (r.stderr.strip().splitlines() or ["exit %d" % r.returncode])[-1]))
    return r.stdout.strip()


def ensure_bare(repo):
    """Create a bare repository at repo unless it exists; safe against a concurrent creator."""
    if not os.path.isdir(repo):
        os.makedirs(os.path.dirname(repo), exist_ok=True)
        tmp = "%s.tmp%d" % (repo, os.getpid())
        git_run("init", "-q", "--bare", tmp)
        with contextlib.suppress(OSError):  # someone else may have won
            os.rename(tmp, repo)
        shutil.rmtree(tmp, ignore_errors=True)
    return repo


def clean_work(jid):
    """Delete a job's work dir (checkout, pushed files, outputs) and its worktree record in the mirror.
    The snapshot commit stays in the mirror, so the code can always be checked out again."""
    work = P("work", jid)
    with contextlib.suppress(OSError):
        with open(os.path.join(work, ".git")) as f:  # "gitdir: <mirror>/worktrees/<name>"
            mirror = os.path.dirname(os.path.dirname(f.read().split(":", 1)[1].strip()))
        with contextlib.suppress(Fail):
            git_run("--git-dir", mirror, "worktree", "remove", "--force", work)
            git_run("--git-dir", mirror, "worktree", "prune")
    existed = os.path.exists(work)
    shutil.rmtree(work, ignore_errors=True)
    with contextlib.suppress(OSError):
        os.remove(work + ".checked-out")
    return "cleaned" if existed else "absent"


def stage_host(req):
    """Prepare a job's work dir (~/.tinyshed/work/ID). With key and no sha: make sure the bare mirror
    repos/KEY.git exists, so the server can push into it. With sha: check the snapshot out there as a
    detached worktree (idempotent). Then create the parent dirs that --push targets need."""
    jid, key, sha = req["id"], req.get("key"), req.get("sha")
    work = P("work", jid)
    out = dict(work=work)
    if key:
        repo = out["repo"] = ensure_bare(P("repos", key + ".git"))
        if sha:
            ok = work + ".checked-out"  # written last: a checkout interrupted halfway is redone, never trusted
            if os.path.exists(ok):
                if git_run("rev-parse", "HEAD", cwd=work) != sha:
                    raise Fail("work dir %s already holds another checkout" % work)
            else:
                if os.path.exists(work):  # leftovers of an interrupted staging; the job has not started yet
                    with contextlib.suppress(Fail):
                        git_run("--git-dir", repo, "worktree", "remove", "--force", work)
                    shutil.rmtree(work, ignore_errors=True)
                    with contextlib.suppress(Fail):
                        git_run("--git-dir", repo, "worktree", "prune")
                git_run("--git-dir", repo, "worktree", "add", "-q", "--detach", "--force", work, sha)
                open(ok, "w").close()
    if not key or sha:
        os.makedirs(work, exist_ok=True)
        for d in req.get("dirs", []):
            os.makedirs(os.path.normpath(os.path.join(work, d)), exist_ok=True)
    return out


def host_paths(db, jid, rels):
    """Absolute paths on this host of a job's files (relative to its working directory): {rel: [abs, is dir, exists]}."""
    j = db.execute("SELECT * FROM jobs WHERE id=?", (jid,)).fetchone()
    if not j:
        raise Fail("no job %s on this host" % jid)
    base = job_cwd(j)
    out = {}
    for rel in rels:
        path = os.path.normpath(os.path.join(base, os.path.expanduser(rel)))
        out[rel] = [path, os.path.isdir(path), os.path.exists(path)]
    return out


def grep_logs(req):
    """Search the logs of the given jobs on this host (plain or zstd). {id: [[line number, text], ...]}, capped."""
    rx = re.compile(req["pattern"], re.I if req.get("ignore_case") else 0)
    per, total, out = int(req.get("per_job", 20)), int(req.get("max", 500)), {}
    for jid in req.get("ids", []):
        if total <= 0:
            break
        path = P("logs", jid + ".log")
        try:
            f = open(path, "rb") if os.path.exists(path) else zopen(path + ".zst")
        except OSError:
            continue
        hits, n = [], 0
        with contextlib.suppress(OSError, EOFError):
            with f:
                for n, raw in enumerate(f, 1):
                    line = raw.decode("utf-8", "replace").rstrip("\r\n")
                    if rx.search(line):
                        hits.append([n, line[:400]])
                        if len(hits) >= min(per, total):
                            break
        if hits:
            out[jid] = hits
            total -= len(hits)
    return out


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
            if not n:  # moved away earlier (`withdraw`) and now moved back: queue it again
                cols = {k: v for k, v in row.items() if k not in ("id", "created")}
                n = db.execute("UPDATE jobs SET %s, not_before=0, cancel=0, force=0 WHERE id=? AND state='moved'"
                               % ",".join(k + "=?" for k in cols), list(cols.values()) + [j["id"]]).rowcount
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
    # pause on a paused job waiting for cores cancels its pending resume
    for op, flag, states in (("pause", 1, ("running", "paused")), ("resume", 0, ("paused", "running"))):
        if op in req:  # a flag the watchdog (pause) or the scheduler (resume) acts on within ~2s
            for jid in req[op]:
                db.execute("UPDATE jobs SET pause=? WHERE id=? AND state IN (%s)" % ",".join("?" * len(states)), (flag, jid) + states)
            out[op + "d"] = {jid: state_of(db, jid) for jid in req[op]}
    for jid, prio in (req.get("prio") or {}).items():
        transition(db, jid, "update", prio=int(prio))
    if "force" in req:  # start now, ignoring priority order and free cores; answer each job's state
        for jid in req["force"]:
            with tx(db):
                n = db.execute("UPDATE jobs SET force=1, not_before=0 WHERE id=? AND state='queued' AND force=0", (jid,)).rowcount
                ev = emit(db, jid, "update") if n else None
            if ev:
                fire(ev)
        out["forced"] = {jid: state_of(db, jid) for jid in req["force"]}
    if "withdraw" in req:  # give a queued job up so the server can move it to another host
        for jid in req["withdraw"]:
            with tx(db):
                n = db.execute("UPDATE jobs SET state='moved', ended=? WHERE id=? AND state='queued'", (time.time(), jid)).rowcount
                ev = emit(db, jid, "moved", dict(final=True)) if n else None
            if ev:
                fire(ev)
        out["withdrawn"] = {jid: state_of(db, jid) for jid in req["withdraw"]}
    if "after" in req:
        out["events"] = events_after(db, req["after"])
    if req.get("status"):
        out["status"] = status(db, cfg)
    if req.get("where"):
        out["where"] = where_info(db, req["where"])
    if req.get("grep"):
        out["grep"] = grep_logs(req["grep"])
    if req.get("stage"):
        out["stage"] = stage_host(req["stage"])
    if req.get("paths"):
        out["paths"] = host_paths(db, req["paths"]["id"], req["paths"]["rels"])
    if "clean" in req:  # only jobs that are over here (finished, or moved to another host) lose their work dir
        out["cleaned"] = {}
        for jid in req["clean"]:
            state = state_of(db, jid)
            out["cleaned"][jid] = clean_work(jid) if state in TERMINAL + ("moved",) or state is None else state
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
        if f and pager.stdin:
            shutil.copyfileobj(f, pager.stdin)
    if pager.stdin:
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
                          git=bool(shutil.which("git")), rsync=bool(shutil.which("rsync")),
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
  sent INT DEFAULT 0, cancel INT DEFAULT 0, eid INT DEFAULT 0, j TEXT, stage TEXT);
CREATE TABLE IF NOT EXISTS events(host TEXT, eid INT, ts REAL, job TEXT, kind TEXT, data TEXT, PRIMARY KEY(host, eid));
"""
SSH = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=8", "-o", "ServerAliveInterval=15",
       "-o", "ControlMaster=auto", "-o", "ControlPath=~/.ssh/tinyshed-%C", "-o", "ControlPersist=300"]
REMOTE_PY = "python3 .tinyshed/tinyshed.py "


def sdb():
    db = connect("server.db", SERVER_SCHEMA)
    if "stage" not in {r[1] for r in db.execute("PRAGMA table_info(jobs)")}:  # v0.2 servers predate file staging
        try:
            db.execute("ALTER TABLE jobs ADD COLUMN stage TEXT")
        except sqlite3.OperationalError as e:
            if "duplicate" not in str(e):
                raise
    return db


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
                           "sent=1, eid=excluded.eid, j=excluded.j WHERE excluded.eid > jobs.eid AND jobs.host IS excluded.host",
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
    submit = []
    for r in db.execute("SELECT id, j, stage FROM jobs WHERE host=? AND sent=0 AND cancel=0 AND state='pending'", (name,)).fetchall():
        st = json.loads(r["stage"]) if r["stage"] else None
        if needs_stage(st, name):  # files first: the job is delivered once its work dir is ready on this host
            if STAGE_MODE == "bg":
                stage_bg(r["id"], name, st)
            elif STAGE_MODE == "sync" and stage_once(db, r["id"], name, st):
                submit.append(unpack(r["j"]))
        else:
            submit.append(unpack(r["j"]))
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


def host_caps(db):
    """Spare cores per live host (idle minus what is already queued or on its way), and total cores."""
    cap, cores = {}, {}
    for h in db.execute("SELECT name, status FROM hosts WHERE err IS NULL AND status IS NOT NULL AND seen>?", (time.time() - 120,)):
        s = json.loads(h["status"])
        unsent = sum(unpack(r["j"]).get("cpus", 1) for r in db.execute("SELECT j FROM jobs WHERE host=? AND sent=0 AND state='pending'", (h["name"],)))
        cap[h["name"]], cores[h["name"]] = s["free"] - s["queued_cpus"] - unsent, s["cores"]
    return cap, cores


def dispatch(db):
    """Late-bind host-less jobs, highest priority first, to the live host with the most idle cores."""
    cap, cores = host_caps(db)
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


# ── file staging (--git / --push before a job starts, --pull after it ends). The server does all copying:
# git push into a bare mirror per repo on the host + a detached worktree per job, and rsync over the same ssh.

STAGE_RUNTIME = ("staged", "err", "err_at", "pulled", "pull_err", "pull_at", "pulled_to", "stale", "cleaned", "clean_err")
SNAPSHOT_MAX_MB = 100  # untracked files a --git snapshot may carry
_staging, _stage_lock, _pulling = set(), threading.Lock(), threading.Lock()
# who stages waiting jobs during sync: "bg" (serve, the dashboard: a thread, so sync never blocks on an upload),
# "sync" (submit, wait, edit, fetch: inline, the command waits for it), None (ls and other quick reads: never,
# so a short-lived process cannot die halfway through an upload)
STAGE_MODE = None


def needs_stage(st, host):
    return bool(st and (st.get("git") or st.get("push")) and st.get("staged") != host)


def set_stage(db, jid, **upd):
    with tx(db):
        r = db.execute("SELECT stage FROM jobs WHERE id=?", (jid,)).fetchone()
        st = dict(json.loads(r["stage"]) if r and r["stage"] else {}, **upd)
        db.execute("UPDATE jobs SET stage=? WHERE id=?", (json.dumps(st), jid))
    return st


def over_ssh(dest, argv_for, env=None, timeout=3600):
    """Run a transfer command (rsync, git push) against a host: argv_for(prefix) gets '' for local hosts, else
    'sshdest:' for each alternative in turn (the one that last worked first). Returns stdout or raises Fail."""
    alts = [""] if is_local(dest) else sorted(dest.split(","), key=lambda d: d != _route.get(dest))
    err = "no route"
    for d in alts:
        r = subprocess.run(argv_for(d + ":" if d else ""), capture_output=True, text=True, timeout=timeout, env=env)
        if r.returncode == 0:
            if d:
                _route[dest] = d
            return r.stdout
        err = (r.stderr.strip().splitlines() or ["exit %d" % r.returncode])[-1]
    raise Fail(err)


def rsync_argv(src, dst):
    return ["rsync", "-a", "-e", shlex.join(SSH), src, dst]


def git_snapshot(path, jid):
    """Commit the working tree as it is (tracked changes + untracked files that are not ignored) without touching
    the index or the files: a throwaway index, write-tree, commit-tree. Kept alive by refs/tinyshed/ID."""
    path = os.path.abspath(os.path.expanduser(path))
    try:
        top = git_run("rev-parse", "--show-toplevel", cwd=path)
    except (Fail, OSError) as e:
        raise Fail("--git: %s is not in a git repository (%s)" % (path, e))
    head = subprocess.run(["git", "rev-parse", "-q", "--verify", "HEAD"], cwd=top, capture_output=True, text=True).stdout.strip() or None
    sizes = []
    for f in git_run("ls-files", "-z", "--others", "--exclude-standard", cwd=top).split("\0"):
        with contextlib.suppress(OSError):
            if f:
                sizes.append((os.path.getsize(os.path.join(top, f)), f))
    if sum(n for n, _ in sizes) > SNAPSHOT_MAX_MB << 20:
        big = ", ".join("%s (%s)" % (f, fmt_mem(n >> 20, None)) for n, f in sorted(sizes, reverse=True)[:5])
        raise Fail("--git: untracked files add up to more than %dM (%s); .gitignore them, or ship them with --push" % (SNAPSHOT_MAX_MB, big))
    with tempfile.TemporaryDirectory() as tmp:
        index = os.path.join(tmp, "index")
        real = os.path.join(top, git_run("rev-parse", "--git-path", "index", cwd=top))
        if os.path.exists(real):
            shutil.copy(real, index)
        env = dict(os.environ, GIT_INDEX_FILE=index, GIT_AUTHOR_NAME="tinyshed", GIT_AUTHOR_EMAIL="tinyshed@localhost",
                   GIT_COMMITTER_NAME="tinyshed", GIT_COMMITTER_EMAIL="tinyshed@localhost")
        git_run("add", "-A", cwd=top, env=env)
        tree = git_run("write-tree", cwd=top, env=env)
        if head and tree == git_run("rev-parse", "HEAD^{tree}", cwd=top):
            sha = head
        else:
            sha = git_run("commit-tree", tree, *(["-p", head] if head else []), "-m", "tinyshed snapshot for job " + jid, cwd=top, env=env)
    branch = subprocess.run(["git", "symbolic-ref", "-q", "--short", "HEAD"], cwd=top, capture_output=True, text=True).stdout.strip()
    key = re.sub(r"[^\w.-]", "_", os.path.basename(top)) + "-" + uuid.uuid5(uuid.NAMESPACE_URL, top).hex[:8]
    # kept in tinyshed's own store, not as a ref in your repo (no clutter in `git log --all`); the commit
    # object left in your repo is unreferenced and goes with git's normal gc
    store = ensure_bare(P("snapshots", key + ".git"))
    git_run("push", "-q", "--no-verify", store, "%s:refs/tinyshed/%s" % (sha, jid), cwd=top)
    return dict(repo=top, store=store, key=key, sha=sha, base=head, dirty=sha != head, branch=branch or None,
                sub=os.path.relpath(path, top))


def stage_job(dest, jid, st, cwd):
    """Put a job's files on its host: the --git snapshot as a worktree, then each --push path under the job's
    working directory. Every step is idempotent, so a failed or repeated staging is simply run again."""
    base = cwd or "."
    rpc_ = lambda **kw: json.loads(remote(dest, ["rpc"], json.dumps(dict(stage=dict(id=jid, **kw)))))["stage"]
    g = st.get("git")
    if g:
        repo = rpc_(key=g["key"])["repo"]
        src = g.get("store") or g["repo"]  # snapshots from before the store lived as refs in the repo itself
        with contextlib.suppress(Fail, OSError):  # a rerun keeps the same snapshot under its own ref too
            git_run("--git-dir" if g.get("store") else "-C", src, "update-ref", "refs/tinyshed/" + jid, g["sha"])
        over_ssh(dest, lambda pre: ["git", "--git-dir" if g.get("store") else "-C", src, "push", "-q", "--no-verify", pre + repo,
                                    "%s:refs/tinyshed/%s" % (g["sha"], jid)], env=dict(os.environ, GIT_SSH_COMMAND=shlex.join(SSH)))
    pushes = [(src, os.path.normpath(os.path.join(base, d)), isdir) for src, d, isdir in st.get("push") or []]
    work = rpc_(key=g and g["key"], sha=g and g["sha"], dirs=[t if isdir else os.path.dirname(t) or "." for _, t, isdir in pushes])["work"]
    for src, target, isdir in pushes:
        if not os.path.exists(src):
            raise Fail("--push %s: no longer exists here" % src)
        over_ssh(dest, lambda pre: rsync_argv(src + ("/" if isdir else ""), pre + os.path.join(work, target) + ("/" if isdir else "")))


def stage_once(db, jid, host, st):
    """Stage a waiting job on host and record the outcome. True when its files are in place."""
    if st.get("err_at") and time.time() - st["err_at"] < 60:  # retry failed staging once a minute
        return False
    r = db.execute("SELECT j FROM jobs WHERE id=?", (jid,)).fetchone()
    dest = db.execute("SELECT dest FROM hosts WHERE name=?", (host,)).fetchone()["dest"]
    try:
        stage_job(dest, jid, st, unpack(r["j"]).get("cwd"))
    except Exception as e:
        set_stage(db, jid, err="%s: %s" % (host, e), err_at=time.time())
        return False
    old = st.get("staged")
    set_stage(db, jid, staged=host, err=None, err_at=None,
              stale=sorted(set(st.get("stale") or []) | ({old} if old and old != host else set())))
    return True


def stage_bg(jid, host, st):
    """Stage a waiting job in the background (sync must not block on a big upload), then deliver it."""
    if st.get("err_at") and time.time() - st["err_at"] < 60:  # retry failed staging once a minute
        return
    with _stage_lock:
        if jid in _staging:
            return
        _staging.add(jid)

    def go():
        try:
            with contextlib.closing(sdb()) as db:
                if not stage_once(db, jid, host, st):
                    return
            sync_host(host)
        finally:
            with _stage_lock:
                _staging.discard(jid)
    threading.Thread(target=go, daemon=True).start()


def self_ignore(path):
    """A results folder tinyshed creates inside a git work tree ignores itself, so results never ride along in
    the next --git snapshot (or in a commit)."""
    parent = os.path.dirname(path)
    inside = subprocess.run(["git", "-C", parent, "rev-parse", "--is-inside-work-tree"], capture_output=True, text=True)
    if inside.returncode == 0 and subprocess.run(["git", "-C", parent, "check-ignore", "-q", path]).returncode != 0:
        with open(os.path.join(path, ".gitignore"), "w") as f:
            f.write("# results pulled by tinyshed: kept out of git and out of --git snapshots\n*\n")


def pull_job(db, jid, to=None, srcs=None):
    """Copy a job's results from its host into local folders: the job's --pull list, or srcs into `to`.
    A directory's contents land in the folder, a file lands inside it. Each folder gets tinyshed-ID.json."""
    r = db.execute("SELECT * FROM jobs WHERE id=?", (jid,)).fetchone()
    st, b = json.loads(r["stage"]) if r["stage"] else {}, unpack(r["j"])
    pulls = [[x, to or d] for x, d in st.get("pull") or []] if not srcs else [[x, to or os.path.abspath("results/{name}/{id}")] for x in srcs]
    if not pulls:
        raise Fail("%s has nothing to pull: name the paths, or submit with --pull" % jid)
    if not r["sent"]:
        raise Fail("%s has not reached a host yet" % jid)
    if st.get("cleaned"):
        raise Fail("%s: its work dir was cleaned after the job ended%s" % (jid, " (results pulled to %s)" % ", ".join(st["pulled_to"]) if st.get("pulled_to") else ""))
    h = db.execute("SELECT dest FROM hosts WHERE name=?", (r["host"],)).fetchone()
    if not h:
        raise Fail("host %s is no longer registered" % r["host"])
    paths = json.loads(remote(h["dest"], ["rpc"], json.dumps(dict(paths=dict(id=jid, rels=[x for x, _ in pulls])))))["paths"]
    done, errs, dirs = [], [], []
    for src, d in pulls:
        path, isdir, exists = paths[src]
        dst = os.path.abspath(os.path.expanduser(d.format(name=b.get("name") or jid, id=jid, host=r["host"], state=r["state"])))
        if not exists:
            errs.append("%s: not found on %s" % (src, r["host"]))
            continue
        fresh = not os.path.exists(dst)
        os.makedirs(dst, exist_ok=True)
        if fresh:
            self_ignore(dst)
        try:
            over_ssh(h["dest"], lambda pre: rsync_argv(pre + path + ("/" if isdir else ""), dst + "/"))
        except Fail as e:
            errs.append("%s: %s" % (src, e))
            continue
        with open(os.path.join(dst, "tinyshed-%s.json" % jid), "w") as f:
            json.dump(dict(id=jid, name=b.get("name"), host=r["host"], state=r["state"], rc=b.get("rc"), cmd=b.get("cmd"),
                           started=b.get("started"), ended=b.get("ended"), git=st.get("git"), source="%s:%s" % (r["host"], path),
                           pulled=time.time()), f, indent=2)
        done.append("%s → %s" % (src, dst))
        dirs.append(dst)
    if not srcs and not to:
        set_stage(db, jid, pulled=None if errs else time.time(), pull_err="; ".join(errs) or None, pull_at=time.time(), pulled_to=dirs)
    return done, errs


def pulls_due(db):
    """Finished jobs whose --pull has not happened yet (failed pulls are retried once a minute)."""
    out = []
    for r in db.execute("SELECT id, state, stage FROM jobs WHERE stage IS NOT NULL AND state IN (%s)" % ",".join("?" * len(TERMINAL)), TERMINAL):
        st = json.loads(r["stage"])
        if st.get("pull") and r["state"] in st.get("pull_on", ()) and not st.get("pulled") \
                and (not st.get("pull_err") or time.time() - (st.get("pull_at") or 0) > 60):
            out.append(r["id"])
    return out


def clean_due(db, ids=None):
    """Ended jobs whose work dir can go: staged files (--git/--push), not --keep-work, not cleaned yet, and
    the --pull for their end state done (or none wanted). Failed cleans are retried once a minute."""
    out = []
    for r in db.execute("SELECT id, state, stage FROM jobs WHERE stage IS NOT NULL AND state IN (%s)" % ",".join("?" * len(TERMINAL)), TERMINAL):
        st = json.loads(r["stage"])
        if (ids is None or r["id"] in ids) and (st.get("git") or st.get("push")) and (st.get("staged") or st.get("stale")) \
                and not st.get("keep") and not st.get("cleaned") and (st.get("pulled") or not (st.get("pull") and r["state"] in st.get("pull_on", ()))) \
                and (not st.get("clean_err") or time.time() - (st.get("clean_at") or 0) > 60):
            out.append(r["id"])
    return out


def clean_job(db, jid):
    """Delete a job's work dir on every host that holds a copy (the one it ran on, and any it was moved off)."""
    r = db.execute("SELECT stage FROM jobs WHERE id=?", (jid,)).fetchone()
    st = json.loads(r["stage"])
    hosts = sorted({st.get("staged")} | set(st.get("stale") or []) - {None})
    errs = []
    for host in hosts:
        h = db.execute("SELECT dest FROM hosts WHERE name=?", (host,)).fetchone()
        if not h:
            continue  # host gone: nothing left to clean
        try:
            got = json.loads(remote(h["dest"], ["rpc"], json.dumps(dict(clean=[jid]))))
            if "cleaned" not in got:
                raise Fail("host runs an older tinyshed")
            if got["cleaned"].get(jid) not in ("cleaned", "absent"):
                raise Fail("still %s there" % got["cleaned"].get(jid))
        except Exception as e:
            errs.append("%s: %s" % (host, e))
    set_stage(db, jid, cleaned=None if errs else time.time(), clean_err="; ".join(errs) or None, clean_at=time.time())
    return errs


def pull_due_bg():
    """Run due pulls in a background thread (serve, the dashboard); one batch at a time."""
    if not _pulling.acquire(blocking=False):
        return

    def go():
        try:
            with contextlib.closing(sdb()) as db:
                for jid in pulls_due(db):
                    try:
                        pull_job(db, jid)
                    except Exception as e:
                        set_stage(db, jid, pull_err=str(e), pull_at=time.time())
                for jid in clean_due(db):  # after the pulls: a work dir goes only once its results are safe
                    clean_job(db, jid)
        finally:
            _pulling.release()
    threading.Thread(target=go, daemon=True).start()


# ───────────────────────────────────────────────────────────────── server: operations

NAME_MAX = 16


def job_name(s):
    """A job's name: 1-16 printable characters, no whitespace."""
    s = str(s).strip()
    if not s or len(s) > NAME_MAX or not s.isprintable() or any(c.isspace() for c in s):
        raise argparse.ArgumentTypeError("a job name is 1-%d characters without spaces (got %r)" % (NAME_MAX, s))
    return s


def submit_parser(**kw):
    p = argparse.ArgumentParser(**kw)
    p.add_argument("-n", "--name", type=job_name, required=True, help="job name, at most %d characters, no spaces" % NAME_MAX)
    p.add_argument("-H", "--host", help="pin to a host (default: first live host with enough idle cores)")
    p.add_argument("-p", "--prio", type=int, default=0, help="higher runs first")
    p.add_argument("-c", "--cpus", type=int, default=1, help="cores to pin with taskset")
    p.add_argument("-m", "--mem", type=size, default=0, help="RSS limit for the whole process tree, e.g. 8G")
    p.add_argument("-t", "--timeout", type=dur, default=0, help="runtime limit per attempt, e.g. 90s 30m 12h 2d (default: twice --expect)")
    p.add_argument("-x", "--expect", type=dur, default=0, help="expected runtime: sets the timeout to twice this, gives an ETA "
                   "and, without --stall, a stall limit of twice the expected time for each checkpoint's progress (at least 10s)")
    p.add_argument("--stall", type=dur, help="kill the job (state stalled) after this long without a checkpoint "
                   "(tshed progress / $TINYSHED_PROGRESS), counted from its start; 0 = never")
    p.add_argument("--stall-on-log", action="store_true", help="log output also counts as a checkpoint (needs --stall)")
    p.add_argument("--trace-lines", action="store_true", help="record each bash process's current line and command, for "
                   "`tshed where` (bash >= 4; costs ~0.1 ms per bash command)")
    p.add_argument("-r", "--retries", type=int, default=0)
    p.add_argument("--retry-on", default=SPEC["retry_on"], help="comma list of error,timeout,oom,stalled,lost (default %(default)s)")
    p.add_argument("--retry-delay", type=dur, default=SPEC["retry_delay"], help="first backoff, doubles per attempt")
    p.add_argument("-C", "--cwd", help="working directory on the host (default ~; with --git/--push: relative to the job's work dir)")
    p.add_argument("--git", nargs="?", const=".", metavar="PATH", help="run in a snapshot of the git repo at PATH (default .): "
                   "committed, uncommitted and untracked-but-not-ignored files, checked out per job on the host; cwd = your subdirectory")
    p.add_argument("--push", action="append", default=[], metavar="SRC[:DEST]", help="copy a local file or folder into the job's "
                   "working directory before it starts (DEST default: its name; repeatable)")
    p.add_argument("--pull", action="append", default=[], metavar="SRC[:DEST]", help="when the job ends, copy SRC (relative to its "
                   "working directory) into the local folder DEST (default results/{name}/{id}; {name} {id} {host} {state}; repeatable)")
    p.add_argument("--pull-on", default="done,error,timeout,oom,stalled,lost", help="end states that trigger --pull (default %(default)s)")
    p.add_argument("--keep-work", action="store_true", help="keep the job's work dir on the host after it ends (default: deleted "
                   "once its results are pulled; the code stays recoverable with tshed checkout)")
    p.add_argument("-e", "--env", action="append", default=[], metavar="K=V")
    p.add_argument("cmd", nargs=argparse.REMAINDER, help="command (run with bash -c)")
    return p


def add_job(db, spec, host=None, jid=None, staged=None):
    """Queue a job on the server. spec["stage"] (--git/--push/--pull) travels in its own column: the host never
    sees it, and a rerun copies it without the previous run's staging/pull state."""
    if host and not db.execute("SELECT 1 FROM hosts WHERE name=?", (host,)).fetchone():
        raise Fail("unknown host %r (see: tshed hosts)" % host)
    j = {k: spec.get(k, d) for k, d in SPEC.items()}
    j.update(id=jid or uuid.uuid4().hex[:8], created=time.time())
    st = {k: v for k, v in (spec.get("stage") or {}).items() if k not in STAGE_RUNTIME}
    if st and staged:
        st["staged"] = staged
    db.execute("INSERT INTO jobs(id,host,prio,state,created,j,stage) VALUES(?,?,?,'pending',?,?,?)",
               (j["id"], host, j["prio"], j["created"], pack(j), json.dumps(st) if st else None))
    return j["id"]


def stage_spec(a, jid):
    """--git/--push/--pull -> the job's stage record and its working directory (relative to the work dir)."""
    st, cwd = {}, a.cwd
    if (a.git or a.push) and cwd and os.path.isabs(os.path.expanduser(cwd)):
        raise Fail("-C must be relative with --git/--push (the job runs inside its own work dir)")
    if a.git:
        st["git"] = g = git_snapshot(a.git, jid)
        cwd = cwd or (g["sub"] if g["sub"] != "." else None)
    for item in a.push:
        src, _, d = item.partition(":")
        src = os.path.abspath(os.path.expanduser(src))
        if not os.path.exists(src):
            raise Fail("--push %s: no such file or folder" % src)
        d = os.path.normpath(d or os.path.basename(src.rstrip("/")))
        if os.path.isabs(d) or d.startswith(".."):
            raise Fail("--push %s: DEST must stay inside the job's working directory" % item)
        st.setdefault("push", []).append([src, d, os.path.isdir(src)])
    for item in a.pull:
        src, _, d = item.partition(":")
        st.setdefault("pull", []).append([src, os.path.abspath(os.path.expanduser(d or "results/{name}/{id}"))])
    if a.pull:
        st["pull_on"] = [x.strip() for x in a.pull_on.split(",") if x.strip()]
    if a.keep_work and st:
        st["keep"] = True
    return st, cwd


def submit(db, argv, stage_now=False):
    """Queue a job. stage_now (the cli, with -H): copy its files before it is queued, so a failure is reported
    here and nothing is left behind; otherwise the server stages it before delivery."""
    a = submit_parser(prog="submit").parse_args(argv) if isinstance(argv, list) else argv
    cmd = a.cmd[1:] if a.cmd[:1] == ["--"] else a.cmd
    if not cmd:
        raise Fail("no command given")
    if a.stall_on_log and not a.stall:
        raise Fail("--stall-on-log needs --stall DURATION")
    jid = uuid.uuid4().hex[:8]
    st, cwd = stage_spec(a, jid)
    spec = dict(name=a.name, cmd=cmd[0] if len(cmd) == 1 else shlex.join(cmd), cwd=cwd, env=dict(e.split("=", 1) for e in a.env),
                prio=a.prio, cpus=a.cpus, mem_mb=a.mem, timeout=a.timeout or 2 * a.expect, retries=a.retries, retry_on=a.retry_on,
                retry_delay=a.retry_delay, expect=a.expect, stall=a.stall, stall_on_log=int(a.stall_on_log),
                trace_lines=int(a.trace_lines), work=int(bool(st.get("git") or st.get("push"))), stage=st)
    staged = None
    if stage_now and a.host and needs_stage(st, a.host):
        h = db.execute("SELECT dest FROM hosts WHERE name=?", (a.host,)).fetchone()
        if not h:
            raise Fail("unknown host %r (see: tshed hosts)" % a.host)
        print("staging files on %s…" % a.host, file=sys.stderr, flush=True)
        try:
            stage_job(h["dest"], jid, st, cwd)
        except Exception as e:
            raise Fail("staging on %s failed, job not queued: %s" % (a.host, e))
        staged = a.host
    return add_job(db, spec, a.host, jid, staged)


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


def waiting_job(db, jid, what):
    r = db.execute("SELECT * FROM jobs WHERE id=?", (jid,)).fetchone()
    if r["state"] not in ("pending", "queued"):
        raise Fail("%s is %s: only waiting jobs can be %s" % (jid, r["state"], what))
    return r


def host_answer(res, key, host):
    if res is None:
        raise Fail("host %s is unreachable; nothing changed" % host)
    if key not in res:
        raise Fail("host %s runs an older tinyshed: run tshed install %s" % (host, host))
    return res[key]


def move_job(db, jid, host):
    """Re-pin a waiting job to another host (None: any host). A job already queued on a host is withdrawn
    from it first; that is conditional on it still being queued there, so it never runs twice."""
    r = waiting_job(db, jid, "moved")
    if host and not db.execute("SELECT 1 FROM hosts WHERE name=?", (host,)).fetchone():
        raise Fail("unknown host %r (see: tshed hosts)" % host)
    if r["host"] == host:
        return
    if r["sent"]:
        state = host_answer(sync_host(r["host"], dict(withdraw=[jid])), "withdrawn", r["host"]).get(jid)
        if state not in ("moved", None):
            raise Fail("%s is already %s on %s; not moved" % (jid, state, r["host"]))
        r = db.execute("SELECT * FROM jobs WHERE id=?", (jid,)).fetchone()
    # eid=0: event ids are per host; the old host's late events no longer match (ingest checks the host)
    db.execute("UPDATE jobs SET host=?, sent=0, state='pending', eid=0, cancel=0 WHERE id=?", (host, jid))


def force_job(db, jid):
    """Start a waiting job now: it skips the queue order and takes cores even if they are busy.
    A host-less job goes to the live host with the most spare cores. Returns the job's state on its host."""
    r = waiting_job(db, jid, "forced")
    host = r["host"]
    if not host:
        cap, _ = host_caps(db)
        if not cap:
            raise Fail("no live host to run %s on" % jid)
        host = max(cap, key=cap.get)
        db.execute("UPDATE jobs SET host=? WHERE id=? AND host IS NULL", (host, jid))
    # one round trip: an undelivered job is submitted, then forced, in the same rpc
    return host, host_answer(sync_host(host, dict(force=[jid])), "forced", host).get(jid)


def edit_job(db, jid, prio=None, host=False, force=False):
    """`tshed edit`: move (host; None = any), then re-prioritise, then force. Returns what changed."""
    done = []
    if host is not False:
        move_job(db, jid, host)
        done.append("host %s" % (host or "(any)"))
    if prio is not None:
        waiting_job(db, jid, "re-prioritised")
        set_prio(db, jid, prio)
        done.append("prio %d" % prio)
    if force:
        h, state = force_job(db, jid)
        done.append("forced on %s (%s)" % (h, "starting" if state == "queued" else state))
    elif host:
        sync_host(host)  # deliver now rather than at the next poll
    return done


def pause_jobs(db, ids, resume=False):
    """Pause (SIGSTOP, cores released) or resume running jobs, one rpc per host. Returns {id: state or error}."""
    op, out, by_host = "resume" if resume else "pause", {}, {}
    for jid in ids:
        r = db.execute("SELECT host, state, sent FROM jobs WHERE id=?", (jid,)).fetchone()
        if r["state"] not in ("running", "paused") or not r["sent"]:
            out[jid] = "%s: only running or paused jobs can be %sd" % (r["state"], op)
        else:
            by_host.setdefault(r["host"], []).append(jid)

    def one(host):
        try:
            got = host_answer(sync_host(host, {op: by_host[host]}), op + "d", host)
            out.update({jid: got.get(jid) or "unknown on %s" % host for jid in by_host[host]})
        except Fail as e:
            out.update({jid: str(e) for jid in by_host[host]})
    par(one, list(by_host))
    return out


def job_stats(db, names=None, since=0):
    """Per job name, from the server's mirror: runs, outcomes, and the runtime spread of successful runs."""
    groups = {}
    for r in db.execute("SELECT id, state, created, j FROM jobs WHERE created>=?", (since,)):
        b = _blob(r)
        if not names or b.get("name") in names:
            groups.setdefault(b.get("name") or "-", []).append(dict(b, state=r["state"], created=r["created"]))
    out: list[dict] = []
    for name, js in groups.items():
        times = sorted(j["ended"] - j["started"] - (j.get("paused_s") or 0) for j in js if j["state"] == "done" and j.get("ended") and j.get("started"))
        pick = lambda q: times[min(len(times) - 1, int(q * len(times)))] if times else None
        fails = sum(j["state"] in FAILURES for j in js)
        out.append(dict(name=name, runs=len(js), done=len(times), failed=fails, cancelled=sum(j["state"] == "cancelled" for j in js),
                        active=sum(j["state"] not in TERMINAL for j in js), fail_pct=round(100 * fails / max(1, fails + len(times))),
                        median=pick(0.5), p90=pick(0.9), max=times[-1] if times else None,
                        expect=next((j.get("expect") for j in sorted(js, key=lambda j: -j["created"]) if j.get("expect")), None),
                        peak_mb=max((j.get("peak_mb") or 0 for j in js), default=0) or None, last=max(j["created"] for j in js),
                        states=sorted({j["state"] for j in js if j["state"] in FAILURES})))
    return sorted(out, key=lambda g: -g["last"])


def resolve(db, prefix):
    """A job id, from a unique id prefix or else a job name (the unfinished job of that name wins)."""
    rows = db.execute("SELECT id FROM jobs WHERE id LIKE ?", (prefix + "%",)).fetchall()
    if not rows:
        named = [r for r in db.execute("SELECT id, state, j FROM jobs").fetchall() if _blob(r).get("name") == prefix]
        rows = [r for r in named if r["state"] not in TERMINAL] or named
        if len(rows) > 1:
            raise Fail("%s: %d jobs have this name; use an id (%s)" % (prefix, len(rows), " ".join(r[0] for r in rows[:6])))
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

    targets: list = []
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
    if not r.get("git", True):
        notes.append("no git: --git unavailable")
    if not r.get("rsync", True):
        notes.append("no rsync: --push/--pull unavailable")
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
                so = p.stdout
                assert so  # stdout=PIPE
                port = int(so.readline())
                conf = subprocess.run(["ssh", "-G", d], capture_output=True, text=True, timeout=10).stdout
                addr = next(l.split()[1] for l in conf.splitlines() if l.startswith("hostname "))
                for info in socket.getaddrinfo(addr, port, type=socket.SOCK_DGRAM):  # every address family the name has
                    with contextlib.suppress(OSError), socket.socket(info[0], socket.SOCK_DGRAM) as sock:
                        sock.sendto(b"tshed", info[4])
                _mosh[d] = so.readline().strip() == "ok"
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

# ── themes. Every color below is a role; apply_theme() rebinds them. 'default' is the 16-color ANSI palette
# (it follows the terminal's own scheme, after gitss); 'mono' is bold/dim/reverse only; the rest are truecolor
# palettes of the usual ratatui / editor themes (downsampled to 256 colors where the terminal lacks truecolor).
# Any btop theme also works, by name (looked up in btop's theme dirs, or `btop:NAME`) or by path to a .theme file.
THEME_ROLES = ("ok", "info", "err", "warn", "cancel", "accent", "host", "key", "dim", "sel", "title", "fg", "bg")
THEMES = {  # ok     info   err    warn   cancel accent host   key    dim    sel    title  fg     bg
    "btop":             "50f095 4897d4 d45454 dcd179 ff40b6 5474e8 80d0a3 b54040 666666 6a2f2f eeeeee cccccc 000000",
    "cendre":           "99af6b 20c9cb d1766e fcba81 a692cd 58bdff 8bcfff ea9875 73665b 2f1e17 e6d5c2 e6d5c2 171311",
    "catppuccin":       "a6e3a1 89dceb f38ba8 f9e2af cba6f7 89b4fa 74c7ec fab387 6c7086 45475a cdd6f4 cdd6f4 1e1e2e",
    "catppuccin-latte": "40a02b 04a5e5 d20f39 df8e1d 8839ef 1e66f5 209fb5 fe640b 8c8fa1 bcc0cc 4c4f69 4c4f69 eff1f5",
    "dracula":          "50fa7b 8be9fd ff5555 f1fa8c bd93f9 ff79c6 8be9fd ffb86c 6272a4 44475a f8f8f2 f8f8f2 282a36",
    "everforest":       "a7c080 83c092 e67e80 dbbc7f d699b6 7fbbb3 83c092 e69875 859289 475258 d3c6aa d3c6aa 2d353b",
    "gruvbox":          "b8bb26 8ec07c fb4934 fabd2f d3869b 83a598 8ec07c fe8019 928374 504945 ebdbb2 ebdbb2 282828",
    "gruvbox-light":    "79740e 427b58 9d0006 b57614 8f3f71 076678 427b58 af3a03 928374 d5c4a1 3c3836 3c3836 fbf1c7",
    "kanagawa":         "98bb6c 7fb4ca e46876 e6c384 957fb8 7e9cd8 7aa89f ffa066 727169 2d4f67 dcd7ba dcd7ba 1f1f28",
    "monokai":          "a6e22e 66d9ef f92672 e6db74 ae81ff 66d9ef 66d9ef fd971f 75715e 49483e f8f8f2 f8f8f2 272822",
    "nord":             "a3be8c 88c0d0 bf616a ebcb8b b48ead 81a1c1 8fbcbb d08770 616e88 434c5e eceff4 d8dee9 2e3440",
    "one-dark":         "98c379 56b6c2 e06c75 e5c07b c678dd 61afef 56b6c2 d19a66 5c6370 3e4451 abb2bf abb2bf 282c34",
    "rose-pine":        "9ccfd8 ebbcba eb6f92 f6c177 c4a7e7 31748f 9ccfd8 f6c177 6e6a86 403d52 e0def4 e0def4 191724",
    "solarized-dark":   "859900 2aa198 dc322f b58900 d33682 268bd2 2aa198 cb4b16 586e75 073642 93a1a1 839496 002b36",
    "solarized-light":  "859900 2aa198 dc322f b58900 d33682 268bd2 2aa198 cb4b16 93a1a1 eee8d5 586e75 657b83 fdf6e3",
    "tokyo-night":      "9ece6a 7dcfff f7768e e0af68 bb9af7 7aa2f7 2ac3de ff9e64 565f89 283457 c0caf5 c0caf5 1a1b26",
}
# ratatui's own look (its table demo): the tailwind palette it ships in style::palette::tailwind. slate-950
# buffer, slate-900 alternate rows, slate-200 text, a c900 header bar and a reversed c400 selection bar.
THEME_EXTRA = {}  # name -> optional roles: head (header bar bg), alt (alternate row bg), sel_fg (flat selected row)
for _n, _c in dict(blue="60a5fa 1e3a8a", emerald="34d399 064e3b", indigo="818cf8 312e81", red="f87171 7f1d1d").items():
    _c400, _c900 = _c.split()  # ok=green info=cyan err=red warn=amber cancel=purple (all c400), dim=slate-500
    THEMES["tailwind-" + _n] = "4ade80 22d3ee f87171 fbbf24 c084fc %s %s %s 64748b %s e2e8f0 e2e8f0 020617" % ((_c400,) * 4)
    THEME_EXTRA["tailwind-" + _n] = dict(head=_c900, alt="0f172a", sel_fg="020617")
BTOP_DIRS = ("~/.config/btop/themes", "~/.local/share/btop/themes", "/opt/homebrew/share/btop/themes",
             "/usr/local/share/btop/themes", "/usr/share/btop/themes")


def btop_dirs():
    dirs = [os.path.join(os.environ["XDG_CONFIG_HOME"], "btop", "themes")] if os.environ.get("XDG_CONFIG_HOME") else []
    btop = shutil.which("btop")
    if btop:  # <prefix>/bin/btop -> <prefix>/share/btop/themes
        dirs.append(os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(btop))), "share", "btop", "themes"))
    return [d for d in dict.fromkeys(dirs + [os.path.expanduser(d) for d in BTOP_DIRS]) if os.path.isdir(d)]


def btop_themes():
    """{name: path} of installed btop themes (the first dir wins, so user themes shadow system ones)."""
    out = {}
    for d in btop_dirs():
        for f in sorted(os.listdir(d)):
            if f.endswith(".theme"):
                out.setdefault(f[:-6], os.path.join(d, f))
    return out


def theme_names():
    # btop-themes/tailwind-*.theme are btop's copies of the built-in tailwind themes: not listed twice
    return ["default", "mono"] + sorted(THEMES) + ["btop:" + n for n in btop_themes() if n not in THEME_EXTRA]


def parse_btop(path):
    """A btop .theme file -> roles. Its named colors map directly (title, hi_fg, selected_bg, inactive_fg,
    main_fg/bg); the semantic ones (ok/err/warn/...) take the theme's color nearest in hue."""
    import colorsys
    with open(path) as f:
        kv = dict(re.findall(r'^\s*theme\[(\w+)\]\s*=\s*"?([^"\n]*)"?', f.read(), re.M))

    def hx(v):
        v = v.strip().lstrip("#")
        if re.fullmatch(r"[0-9a-fA-F]{2}", v):
            return v.lower() * 3  # btop "#cc" = grey
        if re.fullmatch(r"[0-9a-fA-F]{6}", v):
            return v.lower()
        p = v.split()
        return "%02x%02x%02x" % tuple(map(int, p)) if len(p) == 3 and all(x.isdigit() for x in p) else None
    cols = {k: c for k, c in ((k, hx(v)) for k, v in kv.items()) if c}

    def hue(target):
        """The theme's color nearest the target hue (vivid preferred); None (-> fg) if nothing is close."""
        best = None
        for c in dict.fromkeys(cols.values()):
            h, sat, val = colorsys.rgb_to_hsv(*(int(c[i:i + 2], 16) / 255 for i in (0, 2, 4)))
            if sat < 0.15 or val < 0.3:
                continue
            d = min(abs(h * 360 - target), 360 - abs(h * 360 - target)) - 30 * sat * val  # prefer vivid
            if best is None or d < best[0]:
                best = (d, c)
        return best[1] if best and best[0] < 60 else None
    fg = cols.get("main_fg") or "cccccc"
    t = dict(ok=hue(120), info=hue(185), err=hue(0), warn=hue(50), cancel=hue(300), accent=hue(220),
             key=cols.get("hi_fg"), dim=cols.get("inactive_fg"), sel=cols.get("selected_bg"),
             sel_fg=cols.get("selected_fg"), title=cols.get("title"), fg=fg, bg=cols.get("main_bg"))
    t["host"] = t["info"] or t["accent"]
    return {k: v or (fg if k not in ("sel_fg", "bg", "sel") else None) for k, v in t.items()}


def _rgb(c):
    return [int(c[i:i + 2], 16) / 255 for i in (0, 2, 4)]


def _hex(v):
    return "%02x%02x%02x" % tuple(round(min(1, max(0, x)) * 255) for x in v)


def _lum(c):  # WCAG relative luminance
    v = [x / 12.92 if x <= 0.03928 else ((x + 0.055) / 1.055) ** 2.4 for x in _rgb(c)]
    return 0.2126 * v[0] + 0.7152 * v[1] + 0.0722 * v[2]


def contrast(a, b):
    hi, lo = sorted((_lum(a), _lum(b)), reverse=True)
    return (hi + 0.05) / (lo + 0.05)


def _mix(a, b, f):
    return _hex([x * f + y * (1 - f) for x, y in zip(_rgb(a), _rgb(b))])


# minimum WCAG contrast of each role (against the background, against the selection bar)
CONTRAST = dict(fg=(7, 4.5), title=(7, 4.5), dim=(3.2, 2.5))
CONTRAST_TEXT = (4.5, 3.5)


def tune_contrast(t):
    """Make a palette readable without changing its character: the selection bar becomes a tinted lift of
    the background (~1.8:1), and a text role that is too faint keeps its hue but moves away from the
    background (lighter on dark themes, darker on light ones) until it meets CONTRAST against both the
    background and the selection bar. Colors that already pass are left exactly as designed."""
    import colorsys
    t = dict(t)
    bg = t.get("bg") or ("000000" if _lum(t["fg"]) > 0.2 else "ffffff")  # no bg: assume the opposite of fg
    step = 0.02 if _lum(bg) < 0.18 else -0.02
    base = t.get("sel") if t.get("sel") and contrast(t["sel"], bg) >= 1.3 else t["fg"]
    if not 1.6 <= contrast(base, bg) <= 2.4:  # too faint to see, or a loud bar that fights the text on it
        # faint: lift it toward fg (keeps its tint); loud: fade it toward bg
        toward = (lambda f: _mix(t["fg"], base, f)) if contrast(base, bg) < 1.6 else (lambda f: _mix(base, bg, f))
        f = 0.0
        while f < 1 and contrast(toward(f), bg) < 1.8:
            f += 0.02
        base = toward(f)
    t["sel"], sel = base, base
    t.pop("sel_fg", None)  # roles are tuned against the bar itself

    def push(c, need_bg, need_sel):
        h, l, s_ = colorsys.rgb_to_hls(*_rgb(c))
        while (contrast(c, bg) < need_bg or contrast(c, sel) < need_sel) and 0 < l < 1:
            l = min(1.0, max(0.0, l + step))
            c = _hex(colorsys.hls_to_rgb(h, l, s_))
        return c
    for role in THEME_ROLES:
        if role in ("bg", "sel") or not t.get(role):
            continue
        t[role] = push(t[role], *CONTRAST.get(role, CONTRAST_TEXT))
    if contrast(t["dim"], bg) > contrast(t["fg"], bg) * 0.8:  # dim must still read as dim
        t["dim"] = _mix(t["fg"], bg, 0.55)
        t["dim"] = push(t["dim"], *CONTRAST["dim"])
    return t


CONTRAST_DEFAULT, CONTRAST_MAX = 4.0, 10.0


def current_contrast():
    """The contrast level, 0-10: how hard boost() stretches a palette (darker darks, lighter lights).
    0 = every theme exactly as published. TINYSHED_CONTRAST overrides the saved level."""
    v = os.environ.get("TINYSHED_CONTRAST")
    if v is None:
        try:
            with open(P("contrast")) as f:
                v = f.read()
        except OSError:
            return CONTRAST_DEFAULT
    try:
        return min(CONTRAST_MAX, max(0.0, float(v)))
    except ValueError:
        return CONTRAST_DEFAULT


def save_contrast(n):
    n = min(CONTRAST_MAX, max(0.0, float(n)))
    with open(P("contrast"), "w") as f:
        f.write("%g\n" % n)
    os.environ.pop("TINYSHED_CONTRAST", None)
    return n


def boost(t, n):
    """Contrast level n as a tone curve over the whole palette, backgrounds included: darks get darker and
    lights get lighter (an S-curve on lightness around the middle, exponent 1 + 0.12n), hues stay, and the
    state/accent colors gain 5% saturation per level so they get more vivid instead of washing out to
    white. The curve never clips and keeps the order of shades (background < striped row < bar). Text
    that still ends up faint is lifted to WCAG 4.5 against the background (dim text: 3)."""
    import colorsys
    if not n:
        return t
    t = dict(t)
    g = 1 + 0.12 * n
    for role in (*THEME_ROLES, "alt", "head", "sel_fg"):
        if not t.get(role):
            continue
        h, l, s_ = colorsys.rgb_to_hls(*_rgb(t[role]))
        l = 0.5 * (2 * l) ** g if l < 0.5 else 1 - 0.5 * (2 * (1 - l)) ** g
        if role in ("ok", "info", "err", "warn", "cancel", "accent", "host", "key"):
            s_ = min(1.0, s_ * (1 + 0.05 * n))
        t[role] = _hex(colorsys.hls_to_rgb(h, l, s_))
    bg = t.get("bg") or ("000000" if _lum(t["fg"]) > 0.2 else "ffffff")
    step = 0.01 if _lum(bg) < 0.18 else -0.01
    grounds = [bg] + ([t["alt"]] if t.get("alt") else [])
    for role in THEME_ROLES:
        if role in ("bg", "sel") or not t.get(role):
            continue
        c, need = t[role], 3 if role == "dim" else 4.5
        h, l, s_ = colorsys.rgb_to_hls(*_rgb(c))
        while any(contrast(c, x) < need for x in grounds) and 0 < l < 1:
            l = min(1.0, max(0.0, l + step))
            c = _hex(colorsys.hls_to_rgb(h, l, s_))
        t[role] = c
    return t


def load_theme(name):
    """Theme name/path -> roles dict, or 'default' / 'mono'. Raises Fail for unknown names. Built-in palettes
    are used exactly as published; btop themes are contrast-tuned (their semantic roles are guessed by hue)."""
    if name in ("default", "mono"):
        return name
    if name in THEMES:
        return dict(zip(THEME_ROLES, THEMES[name].split()), **THEME_EXTRA.get(name, {}))
    if os.path.isfile(os.path.expanduser(name)):
        return tune_contrast(parse_btop(os.path.expanduser(name)))
    path = btop_themes().get(name.removeprefix("btop:"))
    if path:
        return tune_contrast(parse_btop(path))
    raise Fail("unknown theme %r; `tshed theme` lists them" % name)


def truecolor():
    """24-bit color, like btop's truecolor option: TINYSHED_TRUECOLOR=1/0 forces it, else detect the terminal."""
    force = os.environ.get("TINYSHED_TRUECOLOR")
    if force in ("0", "1"):
        return force == "1"
    t = os.environ.get("TERM", "")
    return (os.environ.get("COLORTERM") in ("truecolor", "24bit") or "direct" in t or "kitty" in t or "ghostty" in t
            or os.environ.get("TERM_PROGRAM") in ("iTerm.app", "WezTerm", "ghostty", "vscode", "Hyper", "rio"))


def sgr_color(hexc, bg=False):
    r, g, b = (int(hexc[i:i + 2], 16) for i in (0, 2, 4))
    if truecolor():
        return "%d;2;%d;%d;%d" % (48 if bg else 38, r, g, b)
    lv = (0, 95, 135, 175, 215, 255)  # xterm-256: 6x6x6 cube or the 24-step grey ramp, whichever is nearer
    q = [min(range(6), key=lambda i: abs(lv[i] - v)) for v in (r, g, b)]
    gi = min(23, max(0, round(((r + g + b) / 3 - 8) / 10)))
    dist = lambda c: sum((x - y) ** 2 for x, y in zip(c, (r, g, b)))
    n = 16 + 36 * q[0] + 6 * q[1] + q[2] if dist([lv[i] for i in q]) <= dist([8 + 10 * gi] * 3) else 232 + gi
    return "%d;5;%d" % (48 if bg else 38, n)


def palette(t, paint_bg=False):
    """Roles -> the escape sequence of every style constant. HEAD styles a table header (a full-width bar
    when HEAD_BAR), ALT is the background of every other row, SEL_FLAT draws the selected row in one color."""
    d = _palette(t, paint_bg)
    for k, v in dict(HEAD=d["BOLD"], HEAD_BAR=False, ALT="", SEL_FLAT=False).items():
        d.setdefault(k, v)
    return d


def _palette(t, paint_bg):
    if t == "default":
        return dict(RESET="\x1b[0m", DIM="\x1b[2m", BOLD="\x1b[1m", GREEN="\x1b[32m", CYAN="\x1b[36m", RED="\x1b[31m",
                    MAGENTA="\x1b[35m", BLUE="\x1b[34m", BOLD_YELLOW="\x1b[1;33m", BOLD_CYAN="\x1b[1;36m",
                    BOLD_RED="\x1b[1;31m", KEY="\x1b[1;33m", HILITE="\x1b[48;5;237;1m",
                    YELLOW="\x1b[33m", HOST="\x1b[36m", NUM="\x1b[33m")
    if t == "mono":
        b = "\x1b[1m"
        return dict(RESET="\x1b[0m", DIM="\x1b[2m", BOLD=b, GREEN="", CYAN="", RED=b, MAGENTA="", BLUE="",
                    BOLD_YELLOW=b, BOLD_CYAN=b, BOLD_RED=b, KEY=b, HILITE="\x1b[7m",
                    YELLOW="", HOST="", NUM="")
    c = lambda role, bold=False: "\x1b[%s%sm" % ("1;" if bold else "", sgr_color(t[role]))
    base = ""
    if paint_bg and t.get("bg"):
        base = "\x1b[%s;%sm" % (sgr_color(t["fg"]), sgr_color(t["bg"], bg=True))
    sel = "\x1b[%s%sm" % (sgr_color(t["sel"], bg=True), ";" + sgr_color(t["sel_fg"]) if t.get("sel_fg") else "") \
        if t.get("sel") else "\x1b[7m"
    extra = {}
    if paint_bg and t.get("head"):
        extra.update(HEAD="\x1b[1;%s;%sm" % (sgr_color(t["title"]), sgr_color(t["head"], bg=True)), HEAD_BAR=True)
    if paint_bg and t.get("alt"):
        extra["ALT"] = "\x1b[%sm" % sgr_color(t["alt"], bg=True)
    return dict(extra, SEL_FLAT=bool(t.get("sel_fg")), RESET="\x1b[0m" + base, DIM=c("dim"), BOLD=c("title", True), GREEN=c("ok"), CYAN=c("info"),
                RED=c("err"), MAGENTA=c("cancel"), BLUE=c("accent"), BOLD_YELLOW=c("warn", True),
                BOLD_CYAN=c("host", True), BOLD_RED=c("err", True), KEY=c("key", True), HILITE=sel,
                YELLOW=c("warn"), HOST=c("host"), NUM=c("key"))


def saved_theme():
    try:
        with open(P("theme")) as f:
            return f.read().strip() or "default"
    except OSError:
        return "default"


def current_theme():
    return os.environ.get("TINYSHED_THEME") or saved_theme()


def apply_theme(name=None, paint_bg=False):
    """Rebind the style constants. A broken saved theme (e.g. btop uninstalled) falls back to default."""
    global STATE_STYLE
    try:
        t = load_theme(name or current_theme())
    except (Fail, OSError, ValueError):
        t = "default"
    if isinstance(t, dict):
        t = boost(t, current_contrast())
        t.update(current_scheme())
    pal = palette(t, paint_bg)
    if not isinstance(t, dict):  # default/mono: plain terminal colors; scheme overrides the style constants
        for role, hexc in current_scheme().items():
            for k in ROLE_STYLES.get(role, ()):
                if k in pal:
                    pal[k] = "\x1b[%sm" % sgr_color(hexc)
            if role == "sel" and pal["HILITE"] not in ("", "\x1b[7m"):
                pal["HILITE"] = "\x1b[%s;1m" % sgr_color(hexc, bg=True)
    globals().update(pal)
    STATE_STYLE = dict(running=(GREEN, "▶"), queued=(YELLOW, "◌"), pending=(CYAN, "◌"), done=(GREEN, "✓"),
                       cancelled=(MAGENTA, "■"), moved=(CYAN, "→"), paused=(YELLOW, "‖"))  # every failure: (BOLD_RED, "●")


def theme_cmd(name, contrast=None):
    if contrast is not None:
        print("contrast %g" % save_contrast(contrast))
        if not name:
            return
    if name:
        load_theme(name)  # validate
        if name in ("default", "mono") or name in THEMES or name.startswith("btop:"):
            pass
        elif os.path.isfile(os.path.expanduser(name)):
            name = os.path.abspath(os.path.expanduser(name))
        else:
            name = "btop:" + name
        os.makedirs(HOME, exist_ok=True)
        with open(P("theme"), "w") as f:
            f.write(name + "\n")
        print("theme", name)
        if os.environ.get("TINYSHED_THEME"):
            print("note: TINYSHED_THEME=%s overrides it in this shell" % os.environ["TINYSHED_THEME"], file=sys.stderr)
        return
    cur = current_theme()
    print("colors: %s (TINYSHED_TRUECOLOR=1/0 to force)" % ("24-bit truecolor" if truecolor() else "256-color approximation"))
    print("contrast: %g (tshed theme --contrast N, 0-%g: darker darks, lighter lights, more vivid colors; 0 = exactly as published; not for default/mono)" % (current_contrast(), CONTRAST_MAX))
    for n in theme_names():
        apply_theme(n)
        mark = "*" if n == cur else " "
        out([("", "%s %-26s " % (mark, n)), (GREEN, "▶ running "), (YELLOW, "◌ queued "), (CYAN, "◌ pending "),
             (BOLD_RED, "● error "), (MAGENTA, "■ cancelled "), (HOST, "host "), (KEY, "[K]"), ("", " "), (DIM, "dim "),
             (HILITE, " selected ")])
    apply_theme()


# ── color schemes: user overrides of individual roles, layered onto any theme (~/.tinyshed/scheme,
# one `role color` per line; TINYSHED_SCHEME="err=ff0000 ..." overrides). The 'default' and 'mono'
# themes are plain terminal colors, so schemes there cover the text roles only.
SCHEME_ROLES = THEME_ROLES + ("alt", "head", "sel_fg")
SCHEMES = {  # named presets, applied wholesale on top of any theme; tuned for dark palettes
    "neon":       dict(ok="39ff14", info="00e5ff", err="ff2a6d", warn="ffb400", cancel="d75cff",
                       accent="17e9e0", host="ff7edb", key="f8f8f2", sel="2b1d3a", sel_fg="f8f8f2"),
    "pastel":     dict(ok="a6e3a1", info="89dceb", err="f38ba8", warn="f9e2af", cancel="cba6f7",
                       accent="89b4fa", host="f5c2e7", key="cdd6f4", sel="45475a", sel_fg="cdd6f4"),
    "solarized":  dict(ok="859900", info="2aa198", err="dc322f", warn="b58900", cancel="d33682",
                       accent="268bd2", host="6c71c4", key="586e75", sel="eee8d5", sel_fg="586e75"),
    "hicon":      dict(ok="00ff00", info="00ffff", err="ff0000", warn="ffff00", cancel="ff00ff",
                       accent="4d4dff", host="ffffff", key="ffffff", sel="808080", sel_fg="ffffff"),
    "colorblind": dict(ok="0072b2", info="56b4e9", err="d55e00", warn="f0e442", cancel="cc79a7",
                       accent="009e73", host="e69f00", key="cccccc", sel="dddddd", sel_fg="000000"),
}
NAMED_COLORS = dict(black="000000", red="cc0000", green="00cc00", yellow="c4a000", blue="3465a4",
                    magenta="a45aa4", cyan="06989a", white="cccccc", gray="808080", grey="808080",
                    bright_black="555555", bright_red="ef2929", bright_green="8ae234", bright_yellow="fce94f",
                    bright_blue="729fcf", bright_magenta="ad7fa8", bright_cyan="34e2e2", bright_white="eeeeee")
ROLE_STYLES = dict(ok=("GREEN",), info=("CYAN",), err=("RED", "BOLD_RED"), warn=("YELLOW", "BOLD_YELLOW"),
                   cancel=("MAGENTA",), accent=("BLUE",), host=("HOST", "BOLD_CYAN"), key=("KEY", "NUM"),
                   dim=("DIM",), title=("BOLD",))


def parse_color(s):
    """'ff8800' / '#ff8800' or a basic ANSI color name -> hex. 'default' means no override."""
    c = s.lstrip("#").lower()
    if c == "default":
        return None
    if c in NAMED_COLORS:
        return NAMED_COLORS[c]
    if len(c) == 6 and all(ch in "0123456789abcdef" for ch in c):
        return c
    raise Fail("bad color %r: use 6 hex digits or a name like %s" % (s, "/".join(sorted(NAMED_COLORS)[:4])))


def scheme_dict(env):
    d = {}
    for kv in env.split():
        k, _, v = kv.partition("=")
        if k not in SCHEME_ROLES:
            raise Fail("unknown role %r; scheme roles: %s" % (k, " ".join(SCHEME_ROLES)))
        c = parse_color(v)
        if c:
            d[k] = c
    return d


def current_scheme():
    """role -> hex overrides on top of the theme. TINYSHED_SCHEME overrides the saved file."""
    env = os.environ.get("TINYSHED_SCHEME")
    if env is not None:
        return scheme_dict(env)
    d = {}
    try:
        with open(P("scheme")) as f:
            for line in f:
                w = line.split()
                if len(w) == 2 and w[0] in SCHEME_ROLES:
                    d[w[0]] = w[1]
    except OSError:
        pass
    return d


def save_scheme(d):
    os.makedirs(HOME, exist_ok=True)
    with open(P("scheme"), "w") as f:
        for k in SCHEME_ROLES:
            if k in d:
                f.write("%s %s\n" % (k, d[k]))


def scheme_cmd(a):
    sch = current_scheme()
    if a.clear:
        try:
            os.remove(P("scheme"))
        except OSError:
            pass
        sch = {}
    pairs, names = [p for p in a.pairs if "=" in p], [p for p in a.pairs if "=" not in p]
    for n in names:  # bare tokens: named presets, applied wholesale
        if n not in SCHEMES:
            raise Fail("unknown scheme %r; `tshed scheme` lists them (%s)" % (n, " ".join(SCHEMES)))
        sch = dict(SCHEMES[n])
    for kv in pairs:
        k, _, v = kv.partition("=")
        if k not in SCHEME_ROLES:
            raise Fail("unknown role %r; scheme roles: %s" % (k, " ".join(SCHEME_ROLES)))
        c = parse_color(v)
        if c:
            sch[k] = c
        else:
            sch.pop(k, None)
    if a.pairs or a.clear:
        save_scheme(sch)
        os.environ.pop("TINYSHED_SCHEME", None)
        apply_theme()
    if os.environ.get("TINYSHED_SCHEME"):
        print("note: TINYSHED_SCHEME=%s overrides the saved scheme in this shell" % os.environ["TINYSHED_SCHEME"], file=sys.stderr)
    styles = dict(ok=GREEN, info=CYAN, err=RED, warn=YELLOW, cancel=MAGENTA, accent=BLUE, host=HOST, key=KEY,
                  dim=DIM, title=BOLD, sel=HILITE, alt=ALT, head=HEAD, sel_fg=HILITE if SEL_FLAT else "")
    active = None if os.environ.get("TINYSHED_SCHEME") else next(
        (n for n, d in SCHEMES.items() if d == sch), None)
    print("scheme: %d override(s) on top of theme %s%s" % (len(sch), current_theme(),
                                                           " (%s)" % active if active else ""))
    for r in SCHEME_ROLES:
        st = styles.get(r, "")
        if r in sch:
            print("* %-8s %s%-16s%s" % (r, st, sch[r] + " sample", RESET) if st
                  else "* %-8s %s" % (r, sch[r]))
        elif not (a.pairs or a.clear) and st:
            print("  %-8s %s%-16s%s  (theme default)" % (r, st, "sample", RESET))
    print("presets:")
    for n, d in SCHEMES.items():
        mark = "*" if n == active else " "
        out([("", "%s %-10s " % (mark, n))] +
            [(styles.get(r, ""), t + " ") for r, t in (("ok", "▶ run"), ("err", "● err"), ("warn", "◌ wait"),
                                                       ("cancel", "■ can"), ("accent", "◆ accent"), ("host", "host"))])
    if not sch and not a.pairs and not a.clear:
        print("(no overrides; `tshed scheme <preset>` picks one, ROLE=COLOR tweaks it)")


# the style constants rebound by apply_theme(); declared so the type checker sees them
RESET = DIM = BOLD = GREEN = CYAN = RED = MAGENTA = BLUE = BOLD_YELLOW = BOLD_CYAN = BOLD_RED = KEY = HILITE = \
    YELLOW = HOST = NUM = HEAD = ALT = ""
HEAD_BAR = SEL_FLAT = False
STATE_STYLE: dict = {}
apply_theme("default")
HOST_HEAD = "%-12s  %-7s %5s %5s  %7s  %10s  %9s  %-8s %4s %4s  %6s  %6s  %s" % ("Host", "State", "Load", "CPU%", "Cores", "Mem free", "Used/Res", "GPU", "Run", "Que", "Seen", "Freq", "Notes")
JOB_HEAD = "%-11s %3s %4s  %-12s  %-8s  %9s  %9s  %5s  %4s  %11s  %-16s  %s" % ("State", "CPU", "Prio", "Host", "ID", "Time", "Progress", "Try", "CPU%", "Mem", "Name", "Command")


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


def fmt_dur(s, q=5):
    s = int(max(0, s or 0)) // q * q  # quantize to 5s so displays advance every 5s; limits pass q=1
    if s < 60:
        return "%ds" % s
    if s < 3600:
        return "%dm %02ds" % (s // 60, s % 60)
    if s < 86400:
        return "%dh %02dm" % (s // 3600, s % 3600 // 60)
    return "%dd %02dh" % (s // 86400, s % 86400 // 3600)


def fmt_short(s):
    """A compact duration for narrow columns: 45s 14m 3h05 2d04."""
    s = int(max(0, s))
    return "%ds" % s if s < 60 else "%dm" % (s // 60) if s < 3600 else "%dh%02d" % (s // 3600, s % 3600 // 60) if s < 86400 \
        else "%dd%02d" % (s // 86400, s % 86400 // 3600)


def job_progress(j):
    """(fraction done, seconds left) from the job's last checkpoint; either may be None.
    The ETA extrapolates the rate up to that checkpoint (host clock throughout)."""
    done, total, _ = parse_progress(j.get("progress"))
    if done is None:
        return None, None
    f, left = min(1.0, done / total), None
    started, beat = j.get("started"), j.get("beat")
    active = (beat or 0) - (started or 0) - (j.get("paused_s") or 0)  # running time up to the checkpoint (pauses before it, roughly)
    if j["state"] == "running" and 0 < f and started and beat and active > 0:
        left = max(0.0, active * (1 - f) / f - (j["now"] - beat))
    return f, left


def clean(s):
    s = (s or "").replace("\t", "    ")
    return "".join(c if c.isprintable() else " " for c in s)


def load_hosts(db):
    return [dict(r, s=json.loads(r["status"] or "{}")) for r in db.execute("SELECT * FROM hosts ORDER BY name")]


_jcache = {}  # job id -> (stored blob, decoded payload)


def _blob(r):
    if _jcache.get(r["id"], (None,))[0] != r["j"]:
        _jcache[r["id"]] = (r["j"], unpack(r["j"]))
    return _jcache[r["id"]][1]


def load_jobs(db, hosts):
    skew = {h["name"]: h["s"].get("skew", 0) for h in hosts}
    live = {h["name"]: h["s"].get("jobs") or {} for h in hosts}
    out = []
    for r in db.execute("SELECT * FROM jobs"):
        j = dict(_blob(r))
        j.update(id=r["id"], host=r["host"], state=r["state"], prio=r["prio"], sent=r["sent"], cancel=r["cancel"],
                 stage=json.loads(r["stage"]) if r["stage"] else None)
        # durations: a run counts from this attempt's start, minus time spent paused; a waiting job shows how
        # long it has been waiting (since submit, or since its previous attempt ended), rendered in brackets
        j["now"] = now = time.time() + skew.get(r["host"], 0)  # the host's clock, like started/ended/beat
        started, paused_s = j.get("started"), j.get("paused_s") or 0
        if j["state"] in ("running", "paused"):
            j["t"] = (j.get("paused_at") or now if j["state"] == "paused" else now) - (started or now) - paused_s
            j["cpu_pct"], j["rss_mb"] = live.get(r["host"], {}).get(r["id"]) or (None, None)
        elif j["state"] in TERMINAL and started:
            j["t"] = (j.get("ended") or started) - started - paused_s
        elif j["state"] in TERMINAL:  # ended before it ever ran (cancelled while waiting)
            j["t"], j["waiting"] = (j.get("ended") or j["created"]) - j["created"], True
        else:
            j["t"], j["waiting"] = (now - j["ended"] if j.get("attempt") and j.get("ended") else time.time() - j["created"]), True
        out.append(j)

    def rank(j):  # running, then waiting by priority, then finished most-recent-first
        if j["state"] in TERMINAL:
            return (2, -(j.get("ended") or j["created"]), 0)
        return (0 if j["state"] in ("running", "paused") else 1, -j["prio"], j["created"])
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
    used, res = s.get("mem_used_mb"), s.get("mem_reserved_mb")
    job_mem = "-" if used is None else "%.1f/%.1fG" % (used / 1024, res / 1024) if res is not None else "%.1f/?G" % (used / 1024)
    over_mem = used is not None and res is not None and used > res
    hot_cpu = s.get("cpu_pct") is not None and s["cpu_pct"] > 100
    warnings = s.get("warnings") or []
    notes = h["err"] or "; ".join(n for n in (
        "agent down" if s and not s.get("agent") else "",
        "v%s, server is v%s: run tshed install" % (s.get("version"), VERSION) if s and s.get("version") != VERSION else "",
        "%.0fW" % s["power_w"] if s.get("power_w") is not None else "",
        *warnings) if n)
    return [(HOST, "%-12s" % h["name"][:12]), ("", "  "), (state[0], "%-7s" % state[1]),
            ("", " %5s" % ("%.1f" % s["load"] if s else "-")),
            (BOLD_RED if hot_cpu else "", " %5s  " % ("%d%%" % s["cpu_pct"] if s and s.get("cpu_pct") is not None else "-")),
            (NUM, "%7s" % ("%d/%d" % (s["free"], s["cores"]) if s else "-")),
            ("", "  %10s  " % mem), (BOLD_RED if over_mem else "", "%9s  " % job_mem),
            ("", "%-8s " % gpu), (GREEN if run else DIM, "%4d" % run), ("", " "), (YELLOW if que else DIM, "%4d" % que),
            (DIM, "  %6s  " % (fmt_dur(time.time() - h["seen"]) if h["seen"] else "never")),
            ("", "%6s  " % ("%.1fGHz" % s["cpu_ghz"] if s.get("cpu_ghz") is not None else "-")),
            (BOLD_RED if h["err"] else YELLOW, clean(notes))]


def fmt_mem(used, limit):
    """Usage against the -m limit in one unit, picked by the larger value: '32/64M', '0.3/3.2G';
    just '324M' / '3.2G' without a limit."""
    if used is None:
        return "-"
    if max(used, limit or 0) < 1024:
        return "%d/%dM" % (used, limit) if limit else "%dM" % used
    g = lambda mb: ("%.1f" % (mb / 1024)).removesuffix(".0")
    return "%s/%sG" % (g(used), g(limit)) if limit else g(used) + "G"


def job_segs(j):
    """Running jobs show live CPU% (of their assigned cores) and RSS; finished ones their peak RSS."""
    pct = j.get("cpu_pct")
    used = j.get("rss_mb") if j["state"] in ("running", "paused") else j.get("peak_mb") or None if j["state"] in TERMINAL else None
    over = used is not None and j.get("mem_mb") and used > 0.9 * j["mem_mb"]
    segs = [state_seg(j), (NUM, " %3d" % (j.get("cpus") or 1)),
            (NUM, " %4d" % j["prio"]), ("", "  "), (HOST, "%-12s" % (j["host"] or "(any)")[:12]),
            ("", "  %-8s  %9s  " % (j["id"], "(%s)" % fmt_dur(j["t"]) if j.get("waiting") else fmt_dur(j["t"]))), progress_seg(j), ("", "  "), (DIM, "%5s" % ("%d/%d" % (j.get("attempt") or 0, (j.get("retries") or 0) + 1))),
            (BOLD_RED if pct is not None and pct > 105 else "", "  %4s" % ("-" if pct is None else "%d%%" % round(pct))),
            (BOLD_RED if over else "", "  %11s" % fmt_mem(used, j.get("mem_mb"))),
            ("", "  "), ("", "%-16s" % clean(j.get("name") or "-")[:NAME_MAX]), ("", "  " + clean(j["cmd"]))]
    if j["state"] in TERMINAL and j["state"] != "done":
        segs = segs[:1] + [(DIM, t) for _, t in segs[1:]]
    elif j["state"] == "done":
        segs = [(DIM, t) for _, t in segs]
    return segs


def progress_seg(j):
    """The Progress column: percent done and, while running, the ETA; amber when the ETA overruns the timeout."""
    f, left = job_progress(j)
    if f is None:
        return DIM, "%9s" % "-"
    late = left is not None and j.get("timeout") and j["t"] + left > j["timeout"]
    return YELLOW if late else "", "%9s" % ("%d%%" % (100 * f) + (" " + fmt_short(left) if left is not None else ""))


def stage_segs(j):
    """Code / files / results of a job that uses --git, --push or --pull; [] otherwise."""
    st = j.get("stage")
    if not st:
        return []
    segs, g = [], st.get("git")
    if g:
        segs += [("", "Code: "), (BOLD, "%s@%s" % (os.path.basename(g["repo"]), g["sha"][:8])),
                 (YELLOW if g["dirty"] else DIM, " +uncommitted" if g["dirty"] else " clean"),
                 (DIM, "%s%s   " % (" on " + g["branch"] if g.get("branch") else "", " (base %s)" % g["base"][:8] if g["dirty"] and g.get("base") else ""))]
    if st.get("push"):
        segs += [("", "Push: "), ("", ", ".join(os.path.basename(x) + ("/" if isdir else "") + (" → " + d if d != os.path.basename(x) else "")
                                              for x, d, isdir in st["push"])), ("", "   ")]
    if g or st.get("push"):
        segs.append((BOLD_RED, "staging failed: %s   " % st["err"]) if st.get("err") else
                    (DIM, "work dir cleaned   ") if st.get("cleaned") else
                    (BOLD_RED, "clean failed: %s   " % st["clean_err"]) if st.get("clean_err") else
                    (GREEN, "staged on %s%s   " % (st["staged"], " (kept)" if st.get("keep") else "")) if st.get("staged") else (DIM, "not staged yet   "))
    if st.get("pull"):
        segs += [("", "Pull: "), ("", ", ".join("%s → %s" % (x, d.replace(os.path.expanduser("~"), "~")) for x, d in st["pull"]))]
        segs.append((BOLD_RED, "   pull failed: %s" % st["pull_err"]) if st.get("pull_err") else
                    (GREEN, "   pulled %s ago" % fmt_dur(time.time() - st["pulled"], 1)) if st.get("pulled") else
                    (DIM, "   when it ends (%s)" % ",".join(st.get("pull_on", ()))))
    return segs


def progress_segs(j):
    """The checkpoint line of `show` and the detail view; empty for jobs that use none of it."""
    if not (j.get("progress") or j.get("expect") or j.get("stall")):
        return []
    f, left = job_progress(j)
    stall = j.get("stall")
    stall = ("%s%s" % (fmt_dur(stall, 1), " (or output)" if j.get("stall_on_log") else "")) if stall \
        else "2x expected per step (>=10s)" if stall is None and j.get("expect") else "-"
    ago = j["now"] - j["beat"] if j["state"] in ("running", "paused") and j.get("beat") else None
    return [("", "Progress: "), (BOLD, clean(j.get("progress") or "-")),
            *([("", "   Done: "), (NUM, "%d%%" % (100 * f))] if f is not None else []),
            *([("", "   ETA: "), (progress_seg(j)[0], fmt_dur(left))] if left is not None else []),
            *([(DIM, "   (updated %s ago)" % fmt_dur(ago))] if ago is not None else []),
            ("", "   Expect: %s   Stall after: %s" % (fmt_dur(j["expect"], 1) if j.get("expect") else "-", stall))]


def meta_segs(j):
    """The one-line summary of a job used by `show` and the detail view."""
    lim = lambda k, f: f(j[k]) if j.get(k) else "-"
    return [("", "State: "), state_seg(j), ("", "  Prio: "), (NUM, str(j["prio"])),
            ("", "   Try: %d/%d" % (j.get("attempt") or 0, (j.get("retries") or 0) + 1)),
            ("", "   Cores: "), (NUM, "%s" % (j.get("cores") or j.get("cpus") or "-")),
            ("", "   Mem peak: %s%s" % (lim("peak_mb", lambda v: "%dM" % v), " of %dM" % j["mem_mb"] if j.get("mem_mb") else "")),
            *([("", "   CPU%%: %s   Mem: %s" % ("-" if j.get("cpu_pct") is None else "%d%%" % round(j["cpu_pct"]),
                                              fmt_mem(j.get("rss_mb"), j.get("mem_mb"))))] if j["state"] == "running" else []),
            ("", "   Timeout: %s" % lim("timeout", lambda v: fmt_dur(v, 1))), ("", "   rc: "),
            (GREEN if j.get("rc") == 0 else BOLD_RED if j.get("rc") else DIM, "-" if j.get("rc") is None else str(j["rc"])),
            ("", "   %s: %s" % ("Waiting" if j.get("waiting") else "Ran", "(%s)" % fmt_dur(j["t"]) if j.get("waiting") else fmt_dur(j["t"]))),
            *([(YELLOW, "   Paused: %s%s" % (fmt_dur((j.get("paused_s") or 0) + (j["now"] - j["paused_at"] if j["state"] == "paused" and j.get("paused_at") else 0), 1),
                                           " (now)" if j["state"] == "paused" else " in total"))] if j.get("paused_s") or j["state"] == "paused" else [])]


def show(j, log):
    t = lambda k: time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(j[k])) if j.get(k) else "-"
    out([("", "Job: "), (BOLD_CYAN, j["id"]), ("", "   Name: "), (BOLD, clean(j.get("name") or "-")), ("", "   Host: "), (BOLD_CYAN, j["host"] or "(any)")])
    out(meta_segs(j))
    if progress_segs(j):
        out(progress_segs(j))
    if stage_segs(j):
        out(stage_segs(j))
    out([("", "Cmd: "), (BOLD, j["cmd"])])
    out([(DIM, "cwd %s   env %s   retry on %s   created %s   started %s   ended %s" % (
        j.get("cwd") or "~", j.get("env") if j.get("env") not in (None, "", "{}", {}) else "-", j.get("retry_on"), t("created"), t("started"), t("ended")))])
    if log:
        out([(DIM, "─" * 60)])
        sys.stdout.write(log if log.endswith("\n") else log + "\n")


def where_lines(w):
    """`tshed where` / the dashboard's W view: one segment list per line."""
    if w["state"] != "running":
        return [[(DIM, "not running (%s): no processes to show" % w["state"])]]
    L = [[(BOLD, "%-8s %-5s %11s  %s" % ("PID", "STAT", "ELAPSED", "COMMAND"))]]
    pad = " " * 28
    for r in w["procs"]:
        L.append([(NUM, "%-8d" % r["pid"]), ("", " %-5s %11s  " % (r["stat"][:5], r["etime"])),
                  ("", "  " * r["depth"] + ("└ " if r["depth"] else "")), (BOLD if not r["depth"] else "", clean(r["cmd"].replace("\\012", " ")))])
        ind = pad + "  " * r["depth"] + ("  " if r["depth"] else "")
        if r.get("line"):
            L.append([("", ind), (DIM, "at "), (HOST, "%s:%d" % (os.path.basename(r["script"]), r["line"])), ("", "  "),
                      (BOLD, clean(r["text"])[:120])])
        t = r.get("trace")
        if t:
            L.append([("", ind), (DIM, "traced "), (HOST, clean(os.path.basename(t["at"].split(":")[0]) + ":" + t["at"].rpartition(":")[2])),
                      *([(DIM, " in "), ("", " ← ".join(t["funcs"]))] if t["funcs"] else []), ("", "  "), (BOLD, clean(t["cmd"])[:160]),
                      (DIM, "   (%s ago)" % fmt_dur(t["age"], 1))])
    if not w["procs"]:
        L.append([(DIM, "(no processes found: the job may be exiting)")])
    for pid, dump in sorted(w["pyspy"].items(), key=lambda kv: int(kv[0])):
        L.append([(BOLD, "py-spy dump of %s:" % pid)])
        L += [[("", "  " + clean(l))] for l in dump.splitlines()]
    if w.get("pyspy_skipped"):
        L.append([(DIM, "(%d more python processes not dumped)" % w["pyspy_skipped"])])
    if w.get("pyspy_missing"):
        L.append([(DIM, "python stacks: install py-spy on the host (pip install py-spy); Linux may need sudo or kernel.yama.ptrace_scope=0")])
    if any(r.get("line") for r in w["procs"]):
        L.append([(DIM, "at = the line where the running command ends: exact for top-level commands, the block's last line "
                        "inside a loop or function" + ("" if w["trace_lines"] else "; exact lines: submit with --trace-lines"))])
    return L


ALT_ON = "\x1b[?1049h\x1b[?25l\x1b[?1000h\x1b[?1006h"   # alternate screen, hide cursor, SGR mouse
ALT_OFF = "\x1b[0m\x1b[?1000l\x1b[?1006l\x1b[?25h\x1b[?1049l"
KEYS_UP, KEYS_DOWN = ("\x1b[A", "\x1bOA", "\x10"), ("\x1b[B", "\x1bOB", "\x0e")
KEYS_PGUP, KEYS_PGDN = ("\x1b[5~", "\x1b[1;5A", "\x1b[1;2A"), ("\x1b[6~", "\x1b[1;5B", "\x1b[1;2B")
KEYS_LEFT, KEYS_RIGHT = ("\x1b[D", "\x1bOD"), ("\x1b[C", "\x1bOC")
PAGE = 10


def clipboard(text):
    """Put text on the clipboard: the local tool when this is a local session, else OSC 52, which the
    terminal itself handles (works over ssh; tmux needs set-clipboard on). Returns what was used."""
    if not os.environ.get("SSH_TTY"):
        for argv, need in ((["pbcopy"], None), (["wl-copy"], "WAYLAND_DISPLAY"), (["xclip", "-selection", "clipboard"], "DISPLAY"),
                           (["xsel", "-bi"], "DISPLAY")):
            if shutil.which(argv[0]) and (not need or os.environ.get(need)):
                try:
                    subprocess.run(argv, input=text.encode(), timeout=5, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                    return argv[0]
                except (OSError, subprocess.SubprocessError):
                    pass
    import base64
    OUT.write("\x1b]52;c;%s\x07" % base64.b64encode(text.encode()).decode())
    OUT.flush()
    return "the terminal (OSC 52)"


def tui(interval):
    """The dashboard, after gitss: alternate screen, synchronized double-buffered frames, type-to-filter,
    capital command keys, SGR mouse. It is also a full server loop (delivery, ingest, callbacks)."""
    import re, select, termios, tty
    fd = sys.stdin.fileno()
    saved = termios.tcgetattr(fd)
    db = sdb()
    apply_theme(paint_bg=True)
    themes = theme_names()
    st: dict = dict(sel=0, top=0, query="", hide=False, detail=None, scroll=None, msg=("", ""), prompt=None, log=(None, ""),
                    fetching=False, fetched=0, synced=0, row0=0, rows=0, syncing=False, where=None, marks=set())
    wake, stop = threading.Event(), threading.Event()

    def say(text, style=""):
        st["msg"] = (style, clean(str(text)))

    def syncer():
        while not stop.is_set():
            st["syncing"] = True
            try:
                sync_all()
                pull_due_bg()
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

    def fetch_where(j, dest):  # on demand only: a py-spy dump briefly pauses python processes
        try:
            w = json.loads(remote(dest, ["rpc"], json.dumps(dict(where=j["id"])), timeout=40))["where"]
            st["where"] = (j["id"], where_lines(w), time.time())
        except Exception as e:
            st["where"] = (j["id"], [[(BOLD_RED, "where: %s" % e)]], time.time())
        wake.set()

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
            hint = "[-n name (required)  -H host  -p prio  -c cpus  -m mem  -t timeout  -x expect  --stall D  -r retries, then the command]"
        text = text[-max(10, W - len(label) - 8):]
        return styled([(BOLD_YELLOW, label + " "), (BOLD_CYAN, text + "█"), ("", "   "), (DIM, hint)], W)

    def table_head(head, W):
        if HEAD_BAR:  # a colored bar replaces the rule under the header
            return [HEAD + head[:W].ljust(W) + RESET]
        return [BOLD + head[:W] + RESET, "─" * min(W, len(HOST_HEAD) + 12)]

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
                    (YELLOW if counts.get("queued") else DIM, "%d queued" % counts.get("queued", 0))]
            if counts.get("pending"):
                segs += [("", "   "), (CYAN, "%d pending" % counts["pending"])]
            if failed:
                segs += [("", "   "), (BOLD_RED, "%d failed" % failed)]
            segs += [("", "   "), (GREEN if up == len(hosts) and hosts else BOLD_RED, "%d/%d hosts up" % (up, len(hosts))),
                     ("", "   "), (DIM, "[Type to filter, Esc to exit]")]
            L.append(styled(segs, W))
        # 2. hosts
        L.extend(table_head(HOST_HEAD, W))
        shown = hosts[:max(1, H // 3 - 2)]
        L.extend(styled(host_segs(h), W, ALT if i % 2 else "") for i, h in enumerate(shown))
        if not hosts:
            L.append(DIM + "  (no hosts yet: tshed install NAME=DEST)" + RESET)
        elif len(shown) < len(hosts):
            L.append(DIM + "  (+%d more hosts)" % (len(hosts) - len(shown)) + RESET)
        L.append("")
        # 3. jobs
        L.extend(table_head("  " + JOB_HEAD, W))
        room = max(1, H - len(L) - 3)
        st["top"] = max(0, min(st["top"], st["sel"], len(jobs) - room))
        if st["sel"] >= st["top"] + room:
            st["top"] = st["sel"] - room + 1
        st.update(row0=len(L) + 1, rows=min(room, len(jobs) - st["top"]))
        for i, j in enumerate(jobs[st["top"]:st["top"] + room], st["top"]):
            gutter = (KEY, "◆ ") if j["id"] in st["marks"] else ("", "  ")  # X / ctrl-click marks
            if i == st["sel"]:  # SEL_FLAT (ratatui's reversed row): the bar's own text color, no per-cell colors
                L.append(styled([("", t) for _, t in [gutter] + job_segs(j)] if SEL_FLAT else [gutter] + job_segs(j), W, HILITE))
            else:
                L.append(styled([gutter] + job_segs(j), W, ALT if i % 2 else ""))
        if not jobs:
            L.append(DIM + ("  (no matching jobs)" if st["query"] or st["hide"] else "  (no jobs yet: press N to submit one)") + RESET)
        L.extend([""] * (H - 3 - len(L)))
        # 4. status message, 5. help line, 6. footer
        L.append(styled([st["msg"]], W))
        n = len(st["marks"])
        L.append(help_line(([("E", "rerun %d marked" % n), ("K", "cancel"), ("P", "prio"), ("Z", "pause/resume"), ("Y", "copy %d commands" % n),
                             ("X", "mark"), ("^A", "mark shown"), ("U", "unmark all")]
                            if n else [("X", "mark"), ("Y", "copy cmd"), ("Z", "pause/resume")]) + [("Enter/→", "detail"), ("W", "where"), ("N", "new"), ("K", "cancel"), ("E", "rerun"), ("P", "prio"), ("F", "force"), ("M", "move"),
                            ("A", "attach"), ("O", "shell"), ("H", "show done" if st["hide"] else "hide done"), ("C", "config"),
                            ("T", "theme"), ("B/D", "contrast"), ("R", "refresh"), ("Esc", "exit")], W))
        age = "syncing…" if st["syncing"] and not st["synced"] else "synced %s ago" % fmt_dur(time.time() - st["synced"]) if st["synced"] else "not synced yet"
        L.append(styled([(DIM, "Server: %s   (%d/%d jobs shown)   " % (HOME.replace(os.path.expanduser("~"), "~"), len(jobs), total)),
                         *([(KEY, "%d marked" % n), (DIM, "   ")] if n else []), (DIM, age)], W))
        return L, H

    def draw_detail(j):
        W, H = shutil.get_terminal_size()
        W -= 1
        L = [prompt_bar(W) if st["prompt"] else styled([("", "Job: "), (BOLD_CYAN, j["id"]), ("", "   Name: "), (BOLD, clean(j.get("name") or "-")), ("", "   Host: "), (BOLD_CYAN, j["host"] or "(any)"), ("", "   "),
                                                         (DIM, "(%s on the host)" % ("~/.tinyshed/logs/%s.log" % j["id"]))], W),
             styled(meta_segs(j), W), *([styled(progress_segs(j), W)] if progress_segs(j) else []),
             *([styled(stage_segs(j), W)] if stage_segs(j) else []),
             styled([("", "Cmd: "), (BOLD, clean(j["cmd"]))], W), "─" * W]
        text = st["log"][1] if st["log"][0] == j["id"] else (j.get("tail") or "")
        lines = [clean(l.rsplit("\r", 1)[-1]) for l in text.rstrip("\n").split("\n")] if text else []
        wh = st["where"] if st["where"] and st["where"][0] == j["id"] else None
        if wh:  # the W view replaces the log until W is pressed again
            L.append(styled([(BOLD, "Where"), (DIM, " (as of %s ago)   [W] back to the log   [R] refresh" % fmt_dur(time.time() - wh[2], 1))], W))
            L += [styled(segs, W) for segs in wh[1][:max(1, H - len(L) - 2)]]
            L.extend([""] * (H - 2 - len(L)))
            L.append(styled([st["msg"]], W))
            L.append(help_line([("Esc/←", "back"), ("W", "log"), ("R", "refresh"), ("K", "cancel"), ("A", "attach"), ("O", "shell")], W))
            return L, H
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
        L.append(help_line([("Esc/←", "back"), ("↑/↓", "scroll"), ("W", "where"), ("Z", "pause/resume"), ("K", "cancel"), ("E", "rerun"), ("P", "prio"), ("F", "force"), ("M", "move"), ("A", "attach"), ("O", "shell"), ("C", "config"), ("R", "refresh")], W))
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
                    and all(w in ("%s %s %s %s %s" % (j["state"], j["host"], j["id"], j.get("name") or "", j["cmd"])).lower() for w in words)]
            if st["detail"]:  # stay on the same job even as the list reorders
                st["sel"] = next((i for i, j in enumerate(jobs) if j["id"] == st["detail"]), st["sel"])
            st["sel"] = max(0, min(st["sel"], len(jobs) - 1))
            cur = jobs[st["sel"]] if jobs else None
            if st["detail"] and not (cur and cur["id"] == st["detail"]):
                st["detail"] = None  # filtered away
            if cur and st["detail"] and cur["sent"] and cur["host"] in dests and not st["fetching"] and time.time() - st["fetched"] > 2 \
                    and (cur["state"] == "running" or st["log"][0] != cur["id"]):
                st["fetching"] = True
                threading.Thread(target=fetch_log, args=(cur, dests[cur["host"]]), daemon=True).start()
            L, H = draw_detail(cur) if st["detail"] else draw_main(hosts, jobs, len(everything), counts, cur)
            OUT.write("\x1b[?2026h\x1b[H" + RESET + "\x1b[K\n".join(L[:H]) + "\x1b[K\x1b[J\x1b[?2026l")  # one atomic frame
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
            step, click, mark = 0, None, None
            m = re.match(r"\x1b\[<(\d+);(\d+);(\d+)([Mm])", key)
            if m:  # SGR mouse: wheel moves, click selects, click on the selection opens it
                button, row = int(m.group(1)), int(m.group(3))
                if button in (64, 65):
                    step = -1 if button == 64 else 1
                elif button in (0, 8, 16) and m.group(4) == "M" and not st["detail"] and st["row0"] <= row < st["row0"] + st["rows"]:
                    if button:  # alt- or ctrl-click: toggle the mark, as in a file manager
                        mark = st["top"] + row - st["row0"]
                    else:
                        click = st["top"] + row - st["row0"]
            elif key in KEYS_UP + KEYS_DOWN:
                step = -1 if key in KEYS_UP else 1
            elif key in KEYS_PGUP + KEYS_PGDN:
                step = -PAGE if key in KEYS_PGUP else PAGE
            if step and st["detail"]:
                st["scroll"] = max(0, (st["rows"] if st["scroll"] is None else st["scroll"]) + step)
            elif step:
                st["sel"] += step
            elif mark is not None:
                st["marks"] ^= {jobs[mark]["id"]}
                st["sel"] = mark
            elif click is not None:
                if click == st["sel"]:
                    st.update(detail=jobs[click]["id"], fetched=0, scroll=None)
                st["sel"] = click
            elif m:
                pass
            elif key in ("Q", "\x03") or (key == "\x1b" and not st["detail"] and not st["query"] and not st["marks"]):
                break
            elif key == "\x1b" or (key in KEYS_LEFT and st["detail"]):
                if st["detail"]:
                    st.update(detail=None, where=None)
                elif st["query"]:
                    st["query"] = ""
                else:  # Esc clears marks before it exits
                    st["marks"] = set()
                    say("Marks cleared", DIM)
            elif key in ("\r", "\n") + KEYS_RIGHT and cur and not st["detail"]:
                st.update(detail=cur["id"], fetched=0, scroll=None)
            elif key == "\x15":
                st["query"] = ""
            elif key == "R":
                st["fetched"] = 0
                say("Refreshing…", DIM)
                if st["detail"] and st["where"] and st["where"][0] == st["detail"] and cur and cur["host"] in dests:
                    threading.Thread(target=fetch_where, args=(cur, dests[cur["host"]]), daemon=True).start()
                wake.set()
            elif key == "W" and cur and st["detail"] and st["where"] and st["where"][0] == cur["id"]:
                st["where"] = None
            elif key == "W" and cur and cur["state"] == "running" and cur["host"] in dests:
                st.update(detail=cur["id"], fetched=0, scroll=None)
                st["where"] = (cur["id"], [[(DIM, "asking %s…" % cur["host"])]], time.time())
                threading.Thread(target=fetch_where, args=(cur, dests[cur["host"]]), daemon=True).start()
            elif key == "H":
                st["hide"] = not st["hide"]
            elif key == "T":  # next theme, saved; TINYSHED_THEME (if set) only picks the starting point
                cur_t = current_theme() if st.get("theme") is None else st["theme"]
                st["theme"] = themes[(themes.index(cur_t) + 1) % len(themes) if cur_t in themes else 0]
                with open(P("theme"), "w") as f:
                    f.write(st["theme"] + "\n")
                os.environ.pop("TINYSHED_THEME", None)
                apply_theme(st["theme"], paint_bg=True)
                say("Theme: %s (%d/%d)   [T] next" % (st["theme"], themes.index(st["theme"]) + 1, len(themes)), KEY)
            elif key in ("B", "D"):  # more / less contrast, saved
                n = save_contrast(current_contrast() + (1 if key == "B" else -1))
                apply_theme(st.get("theme"), paint_bg=True)
                flat = (st.get("theme") or current_theme()) in ("default", "mono")
                say("Contrast: %g/%g%s   [B] more  [D] less%s" % (n, CONTRAST_MAX, " (colors as published)" if not n else "",
                                                             "   (no effect on the %s theme: [T])" % (st.get("theme") or current_theme()) if flat else ""), KEY)
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
            elif key == "K" and st["marks"]:
                live = [j["id"] for j in everything if j["id"] in st["marks"] and j["state"] not in TERMINAL]

                def kill_marked(text, live=live):
                    if text.lower() in ("y", "yes"):
                        for jid in live:
                            cancel(db, jid)
                        st["marks"] = set()
                        say("Cancelling %d jobs…" % len(live), YELLOW)
                        wake.set()
                skipped = len(st["marks"]) - len(live)
                st["prompt"] = ("Cancel %d marked jobs%s? [y/N]:" % (len(live), " (%d already finished, skipped)" % skipped if skipped else ""), "", kill_marked)
            elif key == "P" and st["marks"]:
                waiting = [j["id"] for j in everything if j["id"] in st["marks"] and j["state"] in ("pending", "queued")]

                def prio_marked(text, waiting=waiting):
                    try:
                        n = int(text)
                    except ValueError:
                        return say("Priority must be an integer", BOLD_RED)

                    def go(tdb):
                        bad = []
                        for jid in waiting:
                            try:
                                set_prio(tdb, jid, n)
                            except Fail as e:
                                bad.append("%s: %s" % (jid, e))
                        say("Priority %d for %d jobs%s" % (n, len(waiting) - len(bad), "; failed: " + "; ".join(bad) if bad else ""),
                            BOLD_RED if bad else GREEN)
                    background(go)
                skipped = len(st["marks"]) - len(waiting)
                st["prompt"] = ("Priority for %d marked waiting jobs%s:" % (len(waiting), " (%d not waiting, skipped)" % skipped if skipped else ""), "", prio_marked)
            elif key == "Z" and (st["marks"] or cur):
                picked = [j for j in everything if j["id"] in st["marks"]] if st["marks"] else [cur]
                stop_ids = [j["id"] for j in picked if j["state"] == "running"]
                go_ids = [j["id"] for j in picked if j["state"] == "paused"]
                if not stop_ids and not go_ids:
                    say("Z pauses running jobs and resumes paused ones; nothing to do here", DIM)
                else:
                    def toggle(tdb, stop_ids=stop_ids, go_ids=go_ids):
                        res = {**(pause_jobs(tdb, stop_ids) if stop_ids else {}), **(pause_jobs(tdb, go_ids, resume=True) if go_ids else {})}
                        bad = ["%s: %s" % (k, v) for k, v in res.items() if v not in ("running", "paused")]
                        say("%s%s" % (", ".join(x for x in ("pausing %d" % len(stop_ids) if stop_ids else "", "resuming %d" % len(go_ids) if go_ids else "") if x),
                                      "; failed: " + "; ".join(bad) if bad else ""), BOLD_RED if bad else YELLOW)
                    background(toggle)
            elif key == "K" and cur and cur["state"] not in TERMINAL:
                def kill(text, jid=cur["id"]):
                    if text.lower() in ("y", "yes"):
                        cancel(db, jid)
                        say("Cancelling %s…" % jid, YELLOW)
                        wake.set()
                st["prompt"] = ("Cancel %s (%s)? [y/N]:" % (cur["id"], clean(cur["cmd"])[:40]), "", kill)
            elif key == "X" and cur and not st["detail"]:
                st["marks"] ^= {cur["id"]}
                st["sel"] += 1
            elif key == "\x01" and jobs and not st["detail"]:
                shown = {j["id"] for j in jobs}
                st["marks"] = st["marks"] - shown if shown <= st["marks"] else st["marks"] | shown
                say("%d marked" % len(st["marks"]), KEY)
            elif key == "U":
                st["marks"] = set()
                say("Marks cleared", DIM)
            elif key == "Y" and (st["marks"] or cur):
                picked = [j for j in everything if j["id"] in st["marks"]] if st["marks"] else [cur]
                try:
                    how = clipboard("\n".join(j["cmd"] for j in picked) + "\n")
                    say("Copied %d command%s to %s" % (len(picked), "s" * (len(picked) != 1), how), GREEN)
                except Exception as e:
                    say("Copy failed: %s" % e, BOLD_RED)
            elif key == "E" and st["marks"]:
                picked = [j for j in everything if j["id"] in st["marks"]]

                def rerun(text, picked=picked):
                    text = text.strip()
                    if text and text != "any" and text not in dests:
                        return say("Unknown host %r: Enter keeps each job's host; or any, %s" % (text, ", ".join(dests)), BOLD_RED)
                    ids = [add_job(db, j, (None if text == "any" else text) if text else j["host"] if j["host"] in dests else None) for j in picked]
                    st["marks"] = set()
                    say("Resubmitted %d jobs%s: %s" % (len(ids), " on " + text if text else "", " ".join(ids)), GREEN)
                    wake.set()
                st["prompt"] = ("Rerun %d marked jobs on host [Enter: each job's own | any | %s]:" % (len(picked), " | ".join(dests)), "", rerun)
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
            elif key == "F" and cur and cur["state"] in ("pending", "queued"):
                def force(text, jid=cur["id"]):
                    if text.lower() in ("y", "yes"):
                        say("Forcing %s…" % jid, YELLOW)
                        background(lambda tdb: say("%s: %s" % (jid, "; ".join(edit_job(tdb, jid, force=True))), GREEN))
                st["prompt"] = ("Force-start %s now, ahead of the queue (even on busy cores)? [y/N]:" % cur["id"], "", force)
            elif key == "M" and cur and cur["state"] in ("pending", "queued"):
                def move(text, jid=cur["id"]):
                    text = text.strip()
                    if text:
                        host = None if text == "any" else text
                        say("Moving %s to %s…" % (jid, text), YELLOW)
                        background(lambda tdb: say("%s: %s" % (jid, "; ".join(edit_job(tdb, jid, host=host))), GREEN))
                st["prompt"] = ("Move %s (on %s) to host [%s|any]:" % (cur["id"], cur["host"] or "(any)",
                                                                      "|".join(n for n in dests if n != cur["host"])), "", move)
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
  tshed submit -n train -H gpu1 -p 5 -c 8 -m 32G -t 12h -r 2 -- python train.py
  tshed ls -a;  tshed show ID;  tshed logs ID;  tshed where ID;  tshed wait ID;  tshed cancel ID
  tshed edit ID -p 9 -H gpu2 --force                  re-prioritise, move, or start a waiting job now
  tshed theme dracula                                 pick a color theme (tshed theme: list them)
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
    s = sub.add_parser("where", help="where a running job is: process tree, script line, python stacks",
                       description="Ask the job's host what the job is doing now: its process tree; for each bash script, "
                                   "the line it has reached (exact for top-level commands; inside a loop or function, the "
                                   "line where that block ends); with --trace-lines at submit, the exact line, function "
                                   "stack and command of every bash process; and with py-spy on the host, the stack of "
                                   "every python process.")
    s.add_argument("id")
    s.add_argument("--json", action="store_true", help="the raw answer as JSON")
    s = sub.add_parser("pause", help="pause running jobs (SIGSTOP); their cores go back to the queue",
                       description="Stop every process of each job (SIGSTOP) and release its cores, so queued jobs can use them. "
                                   "Memory stays held. Paused time does not count against the timeout, the stall limit or the run time.")
    s.add_argument("ids", nargs="+", metavar="ID")
    s = sub.add_parser("resume", help="resume paused jobs as soon as their cores are free",
                       description="A resumed job waits for free cores in priority order, like a queued job, then continues "
                                   "(re-pinned to the cores it gets, on Linux).")
    s.add_argument("ids", nargs="+", metavar="ID")
    s = sub.add_parser("grep", help="search job logs on every host",
                       description="Search the captured output of jobs (running or finished, compressed or not) for a Python "
                                   "regular expression, on all hosts in parallel. Without IDs: every job that matches the filters.")
    s.add_argument("pattern")
    s.add_argument("ids", nargs="*", metavar="ID", help="limit to these jobs (ids or names)")
    s.add_argument("-i", "--ignore-case", action="store_true")
    s.add_argument("--name", action="append", metavar="NAME", help="only jobs with this name (repeatable)")
    s.add_argument("--state", action="append", metavar="STATE", help="only jobs in this state, e.g. error (repeatable)")
    s.add_argument("--since", type=dur, help="only jobs submitted within this long, e.g. 2d")
    s.add_argument("-m", "--max", type=int, default=20, help="matches shown per job (default 20)")
    s.add_argument("-l", "--files", action="store_true", help="only list the jobs that match")
    s = sub.add_parser("stats", help="runs, failures and runtimes per job name",
                       description="From the server's records (no host contact): per job name, how many runs, how many "
                                   "succeeded or failed, and the median / 90th percentile / max runtime of successful runs, "
                                   "next to the latest --expect. Use it to pick -x for a new run.")
    s.add_argument("names", nargs="*", metavar="NAME")
    s.add_argument("--since", type=dur, help="only jobs submitted within this long, e.g. 30d")
    s.add_argument("--json", action="store_true", help="one JSON object per name")
    s = sub.add_parser("checkout", help="a --git job's code from its snapshot: into a folder, or its changes as a diff",
                       description="Rebuild the exact code a --git job ran from tinyshed's snapshot store (no host contact, also "
                                   "after its work dir was cleaned): the files into DIR (default ./NAME-ID, must not exist "
                                   "or be empty), or with --diff the changes the snapshot carried on top of its base commit.")
    s.add_argument("id")
    s.add_argument("dir", nargs="?")
    s.add_argument("--diff", action="store_true", help="print the uncommitted changes the job ran with (git diff base..snapshot)")
    s.add_argument("--stat", action="store_true", help="with --diff: only the list of changed files")
    s = sub.add_parser("fetch", help="copy a job's results from its host now",
                       description="Copy files from a job's host into local folders now, whatever its state (a running job "
                                   "gives a snapshot). Without SRC: the job's --pull list. A folder's contents land in the "
                                   "destination folder; a file lands inside it. Each destination gets tinyshed-ID.json.")
    s.add_argument("id")
    s.add_argument("srcs", nargs="*", metavar="SRC", help="paths relative to the job's working directory on the host")
    s.add_argument("--to", metavar="DIR", help="local folder for everything (default: each --pull DEST, or results/{name}/{id})")
    s = sub.add_parser("wait", help="block until jobs finish (exit 0 all done, 1 any failed, 2 timeout)")
    s.add_argument("ids", nargs="+", metavar="ID")
    s.add_argument("-t", "--timeout", type=dur, default=0, help="give up after e.g. 30m")
    s.add_argument("-i", "--interval", type=float, default=5, help="poll seconds")
    s = sub.add_parser("cancel", help="cancel queued or running jobs")
    s.add_argument("ids", nargs="+", metavar="ID")
    s = sub.add_parser("prio", help="change a waiting job's priority")
    s.add_argument("id")
    s.add_argument("prio", type=int)
    s = sub.add_parser("edit", help="change a waiting job: priority, host, or force it to start now",
                       description="Edit a pending or queued job. -H moves it to another host ('any' unpins it); a job "
                                   "already queued on a host is withdrawn from there first, only if it has not started. "
                                   "--force starts it right away, ahead of the queue and even on busy cores.")
    s.add_argument("id")
    s.add_argument("-p", "--prio", type=int, help="new priority (higher runs first)")
    s.add_argument("-H", "--host", help="move to this host; 'any' lets the server pick")
    s.add_argument("-f", "--force", action="store_true", help="start now: skip the queue order, share busy cores if needed")
    s = sub.add_parser("serve", help="headless server loop: deliver jobs, ingest events, run callbacks")
    s.add_argument("-i", "--interval", type=float, default=10, help="poll seconds")
    s = sub.add_parser("tui", help="interactive dashboard (the default); also the server loop while open")
    s.add_argument("-i", "--interval", type=float, default=5, help="poll seconds")
    s = sub.add_parser("theme", help="list color themes, or pick one",
                       description="Without NAME: list the built-in themes and installed btop themes, each as a sample row "
                                   "(* = current). With NAME (a theme name, btop:NAME, or a path to a btop .theme file): save "
                                   "it for this server's cli and dashboard. TINYSHED_THEME overrides the saved choice.")
    s.add_argument("name", nargs="?", metavar="NAME")
    s.add_argument("--contrast", type=float, metavar="N",
                   help="contrast level 0-10 (default 4): each level makes dark shades darker, light ones lighter and colors "
                        "more vivid. 0 = colors exactly as published. Saved; TINYSHED_CONTRAST overrides")
    s = sub.add_parser("scheme", help="list or set per-role color overrides on top of the theme",
                       description="Without arguments: list every color role with its current color (* = overridden) "
                                   "and the named preset schemes. With a preset name (" + " ".join(SCHEMES) + "): apply "
                                   "it wholesale (saved in ~/.tinyshed/scheme). With ROLE=COLOR pairs: override "
                                   "individual colors on top of any theme. COLOR is 6 hex digits or a basic color name "
                                   "(red, bright_cyan, ...); COLOR `default` removes the override. "
                                   "TINYSHED_SCHEME='err=ff0000 warn=ffc000' overrides per shell.")
    s.add_argument("pairs", nargs="*", metavar="PRESET | ROLE=COLOR",
                   help="presets: " + " ".join(SCHEMES) + "; roles: " + " ".join(SCHEME_ROLES))
    s.add_argument("--clear", action="store_true", help="remove every override")
    s = sub.add_parser("progress", help="report a checkpoint from inside a job (does nothing outside one)",
                       description="Write a checkpoint for the running job to $TINYSHED_PROGRESS. Every call resets the "
                                   "--stall clock; a leading DONE/TOTAL or PCT% gives the dashboard a percentage and an ETA. "
                                   "Outside a tinyshed job it does nothing, so scripts run unchanged elsewhere. "
                                   "Without python: printf '%s\\n' \"3/10 msg\" > \"$TINYSHED_PROGRESS\".")
    s.add_argument("words", nargs="*", metavar="[DONE/TOTAL|PCT%] MESSAGE")
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
                kind = "active" if name in ("wait", "cancel", "prio", "edit") else "jobs"
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
            b = unpack(r["j"])
            print("%s\t%s %s %s: %s" % (r["id"], b.get("name") or "-", r["state"], r["host"] or "(any)", clean(b.get("cmd"))[:40]))


def progress_cmd(words):
    """`tshed progress`: one atomic write of the checkpoint file the job's watchdog polls."""
    path = os.environ.get("TINYSHED_PROGRESS")
    if not path:
        return 0
    tmp = "%s.%d" % (path, os.getpid())
    try:
        with open(tmp, "w") as f:
            f.write(" ".join(words) + "\n")
        os.replace(tmp, path)
    except OSError as e:
        raise Fail("progress: %s" % e)
    return 0


def main():
    global ECHO, STAGE_MODE
    p, sub, public = build_parser()
    a = p.parse_args()
    STAGE_MODE = "bg" if a.op in (None, "tui", "serve") else "sync" if a.op in ("submit", "wait", "edit", "fetch") else None

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
    if a.op == "progress":
        return progress_cmd(a.words)
    if a.op == "completion":
        return print(completion(a.shell, sub, public))

    apply_theme()
    if a.op == "theme":
        return theme_cmd(a.name, a.contrast)
    if a.op == "scheme":
        return scheme_cmd(a)
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
            pull_due_bg()
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
                out([(HOST, "%-12s" % name), ("", "  "), (BOLD_RED, "● FAILED "), ("", str(r) or type(r).__name__)])
                continue
            db.execute("INSERT INTO hosts(name,dest) VALUES(?,?) ON CONFLICT(name) DO UPDATE SET dest=excluded.dest", (name, dest))
            out([(HOST, "%-12s" % name), ("", "  "), (GREEN, "✓ ok    ") if r["agent"] else (BOLD_RED, "● BROKEN"),
                 ("", "%-5s %-8s %-10s " % (r["version"], r["python"], r["zstd"])), (NUM, "%5s" % r["cores"]),
                 ("", "  %-13s " % ((r["boot"] or "-") + ("+home" if r["phone_home"] else ""))), (YELLOW, install_notes(r))])
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
                out([(HOST, "%-12s" % name), ("", "  "), (BOLD_RED, "● FAILED "), ("", "%s" % e)])
                rc = 1
                continue
            db.execute("DELETE FROM hosts WHERE name=?", (name,))
            out([(HOST, "%-12s" % name), ("", "  "), (GREEN, "✓ removed"), ("", "  %s%s" % (
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
        jid = submit(db, a, stage_now=True)
        sync_all()
        r = db.execute("SELECT host, state FROM jobs WHERE id=?", (jid,)).fetchone()
        print(jid, r["state"], r["host"] or "(any)")
        if r["state"] == "pending":
            print("note: " + ("host %s is unreachable; the job is delivered on reconnect" % r["host"] if r["host"] else
                              "waiting for a host with enough idle cores; the next sync places it (tshed wait/ls, serve or the dashboard)"), file=sys.stderr)
    elif a.op == "ls":
        sync_all()
        jobs = [j for j in load_jobs(db, load_hosts(db)) if a.all or j["state"] not in TERMINAL]
        if a.json:
            keys = ("id", "name", "host", "state", "prio", "attempt", "rc", "cmd", "created", "started", "ended", "peak_mb",
                    "cpus", "mem_mb", "cpu_pct", "rss_mb", "timeout", "expect", "stall", "progress", "beat", "paused_s", "stage")
            for j in jobs:
                f, left = job_progress(j)
                print(json.dumps(dict({k: j.get(k) for k in keys}, done=f, eta=left, **{"wait" if j.get("waiting") else "ran": round(j["t"], 1)})))
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
    elif a.op == "checkout":
        jid = resolve(db, a.id)
        r = db.execute("SELECT j, stage FROM jobs WHERE id=?", (jid,)).fetchone()
        g = (json.loads(r["stage"]) if r["stage"] else {}).get("git")
        if not g:
            raise Fail("%s was not submitted with --git: there is no snapshot" % jid)
        gd = ["--git-dir", g["store"]] if g.get("store") else ["-C", g["repo"]]
        if subprocess.run(["git", *gd, "cat-file", "-e", g["sha"] + "^{commit}"], capture_output=True).returncode:
            raise Fail("snapshot %s of %s is gone from %s" % (g["sha"][:8], jid, g.get("store") or g["repo"]))
        if a.diff:
            if not g["dirty"]:
                print("%s ran on %s itself: no uncommitted changes" % (jid, g["sha"][:8]))
                return 0
            base = g.get("base") or "4b825dc642cb6eb9a060e54bf8d69288fbee4904"  # the empty tree: a repo without commits
            return subprocess.call(["git", *gd, "diff", *(["--stat"] if a.stat else []), base, g["sha"]])
        dst = os.path.abspath(os.path.expanduser(a.dir or "%s-%s" % (unpack(r["j"]).get("name") or "job", jid)))
        if os.path.exists(dst) and os.listdir(dst):
            raise Fail("%s exists and is not empty" % dst)
        os.makedirs(dst, exist_ok=True)
        arc = subprocess.Popen(["git", *gd, "archive", g["sha"]], stdout=subprocess.PIPE)
        tar = subprocess.run(["tar", "-x", "-C", dst], stdin=arc.stdout)
        if arc.wait() or tar.returncode:
            raise Fail("could not extract the snapshot into %s" % dst)
        print("%s code at %s%s → %s" % (jid, g["sha"][:8], " (+uncommitted changes on %s)" % (g.get("base") or "")[:8] if g["dirty"] else "", dst))
    elif a.op == "fetch":
        jid = resolve(db, a.id)
        host = db.execute("SELECT host FROM jobs WHERE id=?", (jid,)).fetchone()["host"]
        if host in known:
            sync_host(host)  # the latest state, for the manifest
        done, errs = pull_job(db, jid, os.path.abspath(os.path.expanduser(a.to)) if a.to else None, a.srcs or None)
        for x in done:
            print(jid, "pulled", x)
        for e in errs:
            print("tinyshed: %s %s" % (jid, e), file=sys.stderr)
        return 1 if errs else 0
    elif a.op in ("pause", "resume"):
        ids = [resolve(db, i) for i in a.ids]
        rc = 0
        for jid, state in pause_jobs(db, ids, resume=a.op == "resume").items():
            ok = state in ("running", "paused")
            rc |= not ok
            print(jid, ({"pause": {"running": "pausing", "paused": "paused"}, "resume": {"paused": "resuming (waits for free cores)",
                                                                                      "running": "running"}}[a.op].get(state, state)))
        return rc
    elif a.op == "grep":
        try:
            re.compile(a.pattern)
        except re.error as e:
            raise Fail("bad pattern: %s" % e)
        want = {resolve(db, i) for i in a.ids}
        since = time.time() - a.since if a.since else 0
        jobs = [j for j in load_jobs(db, load_hosts(db)) if j["sent"] and j["host"] in known and (not want or j["id"] in want)
                and (not a.name or j.get("name") in a.name) and (not a.state or j["state"] in a.state) and j["created"] >= since]
        by_host, found, errs = {}, {}, []
        for j in jobs:
            by_host.setdefault(j["host"], []).append(j["id"])

        def one(host):
            try:
                req = dict(grep=dict(pattern=a.pattern, ignore_case=a.ignore_case, ids=by_host[host], per_job=1 if a.files else a.max, max=10 ** 6))
                found.update(json.loads(remote(known[host], ["rpc"], json.dumps(req), timeout=300))["grep"])
            except Exception as e:
                errs.append("%s: %s" % (host, e))
        par(one, list(by_host))
        for j in sorted((j for j in jobs if j["id"] in found), key=lambda j: j["created"]):
            hits = found[j["id"]]
            out([(BOLD_CYAN, j["id"]), ("", "  "), (BOLD, clean(j.get("name") or "-")), ("", "  "), (HOST, j["host"]), ("", "  "), state_seg(j)])
            for n, text in [] if a.files else hits:
                out([(DIM, "%7d: " % n), ("", clean(text))])
        for e in errs:
            print("tinyshed: grep on %s" % e, file=sys.stderr)
        return 0 if found else 1
    elif a.op == "stats":
        rows = job_stats(db, set(a.names) or None, time.time() - a.since if a.since else 0)
        if a.json:
            for g in rows:
                print(json.dumps(g))
            return
        if not rows:
            raise Fail("no jobs%s" % (" named " + ", ".join(a.names) if a.names else ""))
        d = lambda v: fmt_dur(v, 1) if v is not None else "-"
        print(paint("%-16s %5s %5s %6s %5s  %9s %9s %9s  %9s  %8s  %s" % ("Name", "Runs", "Done", "Failed", "Fail%", "Median", "P90", "Max",
                                                                        "Expect", "Peak mem", "Last run"), BOLD))
        for g in rows:
            ratio = g["median"] / g["expect"] if g["median"] and g["expect"] else None
            out([("", "%-16s" % clean(g["name"])[:NAME_MAX]), (NUM, " %5d %5d" % (g["runs"], g["done"])),
                 (BOLD_RED if g["failed"] else DIM, " %6d" % g["failed"]), (BOLD_RED if g["fail_pct"] >= 50 else "", " %4d%%" % g["fail_pct"]),
                 ("", "  %9s %9s %9s  " % (d(g["median"]), d(g["p90"]), d(g["max"]))),
                 (YELLOW if ratio and not 0.5 <= ratio <= 1.5 else "", "%9s" % d(g["expect"])),
                 ("", "  %8s  " % fmt_mem(g["peak_mb"], None)), (DIM, "%s ago%s" % (fmt_dur(time.time() - g["last"]),
                                                                                  "; failed as " + ",".join(g["states"]) if g["states"] else "")),
                 *([(GREEN, "  %d active" % g["active"])] if g["active"] else [])])
    elif a.op == "where":
        r = db.execute("SELECT * FROM jobs WHERE id=?", (resolve(db, a.id),)).fetchone()
        if not r["sent"] or r["host"] not in known:
            raise Fail("job %s is %s and not on a host" % (r["id"], r["state"]))
        w = json.loads(remote(known[r["host"]], ["rpc"], json.dumps(dict(where=r["id"]))))["where"]
        if a.json:
            return print(json.dumps(w))
        b = unpack(r["j"])
        out([("", "Job: "), (BOLD_CYAN, r["id"]), ("", "   Name: "), (BOLD, clean(b.get("name") or "-")), ("", "   Host: "), (BOLD_CYAN, r["host"]),
             *([("", "   Progress: "), (BOLD, clean(b["progress"]))] if b.get("progress") else [])])
        for line in where_lines(w):
            out(line)
    elif a.op == "wait":
        ids = [resolve(db, i) for i in a.ids]
        deadline = time.time() + a.timeout if a.timeout else None
        while True:
            sync_all()
            states = {i: db.execute("SELECT state FROM jobs WHERE id=?", (i,)).fetchone()[0] for i in ids}
            if all(s in TERMINAL for s in states.values()) or (deadline and time.time() > deadline):
                break
            time.sleep(a.interval)
        pulled = {}
        due = set(pulls_due(db))
        for i in ids:
            if i in due:
                try:
                    done, errs = pull_job(db, i)
                    pulled[i] = "; ".join(["pulled " + x for x in done] + ["pull failed: " + e for e in errs])
                except Fail as e:
                    pulled[i] = "pull failed: %s" % e
        for i in clean_due(db, set(ids)):
            clean_job(db, i)
        for i, s in states.items():
            print(i, s, *([pulled[i]] if i in pulled else []))
        return 2 if any(s not in TERMINAL for s in states.values()) else 0 if all(s == "done" for s in states.values()) else 1
    elif a.op == "cancel":
        ids = [resolve(db, i) for i in a.ids]
        for jid in ids:
            cancel(db, jid)
        sync_all()
        for jid in ids:
            s = db.execute("SELECT state FROM jobs WHERE id=?", (jid,)).fetchone()[0]
            print(jid, s if s in TERMINAL else "cancelling")
    elif a.op == "edit":
        jid = resolve(db, a.id)
        if a.prio is None and a.host is None and not a.force:
            raise Fail("nothing to change: give -p PRIO, -H HOST or --force")
        host = False if a.host is None else None if a.host in ("any", "") else a.host
        print(jid, "; ".join(edit_job(db, jid, a.prio, host, a.force)))
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

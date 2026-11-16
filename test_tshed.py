#!/usr/bin/env python3
"""Unit tests for tshed.py: stdlib unittest, no ssh, no daemons, never the live ~/.tinyshed.

    python3 -m unittest -v test_tshed      (or ./test_tshed.py)

Every test runs in its own temporary TINYSHED_HOME; process launches (`detach`) are stubbed out.
End-to-end behaviour (agents, watchdogs, ssh) is still exercised with fake hosts, see AGENTS.md.
"""
import contextlib, importlib.util, io, json, os, re, shutil, sqlite3, subprocess, sys, tempfile, time, unittest
from unittest import mock
from typing import Any, cast

os.environ["TINYSHED_HOME"] = tempfile.mkdtemp(prefix="tshed-test-")  # before import: HOME is read at import time
os.environ["NO_COLOR"] = "1"
_spec = importlib.util.spec_from_file_location("tshed", os.path.join(os.path.dirname(os.path.abspath(__file__)), "tshed.py"))
assert _spec is not None and _spec.loader is not None  # narrow ModuleSpec|None for the checker
t = cast(Any, importlib.util.module_from_spec(_spec))  # Any: stubs patch tshed attrs onto it
_spec.loader.exec_module(t)


class Base(unittest.TestCase):
    """A fresh home per test, hooks recorded instead of loaded from hooks.py, no child processes."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory(prefix="tshed-test-")
        self.home = self._tmp.name
        self._saved = {k: getattr(t, k) for k in ("HOME", "detach", "fire", "agent_alive", "disk_free", "sync_all", "tx", "connect")}
        self._dbs = []
        t.connect = lambda *a, _c=t.connect: self._dbs.append(_c(*a)) or self._dbs[-1]  # closed in tearDown
        t.HOME = self.home
        t._disk_warned[0] = 0.0
        self.fired, self.launched = [], []
        t.fire = lambda ev: self.fired.append(ev)
        t.detach = lambda args, **kw: self.launched.append(args) or FakeProc()
        t.agent_alive = lambda: True
        self.cfg({"name": "h1", "cores": "0-1"})

    def tearDown(self):
        for db in self._dbs:
            db.close()
        for k, v in self._saved.items():
            setattr(t, k, v)
        self._tmp.cleanup()

    def cfg(self, cfg):
        with open(os.path.join(self.home, "config.json"), "w") as f:
            json.dump(cfg, f)

    def kinds(self):
        return [e["kind"] for e in self.fired]


class FakeProc:
    def poll(self):
        return None


def job(jid, **kw):
    """A job as the server sends it to a host."""
    return dict(t.SPEC, id=jid, name=jid, cmd="true", created=time.time(), **kw)


# ───────────────────────────────────────────────────────────────── parsers and formatters

class TestParsers(unittest.TestCase):
    def test_dur(self):
        for s, want in (("90", 90), ("90s", 90), ("30m", 1800), ("2h", 7200), ("1d", 86400), (" 1.5H ", 5400), (45, 45)):
            with self.subTest(s=s):
                self.assertEqual(t.dur(s), want)
        with self.assertRaises(ValueError):
            t.dur("soon")

    def test_size(self):
        for s, want in (("500", 500), ("500M", 500), ("8G", 8192), ("8gb", 8192), ("1T", 1 << 20), ("2048K", 2)):
            with self.subTest(s=s):
                self.assertEqual(t.size(s), want)

    def test_parse_progress(self):
        cases = {"3/10 epoch": (3, 10, "epoch"), "45% halfway": (45, 100, "halfway"), "2.5/5": (2.5, 5, ""),
                 "warming up": (None, None, "warming up"), "1/0 bad total": (None, None, "1/0 bad total"),
                 "": (None, None, ""), None: (None, None, "")}
        for s, want in cases.items():
            with self.subTest(s=s):
                self.assertEqual(t.parse_progress(s), want)

    def test_parse_cores(self):
        self.assertEqual(t.parse_cores("0-3"), [0, 1, 2, 3])
        self.assertEqual(t.parse_cores("0,2,4-5"), [0, 2, 4, 5])
        self.assertEqual(t.parse_cores("7"), [7])

    def test_check_cfg(self):
        self.assertEqual(t.check_cfg({"cores": "0-1"}), {"cores": "0-1"})
        for bad in ([], {"cores": "a-b"}, {"cores": 3}):
            with self.subTest(cfg=bad), self.assertRaises(t.Fail):
                t.check_cfg(bad)

    def test_job_name(self):
        self.assertEqual(t.job_name(" train-1 "), "train-1")
        for bad in ("", "has space", "x" * (t.NAME_MAX + 1), "tab\there"):
            with self.subTest(name=bad), self.assertRaises(Exception):
                t.job_name(bad)

    def test_stall_limit(self):
        self.assertEqual(t.stall_limit(dict(stall=0, expect=100), 0.5), 0)  # explicit off
        self.assertEqual(t.stall_limit(dict(stall=60, expect=100), 0.5), 60)  # explicit wins
        self.assertEqual(t.stall_limit(dict(stall=None, expect=1000), 0.1), 200)  # 2x the expected time per step
        self.assertEqual(t.stall_limit(dict(stall=None, expect=10, ), 0.01), t.STALL_FLOOR)
        self.assertEqual(t.stall_limit(dict(stall=None, expect=0), 0.5), 0)
        self.assertEqual(t.stall_limit(dict(stall=None, expect=100), None), 0)

    def test_fmt_dur(self):
        for s, want in ((0, "0s"), (7, "5s"), (59, "55s"), (61, "1m 00s"), (3725, "1h 02m"), (90000, "1d 01h"), (None, "0s"), (-5, "0s")):
            with self.subTest(s=s):
                self.assertEqual(t.fmt_dur(s), want)
        self.assertEqual(t.fmt_dur(7, q=1), "7s")

    def test_fmt_short(self):
        for s, want in ((45, "45s"), (840, "14m"), (11100, "3h05"), (187200, "2d04")):
            with self.subTest(s=s):
                self.assertEqual(t.fmt_short(s), want)

    def test_clean(self):
        self.assertEqual(t.clean("a\tb\x1b[31mc\n"), "a    b [31mc ")
        self.assertEqual(t.clean(None), "")


class TestPack(unittest.TestCase):
    def test_roundtrip(self):
        obj = dict(id="abc", n=3, nested=[1, {"x": None}], s="ünïcode")
        blob = t.pack(obj)
        self.assertIsInstance(blob, bytes)
        self.assertEqual(t.unpack(blob), obj)

    def test_legacy_plain_json_rows(self):
        self.assertEqual(t.unpack('{"a": 1}'), {"a": 1})  # rows written before compression


# ───────────────────────────────────────────────────────────────── host db

class TestHostDb(Base):
    def test_meta(self):
        db = t.hdb()
        self.assertIsNone(t.meta(db, "nope"))
        self.assertEqual(t.meta(db, "k", 1.5), "1.5")  # values come back as strings
        self.assertEqual(t.meta(db, "k"), "1.5")

    def test_epoch_is_stable(self):
        e1 = t.meta(t.hdb(), "epoch")
        self.assertTrue(e1)
        self.assertEqual(t.meta(t.hdb(), "epoch"), e1)

    def test_migrates_old_queue(self):
        """A v0.1 host.db without the newer columns keeps its jobs and gains the columns."""
        db = sqlite3.connect(os.path.join(self.home, "host.db"))
        db.executescript("CREATE TABLE jobs(id TEXT PRIMARY KEY, cmd TEXT, cwd TEXT, env TEXT, prio INT, cpus INT, mem_mb INT, "
                         "timeout REAL, retries INT, retry_on TEXT, retry_delay REAL, created REAL, state TEXT, attempt INT DEFAULT 0, "
                         "not_before REAL DEFAULT 0, cores TEXT, pid INT, rc INT, started REAL, ended REAL, peak_mb INT, cancel INT DEFAULT 0);"
                         "INSERT INTO jobs(id, cmd, state) VALUES('old1', 'sleep 1', 'queued');")
        db.close()
        db = t.hdb()
        cols = {r[1] for r in db.execute("PRAGMA table_info(jobs)")}
        self.assertTrue({"force", "name", "expect", "stall", "progress", "pause", "work", "owner", "origin"} <= cols)
        self.assertEqual(db.execute("SELECT state FROM jobs WHERE id='old1'").fetchone()[0], "queued")


class TestRpc(Base):
    def test_submit_is_idempotent(self):
        req = dict(submit=[job("j1")])
        t.rpc(req)
        t.rpc(req)  # the server may re-send anything
        db = t.hdb()
        self.assertEqual(db.execute("SELECT count(*) FROM jobs").fetchone()[0], 1)
        self.assertEqual(self.kinds(), ["queued"])
        self.assertEqual(t.state_of(db, "j1"), "queued")

    def test_cancel_queued(self):
        t.rpc(dict(submit=[job("j1")]))
        t.rpc(dict(cancel=["j1"]))
        t.rpc(dict(cancel=["j1"]))
        self.assertEqual(t.state_of(t.hdb(), "j1"), "cancelled")
        self.assertEqual(self.kinds(), ["queued", "cancelled"])
        self.assertTrue(self.fired[-1]["data"]["final"])

    def test_cancel_running_only_sets_flag(self):
        t.rpc(dict(submit=[job("j1")]))
        db = t.hdb()
        db.execute("UPDATE jobs SET state='running' WHERE id='j1'")
        t.rpc(dict(cancel=["j1"]))
        r = db.execute("SELECT state, cancel FROM jobs WHERE id='j1'").fetchone()
        self.assertEqual((r[0], r[1]), ("running", 1))  # the watchdog kills it

    def test_withdraw_then_resubmit_requeues(self):
        t.rpc(dict(submit=[job("j1")]))
        out = t.rpc(dict(withdraw=["j1"]))
        self.assertEqual(out["withdrawn"], {"j1": "moved"})
        t.rpc(dict(submit=[job("j1", prio=7)]))
        r = t.hdb().execute("SELECT state, prio FROM jobs WHERE id='j1'").fetchone()
        self.assertEqual((r[0], r[1]), ("queued", 7))
        self.assertEqual(self.kinds(), ["queued", "moved", "queued"])

    def test_events_after_cursor(self):
        t.rpc(dict(submit=[job("j1"), job("j2")]))
        evs = t.rpc(dict(after=0))["events"]
        self.assertEqual([(e["job"], e["kind"]) for e in evs], [("j1", "queued"), ("j2", "queued")])
        self.assertEqual(t.rpc(dict(after=evs[0]["eid"]))["events"][0]["job"], "j2")
        self.assertEqual(t.rpc(dict(after=evs[-1]["eid"]))["events"], [])

    def test_prio_update(self):
        t.rpc(dict(submit=[job("j1")]))
        t.rpc(dict(prio={"j1": "9"}))
        self.assertEqual(t.hdb().execute("SELECT prio FROM jobs WHERE id='j1'").fetchone()[0], 9)
        self.assertEqual(self.fired[-1]["kind"], "update")

    def test_revives_dead_agent(self):
        t.agent_alive = lambda: False
        t.rpc({})
        self.assertIn(["agent"], self.launched)


class TestFinish(Base):
    def start(self, jid="j1", **kw):
        t.rpc(dict(submit=[job(jid, **kw)]))
        db = t.hdb()
        db.execute("UPDATE jobs SET state='running', attempt=attempt+1, started=? WHERE id=?", (time.time(), jid))
        return db

    def test_retryable_failure_requeues_with_backoff(self):
        db = self.start(retries=2, retry_delay=30)
        t.finish(db, "j1", "error", rc=1)
        r = db.execute("SELECT state, not_before FROM jobs WHERE id='j1'").fetchone()
        self.assertEqual(r[0], "queued")
        self.assertAlmostEqual(r[1] - time.time(), 30, delta=2)
        self.assertFalse(self.fired[-1]["data"]["final"])
        db.execute("UPDATE jobs SET state='running', attempt=2 WHERE id='j1'")
        t.finish(db, "j1", "error", rc=1)
        self.assertAlmostEqual(db.execute("SELECT not_before FROM jobs WHERE id='j1'").fetchone()[0] - time.time(), 60, delta=2)

    def test_retries_exhausted_is_final(self):
        db = self.start(retries=0)
        t.finish(db, "j1", "error", rc=1)
        self.assertEqual(t.state_of(db, "j1"), "error")
        self.assertTrue(self.fired[-1]["data"]["final"])

    def test_not_in_retry_on_is_final(self):
        db = self.start(retries=3, retry_on="error")
        t.finish(db, "j1", "timeout")
        self.assertEqual(t.state_of(db, "j1"), "timeout")

    def test_cancel_flag_wins_over_failure(self):
        db = self.start(retries=3)
        db.execute("UPDATE jobs SET cancel=1 WHERE id='j1'")
        t.finish(db, "j1", "error", rc=143)
        self.assertEqual(t.state_of(db, "j1"), "cancelled")

    def test_final_log_is_compressed(self):
        db = self.start()
        os.makedirs(os.path.join(self.home, "logs"), exist_ok=True)
        with open(os.path.join(self.home, "logs", "j1.log"), "w") as f:
            f.write("line one\nlast line\n")
        t.finish(db, "j1", "done", rc=0)
        self.assertFalse(os.path.exists(os.path.join(self.home, "logs", "j1.log")))
        self.assertTrue(os.path.exists(os.path.join(self.home, "logs", "j1.log.zst")))
        self.assertTrue(t.log_tail("j1").endswith("last line\n"))
        self.assertEqual(t.log_tail("j1", 5), "line\n")


class TestSchedule(Base):
    def queue(self, *specs):
        t.rpc(dict(submit=[job(jid, **kw) for jid, kw in specs]))
        return t.hdb()

    def started(self, db):
        return sorted(r[0] for r in db.execute("SELECT id FROM jobs WHERE state='running'"))

    def test_priority_order(self):
        db = self.queue(("lo", dict(prio=1)), ("hi", dict(prio=5)))
        t.schedule(db, dict(cores="0"), [])
        self.assertEqual(self.started(db), ["hi"])
        self.assertEqual(self.launched, [["_run", "hi"]])

    def test_no_backfill_past_a_job_that_does_not_fit(self):
        db = self.queue(("big", dict(prio=5, cpus=2)), ("small", dict(prio=1, cpus=1)))
        db.execute("INSERT INTO jobs(id, state, cores, cpus, prio, created) VALUES('busy', 'running', '0', 1, 0, 0)")
        t.schedule(db, dict(cores="0-1"), [])
        self.assertEqual(self.started(db), ["busy"])

    def test_forced_job_starts_on_busy_cores(self):
        db = self.queue(("big", dict(prio=5, cpus=2)), ("small", dict(prio=1, cpus=1)))
        db.execute("INSERT INTO jobs(id, state, cores, cpus, prio, created) VALUES('busy', 'running', '0,1', 2, 0, 0)")
        t.rpc(dict(force=["small"]))
        t.schedule(db, dict(cores="0-1"), [])
        self.assertEqual(self.started(db), ["busy", "small"])

    def test_cores_are_pinned_disjointly(self):
        db = self.queue(("a", dict(cpus=1)), ("b", dict(cpus=1)), ("c", dict(cpus=1)))
        t.schedule(db, dict(cores="0-1"), [])
        cores = sorted(r[0] for r in db.execute("SELECT cores FROM jobs WHERE state='running'"))
        self.assertEqual(cores, ["0", "1"])
        self.assertEqual(t.used_cores(db), {0, 1})

    def test_backoff_is_respected(self):
        db = self.queue(("a", {}))
        db.execute("UPDATE jobs SET not_before=? WHERE id='a'", (time.time() + 60,))
        t.schedule(db, dict(cores="0"), [])
        self.assertEqual(self.started(db), [])


class TestCheckDisk(Base):
    def setUp(self):
        super().setUp()
        self.free = {"~": 1.0, "/tmp": 50.0}
        t.disk_free = lambda: dict(self.free)
        self.db = t.hdb()

    def disk_events(self):
        return self.db.execute("SELECT count(*) FROM events WHERE kind='critical_disk'").fetchone()[0]

    def test_alerts_once_then_rate_limits(self):
        t.check_disk(self.db)
        t.check_disk(self.db)
        self.assertEqual(self.disk_events(), 1)
        self.assertEqual(self.kinds(), ["critical_disk"])
        ev = self.fired[0]
        self.assertIsNone(ev["job"])
        self.assertEqual(ev["data"], {"~": 1.0, "min_gb": t.DISK_MIN_GB})

    def test_rewarns_after_interval(self):
        t.check_disk(self.db)
        t.meta(self.db, "disk_warn", time.time() - t.DISK_REWARN_S - 1)
        t._disk_warned[0] = 0.0
        t.check_disk(self.db)
        self.assertEqual(self.disk_events(), 2)

    def test_recovery_writes_once_and_rearms(self):
        t.check_disk(self.db)
        self.free["~"] = 100.0
        writes = []
        self.db.set_trace_callback(lambda q: writes.append(q) if q.split()[0] in ("INSERT", "DELETE", "UPDATE") else None)
        for _ in range(5):
            t.check_disk(self.db)
        self.db.set_trace_callback(None)
        self.assertEqual(len(writes), 1)  # one delete, not a write per tick
        self.assertIsNone(t.meta(self.db, "disk_warn"))
        self.free["~"] = 1.0
        t.check_disk(self.db)
        self.assertEqual(self.disk_events(), 2)  # a new drop alerts at once

    def test_full_disk_still_fires_hook(self):
        def full(db):
            raise sqlite3.OperationalError("database or disk is full")
        t.tx = full
        t.check_disk(self.db)
        t.check_disk(self.db)  # rate-limited in memory although nothing could be stored
        self.assertEqual(self.kinds(), ["critical_disk"])
        self.assertIsNone(self.fired[0]["eid"])

    def test_status_reports_disk_without_note(self):
        self.free["~"] = 0.5
        st = t.status(self.db, dict(cores="0"))
        self.assertEqual(st["disk_free"]["~"], 0.5)
        self.assertFalse(any("disk" in w for w in st["warnings"]))  # the red Disk cell says it


# ───────────────────────────────────────────────────────────────── server db

class ServerBase(Base):
    def setUp(self):
        super().setUp()
        self.db = t.sdb()
        self.db.execute("INSERT INTO hosts(name, dest) VALUES('h1', 'local:/nowhere')")

    def ev(self, eid, jid="j1", kind="queued", state="queued", **data):
        return dict(eid=eid, ts=time.time(), job=jid, kind=kind,
                    data=dict(id=jid, prio=0, state=state, created=1.0, **data) if jid else data)


class TestIngest(ServerBase):
    def test_exactly_once(self):
        evs = [self.ev(1), self.ev(2, kind="start", state="running")]
        self.assertEqual(t.ingest(self.db, "h1", "E", evs), 2)
        self.assertEqual(t.ingest(self.db, "h1", "E", evs), 2)  # pull and phone-home overlap
        self.assertEqual(self.db.execute("SELECT count(*) FROM events").fetchone()[0], 2)
        self.assertEqual(self.kinds(), ["queued", "start"])
        self.assertEqual(self.db.execute("SELECT state, sent FROM jobs WHERE id='j1'").fetchone()[:], ("running", 1))

    def test_out_of_order_keeps_newest_state(self):
        t.ingest(self.db, "h1", "E", [self.ev(2, kind="done", state="done")])
        t.ingest(self.db, "h1", "E", [self.ev(1, kind="start", state="running")])
        self.assertEqual(self.db.execute("SELECT state FROM jobs WHERE id='j1'").fetchone()[0], "done")

    def test_host_level_event_has_no_job_row(self):
        t.ingest(self.db, "h1", "E", [self.ev(1, jid=None, kind="critical_disk", **{"~": 1.0})])
        self.assertEqual(self.db.execute("SELECT count(*) FROM jobs").fetchone()[0], 0)
        self.assertEqual(self.kinds(), ["critical_disk"])
        self.assertEqual(self.fired[0]["host"], "h1")

    def test_new_epoch_resends_unfinished_jobs(self):
        t.ingest(self.db, "h1", "E1", [self.ev(1)])
        cursor = t.ingest(self.db, "h1", "E2", [self.ev(5, kind="start", state="running")])
        self.assertEqual(cursor, 0)  # events read against the stale cursor are dropped
        r = self.db.execute("SELECT state, sent, eid FROM jobs WHERE id='j1'").fetchone()
        self.assertEqual(r[:], ("pending", 0, 0))
        self.assertEqual(self.db.execute("SELECT count(*) FROM events").fetchone()[0], 0)

    def test_unknown_host(self):
        with self.assertRaises(t.Fail):
            t.ingest(self.db, "ghost", "E", [])


class TestServerJobs(ServerBase):
    def add(self, name, host=None, **spec):
        return t.add_job(self.db, dict(name=name, cmd="true", **spec), host=host)

    def test_add_job_unknown_host(self):
        with self.assertRaises(t.Fail):
            self.add("x", host="ghost")

    def test_migrates_old_server_db(self):
        """A server.db from before the indexes keeps its rows and gains them on connect; the first, mis-ordered
        per-host index goes."""
        self.db.close()
        for suffix in ("", "-wal", "-shm"):
            with contextlib.suppress(FileNotFoundError):
                os.remove(os.path.join(self.home, "server.db" + suffix))
        db = sqlite3.connect(os.path.join(self.home, "server.db"))
        db.executescript("CREATE TABLE jobs(id TEXT PRIMARY KEY, host TEXT, prio INT, state TEXT, created REAL,"
                         " sent INT DEFAULT 0, cancel INT DEFAULT 0, eid INT DEFAULT 0, j TEXT, stage TEXT);"
                         "CREATE INDEX jobs_out ON jobs(host, state, sent, cancel);"
                         "INSERT INTO jobs(id, host, state) VALUES('old1', 'h1', 'done');")
        db.close()
        db = t.sdb()
        idx = {r[1] for r in db.execute("PRAGMA index_list(jobs)")}
        self.assertTrue({"jobs_host", "jobs_queue"} <= idx)
        self.assertNotIn("jobs_out", idx)
        self.assertEqual(db.execute("SELECT state FROM jobs WHERE id='old1'").fetchone()[0], "done")
        db.close()

    def test_per_host_queries_use_the_whole_index(self):
        """The three per-host sync queries match every column of jobs_host, the cancel one too (state last)."""
        plan = lambda q, args: " ".join(r[3] for r in self.db.execute("EXPLAIN QUERY PLAN " + q, args))
        terminal = ",".join("?" * len(t.TERMINAL))
        for q, args, cols in (("SELECT id FROM jobs WHERE host=? AND sent=0 AND cancel=0 AND state='pending'", ("h1",),
                               "host=? AND sent=? AND cancel=? AND state=?"),
                              ("SELECT id FROM jobs WHERE host=? AND sent=1 AND cancel=1 AND state NOT IN (%s)" % terminal,
                               ("h1",) + t.TERMINAL, "host=? AND sent=? AND cancel=?"),
                              ("SELECT j FROM jobs WHERE host=? AND sent=0 AND state='pending'", ("h1",), "host=? AND sent=?")):
            with self.subTest(q=q):
                self.assertIn("USING INDEX jobs_host (%s" % cols, plan(q, args))

    def test_resolve(self):
        a = t.add_job(self.db, dict(name="train", cmd="true"), jid="abc12345")
        b = t.add_job(self.db, dict(name="train", cmd="true"), jid="abd99999")
        self.assertEqual(t.resolve(self.db, "abc"), a)
        with self.assertRaises(t.Fail):
            t.resolve(self.db, "ab")  # ambiguous prefix
        with self.assertRaises(t.Fail):
            t.resolve(self.db, "train")  # two unfinished jobs share the name
        self.db.execute("UPDATE jobs SET state='done' WHERE id=?", (a,))
        self.assertEqual(t.resolve(self.db, "train"), b)  # the unfinished one wins
        with self.assertRaises(t.Fail):
            t.resolve(self.db, "nothing")

    def test_cancel_unsent_job_never_reaches_host(self):
        jid = self.add("x")
        t.cancel(self.db, jid)
        self.assertEqual(self.db.execute("SELECT state, cancel FROM jobs WHERE id=?", (jid,)).fetchone()[:], ("cancelled", 1))
        self.assertEqual(self.kinds(), ["cancelled"])

    def status(self, name, free, cores=None, queued=0, seen=None, err=None):
        self.db.execute("INSERT OR REPLACE INTO hosts(name, dest, seen, err, status) VALUES(?,?,?,?,?)",
                        (name, "local", time.time() if seen is None else seen, err,
                         json.dumps(dict(free=free, cores=cores or free, queued_cpus=queued))))

    def test_dispatch_picks_most_idle_live_host(self):
        self.status("h1", 2)
        self.status("h2", 8)
        self.status("stale", 64, seen=time.time() - 3600)
        self.status("down", 64, err="unreachable")
        jid = self.add("x", cpus=2)
        self.assertEqual(t.dispatch(self.db), {"h2"})
        self.assertEqual(self.db.execute("SELECT host FROM jobs WHERE id=?", (jid,)).fetchone()[0], "h2")

    def test_dispatch_counts_unsent_jobs(self):
        self.status("h1", 4)
        for i in range(3):
            self.add("x%d" % i, cpus=2)
        t.dispatch(self.db)
        hosts = [r[0] for r in self.db.execute("SELECT host FROM jobs ORDER BY created")]
        self.assertEqual(hosts, ["h1", "h1", None])  # the third does not fit: it waits on the server

    def test_dispatch_priority_first(self):
        self.status("h1", 1)
        lo = self.add("lo", prio=0)
        hi = self.add("hi", prio=9)
        t.dispatch(self.db)
        placed = {r[0]: r[1] for r in self.db.execute("SELECT id, host FROM jobs")}
        self.assertEqual((placed[hi], placed[lo]), ("h1", None))


class TestJobList(ServerBase):
    """The dashboard's cached job list must always equal a fresh load_jobs: order, counts, search, positions."""

    def setUp(self):
        super().setUp()
        self.rng = __import__("random").Random(7)
        self.cache = t.JobList()
        self.clocks = t.host_clocks([])

    def add(self, n):
        for _ in range(n):
            t.add_job(self.db, dict(name=self.rng.choice(["train", "eval", "sweep"]), cmd="python run.py --seed %d" % self.rng.randrange(99),
                                    prio=self.rng.randrange(3), owner=self.rng.choice(["clau-a-x", "maki-b-y", None])))

    def churn(self, n):
        """What syncs do to rows: state changes (with an end time), re-prioritising, being placed on a host."""
        ids = [r[0] for r in self.db.execute("SELECT id FROM jobs")]
        for jid in self.rng.sample(ids, min(n, len(ids))):
            b = t.unpack(self.db.execute("SELECT j FROM jobs WHERE id=?", (jid,)).fetchone()[0])
            state = self.rng.choice(["pending", "queued", "running", "done", "error", "cancelled"])
            b.update(state=state, ended=time.time() + self.rng.random() if state in t.TERMINAL else None,
                     started=time.time() if state != "pending" else None)
            self.db.execute("UPDATE jobs SET state=?, prio=?, host=?, j=? WHERE id=?",
                            (state, self.rng.randrange(3), self.rng.choice([None, "h1"]), t.pack(b), jid))

    def check(self):
        self.cache.refresh(self.db, self.clocks)
        fresh = t.load_jobs(self.db, [])
        self.assertEqual([j["id"] for j in self.cache.jobs], [j["id"] for j in fresh])
        want = {}
        for j in fresh:
            want[j["state"]] = want.get(j["state"], 0) + 1
        self.assertEqual({k: v for k, v in self.cache.counts.items() if v}, want)
        for query, hide in (("", True), ("train", False), ("train 1", False), ("clau", True), ("nothing-matches", False)):
            words = query.lower().split()
            expect = [j["id"] for j in fresh if not (hide and j["state"] in t.TERMINAL)
                      and all(w in ("%s %s %s %s %s %s" % (j["state"], j["host"], j["id"], j.get("name") or "", j["cmd"],
                                                         j.get("owner") or "")).lower() for w in words)]
            with self.subTest(query=query, hide=hide):
                shown = self.cache.shown(query, hide)
                self.assertEqual([j["id"] for j in shown], expect)
                for n in (0, len(shown) // 2, len(shown) - 1) if shown else ():
                    self.assertEqual(self.cache.position(shown, shown[n]["id"]), n)

    def test_rev_counts_every_insert_and_update(self):
        self.add(2)
        revs = [r[0] for r in self.db.execute("SELECT rev FROM jobs ORDER BY rev")]
        self.assertEqual(revs, [1, 2])
        self.db.execute("UPDATE jobs SET prio=5 WHERE rev=1")
        self.assertEqual(sorted(r[0] for r in self.db.execute("SELECT rev FROM jobs")), [2, 3])

    def test_old_server_db_gains_revs(self):
        self.db.execute("DROP TRIGGER jobs_rev_insert")
        self.db.execute("DROP TRIGGER jobs_rev_update")
        self.db.execute("DROP INDEX jobs_rev")
        self.db.execute("ALTER TABLE jobs DROP COLUMN rev")
        self.db.execute("INSERT INTO jobs(id, state, created, j) VALUES('old1', 'done', 1, ?)", (t.pack(dict(t.SPEC, id="old1", cmd="x", created=1.0)),))
        db = t.sdb()
        self.assertEqual(db.execute("SELECT rev FROM jobs WHERE id='old1'").fetchone()[0], 0)
        self.cache.refresh(db, self.clocks)  # rev 0 rows load on the first refresh
        self.assertEqual([j["id"] for j in self.cache.jobs], ["old1"])
        db.execute("UPDATE jobs SET state='error' WHERE id='old1'")
        self.assertGreater(db.execute("SELECT rev FROM jobs WHERE id='old1'").fetchone()[0], 0)

    def test_incremental_changes_match_a_full_load(self):
        self.add(120)
        self.check()  # first load: bulk
        for _ in range(12):  # small batches: each row moved into place
            self.churn(5)
            self.add(2)
            self.check()

    def test_bulk_changes_match_a_full_load(self):
        self.add(50)
        self.check()
        t.JobList.BULK, saved = 4, t.JobList.BULK
        try:
            self.churn(30)
            self.check()
        finally:
            t.JobList.BULK = saved

    def test_quiet_frames_read_nothing(self):
        self.add(10)
        self.assertTrue(self.cache.refresh(self.db, self.clocks))
        self.assertFalse(self.cache.refresh(self.db, self.clocks))
        first = self.cache.shown("train", False)
        self.assertIs(self.cache.shown("train", False), first)  # same search, no change: the same list

    def test_growing_search_narrows_previous_matches(self):
        self.add(40)
        self.cache.refresh(self.db, self.clocks)
        wide = self.cache.shown("python", False)
        narrow = self.cache.shown("python run.py --seed 1", False)
        self.assertTrue(set(map(id, narrow)) <= set(map(id, wide)))
        self.assertEqual([j["id"] for j in self.cache.shown("python", False)], [j["id"] for j in wide])  # shrinking: from scratch


# ───────────────────────────────────────────────────────────────── rendering and cli

class TestGpu(unittest.TestCase):
    def test_parse_gpu(self):
        self.assertEqual(t.parse_gpu("0, NVIDIA GeForce RTX 4070 Ti SUPER, 88, 9318, 16376, 238.63"),
                         dict(index=0, name="NVIDIA GeForce RTX 4070 Ti SUPER", util=88, mem_used=9318, mem_total=16376, power=238.63))
        g = t.parse_gpu("1, NVIDIA H100 80GB HBM3, [N/A], [N/A], 81559, [N/A]")  # unreported fields do not drop the GPU
        self.assertEqual((g["index"], g["util"], g["mem_used"], g["mem_total"], g["power"]), (1, None, None, 81559, None))

    def test_gpu_cell(self):
        g = lambda i, u, mu, mt: dict(index=i, name="NVIDIA RTX A6000", util=u, mem_used=mu, mem_total=mt, power=100.0)
        cases = (([], "-"),
                 ([g(0, 88, 9318, 12282)], "88% 9.1/12G :0"),
                 ([g(0, None, None, 81559)], "?% ?/80G :0"),
                 ([dict(util=50, mem_used=1000, mem_total=8000)], "50% 1.0/7.8G"),  # host not yet upgraded: no index
                 ([g(1, 0, 300, 24576), g(0, 100, 24000, 24564)], "100% 23/24G | 0% 0.3/24G"),  # index order
                 ([g(0, 7, 100, 8192), g(1, None, None, 8192), g(2, 50, 4096, 0)], "7% 0.1/8.0G | ?% ?/8.0G | 50%"))
        for gpus, want in cases:
            with self.subTest(want=want):
                self.assertEqual(t.gpu_cell(gpus), want)

    def test_gpu_width_fits_the_widest_host(self):
        hosts = [dict(s={}), dict(s=dict(gpus=[dict(index=0, util=5, mem_used=1, mem_total=8192)])),
                 dict(s=dict(gpus=[dict(index=i, util=100, mem_used=80000, mem_total=81559) for i in range(8)]))]
        self.assertEqual(t.gpu_width(hosts[:1]), 3)
        self.assertEqual(t.gpu_width(hosts), len(t.gpu_cell(hosts[2]["s"]["gpus"])))

class TestPower(Base):
    def setUp(self):
        super().setUp()
        self._rapl, t.RAPL = t.RAPL, os.path.join(self.home, "powercap")
        self._system_w, t.system_w = t.system_w, lambda: None
        self.db = t.hdb()

    def tearDown(self):
        t.RAPL, t.system_w = self._rapl, self._system_w
        super().tearDown()

    def domain(self, n, uj, name="package-0", mode=0o644):
        d = os.path.join(t.RAPL, n)
        os.makedirs(d, exist_ok=True)
        for f, v in (("energy_uj", uj), ("name", name), ("max_energy_range_uj", 262143328850)):
            with open(os.path.join(d, f), "w") as fh:
                fh.write("%s\n" % v)
        os.chmod(os.path.join(d, "energy_uj"), mode)

    def poll(self, dt=1.0):
        """Two readings dt apart: the first only stores a baseline."""
        t.rapl_w(self.db)
        prev = json.loads(t.meta(self.db, "power"))
        t.meta(self.db, "power", json.dumps(dict(prev, ts=prev["ts"] - dt)))

    def test_domains_skip_subdomains_and_mmio(self):
        for n in ("intel-rapl:0", "intel-rapl:0:0", "intel-rapl:1", "intel-rapl-mmio:0"):
            self.domain(n, 0)
        self.assertEqual(t.rapl_domains(), ["intel-rapl:0", "intel-rapl:1"])

    def test_packages_summed(self):
        self.domain("intel-rapl:0", 1_000_000)
        self.domain("intel-rapl:1", 1_000_000, "package-1")
        self.domain("intel-rapl-mmio:0", 1_000_000)  # same package again: must not double the reading
        self.poll()
        for n, uj in (("intel-rapl:0", 41_000_000), ("intel-rapl:1", 21_000_000), ("intel-rapl-mmio:0", 41_000_000)):
            self.domain(n, uj)
        w, kind = t.rapl_w(self.db)
        self.assertEqual(kind, "cpu")
        self.assertAlmostEqual(w, 60, delta=1)

    def test_psys_is_the_whole_machine(self):
        self.domain("intel-rapl:0", 0)
        self.domain("intel-rapl:1", 0, "psys")
        self.poll()
        self.domain("intel-rapl:0", 20_000_000)
        self.domain("intel-rapl:1", 35_000_000, "psys")
        w, kind = t.rapl_w(self.db)
        self.assertEqual(kind, "system")
        self.assertAlmostEqual(w, 35, delta=1)

    def test_counter_wrap(self):
        self.domain("intel-rapl:0", 262143328850 - 5_000_000)
        self.poll()
        self.domain("intel-rapl:0", 5_000_000)
        self.assertAlmostEqual(t.rapl_w(self.db)[0], 10, delta=0.5)

    def test_first_reading_is_only_a_baseline(self):
        self.domain("intel-rapl:0", 1)
        self.assertEqual(t.rapl_w(self.db), (None, None))

    @unittest.skipIf(os.geteuid() == 0, "root reads anything")
    def test_unreadable_rapl(self):
        self.domain("intel-rapl:0", 1, mode=0o400)
        os.chmod(os.path.join(t.RAPL, "intel-rapl:0", "energy_uj"), 0)
        self.assertIs(t.rapl_readable(), False)
        self.assertEqual(t.rapl_w(self.db), (None, None))
        self.assertIn("RAPL", t.install_notes(dict(agent=True, taskset=True, boot="systemd", mosh=True, rapl=False)))

    def test_no_rapl(self):
        self.assertIsNone(t.rapl_readable())
        self.assertEqual(t.power_parts(self.db, []), {})

    def test_parts_cpu_plus_gpu(self):
        self.domain("intel-rapl:0", 0)
        self.poll()
        self.domain("intel-rapl:0", 65_000_000)
        parts = t.power_parts(self.db, [dict(power=200.0), dict(power=39.0), dict(power=None)])
        self.assertAlmostEqual(parts["cpu"], 65, delta=1)
        self.assertEqual(parts["gpu"], 239.0)

    def test_parts_gpu_only_when_rapl_unreadable(self):
        self.assertEqual(t.power_parts(self.db, [dict(power=238.63)]), {"gpu": 238.63})

    def test_system_reading_already_includes_gpu(self):
        t.system_w = lambda: 90.0  # battery / macOS
        self.assertEqual(t.power_parts(self.db, [dict(power=50.0)]), {"system": 90.0})

    def test_power_note(self):
        self.assertEqual(t.power_note(dict(power_w=303.6, power_parts=dict(cpu=65, gpu=238.6))), "304W")
        self.assertEqual(t.power_note(dict(power_w=238.6, power_parts=dict(gpu=238.6))), "gpu 239W")
        self.assertEqual(t.power_note(dict(power_w=12.0, power_parts=dict(system=12.0))), "12W")
        self.assertEqual(t.power_note(dict(power_w=65.0)), "65W")  # host not yet upgraded: no breakdown
        self.assertEqual(t.power_note(dict(power_w=None)), "")
        self.assertIn("gpu 239W", row(status(power_w=238.6, power_parts=dict(gpu=238.6))))


def status(**kw):
    s = dict(load=0.4, cpu_pct=6, free=32, cores=32, mem_avail_mb=118 << 10, mem_total_mb=125 << 10, mem_used_mb=2048,
             mem_reserved_mb=8192, gpus=[], counts={}, cpu_ghz=3.1, power_w=None, agent=True, version=t.VERSION,
             disk_free={"~": 412, "/tmp": 3.5})
    s.update(kw)
    return s


def row(s, **h):
    return "".join(x for _, x in t.host_segs(dict(dict(name="gpu1", err=None, seen=time.time(), s=s), **h)))


class TestHostRow(unittest.TestCase):
    def col(self, text, name):
        """The text under column `name`: the header right-aligns, so take the header cell's span."""
        m = re.search(r"\s*" + re.escape(name), t.host_head())
        return text[m.start():m.end()].strip()

    def test_disk_cell(self):
        for dfree, want in (({"~": 412, "/tmp": 3.5}, "412~ 3.5t"), ({"~": 18}, "18~"), ({}, ""),
                            ({"~": 9999, "/tmp": 12345}, "9999~ 12Tt"), ({"~": 0.4}, "0.4~")):
            with self.subTest(dfree=dfree):
                self.assertEqual(self.col(row(status(disk_free=dfree)), "Disk free"), want)

    def test_columns_stay_aligned(self):
        """Every cell before Notes has a fixed width, whatever the values: Notes starts where its header does,
        with the GPU column sized to the widest host in the table."""
        eight = [dict(index=i, util=100, mem_used=80000, mem_total=81559) for i in range(8)]
        table = [status(), status(disk_free={"~": 9999, "/tmp": 12345}), status(disk_free={}),
                 status(gpus=[dict(util=50)] * 3, cpu_pct=150, mem_used_mb=9000, power_w=65.0),
                 status(gpus=[dict(index=0, util=88, mem_used=9318, mem_total=12282)]), status(gpus=eight), {}]
        hosts = [dict(name="a-very-long-hostname", err=None if s else "ssh: timeout", seen=time.time(), s=s) for s in table]
        gw = t.gpu_width(hosts)
        notes_at = t.host_head(gw).index("Notes")
        for h in hosts:
            with self.subTest(s=h["s"]):
                self.assertEqual(sum(len(x) for _, x in t.host_segs(h, gw)[:-1]), notes_at)

    def test_notes(self):
        self.assertIn("run tshed install", row(status(version="0.1")))
        self.assertIn("agent down", row(status(agent=False)))
        self.assertIn("ssh: no route", row(status(), err="ssh: no route"))
        self.assertNotIn("disk", row(status(disk_free={"~": 0.1})))


SID = "cf93301b-ae23-4aed-8cf3-f5ee3fa033ed"
NO_AGENT = {k: "" for k in ("CLAUDE_CODE_SESSION_ID", "MAKI_SESSION_ID", "TINYSHED_OWNER", "TINYSHED_JOB")}


@unittest.skipUnless(shutil.which("git"), "needs git")
class TestOwner(Base):
    def setUp(self):
        super().setUp()
        self.repo = os.path.join(self.home, "my-proj")
        git = lambda *a, cwd=self.repo: subprocess.run(["git", *a], cwd=cwd, check=True, capture_output=True)
        os.makedirs(self.repo)
        git("init", "-q", "-b", "main")
        git("-c", "user.email=t@t", "-c", "user.name=t", "commit", "-q", "--allow-empty", "-m", "init")
        self.wt = os.path.join(self.home, "my-proj-feature")
        git("worktree", "add", "-q", "-b", "feature", self.wt)
        os.makedirs(os.path.join(self.repo, "sub"))

    def origin(self, cwd=None, **env):
        with mock.patch.dict(os.environ, dict(NO_AGENT, **env)):
            return t.owner_origin(cwd or self.repo)

    def test_claude_session(self):
        oid, o = self.origin(os.path.join(self.repo, "sub"), CLAUDE_CODE_SESSION_ID=SID)
        self.assertEqual(oid, "clau-my-proj-" + SID)
        self.assertEqual((o["agent"], o["session"], o["root"], o["branch"]), ("clau", SID, "my-proj", "main"))
        self.assertEqual(os.path.realpath(o["repo"]), os.path.realpath(self.repo))
        self.assertNotIn("worktree", o)
        self.assertEqual(len(o["commit"]), 40)

    def test_worktree_shares_the_project_root(self):
        oid, o = self.origin(self.wt, MAKI_SESSION_ID=SID)
        self.assertEqual(oid, "maki-my-proj-" + SID)  # the main repository's folder, not the worktree's
        self.assertEqual(o["branch"], "feature")
        self.assertEqual(os.path.realpath(o["worktree"]), os.path.realpath(self.wt))

    def test_person_outside_git(self):
        plain = os.path.join(self.home, "scratch dir")
        os.makedirs(plain)
        oid, o = self.origin(plain, USER="ana")
        self.assertEqual(oid, "user-scratch_dir")
        self.assertEqual((o["agent"], o["session"], o["user"]), ("user", None, "ana"))
        self.assertNotIn("repo", o)

    def test_inherited_inside_a_job(self):
        oid, o = self.origin(TINYSHED_OWNER="clau-my-proj-" + SID, TINYSHED_JOB="abcd1234")
        self.assertEqual(oid, "clau-my-proj-" + SID)
        self.assertEqual((o["parent"], o["agent"], o["session"]), ("abcd1234", "clau", SID))

    def test_explicit_owner(self):
        with mock.patch.dict(os.environ, dict(NO_AGENT, CLAUDE_CODE_SESSION_ID=SID)):
            oid, o = t.owner_origin(self.repo, explicit="nightly-ci")
        self.assertEqual(oid, "nightly-ci")
        self.assertNotIn("parent", o)
        for bad in ("", "has space", "x" * 129):
            with self.subTest(owner=bad), self.assertRaises(Exception):
                t.owner_id(bad)

    def test_parse_owner(self):
        self.assertEqual(t.parse_owner("clau-my-proj-" + SID), dict(agent="clau", root="my-proj", session=SID))
        self.assertEqual(t.parse_owner("maki-my-proj-2RDrm2Mmo11eosunnUWMWy"),  # maki's base58 session ids
                         dict(agent="maki", root="my-proj", session="2RDrm2Mmo11eosunnUWMWy"))
        for other in ("user-my-proj", "nightly-ci", "clau-x-not0a0uuid", "maki-x-has0zero", None):
            with self.subTest(owner=other):
                self.assertIsNone(t.parse_owner(other))

    def test_session_file(self):
        fake = os.path.join(self.home, "userhome")
        claude = os.path.join(fake, ".claude", "projects", "-Users-x-my-proj")
        os.makedirs(claude)
        open(os.path.join(claude, SID + ".jsonl"), "w").close()
        os.makedirs(os.path.join(fake, ".maki", "sessions"))
        open(os.path.join(fake, ".maki", "sessions", SID + ".jsonl"), "w").close()
        with mock.patch.dict(os.environ, HOME=fake):
            self.assertEqual(t.session_file(dict(agent="clau", session=SID)), os.path.join(claude, SID + ".jsonl"))
            self.assertTrue(t.session_file(dict(agent="maki", session=SID)).endswith(".maki/sessions/%s.jsonl" % SID))
            self.assertIsNone(t.session_file(dict(agent="clau", session="0" * 8 + SID[8:])))
            self.assertIsNone(t.session_file(dict(agent="user")))
            self.assertIsNone(t.session_file(dict(agent="maki", session="../../etc/passwd")))

    def test_owner_travels_to_the_host_and_back(self):
        origin = json.dumps(dict(agent="clau", session=SID, repo=self.repo))
        t.rpc(dict(submit=[job("j1", owner="clau-my-proj-" + SID, origin=origin)]))
        r = t.hdb().execute("SELECT owner, origin FROM jobs WHERE id='j1'").fetchone()
        self.assertEqual((r[0], r[1]), ("clau-my-proj-" + SID, origin))
        self.assertEqual(self.fired[-1]["data"]["owner"], "clau-my-proj-" + SID)  # events carry it to the server mirror

    def test_submit_records_owner_and_lookup_prefers_direct_origin(self):
        db = t.sdb()
        with mock.patch.dict(os.environ, dict(NO_AGENT, CLAUDE_CODE_SESSION_ID=SID)), contextlib.chdir(self.wt):
            jid = t.submit(db, ["-n", "train", "--", "true"])
        with mock.patch.dict(os.environ, dict(NO_AGENT, TINYSHED_OWNER="clau-my-proj-" + SID, TINYSHED_JOB=jid)), \
                contextlib.chdir(self.home):
            t.submit(db, ["-n", "child", "--", "true"])
        b = t.unpack(db.execute("SELECT j FROM jobs WHERE id=?", (jid,)).fetchone()[0])
        self.assertEqual(b["owner"], "clau-my-proj-" + SID)
        (g,) = t.owners(db, "my-proj")
        self.assertEqual((g["jobs"], g["states"]), (2, {"pending": 2}))
        self.assertEqual(g["origin"]["branch"], "feature")  # the session's own submit, not its job's
        self.assertEqual(g["origin"]["session"], SID)
        self.assertEqual(t.owners(db, "nobody"), [])


class TestCli(Base):
    def run_cli(self, *argv):
        saved = sys.argv
        sys.argv = ["tshed"] + list(argv)
        buf = io.StringIO()
        try:
            with contextlib.redirect_stdout(buf), contextlib.redirect_stderr(io.StringIO()):
                t.main()
        finally:
            sys.argv = saved
        return buf.getvalue()

    def test_hosts_shows_last_known_state_without_syncing(self):
        def boom():
            raise AssertionError("hosts must not contact hosts")
        t.sync_all = boom
        db = t.sdb()
        db.execute("INSERT INTO hosts(name, dest, seen, status) VALUES('gpu1', 'gpu1.local', ?, ?)",
                   (time.time() - 30, json.dumps(status())))
        out = self.run_cli("hosts")
        self.assertIn("gpu1", out)
        self.assertIn("30s", out)  # Seen: the age of what is shown
        js = json.loads(self.run_cli("hosts", "--json"))
        self.assertEqual(js["name"], "gpu1")
        self.assertAlmostEqual(js["seen"], time.time() - 30, delta=5)

    def test_hosts_rm(self):
        t.sdb().execute("INSERT INTO hosts(name, dest) VALUES('gpu1', 'x')")
        self.run_cli("hosts", "--rm", "gpu1")
        self.assertEqual(t.sdb().execute("SELECT count(*) FROM hosts").fetchone()[0], 0)
        with self.assertRaises(t.Fail):
            self.run_cli("hosts", "--rm", "gpu1")

    def test_ls_owner_filters_and_owner_command(self):
        t.sync_all = lambda: None
        db = t.sdb()
        for name, owner in (("a", "clau-proj-" + SID), ("b", "maki-other-" + SID), ("c", None)):
            t.add_job(db, dict(name=name, cmd="true", owner=owner))
        names = lambda *argv: sorted(json.loads(l)["name"] for l in self.run_cli("ls", "--json", *argv).splitlines())
        self.assertEqual(names(), ["a", "b", "c"])
        self.assertEqual(names("--owner", "proj"), ["a"])
        self.assertEqual(names("--owner", SID[:8]), ["a", "b"])
        with mock.patch.dict(os.environ, dict(NO_AGENT, TINYSHED_OWNER="maki-other-" + SID)):
            self.assertEqual(names("--mine"), ["b"])
            self.assertEqual(self.run_cli("owner").splitlines()[0], "maki-other-" + SID)
        out = self.run_cli("owner", "proj")
        self.assertIn("clau-proj-" + SID, out)
        self.assertIn("1 pending", out)
        with self.assertRaises(t.Fail):
            self.run_cli("owner", "nobody")

    def test_completion_covers_public_commands(self):
        p, sub, public = t.build_parser()
        self.assertIn("hosts", public)
        for shell in ("bash", "zsh", "fish"):
            with self.subTest(shell=shell):
                script = t.completion(shell, sub, public)
                for name in public:
                    self.assertIn(name, script)
        self.assertNotIn("_run", public)  # internal commands have no help= and stay hidden


if __name__ == "__main__":
    unittest.main()

# AGENTS.md — tinyshed

Priority-queue scheduler for long jobs: one server, N hosts, ssh between. Everything is in
`tinyshed.py`; the same file runs on the server and is copied to each host. The user-facing command
is `tshed` (a symlink to it). Never call it `ts`: that name belongs to TypeScript tooling.

To *use* tinyshed (submit/monitor jobs) read `skills/tinyshed/SKILL.md` instead. This file is for
changing the code.

## Files

| file | what |
|---|---|
| `tinyshed.py` | all code; sections in order: shared (sqlite, hooks, zstd) → host state → watchdog → agent → rpc/setup → server state/transport → server ops → tables + TUI → cli |
| `README.md` | reference: design, commands, hooks, limits |
| `HUMANS.md` | tutorial |
| `skills/tinyshed/SKILL.md` | agent usage skill |
| `btop-themes/` | btop `.theme` files matching the built-in `tailwind-*` themes (for btop itself; tinyshed does not read them) |
| `IDEAS_HEAP.md` | considered, not scheduled (binary packaging). Do not build from it unasked |
| `test_tinyshed.py` | unit tests (stdlib unittest; temp `TINYSHED_HOME`, no ssh, no daemons). Not installed on hosts |

## Invariants — do not break

- **One file, stdlib only**, Python ≥ 3.12. Install is `cat > ~/.tinyshed/tinyshed.py` over ssh.
- **Host is authoritative.** `host.db` (sqlite WAL) owns the queue and event log; `server.db` is a
  mirror built only from events (`ingest`). Never make a host wait on the server.
- **Every rpc op is idempotent** (`INSERT OR IGNORE` on job id, cancel flag, event cursor). The server
  may re-send anything.
- **Events are exactly-once on the server**: PK `(host, eid)`; pull (`sync_host`) and phone-home
  (`ingest` cli) overlap. Hooks fire only for newly inserted events, after commit.
- **Job state changes go through `transition`/`finish`** (update + event in one transaction). Claims
  are conditional `UPDATE … WHERE state='queued'`.
- **Watchdog liveness = flock** on `run/ID.lock`, inherited from the agent. No pid checks.
- **Nothing is pruned; everything at rest is zstd** — except `--git`/`--push` work dirs on hosts, which are
  scratch: deleted (`clean_job`, server-driven) once the job ended and its `--pull` succeeded. Snapshots
  (server store + host mirror), logs and job records stay. Everything kept is zstd (`pack`/`unpack` for payloads, `compress_log` on
  final state). No fallback codec: `zmod()` exits if zstd is missing.
- **Remote commands go through `sh -c`** (`remote()`): hosts' login shells include fish.
- **Completions are generated from argparse** (`completion()`); public commands are the ones with `help=`.
- **cli and TUI share row renderers** (`host_segs`/`job_segs`/`meta_segs` → `styled`); the theme follows gitss.
- Watchdogs are detached from the agent; `install` must stay safe to run while jobs are running.
- Schema changes need a migration path: existing `host.db` files hold live queues.

## Testing

Unit tests: `python3 -m unittest -v test_tinyshed` (under a second). They cover parsers, the host queue (rpc idempotency,
retries, scheduling), server ingest/dispatch/resolve, host-row rendering and the cli; each test gets its own temporary
`TINYSHED_HOME` and `detach`/`fire` are stubbed. Add a test with each fix. Agents, watchdogs and ssh are not covered:
exercise those with fake hosts on this machine. The default `~/.tinyshed` is the user's live server, with real hosts
and other people's running jobs: never test against it.

```sh
export TINYSHED_HOME=/tmp/ts/server
./tinyshed.py install a=local:/tmp/ts/a b=local:/tmp/ts/b --cores 0-1 --home local
./tinyshed.py submit -H a -t 2s -- sleep 30 ; ./tinyshed.py wait -i 1 <id> ; ./tinyshed.py ls -a
./tinyshed.py uninstall a b --purge ; rm -rf /tmp/ts     # agents are daemons: always clean up
```

- `local:DIR` hosts skip ssh; `--home local` exercises phone-home.
- Linux-only paths (`taskset`, `/proc` RSS, orphan kill, real sshd): run the same script in a Linux
  container. On macOS the RSS limit cannot be tested (compressed memory hides RSS).
- TUI: drive it through a pty (`pty.fork`, write keys, read frames) and keep draining its output.
- After code changes update README/HUMANS/SKILL where behaviour or flags changed.

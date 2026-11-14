# tinyshed

A minimal priority-queue scheduler for long jobs: one server commanding N hosts over ssh.
One file (`tinyshed.py`, run as `tshed`), stdlib only. Needs Python ≥ 3.12 and zstd on every machine: built in from
Python 3.14, otherwise the `zstd` binary or `pip install pyzstd` / `zstandard`. `install` refuses a host without it.

```
server                                  host (each)
~/.tinyshed/server.db  (mirror)         ~/.tinyshed/host.db   (authoritative, sqlite WAL)
tinyshed tui | serve  ── ssh: rpc ──▶   agent ──▶ watchdog ──▶ taskset -c … bash -c CMD
                      ◀─ ssh: ingest ── (phone home, optional)
```

## Quick start

```sh
ln -s $PWD/tinyshed.py ~/bin/tshed                                  # the command is `tshed`
tshed install gpu1=gpu1.local,gpu1 gpu2=me@gpu2 --home me@thisbox   # ~1s per host, in parallel
tshed hosts                                                         # ping: load, CPU%, idle cores, memory, GPUs
tshed submit -p 5 -c 8 -m 32G -t 12h -r 2 -- python train.py --lr 3e-4
tshed submit -H gpu1 -C ~/proj -e CUDA_VISIBLE_DEVICES=0 -- ./run.sh
tshed                                                               # dashboard
tshed serve                                                         # or: headless server loop
```

| command | |
|---|---|
| `install [[NAME=]DEST…]` | check the host, copy the file, (re)start the agent, start it at boot, register. No hosts = upgrade all. `DEST` may list alternatives tried in order (`gpu1.local,gpu1`: LAN, then tailscale) or be `local[:DIR]`. Options left out keep their installed value |
| `uninstall NAME… [--purge] [--force]` | stop the agent, remove the boot entry, forget the host; data stays unless `--purge` |
| `hosts [--json] [--rm NAME]` | ping all hosts |
| `config [--edit] [--host HOST…]` | show, or edit in `$VISUAL`/`$EDITOR`, this machine's `config.json` (`name`, `cores`, `home`). `--host` (repeatable) reads/writes a registered host's config over ssh; edits are validated before saving. Dashboard: `C` |
| `overview [-n N] [-a]` | compact, non-interactive digest of `hosts` + `ls`: hosts table, `---LS` divider, active-jobs table, each capped at N lines (default 30). One sync feeds both; `-a` adds finished jobs |
| `submit [flags] -- CMD` | queue a job |
| `ls [-a] [--json]`, `show ID`, `logs [-f] ID` | list, inspect, read output (ids may be prefixes) |
| `wait ID… [-t 2h]` | block; exit 0 done / 1 failed / 2 timeout |
| `cancel ID…`, `prio ID N` | |
| `completion fish`/`zsh`/`bash` | tab completion, including host names and job ids |

Output is coloured on a terminal and plain in a pipe (`NO_COLOR` is respected). Tutorial: `HUMANS.md`;
agent skill: `skills/tinyshed/`.

## How it works

- **State.** Each host owns its queue and an append-only event log in `host.db`. The server keeps a
  mirror plus a per-host event cursor. Nothing on a host depends on the server being reachable: the
  agent keeps scheduling, retrying and logging, and the server catches up from the event log later.
- **Config.** Each host's `~/.tinyshed/config.json` holds `name`, `cores` (the CPU set jobs may use) and
  `home` (the phone-home command). `install --cores` sets `cores`; `tshed config --edit` or the
  dashboard's `C` edits the local file in `$EDITOR`, and `tshed config --host NAME --edit` edits a
  host's over ssh. Edits are validated (JSON + `cores`) before writing; the agent reloads config every
  second, so no restart is needed. Don't change `name` by hand: it must match the server's host registry.
- **CPU%.** The `hosts` table has two CPU numbers: `Load` is the system-wide 1-minute load average,
  and `CPU%` is the measured busy percentage of the cores tinyshed owns (`--cores`, or the process's
  CPU affinity). It comes from per-core OS tick counters (Linux `/proc/stat`, macOS
  `host_processor_info`) sampled between polls, so it is an average over the poll interval. It reads
  `-` until two samples exist or where the OS has no per-core counters. On macOS jobs are unpinned
  (no `taskset`), so a configured core subset is only a scheduling budget and its `CPU%` need not
  track job usage.
- **Transport.** Every server→host interaction is one `ssh host tinyshed.py rpc` call (JSON in/out)
  that delivers pending jobs and cancels, pulls events after the cursor and reads resources. All of it
  is idempotent, so retries are safe. ssh connections are multiplexed (`ControlMaster`), ~0.1s a poll.
- **Phone home.** With `--home`, agents push new events via `ssh server tinyshed.py ingest`, backing off
  while the server is unreachable. Push and pull overlap safely; each event is processed exactly once.
  The host's key can be restricted on the server with a forced command in `authorized_keys`
  (`command="…/tinyshed.py ingest"`, the exact command is in the host's `config.json`).
- **Scheduling.** Host-pinned jobs (`-H`) go straight to that host's queue. Unpinned jobs wait on the
  server and are bound, highest priority first, to the live host with the most idle cores (needs `tui`
  or `serve` running). On a host: highest `prio` first, FIFO within a priority, strictly — a big job
  at the head is not overtaken by smaller ones.
- **Watchdog.** One detached process per running job: pins it to its allotted cores with `taskset`,
  captures stdout+stderr to `~/.tinyshed/logs/ID.log` (zstd-compressed when the job ends), enforces the runtime timeout, polls the summed
  RSS of the job's process tree against the memory limit, and kills the whole process group (TERM,
  then KILL after 10s) on timeout / over-memory / cancel. If the watchdog itself dies (kill -9, reboot)
  the agent notices, kills the orphans and marks the attempt `lost`.
- **Retry.** `-r N` retries with exponential backoff on the kinds in `--retry-on` (default
  `error,lost`; add `timeout,oom` if those are transient for you).
- **Job states.** `pending` (still on server) → `queued` → `running` → `done | error | timeout | oom |
  lost | cancelled`. Jobs see `TINYSHED_JOB`, `TINYSHED_ATTEMPT`, `TINYSHED_CORES`.

## Callbacks

Put a `hooks.py` in `~/.tinyshed/` on the server (fires when an event is ingested) and/or on a host
(fires when it happens). Define any of:

`OnJobQueued` `OnJobStart` `OnJobCompletion` `OnJobError` `OnJobTimeout` `OnJobOOM` `OnJobLost`
`OnJobCancel` `OnHostDown` `OnHostUp` (server) `OnEvent` (everything)

```python
import subprocess

def OnJobCompletion(ev):                 # ev: host, job, kind, ts, data
    print(ev["host"], ev["job"], "finished in", ev["data"]["ended"] - ev["data"]["started"])

def OnJobError(ev):
    if ev["data"]["final"]:              # False while retries remain
        subprocess.run(["notify", ev["data"]["cmd"], ev["data"]["tail"][-500:]])
```

`ev["data"]` is the full job row (cmd, prio, attempt, rc, peak_mb, …) plus `final` and the last 2 kB
of output as `tail`. Hook output and tracebacks go to `~/.tinyshed/hooks.log`; the file is reloaded
when edited.

## TUI

Built like `gitss`: alternate screen, atomic synchronized frames, type-to-filter, capital command keys,
mouse wheel and click. `Enter`/`→` detail with a scrollable live log, `N` new job (same flags as
`submit`), `K` cancel, `E` rerun, `P` priority, `A` follow the log on the host (mosh), `O` shell on the
host (mosh), `H` hide finished, `R` refresh, `Esc`/`Q` quit. While open, the TUI is also the server loop.

## Notes and limits

- **Why ssh, not mosh or sqlite, for transport.** mosh is an interactive terminal protocol: it cannot
  carry stdin/stdout or exit codes of a command, and it needs ssh to start anyway — so it is used
  where it shines (`A`/`O`, `logs -f`) and ssh does the RPC. SQLite has no network protocol; SQLCipher
  only encrypts the file at rest and is not in the stdlib. ssh already gives encryption and auth.
- The memory limit is enforced by polling (every 2s), not by cgroups: a fast allocator can overshoot
  before it is killed, and shared pages are counted once per process.
- Jobs run via `bash -c` with the agent's (non-login) environment; use `bash -lc '…'`, `-e`, or
  absolute paths if you rely on conda/modules.
- `taskset` and tree-RSS via `/proc` are Linux features; elsewhere jobs run unpinned and RSS falls back
  to `ps` (on macOS compressed memory makes that unreliable).
- `install` starts the agent at boot: an `@reboot` crontab line, or where there is no crontab a
  systemd user unit plus `loginctl enable-linger` (`--no-boot` to skip). A dead agent is also revived
  by any contact from the server.
- Remote commands run through `sh -c`, so a host's login shell (fish, …) does not matter.
- Attach (`A`, `O`, `logs -f`) uses mosh only where it can work: tshed asks the host to listen on a UDP
  port in mosh's range (60000-61000) and sends it a datagram. No `mosh-server`, or the datagram is
  dropped by a firewall → `ssh -t` instead. (On macOS hosts the application firewall decides per
  program, so the probe can say "blocked" while mosh itself is allowed; the fallback is still safe.)
- GPUs are reported (`nvidia-smi`) but not allocated; pass `CUDA_VISIBLE_DEVICES` with `-e`.
- **Retention.** Nothing is ever pruned. To keep that cheap, everything at rest is zstd: job and event
  payloads are zstd blobs in sqlite, and a job's log becomes `logs/ID.log.zst` once the job is final
  (`logs`, the TUI and `A` read either form). Read a payload by hand with
  `sqlite3 host.db "select writefile('/dev/stdout', data) from events where id=1" | zstd -d`.

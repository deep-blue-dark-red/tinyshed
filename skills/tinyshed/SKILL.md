---
name: tinyshed
description: Run long jobs on the user's remote hosts with tinyshed / tshed (priority queue, timeout, memory limit, retries, captured logs). Use when asked to queue, schedule, run remotely, monitor, cancel or fetch logs of a long-running job, or to check what hosts have free cores/memory/GPUs.
---

# tinyshed

CLI: `tshed` (on PATH; source `tinyshed.py`). Run it from this machine (the
server); it reaches hosts over ssh itself. Never run bare `tshed` or `tshed tui` — that is the
interactive dashboard.

## Loop

```sh
tshed hosts [--json]          # hosts, up/down, load, CPU%, idle cores, memory, GPUs
tshed submit [flags] -- CMD   # prints: ID STATE HOST
tshed wait ID... [-t 2h]      # blocks; exit 0 all done, 1 any failed, 2 timeout
tshed show ID                 # state, limits, rc, times + last 2 kB of output
tshed logs ID [-n BYTES]      # tail of stdout+stderr (default 20000 bytes)
tshed ls [-a] --json          # one JSON object per job; -a includes finished
tshed overview [-n N] [-a]    # one-screen snapshot: hosts + active jobs (no dashboard)
tshed config [--edit] [--host HOST]  # show/edit config.json in $EDITOR (cores, name, home)
tshed cancel ID...            tshed prio ID N
```

IDs may be unique prefixes. Parse the ID as the first word of `submit` output.

## submit flags

| flag | meaning |
|---|---|
| `-H HOST` | pin to a host. **Always pass it** unless the user runs `tshed serve`/the dashboard: unpinned jobs stay `pending` until one of those is running |
| `-p N` | priority, higher first (default 0) |
| `-c N` | cores, pinned with taskset (default 1) |
| `-m 8G` | RSS limit for the process tree → state `oom` |
| `-t 12h` | runtime limit per attempt (`s m h d`) → state `timeout` |
| `-r N` | retries; `--retry-on error,lost` (default) may add `timeout,oom` |
| `-C DIR` | working directory on the host (default `~`) |
| `-e K=V` | env var, repeatable (e.g. `CUDA_VISIBLE_DEVICES=0`; GPUs are not allocated for you) |

CMD runs on the host as `bash -c` with a non-login environment: paths are the **host's**, not
local ones; use absolute paths or `bash -lc '…'` when conda/modules are needed. Quote CMD as one
argument if it contains shell syntax: `tshed submit -H gpu1 -- 'cd proj && make 2>&1 | tail'`.

## States

`pending` (not yet on a host) → `queued` → `running` → `done` | `error` | `timeout` | `oom` | `lost`
| `cancelled`. `lost` = the watchdog died (e.g. host reboot). A host at capacity runs strictly by
priority, so a queued job may wait behind a larger one.

## Rules

- The queue is shared with the user and other agents (`tshed ls` shows their jobs too). Do not
  cancel or reprioritise jobs you did not submit, and do not `tshed install`/`uninstall`,
  `tshed config --edit`, or edit `~/.tinyshed/hooks.py`, unless asked.
- For jobs longer than a few minutes, do not block on `tshed wait` without `-t`; report the ID and
  stop, or wait with a timeout and check back.
- `tshed hosts` shows `down` + the ssh error for unreachable hosts. Jobs already on a host keep
  running while it is unreachable; state and logs catch up on reconnect.
- No hosts listed → nothing is installed yet; ask the user which hosts to use.

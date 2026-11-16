---
name: tshed
description: Run long jobs on the user's remote hosts with tshed - priority queue, expected runtime and 2x timeout, progress checkpoints with stall detection, memory limit, retries, captured logs; ships a git snapshot of local code (+ files) to the host and pulls results back into local folders. Use when asked to queue, schedule or run something remotely or on another machine, run a repo/script on a GPU or big host, monitor, pause, cancel, debug a stuck or failed job, search job logs, fetch results, estimate how long a job takes, or check which hosts have free cores/memory/GPUs.
---

# tshed

CLI: `tshed` (on PATH: a symlink to `tshed.py` of the tshed repo). Run it from this machine (the
server); it reaches hosts over ssh itself. Never run bare `tshed`, `tshed tui` or `tshed logs -f`:
those are interactive.

## Requirements for every job you enqueue

1. **A name**: `-n NAME`, 1-16 characters, no spaces, short and descriptive (`-n bert-ft-lr3`).
2. **The expected total runtime in seconds**: `-x SECONDS` (e.g. `-x 5400` for 1.5 h). Estimate it
   from earlier runs of the same name (`tshed stats NAME --json`: `median`, `p90`), a small trial run,
   or the input size. The timeout defaults to twice this; pass `-t` only with a reason.
3. **Progress markers in the script**: report a checkpoint after each unit of work of the main loop,
   as `DONE/TOTAL message` (or `PCT% message`). With `-x`, a job that goes more than twice the expected
   time for the progress its last checkpoint made (at least 10 s) without a new one is killed as
   `stalled`: reporting every item, every Nth item or every few percent all work.
   ```sh
   n=${#files[@]}; i=0
   for f in "${files[@]}"; do
     process "$f"
     tshed progress "$((++i))/$n" "$f"     # no-op outside tshed; ~0.1 s per call
   done
   # tight loops (many iterations per second): write the file directly, no python startup
   printf '%d/%d %s\n' "$i" "$n" "$f" > "$TINYSHED_PROGRESS"
   ```
   ```python
   import os
   def progress(done, total, msg=""):  # python jobs: same file, no-op outside tshed
       if p := os.environ.get("TINYSHED_PROGRESS"):
           with open(p, "w") as f: f.write(f"{done}/{total} {msg}\n")
   ```
   Never checkpoint from a background `while sleep` loop: that keeps a hung job alive. For one opaque
   command you cannot instrument, use `--stall DUR --stall-on-log` (its output counts) if it prints regularly.

## Template

```sh
cd ~/proj/train                                   # local git repo with the code (edits need not be committed)
tshed stats train-lr3 --json                      # earlier runtimes, if any -> pick -x
tshed ls >/dev/null; tshed hosts --json           # where is there room? (hosts shows the last sync; ls refreshes)
tshed submit -n train-lr3 -H gpu1 -c 8 -m 32G -x 5400 \
    --git --push ~/data/imgs:data --pull out:~/results/{name}/{id} \
    -- python train.py --lr 3e-4                  # prints: ID STATE HOST
tshed wait -t 10m ID                              # exit 0 done, 1 failed, 2 still running after -t
# done -> "ID done pulled out → /Users/…/results/train-lr3/ID"; read results there
```

Parse the ID as the first word of `submit` output. IDs may be unique prefixes, or a job name (an
unfinished job of that name wins).

## Code and results

- Code that lives in a local git repo: submit with `--git` from inside it; never assume the host has
  a checkout. The job runs on an exact snapshot (commits + uncommitted + untracked-not-ignored files),
  in its own checkout, in the subdirectory you submitted from. Ignored files (data, venvs) are not
  shipped: `--push` them. Untracked files over 100 MB in total stop the submit.
- Name every output you need with `--pull SRC:DEST` (SRC relative to the job's working directory,
  DEST a local folder; `{name} {id} {host} {state}`). Results are pulled when the job ends (also on
  failure, not on cancel), by `tshed wait` before it returns, and by `serve`/the dashboard. Each DEST
  gets `tshed-ID.json` (job, host, state, rc, command, times, git snapshot).
- **After the pull the job's work dir on the host is deleted.** Anything not in `--pull` is gone;
  use `--keep-work` only when someone must inspect the host afterwards.
- `tshed fetch ID [SRC...] [--to DIR]` copies results now, also from a running job (a snapshot).
- What code did a job run? `tshed checkout ID --diff` (only its uncommitted changes) or
  `tshed checkout ID DIR` (the whole tree), from the server's snapshot store; never look on the host.
- With `-H`, a staging failure aborts the submit: nothing is queued; fix and resubmit.

## Commands

```sh
tshed hosts [--json]          # up/down, load, CPU%, idle cores, memory, job RAM used/reserved, GPUs, freq
tshed submit -n NAME -x SECONDS [flags] -- CMD
tshed wait ID... [-t 2h]      # blocks; prints "ID STATE [pulled …]"; exit 0 all done, 1 any failed, 2 timeout
tshed show ID                 # state, limits, rc, times, progress + ETA, code/push/pull status, last 2 kB of output
tshed logs ID [-n BYTES]      # tail of stdout+stderr (default 20000 bytes)
tshed ls [-a] --json [--mine] # one JSON object per job (-a: finished too; --mine: yours only); see fields below
tshed owner [TEXT]            # your owner id (clau-PROJECT-SESSION); with TEXT: whose jobs, from where
tshed overview [-n N] [-a]    # one-screen snapshot: hosts + active jobs
tshed where ID [--json]       # running job: process tree, each bash script's line, python stacks (py-spy)
tshed grep PATTERN [ID...] [-i] [-l] [--name N] [--state error] [--since 2d] [-m N]   # search logs on all hosts
tshed stats [NAME...] [--since 30d] [--json]   # per name: runs, done, failed, median/p90/max runtime, latest -x
tshed fetch ID [SRC...] [--to DIR]
tshed checkout ID [DIR] | --diff [--stat]
tshed pause ID... / tshed resume ID...   # SIGSTOP + release cores / continue once cores are free
tshed cancel ID...            tshed prio ID N
tshed edit ID [-p N] [-H HOST|any] [--force]   # waiting jobs only: re-prioritise, move, start now
tshed config [--host HOST]    # show config.json (cores, name, home)
```

`ls --json` fields: `id name host state prio attempt rc cmd created started ended timeout expect
stall cpus mem_mb peak_mb paused_s stage owner`; running jobs add `cpu_pct` (% of their `-c` cores) and
`rss_mb`; `progress` (last checkpoint), `done` (0-1), `eta` (seconds left); `ran` (run time of this
attempt, pauses excluded) or `wait` (time waiting so far).

## submit flags

| flag | meaning |
|---|---|
| `-n NAME` | **required**: 1-16 characters, no spaces |
| `-x 5400` | **required**: expected runtime (seconds, or `s m h d`); `-t` defaults to 2 × this |
| `-H HOST` | pin to a host. Without it the server places the job on the live host with the most idle cores at the next sync (`wait`, `ls`, …); pin when the job needs a specific GPU, data location or machine |
| `-p N` | priority, higher first (default 0) |
| `-c N` | cores, pinned with taskset (default 1) |
| `-m 8G` | RSS limit for the process tree → state `oom` |
| `-t 12h` | runtime limit per attempt → state `timeout` |
| `--stall DUR` | killed as `stalled` after DUR without a checkpoint, counted from start; `0` = off (default with `-x`: 2 × the expected time for the last checkpoint's progress, ≥ 10 s) |
| `--stall-on-log` | log output also counts as a checkpoint (needs `--stall`) |
| `-r N` | retries with doubling backoff; `--retry-on error,lost` (default) may add `timeout,oom,stalled` |
| `--git [PATH]` | run on a snapshot of the repo at PATH (default `.`), see Code and results |
| `--push SRC[:DEST]` | copy a local file/folder into the job's working directory first (repeatable) |
| `--pull SRC[:DEST]` | copy SRC into local folder DEST when the job ends (repeatable; default DEST `results/{name}/{id}`; `--pull-on STATES`) |
| `--keep-work` | keep the job's work dir on the host after it ends |
| `--trace-lines` | record every bash process's exact line, function and command for `tshed where` (~0.1 ms per bash command) |
| `-C DIR` | working directory on the host (default `~`; with `--git`/`--push`: relative to the repo root / work dir) |
| `-e K=V` | env var, repeatable (e.g. `CUDA_VISIBLE_DEVICES=0`; GPUs are not allocated for you) |

CMD runs on the host as `bash -c` with a non-login environment: paths are the **host's** (with
`--git`, relative paths inside the repo work); use `bash -lc '…'` when conda/modules are needed.
Quote CMD as one argument if it contains shell syntax: `-- 'make 2>&1 | tail'`. Inside the job,
`tshed` on PATH is the host's own copy (for `tshed progress`), and `TINYSHED_JOB`, `TINYSHED_ATTEMPT`,
`TINYSHED_CORES`, `TINYSHED_PROGRESS` are set.

## States

`pending` (not on a host yet: unplaced, or being staged) → `queued` → `running` (⇄ `paused`) → `done` |
`error` | `timeout` | `oom` | `stalled` | `lost` | `cancelled`. `stalled` = no checkpoint within the stall
limit; `lost` = the watchdog died (e.g. host reboot). A host at capacity runs strictly by priority, so a
queued job may wait behind a larger one. In `ls`, a waiting job's Time is in brackets: `(5m 00s)`.

## When a job fails or looks stuck

1. `tshed show ID`: end state, rc, last checkpoint, staging/pull errors, end of the log.
2. Failed: `tshed logs ID` for more output; `tshed grep -i 'error|traceback' --name NAME --since 7d`
   to see whether earlier runs failed the same way; `tshed stats NAME` for the failure rate.
3. Running but no progress and a quiet log: `tshed where ID` before anything else. It shows which
   process runs, the line each bash script is on (`at FILE:N` = where the current command ends; inside a
   loop that is the loop's last line; `traced` lines, from `--trace-lines`, are exact) and python stacks
   when py-spy is on the host. Report it; do not cancel unless asked.
4. Code question ("what did it run?"): `tshed checkout ID --diff`.

## Rules

- The queue is shared with the user and other agents (`tshed ls` shows their jobs too). Every job
  records its owner, detected from your session (`clau-PROJECT-SESSION`): `tshed ls --mine` lists
  yours, the `owner` field in `--json` says whose a job is. Do not pass `--owner` unless asked. Do not
  cancel, pause, re-prioritise or edit (`prio`/`edit`, including `--force`) jobs you did not submit,
  and do not `tshed install`/`uninstall`, `tshed config --edit`, or edit `~/.tinyshed/hooks.py`, unless asked.
- For jobs longer than a few minutes, do not block on `tshed wait` without `-t`; report the ID and
  stop, or wait with a timeout and check back. Results are pulled by whichever `wait` sees the job end.
- `tshed hosts` shows `down` + the ssh error for unreachable hosts, as of the last sync (`seen` in
  `--json`); it never contacts hosts itself, `tshed ls` does. Jobs already on a host keep
  running while it is unreachable; state and logs catch up on reconnect. `pause`/`resume`/`prio` need
  the host reachable.
- No hosts listed → nothing is installed yet; ask the user which hosts to use.

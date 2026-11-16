# tinyshed

A minimal priority-queue scheduler for long jobs: one server commanding a handful of machines over ssh.

You have a laptop and a few boxes with cores and GPUs. tinyshed queues jobs on them by priority,
pins each job to its own cores, enforces runtime and memory limits, notices when a job stops making
progress, retries what failed, keeps every log, and moves your code there and your results back.
It is one file, `tinyshed.py` (the command is `tshed`), stdlib-only Python, with no daemon on the
server and nothing to install on the hosts except that same file.

- **Hosts are authoritative.** Each host keeps its own queue and event log in sqlite and keeps
  scheduling, retrying and logging while the server is unreachable; the server catches up later.
- **Expected runtime and progress.** Every job can declare how long it should take (`-x`; the timeout
  defaults to twice that) and report checkpoints; a job that stops checkpointing is killed as `stalled`.
- **Code in, results out.** `--git` runs a job on an exact snapshot of your working tree (uncommitted
  edits included), `--pull` copies its outputs into local folders when it ends.
- **Inspectable.** `tshed where` shows a running job's process tree, the line each bash script is on
  and python stacks; `grep` searches every log on every host; `stats` gives runtimes per job name.
- **A dashboard** with type-to-filter, multi-select, pause/resume, live logs, mosh attach and themes.

```
server (your machine)                   host (each)
~/.tinyshed/server.db  (mirror)         ~/.tinyshed/host.db   (authoritative, sqlite WAL)
tshed / tshed serve   ── ssh: rpc ──▶   agent ──▶ watchdog ──▶ taskset -c … bash -c CMD
                      ◀─ ssh: ingest ── (phone home, optional)
```

## Requirements

- Python ≥ 3.12 on the server and every host, and zstd: built in from Python 3.14, otherwise the `zstd`
  binary or `pip install pyzstd` / `zstandard`. `install` refuses a host without it.
- ssh from the server to each host (key-based; `~/.ssh/config` aliases work).
- On hosts, optionally: `taskset` (core pinning, Linux), `git` and `rsync` (`--git`, `--push`, `--pull`),
  `mosh-server` (attach), `nvidia-smi` (GPU stats), `py-spy` (python stacks in `where`).
- Linux hosts get everything; macOS works as a host too, without core pinning and with rougher RSS.

## Quick start

```sh
ln -s "$PWD/tinyshed.py" ~/bin/tshed                    # anywhere on PATH; the command is `tshed`
tshed install gpu1=gpu1.local,gpu1 gpu2=me@gpu2         # copy itself to each host, start its agent (~1 s per host)
tshed hosts                                             # load, CPU%, idle cores, memory, GPUs
tshed submit -n hello -H gpu1 -x 60 -- 'echo hi from $(hostname); sleep 30'
tshed ls                                                # or just `tshed` for the dashboard
tshed completion fish > ~/.config/fish/completions/tshed.fish   # or zsh / bash
```

`HUMANS.md` is a step-by-step tutorial. `skills/tinyshed/SKILL.md` teaches coding agents to use it.

## A job, end to end

```sh
cd ~/proj/train                         # a git repo; uncommitted edits are fine
tshed stats train --json                # earlier runtimes of this name, to pick -x
tshed submit -n train -H gpu1 -c 8 -m 32G -x 3h -r 1 \
    --git --push ~/data/imgs:data --pull out:~/results/{name}/{id} \
    -- python train.py --lr 3e-4
tshed wait -t 4h train                  # → 1a2b3c4d done pulled out → /Users/me/results/train/1a2b3c4d
```

The job runs on gpu1 in its own checkout of your working tree, in the same subdirectory, with
`data/` copied next to it. It gets 8 pinned cores and a 32 GB RSS limit, and a 6 h timeout (2 × `-x`).
If it exits with an error (or its host reboots), it is retried once. When it ends, `out/` is copied to the local folder, along with
`tinyshed-ID.json` recording what produced it. The checkout on gpu1 is then deleted. The code stays
recoverable with `tshed checkout ID`.

Inside the script, report progress after each unit of work:

```python
import os

def progress(line):                     # no-op outside tinyshed
    if p := os.environ.get("TINYSHED_PROGRESS"):
        with open(p, "w") as f:
            f.write(line + "\n")

for i, shard in enumerate(shards, 1):
    train_on(shard)
    progress(f"{i}/{len(shards)} {shard}")
```

With `-x 3h` and 12 shards, a shard that takes over 2 × 15 min (twice its share of the expected time) kills the job as `stalled` instead of
letting it hang until the timeout. The dashboard shows percent done and an ETA.

## Commands

| command | |
|---|---|
| **hosts** | |
| `install [[NAME=]DEST…] [--cores 0-15] [--home DEST] [--no-boot]` | check each host, copy the file, (re)start its agent, start it at boot, register it. No hosts = upgrade all. `DEST` is an ssh destination, alternatives tried in order (`gpu1.local,gpu1`: LAN, then VPN name), or `local[:DIR]`. Options left out keep their installed value. Safe while jobs run |
| `uninstall NAME… [--purge] [--force]` | stop the agent, remove the boot entry, forget the host; data stays unless `--purge` |
| `hosts [--json] [--rm NAME]` | last known state of every host, without contacting them (`Seen` = its age; `ls`, `serve` and the dashboard refresh it): see [Host columns](#host-columns) |
| `config [--edit] [--host HOST…]` | show or edit (in `$VISUAL`/`$EDITOR`) this machine's `config.json`, or a host's over ssh; validated before saving, applied within a second |
| **jobs** | |
| `submit -n NAME [flags] -- CMD` | queue a job; prints `ID STATE HOST`. See [Submitting](#submitting) |
| `ls [-a] [--json]` | active jobs (`-a`: finished too) |
| `overview [-n N] [-a]` | hosts and active jobs in one non-interactive screen |
| `show ID` | one job: state, limits, rc, times, progress, code and file status, end of its log |
| `logs ID [-n BYTES] [-f]` | its captured stdout+stderr; `-f` follows it live on the host (mosh/ssh, interactive) |
| `wait ID… [-t 2h] [-i 5]` | block until done; exit 0 all done, 1 any failed, 2 timed out. Pulls results of the jobs it waited for |
| `cancel ID…` · `prio ID N` | |
| `edit ID [-p N] [-H HOST\|any] [--force]` | change a waiting job: priority, host, or start it now. See [Controlling jobs](#controlling-jobs) |
| `pause ID…` · `resume ID…` | stop a running job and free its cores / continue it when cores are free |
| **inspecting** | |
| `where ID [--json]` | what a running job is doing: process tree, bash script lines, python stacks. See [Where is it?](#where-is-it) |
| `grep PATTERN [ID…] [-i] [-l] [--name N] [--state S] [--since 2d] [-m N]` | search job logs on every host in parallel (Python regex); exit 1 if nothing matched |
| `stats [NAME…] [--since 30d] [--json]` | per job name: runs, done, failed, fail %, median / p90 / max runtime, latest `-x`, peak memory |
| **code and results** | |
| `fetch ID [SRC…] [--to DIR]` | copy results from the job's host now, in any state (a running job gives a snapshot) |
| `checkout ID [DIR] [--diff [--stat]]` | a `--git` job's exact code, or just the uncommitted changes it ran with |
| `progress [DONE/TOTAL\|PCT%] [MESSAGE]` | run inside a job: report a checkpoint. Does nothing outside a job |
| **server and looks** | |
| `tshed` / `tui [-i 5]` | the dashboard; while open it is also the server loop |
| `serve [-i 10]` | the server loop without a screen: delivers jobs, ingests events, pulls results, runs hooks |
| `theme [NAME] [--contrast N]` · `scheme […]` | color themes and per-role overrides: see [Themes](#themes) |
| `completion fish\|zsh\|bash` | tab completion, including host names and job ids |

An `ID` may be any unique prefix of a job id, or a job name (an unfinished job of that name wins).
Output is colored on a terminal and plain in a pipe; `NO_COLOR` is respected.

## Submitting

```sh
tshed submit -n NAME [-H HOST] [-p PRIO] [-c CPUS] [-m MEM] [-x EXPECT] [-t TIMEOUT] [flags] -- CMD
```

| flag | |
|---|---|
| `-n NAME` | required: 1-16 printable characters, no spaces; usable instead of the id |
| `-H HOST` | run on this host. Without it the server places the job on the live host with the most idle cores at the next sync (any `tshed` command, `serve` or the dashboard) |
| `-p N` | priority, higher first (default 0) |
| `-c N` | cores to reserve and pin with `taskset` (default 1) |
| `-m 8G` | RSS limit of the whole process tree → `oom` |
| `-x 2h` | expected runtime: sets the default timeout and stall limit, gives an ETA. See [Progress](#progress-and-stalls) |
| `-t 12h` | runtime limit per attempt → `timeout` (default 2 × `-x`, else none) |
| `--stall DUR` · `--stall-on-log` | kill after DUR without a checkpoint → `stalled`; log output counts as one too |
| `-r N` · `--retry-on KINDS` · `--retry-delay DUR` | retry up to N times with doubling backoff on `error,lost` (default; add `timeout,oom,stalled` if those are transient for you) |
| `--git [PATH]` · `--push SRC[:DEST]` · `--pull SRC[:DEST]` · `--pull-on STATES` · `--keep-work` | ship code and files, bring results back. See [Code and results](#code-and-results) |
| `--trace-lines` | record every bash process's exact line for `where` (~0.1 ms per bash command) |
| `-C DIR` | working directory on the host (default `~`; with `--git`/`--push` relative to the job's own work dir) |
| `-e K=V` | environment variable, repeatable |

`CMD` runs on the host through `bash -c`, in a non-login environment: paths are the host's, and
conda/modules need `bash -lc '…'`. Quote it as one argument if it has shell syntax:
`-- 'make 2>&1 | tail'`. Jobs see `TINYSHED_JOB`, `TINYSHED_ATTEMPT`, `TINYSHED_CORES` and
`TINYSHED_PROGRESS`, and `~/.tinyshed/bin` first on `PATH` (so `tshed progress` works on every host).

**States.** `pending` (on the server: not placed yet, or its files are being staged) → `queued` →
`running` (⇄ `paused`) → `done` | `error` | `timeout` | `oom` | `stalled` | `lost` | `cancelled`.
`lost` means the job's watchdog died (kill -9, reboot); a retry, if allowed, starts it again.

## Progress and stalls

`-x/--expect` declares how long a job should take. The timeout defaults to twice that. A job reports
progress by writing one line to the file `$TINYSHED_PROGRESS`: `DONE/TOTAL message`, `PCT% message`,
or any text.

```sh
for f in data/*.csv; do
  ./crunch "$f"
  tshed progress "$((++i))/$n" "$f"                  # atomic write; ~0.1 s of python startup
done
printf '%d/%d %s\n' "$i" "$n" "$f" > "$TINYSHED_PROGRESS"   # the same without python, for tight loops
```

Every write is a checkpoint, even an empty one: the watchdog checks the file's mtime, inode and size
every second, so a torn write still counts and nothing needs locking. `tshed progress` does nothing
outside a job, so scripts run unchanged elsewhere.

| | stall limit |
|---|---|
| `--stall 10m` | 10 min without a checkpoint, counted from the start; `0` = never |
| `-x` and a `DONE/TOTAL` or `PCT%`, no `--stall` | twice the time the expected rate allows for the progress of the last step, at least 10 s. A job reporting 3% per checkpoint with `-x 1h` gets 2 × 3% × 1 h ≈ 3.6 min per checkpoint, however often it reports |
| `--stall-on-log` (with `--stall`) | log output also counts: for commands you cannot edit that print as they go |

A stalled job is killed (state `stalled`, retryable). Checkpoints become `progress` events at most
every 30 s, so a fast loop does not grow the event log. The dashboard's Progress column shows percent
and ETA (amber when the ETA runs past the timeout), and `ls --json` has `progress`, `done` and `eta`.
Checkpoint after each unit of work, never from a background `while sleep` loop: that keeps a hung job
looking alive.

## Code and results

The server does all copying, over the same ssh connection as everything else.

- **`--git [PATH]`** snapshots the repository containing PATH (default `.`) exactly as it is on disk:
  commits, staged and unstaged changes, and untracked files that are not ignored. It builds the
  snapshot commit with a throwaway index, so your index and files are untouched, and stores it in
  `~/.tinyshed/snapshots/` on the server, not as a ref in your repository (`git log --all` stays clean).
  The server pushes it into a bare mirror on the host (only new objects travel) and checks it out as a
  worktree in `~/.tinyshed/work/ID`. The job runs in the subdirectory you submitted from; `-C` is
  relative to the repository root. Ignored files (data, venvs) are not shipped, so use `--push` for
  them. Untracked files over 100 MB in total stop the submit, listing the largest. Submodules and
  git-lfs contents are not shipped.
- **`--push SRC[:DEST]`** (repeatable) rsyncs a local file or folder into the job's working directory
  before it starts. DEST defaults to SRC's name and must stay inside that directory.
- **`--pull SRC[:DEST]`** (repeatable) rsyncs SRC, relative to the job's working directory, into the
  local folder DEST when the job ends in one of `--pull-on`. The default is every end state except
  `cancelled`. A folder's contents land in DEST; a file lands inside it. DEST may use `{name}` `{id}`
  `{host}` `{state}` and defaults to `results/{name}/{id}`. Each DEST gets `tinyshed-ID.json`: job,
  host, state, rc, command, times, snapshot and source. A DEST that tinyshed creates inside a git work
  tree gets a `.gitignore` of `*`, so results never end up in the next snapshot.

**When.** With `-H`, `submit` stages the files before queueing; if that fails, nothing is queued.
Other jobs are staged on their host before delivery: inline by `submit`, `wait` and `edit`, in the
background by `serve` and the dashboard. Pulls run as jobs end, in `serve` and the dashboard and at
the end of `wait`; `fetch` pulls on demand. Failures are shown by `show` and retried once a minute.
Every step is idempotent, and a checkout interrupted halfway is redone.

**Cleanup.** Work dirs are scratch. Once a job has ended and its pull succeeded (or it wanted none),
its work dir is deleted on every host that has a copy. Anything not in `--pull` goes with it;
`--keep-work` keeps it. The snapshot is kept for good: it holds only what differs from history. So
are the log and the job record. `tshed checkout ID` rebuilds the code, `tshed checkout ID --diff`
shows just its uncommitted changes.

## Inspecting jobs

**`ls` and the dashboard**: `State CPU Prio Host ID Time Progress Try CPU% Mem Name Command`.

- `Time` is the run time of the current attempt, without paused time. Waiting jobs show how long they
  have been waiting, in brackets: `(12m 05s)`.
- `CPU%` is the CPU time of the job's process tree over elapsed time × reserved cores: a 2-core job
  with one busy thread reads 50%.
- `Mem` is the tree's live RSS against its `-m` limit (`0.3/3.2G`), red above 90%. Finished jobs show
  their peak.
- `ls --json` adds every field: `ran`/`wait`, `progress`, `done`, `eta`, `paused_s`, `stage`, …

**`grep`** searches the logs of matching jobs on all hosts at once, live and compressed.
`tshed grep -i 'out of memory' --state error --since 2d` finds every recent OOM. `-l` only lists jobs.

**`stats`** works from the server's records and contacts no host. Per job name it reports runs,
outcomes, fail %, median / p90 / max runtime of successful runs, the latest `-x` (amber when the
median is off by more than 50%) and peak memory.

### Where is it?

`tshed where ID` asks the host what a running job is doing right now, at increasing levels of detail:

- **Process tree** (always): every process of the job with state, elapsed time and command line.
  Usually enough on its own.
- **Script line** (bash, no setup): before running a command, bash seeks its script file to just past
  that command. `where` reads that offset (`/proc/PID/fdinfo/255`, or `lsof -o` on macOS) and prints
  `at script.sh:N` with the line's text. This is exact for top-level commands. Inside a loop, function
  or `if`, it shows the block's last line, because bash parses the whole block first.
- **Exact line, function stack and command** (`submit --trace-lines`): every bash of the job reads
  `$BASH_ENV`. tinyshed points it at a DEBUG trap that records `file:line`, `FUNCNAME` and the command
  about to run, one record per bash process, so nested scripts read like a stack:
  `outer.sh:4 in stage` → `inner.sh:3 python3 …`. The job's own `BASH_ENV` is sourced first; `$?`
  and `$_` are preserved.
  - Cost: about 0.1 ms per bash command, so 20 000 empty loop iterations take 2.4 s instead of 0.04 s.
  - Needs bash ≥ 4. `sh`/dash scripts and scripts with their own DEBUG trap are not traced.
- **Python stacks**: with `py-spy` on the host, a `py-spy dump` of up to four python processes. On
  Linux this needs ptrace permission (root or `kernel.yama.ptrace_scope=0`; in a container,
  `--cap-add SYS_PTRACE`).

`where` only reads at the moment you ask; only `--trace-lines` costs anything while the job runs.

## Controlling jobs

- **Priority** (`prio`, `edit -p`, dashboard `P`) of a waiting job.
- **Move** (`edit -H HOST`, `M`). A job not yet delivered is re-pinned. A job queued on a host is first
  withdrawn there, which succeeds only if it has not started (a conditional update), and is then sent
  to the new host under the same id, with its files restaged. If the old host is unreachable or the
  job already started, nothing changes. `-H any` lets the server choose.
- **Force** (`edit --force`, `F`) starts a waiting job now, ahead of priority order and retry backoff.
  It takes idle cores first, then shares busy ones.
- **Pause** (`pause`, `Z`). SIGSTOP to the job's process group: it becomes `paused` and its cores go back
  to the scheduler, so queued jobs can start. Memory stays held.
  - **Resume** (`resume`, `Z` again) does not continue it at once. The job waits for cores in
    priority order like a queued job, then is re-pinned to whatever cores it gets (Linux) and sent
    SIGCONT. Pausing again before that cancels the resume.
  - Paused time does not count toward the timeout, the stall limit or the run time.
- **Cancel** (`cancel`, `K`) kills the process group: TERM, then KILL after 10 s. It works on paused jobs
  too.

Priority, move, force, pause and resume need the job's host to be reachable. Cancel is remembered and
delivered when the host comes back.

## Dashboard

`tshed` with no arguments opens a full-screen dashboard of hosts and jobs. It redraws atomically
(synchronized frames), takes mouse wheel and clicks, and filters as you type lowercase letters
(`Esc` or `Ctrl-U` clears). Capital letters are commands:

| key | |
|---|---|
| `Enter` / `→`, `Esc` / `←` | job detail with a scrollable live log, and back |
| `N` | new job (same flags as `submit`) |
| `K` `E` `P` | cancel / rerun / priority |
| `F` `M` | force-start / move to another host (waiting jobs) |
| `Z` | pause a running job, resume a paused one |
| `W` | where: process tree and script lines of the selected job (`W` again: back to the log) |
| `A` `O` | follow the job's log on its host / a shell there (mosh, else ssh) |
| `X`, ctrl-click, `Ctrl-A`, `U` | mark a job (and move down), toggle a row, mark everything shown, clear marks |
| `Y` | copy the commands of the marked jobs (or the selected one) to the clipboard |
| `H` `R` `C` | hide finished jobs / refresh / edit this machine's config |
| `T` `B` `D` | next theme / more / less contrast |
| `Q`, `Esc` | quit (`Esc` first clears the filter, then the marks) |

With jobs marked, `E` reruns all of them and asks for a host. Enter keeps each job's own host;
`any` or a host name also work. `K` cancels them all, `P` re-prioritizes the waiting ones, and `Z`
pauses or resumes them. Marks follow jobs, not rows, so they survive sorting and filtering. So to
rerun last night's failures elsewhere: type `error`, `Ctrl-A`, `E`, `gpu2`, `Enter`.

`Y` uses `pbcopy`, `wl-copy`, `xclip` or `xsel` locally, and OSC 52 over ssh. Inside tmux, OSC 52
needs `set -g set-clipboard on`.

## Hooks

Put a `hooks.py` in `~/.tinyshed/` on the server (runs when the server learns of an event) and/or on a
host (runs when the event happens, even while the server is unreachable). Define any of:

`OnJobQueued` `OnJobStart` `OnJobProgress` `OnJobPaused` `OnJobResumed` `OnJobCompletion` `OnJobError`
`OnJobTimeout` `OnJobOOM` `OnJobStalled` `OnJobLost` `OnJobCancel`, `OnHostDown` `OnHostUp` (server),
`OnCriticalDisk` (host: `~` or `/tmp` dropped under 2 GB free; `data` maps the low paths to their
free GB, plus `min_gb`; if the disk is too full to log the event, the host hook still runs with
`eid` None and the server never hears of it), `OnEvent` (everything).

```python
import subprocess

def OnJobCompletion(ev):                 # ev: host, job, kind, ts, data
    print(ev["host"], ev["job"], "finished in", ev["data"]["ended"] - ev["data"]["started"])

def OnJobError(ev):
    if ev["data"]["final"]:              # False while retries remain
        subprocess.run(["notify-send", ev["data"]["name"], ev["data"]["tail"][-500:]])
```

`ev["data"]` is the full job row plus `final` and, for end events, the last 2 kB of output as `tail`.
Hook output and tracebacks go to `~/.tinyshed/hooks.log`; the file is reloaded when edited. Server
hooks need something listening: `serve`, the dashboard, or phone-home.

## How it works

- **State.** Each host owns its queue and an append-only event log in `host.db` (sqlite, WAL). The
  server keeps a mirror built only from those events, plus a per-host cursor. Events are recorded on
  the server exactly once, and hooks fire only for new ones.
- **Transport.** Every server→host interaction is one `ssh host tinyshed.py rpc` call, JSON in and out.
  It delivers new jobs and cancels, pulls events after the cursor and reads resources. Every operation
  is idempotent, so a lost connection just means "try again". Connections are multiplexed
  (`ControlMaster`), about 0.1 s per poll. Remote commands go through `sh -c`, so any login shell works.
- **Phone home** (`install --home DEST`, DEST being the server as the hosts see it). Agents push new
  events to the server over ssh as they happen, backing off while it is unreachable. Push and pull
  overlap safely. A host's key can be restricted with a forced command in `authorized_keys`; the exact
  command is in the host's `config.json`.
- **Scheduling.** On a host: highest priority first, FIFO within a priority, strictly, so a big job at
  the head is never overtaken by smaller ones. Unpinned jobs wait on the server and go, highest priority
  first, to the live host with the most idle cores.
- **Watchdog.** Each running job has its own watchdog process, detached from the agent so agents can be
  upgraded mid-job. The watchdog:
  - pins the job with `taskset` and captures its output to `~/.tinyshed/logs/ID.log`;
  - enforces the timeout, the memory limit (summed RSS of the process tree, polled every 2 s) and the
    stall limit;
  - kills the whole process group when a limit is hit.

  The agent detects a dead watchdog through a lock the watchdog holds, kills its orphans and marks the
  attempt `lost`.
- **Retention.** Nothing is pruned except work dirs whose results were pulled. Payloads are zstd blobs
  in sqlite, and a log becomes `ID.log.zst` when its job is final. To read a payload by hand:
  `sqlite3 host.db "select writefile('/dev/stdout', data) from events where id=1" | zstd -d`.
- **Boot.** `install` starts the agent at boot with an `@reboot` crontab line, or else a systemd user
  unit with `loginctl enable-linger` (`--no-boot` skips both). Any contact from the server also
  revives a dead agent.

### Host columns

`Host State Load CPU% Cores Mem free Used/Res Disk free GPU Run Que Seen Freq Notes`

- **`Load`** is the 1-minute load average.
- **`CPU%`** is the measured busy share of the cores tinyshed owns (`--cores`, or the CPU affinity),
  from per-core OS counters between polls. Above 100% is impossible while jobs are pinned, so it is
  flagged.
- **`Cores`** is idle / total.
- **`Mem free`** is whole-machine available / total.
- **`Used/Res`** is running jobs' live RSS over the sum of their `-m` limits. It is red if used exceeds
  reserved, and `?` if a job set no limit.
- **`Disk free`** is free GB on the host's home and `/tmp` (`451~ 42t`, `12T` from 10 000 GB); a path that cannot be
  stat'ed is left out (`18~`), an empty cell means neither was reported. Red, plus an `OnCriticalDisk`
  event per host, while either stays under 2 GB (re-warned hourly until it recovers).
- **`GPU`** is NVIDIA utilization and memory used / total per GPU, on one line, in index order. One GPU
  also shows its index (`88% 9.1/12G :0`); several are separated by `|` (`88% 9.1/12G | 0% 0.3/24G`),
  and the column widens to fit the host with the most. Models and power per GPU are in `hosts --json`.
- **`Freq`** is the mean clock of tinyshed's cores (`-` on Apple Silicon).
- **`Notes`** carries the host's power draw (see Power below), version mismatches (run `tshed install`)
  and warnings.
- `hosts --json` has all of it.

**Power.** `310W` in Notes is the CPU packages (RAPL) plus the NVIDIA GPUs' board power; where one reading
covers the whole machine (RAPL `psys`, a laptop battery, macOS) that reading alone. `gpu 239W` means only
the GPUs could be read. On Linux >= 5.10 the RAPL counters are root-only (`energy_uj` is mode 0400), so the
agent sees no CPU power and `tshed install` says so. To let it read them, on each host:

```sh
echo 'ACTION=="add", SUBSYSTEM=="powercap", RUN+="/bin/chmod o+r /sys%p/energy_uj"' \
  | sudo tee /etc/udev/rules.d/99-tinyshed-rapl.rules
sudo chmod o+r /sys/class/powercap/intel-rapl:*/energy_uj     # now; the rule covers reboots
```

This reverts a mitigation for the PLATYPUS power side channel (CVE-2020-8694): fine on single-user boxes,
not on shared ones. `hosts --json` has the parts as `power_parts` (`cpu`, `gpu` or `system`).

**Config.** `~/.tinyshed/config.json` on each host holds `name`, `cores` (the CPU set jobs may use) and
`home` (the phone-home command). Set it with `install --cores …`, `tshed config --edit`,
`tshed config --host NAME --edit` or the dashboard's `C`. Don't rename a host by hand: the name must
match the server's registry.

## Themes

`tshed theme` lists every theme as a sample row; `tshed theme NAME` saves one (`TINYSHED_THEME`
overrides per shell); `T` in the dashboard cycles. The dashboard paints the theme's background, plain
commands only recolor text.

- `default` uses the terminal's own 16 ANSI colors; `mono` uses bold, dim and reverse only.
- Truecolor palettes: `btop` `cendre` `catppuccin` `catppuccin-latte` `dracula` `everforest` `gruvbox`
  `gruvbox-light` `kanagawa` `monokai` `nord` `one-dark` `rose-pine` `solarized-dark` `solarized-light`
  `tokyo-night`.
- `tailwind-blue` `tailwind-emerald` `tailwind-indigo` `tailwind-red` follow the look of ratatui's table
  demo: a colored header bar, striped rows and a reversed selection bar.
  - `btop-themes/` has matching btop themes, so btop can sit next to the dashboard in the same colors:
    `cp btop-themes/*.theme ~/.config/btop/themes/`.
- Any btop theme: `btop:NAME` from btop's theme directories, or a path to a `.theme` file. State colors
  take the theme's color nearest in hue, and faint colors are lifted to readable contrast.
- **Contrast** (`--contrast 0-10`, default 4; `B`/`D` in the dashboard; `TINYSHED_CONTRAST`): darker
  darks, lighter lights, more vivid state colors. Faint text is lifted to WCAG 4.5:1. `0` draws the
  palettes exactly as published.
- Without truecolor (`COLORTERM=truecolor`, or a known terminal such as ghostty, kitty, iTerm2 or
  WezTerm), colors are downsampled to 256. `TINYSHED_TRUECOLOR=1`/`0` forces either.

**Color schemes** override single roles on top of any theme, so a personal tweak survives `T`:

```sh
tshed scheme                          # every role with a sample (* = overridden), and the presets
tshed scheme err=ff00ff warn=bright_yellow sel=303050
tshed scheme pastel err=ff0055        # a preset (neon pastel solarized hicon colorblind), then a tweak
tshed scheme err=default              # drop one override;  --clear drops all
```

Overrides are saved in `~/.tinyshed/scheme` (`TINYSHED_SCHEME='err=ff0000 ok=00ff00'` overrides per
shell). The roles are `ok info err warn cancel accent host key dim sel title fg bg`, plus `alt head
sel_fg`. `colorblind` uses the Okabe-Ito palette. Overrides are applied after the contrast curve, so
they are used exactly as given.

## Limits

- The memory limit is enforced by polling, not cgroups: a fast allocator can overshoot before it is
  killed. Shared pages are counted once per process.
- GPUs are reported (`nvidia-smi`) but not allocated: pass `-e CUDA_VISIBLE_DEVICES=…`.
- Core pinning (`taskset`) and process-tree RSS via `/proc` are Linux features. On macOS hosts jobs
  run unpinned, and RSS comes from `ps`, which compressed memory makes unreliable.
- Attach (`A`, `O`, `logs -f`) uses mosh only where a probe shows its UDP range (60000-61000)
  reaches the host; otherwise it uses `ssh -t`.
- **Why ssh, not mosh or sqlite, for transport.**
  - mosh is an interactive terminal protocol and cannot carry a command's stdin, stdout or exit code.
  - sqlite has no network protocol.
  - ssh already provides authentication and encryption.

## Development

Unit tests: `./run_tests.sh` (stdlib unittest, under a second, each test in its own temporary home;
`-k PATTERN` for a subset). Agents, watchdogs and ssh are exercised against fake hosts on one machine.
Never point a test at your real `~/.tinyshed`:

```sh
export TINYSHED_HOME=/tmp/ts/server
./tinyshed.py install a=local:/tmp/ts/a b=local:/tmp/ts/b --cores 0-1 --home local
./tinyshed.py submit -n t -H a -x 10s -- sleep 5 ; ./tinyshed.py wait -i 1 t ; ./tinyshed.py ls -a
./tinyshed.py uninstall a b --purge ; rm -rf /tmp/ts     # agents are daemons: always clean up
```

`local:DIR` hosts skip ssh, and `--home local` exercises phone-home. Linux-only paths (`taskset`,
`/proc`, re-pinning) need a Linux container. `AGENTS.md` lists the invariants the code relies on.

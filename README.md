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
tshed hosts                                                         # ping: load, CPU%, used/reserved RAM, freq, idle cores, memory, GPUs
tshed submit -n lr3e-4 -p 5 -c 8 -m 32G -t 12h -r 2 -- python train.py --lr 3e-4
tshed submit -n run -H gpu1 -C ~/proj -e CUDA_VISIBLE_DEVICES=0 -- ./run.sh
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
| `submit -n NAME [flags] -- CMD` | queue a job. `-n` is required: 1-16 characters, no spaces; it shows in `ls`/the dashboard and can stand in for the id anywhere an ID is taken (an unfinished job of that name wins; several → use the id) |
| `checkout ID [DIR] [--diff [--stat]]` | a `--git` job's exact code from the snapshot store, into DIR (default `./NAME-ID`), or `--diff`: only the uncommitted changes it ran with. Works after the work dir is cleaned; no host contact |
| `fetch ID [SRC…] [--to DIR]` | copy a job's results from its host now, in any state (a running job gives a snapshot); without SRC its `--pull` list (see Files) |
| `pause ID…`, `resume ID…` | stop running jobs (SIGSTOP) and give their cores to the queue; resume continues them once cores are free (see Pause). Dashboard: `Z` |
| `grep PATTERN [ID…] [-i] [-l] [--name N] [--state S] [--since 2d] [-m N]` | search job logs (live and compressed) on every host in parallel; Python regex; exit 1 if nothing matched |
| `stats [NAME…] [--since 30d] [--json]` | per job name: runs, done, failed, fail %, median / p90 / max runtime of successful runs, latest `-x` (amber when the median is off by more than 50%), peak memory. From the server's records only |
| `where ID [--json]` | what a running job is doing now: process tree, each bash script's line, python stacks (see Where is it?). Dashboard: `W` |
| `progress [DONE/TOTAL\|PCT%] [MESSAGE]` | run *inside* a job: report a checkpoint (see Progress checkpoints). Does nothing outside a job |
| `ls [-a] [--json]`, `show ID`, `logs [-f] ID` | list, inspect, read output (ids may be prefixes) |
| `wait ID… [-t 2h]` | block; exit 0 done / 1 failed / 2 timeout |
| `cancel ID…`, `prio ID N` | |
| `edit ID [-p N] [-H HOST\|any] [--force]` | change a waiting (`pending`/`queued`) job: priority, host (move; `any` unpins), or force it to start now. Dashboard: `P`, `M`, `F` |
| `theme [NAME] [--contrast N]` | list color themes with samples, or pick one (saved in `~/.tinyshed/theme`; `TINYSHED_THEME` overrides); `--contrast` stretches the palette: darker darks, lighter lights (level 0-10, default 4). Dashboard: `T` cycles, `B`/`D` contrast |
| `scheme [PRESET\|ROLE=COLOR…] [--clear]` | list color roles with samples (* = overridden) and the preset schemes, apply a preset wholesale (`neon pastel solarized hicon colorblind`), or set individual colors on top of any theme (saved in `~/.tinyshed/scheme`; `TINYSHED_SCHEME='err=ff0000 …'` overrides). `COLOR` is 6 hex digits or a basic name (`red`, `bright_cyan`, …); `ROLE=default` removes that override |
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
  track job usage. A value above 100% is impossible with sound counters (jobs are pinned to their
  cores), so it is flagged red and adds a warning to Notes.
- **Used/Res.** The live RSS of running jobs over the sum of their `-m` RSS limits, in GB
  (`2.0/8.0G`). It turns red when used exceeds reserved: the watchdog kills a job that passes its
  `-m` limit, so that should not happen (shared pages are counted per process, which can over-report).
  `?` means at least one running job set no `-m`. Separate from `Mem free`, which is whole-machine
  available/total. Both anomalies also appear in `hosts --json` under `warnings`.
- **Job CPU% / Mem.** Each running job's row in `ls` and the dashboard shows its own usage. `CPU%`
  is the CPU time of the job's whole process tree since the previous poll, divided by elapsed time ×
  cores assigned (`-c`): a 2-core job with one busy thread reads 50%. `Mem` is the tree's live RSS over
  its `-m` limit in one unit (`32/64M`, `0.3/3.2G`, or just `324M` / `3.2G` without `-m`), red above 90% of the limit.
  Finished jobs show their peak RSS (`-` if the watchdog never sampled it). Both are measured on the host
  in each status poll (`-` until two polls); on macOS, CPU time of exited children is lost.
- **Freq / power.** `Freq` is the mean current clock of tinyshed's cores, from per-core OS counters
  (Linux sysfs `scaling_cur_freq`, else `/proc/cpuinfo`; Intel macOS `hw.cpufrequency`). Apple Silicon
  hides the clock, so it reads `-` there. Whole-host power draw appears in Notes in watts when the OS
  exposes a counter: Linux RAPL package energy (a delta between polls) or battery discharge rate. On
  macOS, where `powermetrics` needs root, the battery's own draw is used while discharging and the
  charger's wattage when plugged in (an upper bound, not a measured reading).
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
  then KILL after 10s) on timeout / over-memory / stall / cancel. If the watchdog itself dies (kill -9, reboot)
  the agent notices, kills the orphans and marks the attempt `lost`.
- **Retry.** `-r N` retries with exponential backoff on the kinds in `--retry-on` (default
  `error,lost`; add `timeout,oom,stalled` if those are transient for you).
- **Job states.** `pending` (still on server) → `queued` → `running` (⇄ `paused`) → `done | error | timeout | oom |
  stalled | lost | cancelled`. Jobs see `TINYSHED_JOB`, `TINYSHED_ATTEMPT`, `TINYSHED_CORES`,
  `TINYSHED_PROGRESS`, and `~/.tinyshed/bin` (a `tshed` shim for this host) first on `PATH`.

## Progress checkpoints

`-x/--expect DUR` declares the expected runtime: the timeout defaults to twice it, and the dashboard
shows an ETA. A job reports progress by writing one line, `DONE/TOTAL message`, `PCT% message` or any
text, to the file `$TINYSHED_PROGRESS`; `tshed progress 3/10 msg` does that atomically (about 0.1 s of
python startup), and a plain `printf '3/10 msg\n' > "$TINYSHED_PROGRESS"` costs nothing for tight loops.
Every write is a checkpoint, even an empty one (`touch`): the watchdog polls the file's identity
(mtime, inode, size) every second, so a torn write still counts and needs no locking.

| flag | effect |
|---|---|
| `-x 2h` | expected runtime; `-t` defaults to 2 × this |
| `--stall 10m` | killed as `stalled` after 10 min without a checkpoint, counted from the start; `0` = off |
| (neither) | with `-x` and a reported TOTAL: stall limit 2 × expected / TOTAL per step, from the first checkpoint with a total |
| `--stall-on-log` | log output also counts as a checkpoint (needs `--stall`): for opaque commands that print |

A changed checkpoint becomes a `progress` event (hook `OnJobProgress`) at most every 30 s, plus one
before the final event, so `ls`, `show`, the dashboard (`Progress` column: percent and ETA, amber when
the ETA runs past the timeout) and `ls --json` (`progress`, `done`, `eta`) see it without growing the
event log per iteration. A background `while sleep 60; do tshed progress; done &` defeats the point:
it keeps a hung job alive. Checkpoint after each unit of work instead.

## Files: `--git`, `--push`, `--pull`

The server does all copying, over the same ssh connection as everything else; hosts need `git` and
`rsync` (`install` notes when one is missing).

```sh
cd ~/proj/train
tshed submit -n sweep -H gpu1 -x 3h --git --push ~/data/imgs:data --pull out:~/results/{name}/{id} -- python train.py
```

- **`--git [PATH]`** snapshots the repository that contains PATH (default `.`) exactly as it is on
  disk: commits, staged and unstaged changes, and untracked files that are not ignored. It does so
  without touching your index or files (a throwaway index, `write-tree`, `commit-tree`) and keeps it in
  tinyshed's snapshot store on the server, `~/.tinyshed/snapshots/NAME-HASH.git` (`refs/tinyshed/ID`):
  your repository gets no refs, so `git log --all` stays clean (the commit object left in it is
  unreferenced and goes with git's normal gc). The server pushes it into a bare mirror on the host
  (`~/.tinyshed/repos/NAME-HASH.git`, so only new objects travel) and checks it out as a detached
  worktree in `~/.tinyshed/work/ID`. The job runs in the subdirectory you submitted from; `-C` is then
  relative to the repository root. `show` prints `Code: repo@SHA +uncommitted on BRANCH (base SHA)`.
  Ignored files (datasets, venvs) are not shipped: use `--push` for them. Untracked files over 100 MB in
  total stop the submit with the largest ones listed. Submodule contents and git-lfs objects are not
  shipped.
- **`--push SRC[:DEST]`** (repeatable) rsyncs a local file or folder into the job's working directory
  before it starts; DEST (default: SRC's name) is relative to that directory and must stay inside it.
  Without `--git` the work dir starts empty.
- **`--pull SRC[:DEST]`** (repeatable): when the job ends in one of `--pull-on` (default
  `done,error,timeout,oom,stalled,lost`: everything but `cancelled`), SRC (relative to the job's working
  directory on the host) is rsynced into the local folder DEST. A folder's contents land in DEST, a
  file lands inside it. DEST may use `{name}` `{id}` `{host}` `{state}`; default `results/{name}/{id}`,
  relative to where you ran `submit`. Each DEST gets `tinyshed-ID.json` (job, host, state, rc, command,
  times, git snapshot, source path). A DEST folder that tinyshed creates inside a git work tree gets a
  `.gitignore` of `*`, so results never ride along in the next `--git` snapshot.

**When it happens.** With `-H`, `submit` stages the files before the job is queued; if that fails,
nothing is queued. A job without `-H` (or moved with `edit -H`) is staged on its host before it is
delivered: inline by `submit`, `wait` and `edit`, in a background thread by `serve` and the dashboard;
quick commands (`ls`, `show`, …) never stage. Pulls run in `serve` and the dashboard as jobs finish,
and at the end of `tshed wait` for the jobs it waited on (its output then says `pulled SRC → DEST`);
`tshed fetch` pulls on demand. Failed staging or pulls are retried once a minute and shown by `show`.
Every step is idempotent; a checkout interrupted halfway is redone, never trusted.

**Cleanup.** A job's work dir (checkout, pushed files, outputs) is scratch: once the job has ended and
its `--pull` for that end state succeeded (or it wants none, e.g. `cancelled`), the server deletes it on
every host that holds a copy, including hosts the job was moved away from. That happens next to the
pulls: in `serve`, the dashboard and at the end of `tshed wait`. A failed pull keeps the work dir (and
is retried); `--keep-work` keeps it for good. Files you did not `--pull` go with it. What stays,
forever: the snapshot (store on the server, mirror on the host; a snapshot stores only what differs
from history, so this is the modified files), the log, and the job's record. `tshed checkout ID`
rebuilds the code any time; `tshed checkout ID --diff` shows just its changes. Snapshots from before
the store existed live as `refs/tinyshed/ID` in your repository; drop them with
`git for-each-ref --format='%(refname)' refs/tinyshed | xargs -n1 git update-ref -d`.

## Durations

The Time column (and `Ran:` in `show`) counts from the start of the current attempt and leaves out time
spent paused. Waiting jobs show their waiting time in brackets, `(12m 05s)`: since submission, or since
the previous attempt ended for a retry; a job cancelled before it ever ran shows how long it waited.
`ls --json` carries it as `ran` or `wait` (seconds).

## Pause

`tshed pause ID` (dashboard: `Z`) sends SIGSTOP to the job's process group, marks it `paused` and
releases its cores, so the scheduler can start queued jobs on them. Memory stays held, and `Mem` keeps
showing it. `tshed resume ID` does not restart it at once: a resumed job competes for cores in priority
order exactly like a queued job, and when it fits it gets free cores (re-pinned with
`sched_setaffinity` on Linux, every thread of the session) and SIGCONT. `pause` again before that
cancels the pending resume. Paused time stops the timeout, the stall limit and the run time; the log
records `=== tinyshed: paused …` and `resumed after Ns, cores=…`. Cancelling a paused job works (it is
woken so the TERM lands). Pause and resume need the host to be reachable (like `prio`); hooks:
`OnJobPaused`, `OnJobResumed`.

## Where is it?

`tshed where ID` asks the host what a running job is doing, at three levels of detail:

- **Process tree** (always): every process of the job (descendants plus its session/process group),
  with state, elapsed time and command line. Usually enough on its own: you see which step is running.
- **Script line** (bash, no setup): bash reads a script through fd 255 and, before running a command,
  seeks it back to just past that command. `where` reads that offset (`/proc/PID/fdinfo/255`, `lsof -o` on
  macOS) and prints `at script.sh:N` with the line's text. Exact for top-level commands; inside a loop,
  function or `if` it is the block's last line, because bash parses a whole block before running it.
  Not available for `bash -c '…'` one-liners (no script file).
- **Exact line, function stack and command** (opt-in, `submit --trace-lines`): the watchdog points
  `BASH_ENV` at `~/.tinyshed/bin/trace.bash`, which every non-interactive bash of the job reads. It
  sources the job's own `BASH_ENV` first, then sets a DEBUG trap that writes `file:line`, `FUNCNAME` and
  `$BASH_COMMAND` before each command to `run/ID.where/PID`, one file per bash process. So nested scripts
  read like a stack (`outer.sh:4 in stage` → `inner.sh:3 python3 …`) and parallel ones do not overwrite
  each other. `$?` and `$_` are preserved and a failed write never affects the script. Costs about 0.1 ms
  per bash command (a pure-bash loop of 20 000 empty iterations: 0.04 s → 2.4 s), which is negligible for
  scripts that mostly run programs. Needs bash >= 4 (macOS `/bin/bash` 3.2 records nothing); `sh`/dash
  scripts and scripts that set their own DEBUG trap are not traced; `set -T` is on in traced shells.
- **Python stacks**: when `py-spy` is installed on the host, `where` adds `py-spy dump` for up to four
  python processes of the job (the dump pauses each briefly). On Linux it needs ptrace permission (root,
  or `kernel.yama.ptrace_scope=0`); in a container, `--cap-add SYS_PTRACE`.

`where` only reads, at the moment you ask: levels 1, 2 and 4 cost the job nothing in between.

## Callbacks

Put a `hooks.py` in `~/.tinyshed/` on the server (fires when an event is ingested) and/or on a host
(fires when it happens). Define any of:

`OnJobQueued` `OnJobStart` `OnJobCompletion` `OnJobError` `OnJobTimeout` `OnJobOOM` `OnJobStalled`
`OnJobLost` `OnJobCancel` `OnJobProgress` `OnJobPaused` `OnJobResumed` `OnHostDown` `OnHostUp` (server) `OnEvent` (everything)

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
`submit`), `K` cancel, `E` rerun, `P` priority, `F` force-start, `M` move to another host, `T` next theme, `A` follow the log on the host (mosh), `O` shell on the
host (mosh), `H` hide finished, `R` refresh, `W` where (process tree and script lines, see Where is it?), `Esc`/`Q` quit.
While open, the TUI is also the server loop.

`Z` pauses the selected running job, or resumes a paused one.

**Marking several jobs.** `X` marks or unmarks the selected job and moves down; ctrl- or alt-click
toggles a row; `Ctrl-A` marks every job shown (filter first, e.g. type `error`), again to unmark them;
`U` (or `Esc`, before it quits) clears all marks. Marks follow the job, not the row, and survive
filtering. With marks set, `E` reruns every marked job as a new one, asking for the host: `Enter`
keeps each job's own host, `any` lets the server pick, or a host name. `K` cancels every marked job that is not finished, `P` sets
the priority of every marked waiting job, `Z` pauses the marked running ones and resumes the marked
paused ones. `Y` copies the marked jobs'
commands, one per line (the selected job's when nothing is marked), with `pbcopy` / `wl-copy` /
`xclip` / `xsel` in a local session, else through the terminal with OSC 52 (works over ssh; inside tmux
it needs `set -g set-clipboard on`).

## Editing jobs

- **Move** (`edit -H`, `M`). An undelivered job is just re-pinned. A job already queued on a host is
  first *withdrawn* there: the host flips it `queued → moved` only if it has not started (a conditional
  update, like a claim), and the server re-sends it to the new host under the same id. A host that is
  unreachable, or where the job already started, refuses and nothing changes. Moving a job back to a host
  that once gave it up re-queues the old row. Retry attempts start over on the new host.
- **Force** (`edit --force`, `F`). The host marks the job `force`; its scheduler starts forced jobs first,
  regardless of priority order, retry backoff or free cores: it takes idle cores first, then shares busy
  ones (so `CPU%` of the sharing jobs drops). A host-less job is placed on the live host with the most
  spare cores. The flag clears when the job starts, so a retry queues normally.
- **Priority** (`edit -p`, `P`, `prio`) as before. All three only apply to `pending`/`queued` jobs; the
  host answers each op with the job's state, and a host running an older tinyshed is reported (upgrade
  with `tshed install`).

## Themes

`tshed theme` lists every theme as a sample row; `tshed theme NAME` saves the choice for this server
(cli and dashboard); `T` in the dashboard cycles and saves. `TINYSHED_THEME=NAME` overrides per shell.

- `default`: the terminal's own 16 ANSI colors (the gitss look); `mono`: bold/dim/reverse only.
- Built-in truecolor palettes: `btop`, `cendre`, `catppuccin`, `catppuccin-latte`, `dracula`, `everforest`,
  `gruvbox`, `gruvbox-light`, `kanagawa`, `monokai`, `nord`, `one-dark`, `rose-pine`,
  `solarized-dark`, `solarized-light`, `tokyo-night`.
- ratatui's own look, from the tailwind palette it ships (`ratatui::style::palette::tailwind`) and laid
  out like its table demo: `tailwind-blue`, `tailwind-emerald`, `tailwind-indigo`, `tailwind-red`.
  slate-950 background, slate-200 text, and in the dashboard a colored header bar (c900) instead of a
  rule, alternating slate-900 rows, and a reversed selection bar (c400, one text color).
  `btop-themes/` has the same four as btop themes, so btop can sit next to the dashboard in matching
  colors: `cp btop-themes/*.theme ~/.config/btop/themes/`, then pick one in btop's options menu.
- Any btop theme: `btop:NAME` from btop's theme dirs (`~/.config/btop/themes`, `$XDG_CONFIG_HOME`,
  the btop install prefix, `/usr/share/btop/themes`, …), or a path to a `.theme` file. `title`,
  `hi_fg` (keys), `selected_bg/fg`, `inactive_fg` and `main_fg/bg` map directly; the state colors
  (ok/error/warning/…) take the theme's color nearest in hue.
- Contrast is adjustable: `tshed theme --contrast N` (level 0-10, default 4, saved in
  `~/.tinyshed/contrast`; `TINYSHED_CONTRAST` overrides), or `B` / `D` (more / less) in the dashboard.
  It is a tone curve over the whole palette: dark shades get darker (the painted background, striped
  rows, bars), light ones lighter (text), hues stay, and state/accent colors gain 5% saturation per
  level so they get more vivid rather than washing out. Text that is still faint is lifted to WCAG 4.5
  (dim: 3). `0` draws every built-in palette exactly as published. Plain commands do not paint a
  background, so there only the text side shows. No effect on `default` and `mono`.
- btop themes are additionally normalised first, since their state colors are guessed: faint colors are
  lifted to 4.5:1 and the selection bar becomes a tinted lift of the background (`CONTRAST`,
  `CONTRAST_TEXT` in the code).

In the dashboard a theme paints its own background; plain commands only recolor text. Without truecolor
(`COLORTERM=truecolor`, or a known terminal such as ghostty/kitty/iTerm2/WezTerm) colors are
downsampled to the 256-color palette; `TINYSHED_TRUECOLOR=1` / `0` forces either (like btop's truecolor option). A saved theme that no longer resolves falls back to `default`.

## Color schemes

A scheme is a set of per-role overrides layered onto whatever theme is active, so one personal tweak
survives `T` in the dashboard and works on every theme (it does nothing on `default` and `mono` for
background-ish roles, since those use the terminal's own colors):

```sh
tshed scheme                          # list every role with a sample (* = overridden)
tshed scheme err=ff00ff warn=bright_yellow sel=303050
tshed scheme err=default              # back to the theme's error color
tshed scheme --clear                  # remove every override
```

Or start from a named preset, applied wholesale (`tshed scheme pastel`), and tweak from there
(`tshed scheme pastel err=ff0055`): `neon`, `pastel`, `solarized`, `hicon` (maximum contrast) and
`colorblind` (Okabe-Ito, safe for common color-vision types). Presets are tuned for dark palettes;
`tshed scheme` shows each as a sample row and marks the active one.

Overrides are saved in `~/.tinyshed/scheme` (`TINYSHED_SCHEME='err=ff0000 ok=00ff00'` overrides per
shell, `err=default` in it drops that role's override). Roles: `ok info err warn cancel accent host
key dim sel title fg bg` plus the theme-internal `alt head sel_fg`. Overrides are applied after the
contrast curve, so they are used exactly as given.


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
- **Retention.** Nothing is ever pruned except `--git`/`--push` work dirs, once their results are pulled (see Files). To keep that cheap, everything at rest is zstd: job and event
  payloads are zstd blobs in sqlite, and a job's log becomes `logs/ID.log.zst` once the job is final
  (`logs`, the TUI and `A` read either form). Read a payload by hand with
  `sqlite3 host.db "select writefile('/dev/stdout', data) from events where id=1" | zstd -d`.

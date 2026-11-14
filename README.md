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
| `ls [-a] [--json]`, `show ID`, `logs [-f] ID` | list, inspect, read output (ids may be prefixes) |
| `wait ID… [-t 2h]` | block; exit 0 done / 1 failed / 2 timeout |
| `cancel ID…`, `prio ID N` | |
| `edit ID [-p N] [-H HOST\|any] [--force]` | change a waiting (`pending`/`queued`) job: priority, host (move; `any` unpins), or force it to start now. Dashboard: `P`, `M`, `F` |
| `theme [NAME] [--contrast N]` | list color themes with samples, or pick one (saved in `~/.tinyshed/theme`; `TINYSHED_THEME` overrides); `--contrast` stretches the palette: darker darks, lighter lights (level 0-10, default 4). Dashboard: `T` cycles, `B`/`D` contrast |
| `scheme [ROLE=COLOR…] [--clear]` | list color roles with samples (* = overridden), or set individual colors on top of any theme (saved in `~/.tinyshed/scheme`; `TINYSHED_SCHEME='err=ff0000 …'` overrides). `COLOR` is 6 hex digits or a basic name (`red`, `bright_cyan`, …); `ROLE=default` removes that override |
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
`submit`), `K` cancel, `E` rerun, `P` priority, `F` force-start, `M` move to another host, `T` next theme, `A` follow the log on the host (mosh), `O` shell on the
host (mosh), `H` hide finished, `R` refresh, `Esc`/`Q` quit. While open, the TUI is also the server loop.

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
- **Retention.** Nothing is ever pruned. To keep that cheap, everything at rest is zstd: job and event
  payloads are zstd blobs in sqlite, and a job's log becomes `logs/ID.log.zst` once the job is final
  (`logs`, the TUI and `A` read either form). Read a payload by hand with
  `sqlite3 host.db "select writefile('/dev/stdout', data) from events where id=1" | zstd -d`.

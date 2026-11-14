# tinyshed tutorial

A walk from nothing to a small cluster running your jobs. `README.md` is the reference; this is the
guided tour. The command is `tshed`: a symlink to `tinyshed.py` somewhere on your `PATH`
(`ln -s $PWD/tinyshed.py ~/bin/tshed`).

## 0. What you need

- This machine (the **server**) can `ssh` to each **host** without a password prompt.
- Every machine has Python ≥ 3.12 and zstd: built into Python 3.14+, otherwise the `zstd` package
  (`apt install zstd` / `pacman -S zstd`) or `pip install pyzstd`.
- `mosh` is optional; it is only used to attach to logs and shells.

## 1. Install the hosts

```sh
tshed install gpu1=me@gpu1.lan gpu2=me@gpu2.lan
```

```
Host          Result  Ver   Python   Zstd       Cores  Boot          Notes
gpu1          ✓ ok    0.2   3.13.1   binary        32  systemd
gpu2          ✓ ok    0.2   3.14.2   stdlib        16  cron          no mosh-server: attach/shell use ssh
```

That checked each host (ssh, Python, zstd), copied one file to `~/.tinyshed/`, started its agent and
arranged for it to start at boot (crontab `@reboot`, or a systemd user unit where there is no cron).
The Notes column tells you what a host is missing. `DEST` can list alternatives tried in order:
`gpu1=gpu1.local,gpu1` means LAN first, then the tailscale name.

`tshed install` with no hosts upgrades every registered host; `tshed install gpu1` upgrades one.
Running jobs are never disturbed, and options you leave out keep their installed value:

- `--cores 0-23` — only let jobs use these cores (`all` to reset).
- `--home me@server.lan` — let hosts push events back to the server (section 7; `none` to disable).
- `--no-boot` — don't start the agent at boot.

`tshed uninstall gpu1` stops the agent and removes the boot entry but keeps the host's data;
`--purge` deletes it too. It refuses while the host has unfinished jobs unless you pass `--force`.

Check on them:

```sh
tshed hosts
```

```
Host          State    Load  CPU%    Cores    Mem free   Used/Res  GPU       Run  Que    Seen    Freq  Notes
gpu1          ✓ up      0.4    6%    32/32    118/125G   2.0/8.0G  2x 0%       0    0      0s  3.1GHz  65W
gpu2          ✓ up      1.2   12%    16/16      57/62G   1.5/4.0G  -           0    0      0s  3.2GHz
```

`Load` is the machine-wide 1-minute load average; `CPU%` is the busy percentage of the cores
tinyshed owns (`-` until two polls have happened). `Used/Res` is the live RAM of running jobs over
their combined `-m` limits; `Mem free` is the whole machine's available/total; `Freq` is the mean
clock of those cores. A red `CPU%` (>100%) or `Used/Res` (used over reserved) means a limit was
breached — it should not happen, since the watchdog kills jobs that pass their `-m` limit, and the
reason appears in Notes, which also carries the host's power draw in watts where the OS reports one.

Turn on tab completion (commands, flags, host names, and job ids described by their command):

```sh
tshed completion fish > ~/.config/fish/completions/tshed.fish
eval "$(tshed completion zsh)"      # in ~/.zshrc, after compinit
eval "$(tshed completion bash)"     # in ~/.bashrc
```

## 2. Your first job

```sh
tshed submit -n hello -H gpu1 -- 'echo hello from $(hostname); sleep 20; echo bye'
# 3f9a1c2e queued gpu1
tshed ls
tshed show 3f9          # state, limits, exit code, times, end of the log
tshed show hello        # a job's name works wherever an id does
tshed logs 3f9          # ids can be shortened to any unique prefix
tshed wait 3f9          # blocks until it finishes; exit code tells you how it went
```

While it runs, `ls` shows the job's own `CPU%` (the busy share of the cores it was given, so a
`-c 2` job using one core reads 50%) and `Mem` (live RAM over its `-m` limit, like `0.3/3.2G`).

`-n` names the job (required; up to 16 characters, no spaces).

Everything after `--` runs on the host through `bash -c`, in your home directory, with stdout and
stderr captured to `~/.tinyshed/logs/ID.log` on that host. The environment is a plain non-login
shell: if you need conda or modules, write `bash -lc '…'`, or use `-C dir` and `-e KEY=value`.

## 3. Limits, priorities, retries

```sh
tshed submit -n train -H gpu1 -p 10 -c 8 -m 32G -t 12h -r 2 -C ~/proj -e CUDA_VISIBLE_DEVICES=0 -- python train.py
```

| flag | effect |
|---|---|
| `-p 10` | priority; higher runs first, FIFO among equals |
| `-c 8` | reserves 8 cores and pins the job to them with `taskset` |
| `-m 32G` | kills the job if its whole process tree exceeds 32 GB RSS → `oom` |
| `-t 12h` | kills it after 12 hours → `timeout` |
| `-r 2` | up to 2 retries with doubling backoff, by default only after `error` or `lost` |

A host starts jobs strictly in priority order as cores free up. A big job at the head of the queue
is not overtaken by small ones, so it cannot starve. Change your mind later, while the job is still
waiting:

```sh
tshed edit train -p 50          # new priority
tshed edit train -H gpu2        # move it to another host (-H any: let the server choose)
tshed edit train --force        # start it now, ahead of the queue, even on busy cores
tshed cancel train
```

A move only happens if the job has not started on its current host; otherwise nothing changes.

`lost` means the job's watchdog vanished — typically the host rebooted. With retries left the job
simply starts again.

## 4. Let the server pick the host

Leave out `-H` and the job waits on the server until some host has enough idle cores:

```sh
tshed submit -n sim -c 16 -- ./simulate.sh        # → pending (waiting for a host)
```

This needs a server loop running — either the TUI or, headless:

```sh
tshed serve                                 # e.g. in tmux, or as a service
```

Pinned jobs (`-H`) don't need it.

## 5. The dashboard

```sh
tshed
```

Arrow keys or the mouse wheel move, a click selects (click again to open), typing lowercase filters
(`Esc` or `Ctrl-U` clears), capitals act:

| key | action |
|---|---|
| `Enter` / `→` | job detail with a scrollable, live log (`Esc` / `←` back) |
| `N` | new job (same flags as `submit`) |
| `K` / `E` / `P` | cancel / rerun / change priority |
| `F` / `M` | force-start now / move to another host (waiting jobs) |
| `T` | next color theme (saved; `tshed theme` lists them) |
| `B` / `D` | more / less contrast: darker darks, lighter lights (saved; `tshed theme --contrast N`) |
| `A` | follow the log on the host (mosh) |
| `O` | shell on the host (mosh) |
| `H` / `R` | hide finished / refresh |
| `Esc` / `Q` | quit |

While it is open the TUI is also a server loop, like `tshed serve`.

For a quick look without opening it, `tshed overview` prints the hosts table and the active-jobs
table in one screen (equivalent to `hosts | head -30; echo ---LS; ls | head -30`). `-n N` caps
each table at N lines (30 by default), and `-a` includes finished jobs.

Colors follow your terminal by default. `tshed theme` shows every theme as a sample row: built-in
catppuccin, dracula, gruvbox, nord, tokyo-night, solarized and friends, plus every installed btop
theme (`btop:NAME`). Pick one with `tshed theme tokyo-night`. For the look of ratatui's own demos (tailwind
palette, header bar, striped rows) pick `tailwind-blue`, `-emerald`, `-indigo` or `-red`.

Happy with a theme except for one or two colors? Override individual roles on top of any theme:
`tshed scheme err=ff00ff warn=bright_yellow sel=303050` (saved in `~/.tinyshed/scheme`; they survive
`T` in the dashboard). `tshed scheme` lists every role with a sample, `tshed scheme err=default`
undoes one, `--clear` undoes all, and `TINYSHED_SCHEME='err=ff0000'` overrides per shell.

## 6. Callbacks

Create `~/.tinyshed/hooks.py` on the server:

```python
import subprocess

def OnJobCompletion(ev):
    subprocess.run(["notify-send", "done: " + ev["data"]["cmd"]])

def OnJobError(ev):
    if ev["data"]["final"]:                      # False while retries remain
        print(ev["host"], ev["job"], ev["data"]["tail"][-500:])

def OnHostDown(ev):
    print("lost contact with", ev["host"], ev["data"]["err"])
```

Available: `OnJobQueued OnJobStart OnJobCompletion OnJobError OnJobTimeout OnJobOOM OnJobLost
OnJobCancel OnHostDown OnHostUp OnEvent`. Anything a hook prints, and any traceback, lands in
`~/.tinyshed/hooks.log`. The same file on a *host* runs there, at the moment the event happens, even
when the server is unreachable. Server hooks run when the server learns of the event — so something
must be listening: `tshed serve`, the TUI, or phone-home.

## 7. Phone home

By default the server learns about events when it polls. To have hosts push them the moment they
happen, give them a way back:

```sh
tshed install gpu1=me@gpu1.lan gpu2=me@gpu2.lan --home me@server.lan
```

Each host needs an ssh key the server accepts. You can lock that key down to the one command it
needs: look at `home.cmd` in the host's `~/.tinyshed/config.json` and put it in the server's
`authorized_keys` as `command="…" ssh-ed25519 AAAA…`.

## 8. When the network goes away

Nothing special happens, on purpose. Each host keeps its own queue in `~/.tinyshed/host.db` and
carries on starting, timing out and retrying jobs. `tshed hosts` shows it as `down` with the ssh error.
Jobs you submit meanwhile stay `pending` on the server. On reconnect, queued submissions are
delivered and the server replays everything it missed — your hooks fire then, each exactly once.

## 9. Where things live

| where | what |
|---|---|
| server `~/.tinyshed/server.db` | mirror of all jobs, hosts, ingested events |
| host `~/.tinyshed/host.db` | that host's queue and event log (the source of truth) |
| host `~/.tinyshed/config.json` | `name`, `cores`, `home`; edit with `tshed config --edit [--host NAME]` |
| host `~/.tinyshed/logs/ID.log` | live output; becomes `ID.log.zst` when the job is final |
| host `~/.tinyshed/agent.log` | agent and watchdog diagnostics |
| `~/.tinyshed/hooks.py`, `hooks.log` | callbacks and their output |

Nothing is ever deleted; payloads and finished logs are zstd-compressed.

## 10. Troubleshooting

- **Host shows `down`** — the error column is ssh's own message; try `ssh me@host true`.
- **Job stuck `pending`** — it has no `-H` and no `tshed serve`/TUI is running, or no host has enough
  idle cores, or its host is down.
- **Job stuck `queued`** — the host is busy, or a higher-priority job that doesn't fit yet is ahead.
- **`command not found` in the log** — non-login shell; use absolute paths or `bash -lc`.
- **Install fails with "zstd is required"** — install the `zstd` package on that host.
- **`A`/`O` used ssh instead of mosh** — the host has no `mosh-server`, or UDP 60000-61000 doesn't get
  through. ufw: `sudo ufw allow from 192.168.0.0/24 to any port 60000:61000 proto udp`.
- **"unable to open database file"** in an old dashboard — upgrade and restart it (fixed in 0.2).
- **Agent died** — any `tshed hosts` restarts it; look at `agent.log` on the host for why.

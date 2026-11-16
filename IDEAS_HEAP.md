# Ideas heap

Unscheduled. This carries possibilities that have been thought about and not decided, with enough
analysis attached that picking one up does not mean starting over.

---

## 1. Ship tshed as a self-contained binary per platform

**Origin.** User, 2026-11-14: "can this be packaged into a binary (arch/debian-x86 amd +/- arm64
macos?)", then "reserve for later". Same message fixed the assumption that every machine has
Python > 3.11.

### 1.1 What the problem reduces to

Every machine already has an interpreter, so a binary does not remove a Python dependency that
hurts today. What it would remove is the **zstd** dependency: stdlib `compression.zstd` exists only
from 3.14, so 3.12/3.13 hosts currently need the `zstd` binary or `pyzstd`/`zstandard`
(`zmod()` in `tshed.py` exits otherwise). A binary built on 3.14 carries its own.

### 1.2 Why "just freeze it" is not the whole change

The file re-executes itself as a script in several places, all of which assume
`python3 …/tshed.py`:

- `detach()` — spawns the agent and every watchdog as `[sys.executable, SELF]`
- `setup()` — the `@reboot` crontab line
- `REMOTE_PY`, `install()`'s `cat > .tinyshed/tinyshed.py`, `attach_argv()` — the server's idea of
  the remote command
- `remote()` for `local:` hosts; `main()` building the phone-home `home.cmd`

Frozen, `sys.executable` is the binary and `__file__` points inside the bundle, so each needs a
"how do I invoke myself" helper. And `install` must learn the host's `uname -s -m` and send the
matching build, which makes it two ssh round trips or a shell prologue.

### 1.3 Shapes, costed

Sizes and start-up times below are **not measured**; nothing was built.

| shape | removes | build matrix | install payload | per-invocation cost | code change |
|---|---|---|---|---|---|
| A. status quo: script + host zstd | — | none | 57 kB script | python start | none |
| B. zipapp `.pyz` (+ vendored pure-python? none exists for zstd) | nothing | none | ~same | same | small; **buys nothing** — still needs python and zstd |
| C. PyInstaller one-file, Python 3.14 | python + zstd on hosts | one build per OS/arch, no cross-compiling: linux x86_64 in an old-glibc Debian container (so Arch also runs it), macOS arm64 native, optionally linux arm64 | several MB per target (unmeasured) | unpacks to a temp dir on every start — paid on every rpc poll, phone-home and watchdog spawn | self-invocation helper + arch-aware install |
| D. PyInstaller one-dir / Nuitka standalone, shipped as a tarball | same as C | same as C | larger, many files | no unpack | same as C plus untar on install |
| E. keep script on hosts, binary only for the server | nothing on hosts | 1–2 builds | — | — | small; solves no stated problem |

B looks cheapest and buys the least. C is the one that was discussed; its recurring cost is the
per-start unpack, which matters because the transport is "one short-lived process per ssh call".

### 1.4 The precedent it has to answer to

The standing design is "one file, stdlib only; install is `cat` over ssh" (README, AGENTS.md
invariants). C/D replace a readable 57 kB script that upgrades in ~1 s with an opaque per-arch
artifact that needs a build step before every upgrade. That is a real reversal, justified only if
host-side dependencies become the actual pain.

### 1.5 What this does not solve

- Hosts still need `bash`, `taskset` (util-linux) and, for attach, `tail`/`less`/`mosh`.
- `hooks.py` is still executed as Python source — inside the bundled interpreter, so hooks could no
  longer import packages installed for the host's system Python.
- Mixed fleets get harder, not easier: one more artifact per architecture to keep in step with
  `VERSION`.
- The musl case (Alpine) is not covered by a glibc build.

### 1.6 Open questions

1. Is any host actually unable to get `zstd` or Python ≥ 3.14? If none, shape A already suffices.
2. Do hooks need third-party imports from the host's Python? If yes, C/D break them.
3. Is linux arm64 a real target, or only macOS arm64 (server) + linux x86_64 (hosts)? Decides the
   build matrix and whether a container build alone is enough.
4. Measured one-file start-up on the slowest host: if it is a noticeable fraction of the ~0.1 s
   poll, D (no unpack) or a long-lived rpc process is needed instead of C.

### 1.7 Verdict

Not scheduled. Becomes schedulable when question 1 has a concrete "yes, host X" — i.e. a machine
where neither a `zstd` package nor a 3.14 interpreter can be installed — and question 2 is "no".
Then build shape C for the targets named by question 3 and measure question 4 before committing.

---

## 2. Smaller things that fell out

- **zstd on the wire.** Only data at rest is compressed; rpc/phone-home JSON goes through ssh
  uncompressed. Payloads are small (event batches with ≤2 kB tails), so nobody has shown it matters.
- **cgroup memory limit.** The RSS limit is a 2 s poll in the watchdog; `systemd-run --user --scope
  -p MemoryMax=` would make it a hard limit where available. Raised during the initial build, left
  out to keep one mechanism that works everywhere.
- **Server-side list cost with keep-forever.** `load_jobs` reads every job row on each TUI refresh
  (decoded payloads are cached, the rows are not). Fine at thousands of jobs; unmeasured beyond.

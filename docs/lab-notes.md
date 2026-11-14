# Lab notes

[简体中文](lab-notes.zh-CN.md)

What things actually looked like once it was running. Every number here is measured, not estimated; machine names and addresses have been replaced with placeholders.

## What happens after a real push

One line changed in a README and pushed to `main`, three workflows started automatically, and from push to the last run finishing took 2 minutes 39 seconds:

| Time | What happened |
|---|---|
| 14:00:23 | `git push` succeeds |
| 14:00:29 | The relay receives the push webhook (6 seconds, round trip through the public internet) |
| 14:00:31 | GitHub creates 3 runs; 7 jobs start queuing |
| 14:00:37 | The first job is already running on `lab-linux-1` |
| 14:01:02 | Self-hosted Smoke completes (32s) |
| 14:02:39 | Three Platforms completes (2m 08s) |
| 14:03:02 | Syntax Check completes (2m 30s); all three succeeded |

The result was cross-checked in three independent places — one of them alone does not count:

1. **GitHub side**: `gh run list` / `gh api .../runs/<id>/jobs`; each job's `runner_name` and `labels` state directly which machine it landed on.
2. **Machine side**: `~/actions-runner/_diag/Worker_*.log` contains the full sha and run id for this run; the files under `_work/<repo>/<repo>/` are exactly what this checkout produced.
3. **Relay side**: the `workflow_run` stream on the Actions view — 32 events pushed live from `requested` to `completed`.

## Three machines, three platforms

| Machine | OS | Measured specs | Works | Does not work |
|---|---|---|---|---|
| `lab-linux-1` | Ubuntu 24.04 | 6 cores / 31G / 145G free / gcc 13.3 / node 24 | Everything, including compilation | — |
| `lab-mac-1` | macOS 15.8 (Intel) | 3 cores / 5G / 131G / clang + swiftc / node 24 | Tests, x64 builds, checkout | `macos-14` is arm64 so an Intel box cannot produce arm builds; with only the CLT and no Xcode, notarization has no certificate |
| `lab-win-1` | Win11 LTSC 26100 | 3 cores / **3G** / 48G / node 24 / MinGit | Tests, checkout, scripts | No `cl` / `MSBuild` / `makensis` / `docker` / `pwsh`, so Electron + NSIS packaging is impossible |

All three use the same form — just address / port / user / password:

| Platform | How far it gets |
|---|---|
| Linux | Full cycle: install → online → click "uninstall" in the panel → service stopped, directory deleted, registration removed; a second runner on the same machine lands in `~/actions-runner-2` automatically |
| macOS | Full cycle: download → unpack → register → LaunchAgent installed and started → online in 38 seconds |
| Windows | Full cycle: download → register → `run.cmd` launched via WMI → online in 49 seconds (the archive has no `svc.cmd`, so no service is possible and a process has to hold it) |

Weak-network measurement: the first install on a machine took 64 seconds (downloading the 217MB archive); the second install hit the cache and finished in **15 seconds**.

## Free tier

20 repositories, 300 events/day, under 4% of the free allowance, and it does not grow over time (it resets daily):

| Dimension | Peak usage |
|---|---|
| Workers requests | 0.44% |
| Durable Object requests | 0.58% |
| Duration | 0.12% |
| SQL storage | 3.07% (the only thing that accumulates) |

The critical rule: **heartbeats must be sent by the client**. The moment the server uses `setInterval`, duration usage jumps from 0.12% to 85%.

## On-demand runners (experimental)

When a job queues on GitHub, the relay brings a machine up, registers it as a runner, and it disappears once the job finishes. The design, timing and pitfalls are in [the on-demand runner write-up](jit-on-demand-runner.md) (Chinese), and the script is [examples/jit-scheduler.py](examples/jit-scheduler.py).

Key points: a registration token is valid for 1 hour; the random suffix in a runner name distinguishes multiple instances on one machine; and a runner must be explicitly unregistered afterwards or the list fills with offline zombie runners.

# Known limitations

[简体中文](known-limitations.zh-CN.md)

This file is about what does **not** work, and why. Most of it is not missing implementation — it is GitHub not offering the data.

## The webhook payload only has run-level information

The `workflow_run` payload contains: run id, workflow name, branch, status, conclusion, trigger, actor, commit sha.

It does not contain: the jobs inside a run, the individual steps, or logs. Those are only available through the GitHub REST API (`/repos/{owner}/{repo}/actions/runs/{id}/jobs`, `/logs`), which is why "load jobs and steps" makes the panel call GitHub using the local `gh` login — it does not read that from the event.

## Live logs while a run is in progress: exactly one way works

Four channels were tested one by one (measured 2026-11, runner 2.338.2):

| Channel | Available while running? | Measurement |
|---|---|---|
| `/actions/runs/{id}/logs`, `/actions/jobs/{id}/logs` | **No**, 404 | 404 for the whole 40-second run, turning 200 about 6 seconds after the job ends; `gh run view --log` just says `is still in progress` |
| Step states from `runs/{id}/jobs` | Yes, but state only, no text | Polling every 4 seconds shows "1/2 completed" |
| `_diag/Worker_*.log` on the machine | Runner events are there, **step stdout is not** | 20 marker lines printed, 0 grep hits |
| The step `tee`-ing to a local file | **Yes, under 2 seconds of latency** | Sampling every 6 seconds found 3 new lines every time |

Conclusion: to watch scrolling logs on the relay page, the workflow itself has to write a file on the machine and the relay has to tail that machine. GitHub-side text logs only exist once a run has finished.

```yaml
- name: stream and persist at the same time
  run: |
    set -o pipefail            # keep failure semantics, otherwise tee's exit code masks them
    for i in $(seq 1 30); do
      echo "[$i/30] $(date '+%H:%M:%S') running"
      sleep 2
    done 2>&1 | stdbuf -oL tee -a /tmp/gha-live.log
```

- **`stdbuf -oL` is mandatory.** Piping straight into `tee` leaves stdout block-buffered, so output appears only every few dozen seconds and looks like nothing is happening.
- The file stays on the machine and is **not cleaned up**; with several machines each has to be tailed; anything in `/tmp` disappears on reboot.
- This is a known GitHub limitation, not a bug in this project. Community threads [discussions#75518](https://github.com/orgs/community/discussions/75518) has been open since 2023-11 with no official reply as of 2026-11; [#154834](https://github.com/orgs/community/discussions/154834) says that since 2025-03 even partial logs are unavailable. Two widely repeated claims were also checked: `_diag/pages/*.log` stays at 0 bytes for the entire step (it is written only after the step ends, then uploaded and deleted), and `?tail_logs=true` is not a live stream either — it merely returns 200 about 12 seconds before the run ends.

## Log archiving is not implemented

`/logs` returns a zip. It is large and a poor fit for this relay, so the panel shows job and step state only, and logs are read on the GitHub website.

## Writing `.github/workflows/*` needs the `workflow` scope

A local `gh` token usually only carries `repo / read:org / gist / delete_repo`; writing workflow files through the API is rejected with a 404 (writing ordinary files is fine). The workaround is to push over SSH — SSH keys are not subject to OAuth scopes.

## The LAN endpoints are unauthenticated

`/stream`, `/stats`, `/actions` and the status page itself **do not authenticate**: the design assumption is "it runs on a trusted LAN, whoever can connect can look". They only expose public metadata about your own repositories (repository name, branch, commit title) and never a token.

If you need access across network segments, do not expose the port — use an SSH tunnel:

```bash
ssh -L 18091:localhost:18091 you@10.0.0.5
```

The genuinely sensitive endpoints (`/github/*`, `/runners/*`, `/relay/deploy`, which use the local `gh` credentials and the SSH password) **only accept local requests**; LAN access always gets 403.

`GET /config` is readable from the LAN (the token is masked), but **`POST /config` is local-only**: changing the config rewrites `ws_url` and the token, which is equivalent to pointing the entire event stream somewhere else. So opening the panel from a phone or tablet works for reading, but saving settings returns 403 — go back to the machine running the receiver.

## Self-hosted runners on a public repository are dangerous

Public repository + self-hosted means any PR can execute code on your machine. GitHub warns about this too. Either restrict it to private repositories or add an approval gate in the workflow (`environment` with required reviewers).

## Python unit tests do not run on Windows

`unit_machines` asserts that the key file's permissions are `600`, which has no equivalent in the Windows permission model. So Windows only runs the four JS frontend tests; the Python unit tests run on Linux and macOS.

## SQLite is single-process

The local database is SQLite and writes are serialized. At a few thousand rows that is plenty (`/stats` caches hot queries and single-flights them), but do not expect it to behave like a time-series database with hundreds of thousands of rows — if you need long retention, periodically export old rows to JSONL and delete them.

Remote D1 cleans itself up: the Durable Object uses `setAlarm` daily to delete events older than `RETENTION_DAYS` (90 by default), so SQL storage does not grow without bound — measured at 300 events/day it occupies about 3% of the free allowance.

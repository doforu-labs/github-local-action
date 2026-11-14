# Running jobs on your own machines

[简体中文](self-hosted-runner.zh-CN.md)

`runs-on: ubuntu-latest` means a machine GitHub hosts, running in Microsoft's cloud. To land jobs on a box in your LAN, install a self-hosted runner: **no public IP required**, because the runner dials out (polling plus a long-lived connection) — the opposite direction from the relay.

## Manual install (Linux example)

```bash
# 1) get a registration token (valid for 1 hour)
gh api -X POST /repos/<owner>/<repo>/actions/runners/registration-token --jq .token

# 2) install on the target machine
mkdir -p ~/actions-runner && cd ~/actions-runner
curl -fL -o runner.tar.gz \
  https://github.com/actions/runner/releases/download/v2.338.2/actions-runner-linux-x64-2.338.2.tar.gz
tar xzf runner.tar.gz
./config.sh --url https://github.com/<owner>/<repo> --token <token> \
  --name linux-1 --labels self-hosted,linux,x64 --work _work --unattended

# 3) install as a system service so it starts on boot
sudo ./svc.sh install $USER && sudo ./svc.sh start
```

Then pick the machine by label:

```yaml
jobs:
  smoke:
    runs-on: [self-hosted, linux, x64]
```

## Install from the panel (recommended)

The **Machines** page takes address, port, username and password; one click connects, installs everything and streams the log live. **No machine name or directory to fill in**: the name is read from the machine's own hostname, the directory defaults to `~/actions-runner`, and if one already exists it shifts to `-2`, `-3` automatically.

Each machine has two buttons, and they mean different things:

- **Uninstall**: connect, stop the service, uninstall the service, delete the directory, then remove the registration on GitHub
- **Remove registration**: only delete the GitHub-side record; nothing on the machine is touched

If you would rather not hand the password to the page, expand "just generate the command" and copy a ready-made command to the machine.

The flow (`POST /runners/ssh/add`) does this in order:

1. **Probe first, leave GitHub alone**: connect → `uname -s` to identify the OS (Windows has no uname, so it falls back to `$env:OS`) → read the hostname for the machine name → inspect the directory
2. Directories come in four states: `FRESH` (empty), `STALE` (leftovers from an unfinished setup — wipe and restart), `CONFIGURED` (a configured runner already lives there), `DIRTY` (other files). In "auto-shift" mode the last two are not an error, it just tries the next directory name; only an explicitly requested directory is refused. **When the directory is unusable, no registration token is ever requested**
3. **Dodge name clashes first**: fetch the repository's runner list before installing and switch to `-2`, `-3` if the name is taken (`config.sh` with a duplicate name displaces the other registration, so `--replace` is not used)
4. Request a registration token → download (check `~/.cache/github-local-action` first and skip the download if the archive is there) → unpack → `config.sh` → install the service → start the service
5. Poll GitHub until it reports `online` (up to 90 seconds); only once an id comes back is the job done

### Implementation trade-offs

- **The password never goes on a command line**: the sudo password is read from the first line of stdin (`read -r SUDOPW`), otherwise `ps` would expose it. The registration token still appears in the remote `config.sh` arguments for a few seconds — that is the runner's own interface and cannot be avoided.
- **The password is neither written to disk nor kept long**: it lives in memory for **30 minutes**, which is what backs the "uninstall" button during that window. It is gone after expiry or a process restart, and the page says so when you would need to re-enter it.
- **Verify before deleting**: `rm -rf` only runs when one of `config.sh` / `.runner` / `runsvc.sh` exists in the directory. The same gate rejects these at the entry point: `~/.ssh`, `~/.cache/*` (hidden directories), `~/..` (traversal), `~`, `/`, and anything with spaces — all 400.
- **Never delete other people's files**: a directory with non-runner content is refused; only "a runner directory left over from an unfinished setup" gets reset.
- **Downloads have a progress heartbeat**: a 217MB archive over a weak link can take ten-plus minutes, so `curl` runs in the background and reports bytes downloaded every 10 seconds — otherwise it looks hung.
- **A second runner on the same machine needs no directory argument**: `config.sh` refuses to configure a directory twice, so once the first runner owns `~/actions-runner` the second lands in `~/actions-runner-2`, and its name becomes `<hostname>-2`.

## Platform differences (all found the hard way)

| | Linux | macOS | Windows |
|---|---|---|---|
| Service | `svc.sh` (needs sudo) | `svc.sh` (**must not use sudo** — it installs a LaunchAgent for the current user) | There is **no** `svc.cmd` in the archive; only `run.cmd` |
| Survives disconnect? | Service-managed, yes | LaunchAgent-managed | OpenSSH's job object kills children when the session ends, so it has to be launched from WMI via `Invoke-CimMethod Win32_Process Create` |
| Uninstall | Stop by service name | Stop by LaunchAgent | Filter with `Get-CimInstance Win32_Process` by `ExecutablePath` prefix — never kill by process name |
| Extra pitfalls | — | With only the CLT and no Xcode you cannot produce arm64 builds, and notarization lacks a certificate | No `pwsh`; execution policy defaults to Restricted; Chinese text in scripts gets mangled |

The three most common Windows traps:

1. **No `pwsh`**. `shell: pwsh` simply fails with `command not found` — PowerShell 7 is preinstalled on hosted images, but a self-hosted box only has Windows PowerShell 5.1, so write `shell: powershell`.
2. **Execution policy is Restricted**. The runner executes scripts through `powershell -command ". 'script.ps1'"`, which is blocked with `running scripts is disabled`. Change the machine-level policy with `Set-ExecutionPolicy -Scope LocalMachine RemoteSigned` (requires admin).
3. **Chinese text in scripts gets mangled**. PS 5.1 parses `.ps1` using the system code page, while the runner writes UTF-8 without a BOM — Chinese strings are shredded and the whole script fails to parse. Either install PS 7, or keep the Windows steps ASCII-only.

Also: **Windows machines have no git by default**, so `actions/checkout` fails outright. Unpack MinGit portable into `C:\tools\MinGit` and put its `cmd` directory on PATH.

One more easy-to-forget rule: **on Windows, uninstall only kills processes inside its own directory**. A machine can host several runners in several directories; an early version killed by process name, so uninstalling `actions-runner-2` also killed `actions-runner`, nothing restarted it, and that machine went offline.

## Security premises

- **Self-hosted runners are only appropriate for private repositories.** Public repository + self-hosted means anyone's PR can execute code on your machine; GitHub warns about this itself.
- **The self-hosted workflows in this repo are off by default**: every job in `platform-check.yml` / `platform-queue.yml` carries `if: vars.RUN_SELF_HOSTED == 'true'`. To enable them, create a repository variable `RUN_SELF_HOSTED` = `true` under Settings → Secrets and variables → Actions → **Variables**. Forks do not have that variable, so jobs never land on someone else's machine — and no repository name is hardcoded in the workflow.
- **A runner is an executor, not a privilege escalation**: it runs as a normal user, `sudo` has to be configured on the machine itself, and a job gets the repository code, not your login state.
- **The machine's SSH password passes through the panel** (it lives only in local memory for 30 minutes and is never written to disk). That is why `/runners/*` and `/github/*` **only accept local requests**; LAN devices get 403.

## On-demand mode (experimental)

When the machine is not always on, it can be brought up on demand: a job queues on GitHub → your machine is notified → a runner is registered temporarily → it disappears when the job finishes. The experimental script is [`docs/examples/jit-scheduler.py`](examples/jit-scheduler.py) and the measurements are in [lab notes](lab-notes.md).

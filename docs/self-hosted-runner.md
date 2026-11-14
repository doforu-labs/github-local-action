# 让 job 落在自己的机器上

`runs-on: ubuntu-latest` 是 GitHub 托管的机器，跑在微软的云上。想让 job 落在局域网里的机器上，装一个 self-hosted runner 就行：**不需要公网 IP**，是 runner 主动连出去（轮询 + 长连接），方向跟中继正好相反。

## 手动装一台（Linux 举例）

```bash
# 1) 拿注册令牌（1 小时有效）
gh api -X POST /repos/<owner>/<repo>/actions/runners/registration-token --jq .token

# 2) 在被控机器上装
mkdir -p ~/actions-runner && cd ~/actions-runner
curl -fL -o runner.tar.gz \
  https://github.com/actions/runner/releases/download/v2.338.2/actions-runner-linux-x64-2.338.2.tar.gz
tar xzf runner.tar.gz
./config.sh --url https://github.com/<owner>/<repo> --token <token> \
  --name linux-1 --labels self-hosted,linux,x64 --work _work --unattended

# 3) 装成系统服务，开机自启
sudo ./svc.sh install $USER && sudo ./svc.sh start
```

然后用标签挑机器：

```yaml
jobs:
  smoke:
    runs-on: [self-hosted, linux, x64]
```

## 面板上装（推荐）

「机器」页填地址、端口、用户名、密码，点一下就连过去装好，日志实时往下走。**机器名和目录不用填**：名字读机器自己的主机名，目录默认 `~/actions-runner`，那儿已经有一个就自动顺延到 `-2`、`-3`。

每台机器两个按钮，意思不一样：

- **卸载**：连上去把服务停了、服务卸载了、目录删了，再删 GitHub 上的注册
- **移除注册**：只删 GitHub 那一份，机器上不动

不想把密码给页面，可以展开「只生成命令，自己到机器上敲」，拿一段现成的命令粘过去。

这条流程（`POST /runners/ssh/add`）按顺序做这些事：

1. **先探，不动 GitHub**：连上 → `uname -s` 认系统（Windows 没有 uname，退回去问 `$env:OS`）→ 读主机名定机器名 → 看目录里是什么
2. 目录分四种：`FRESH` 空、`STALE` 上次没配完的残留（清掉重来）、`CONFIGURED` 已经有个配好的 runner、`DIRTY` 有别的文件。后两种在「自己顺延」模式下不算错，直接试下一个目录名；只有调用方明确指定了目录才拒。**目录不可用的时候一个令牌都不会去要**
3. **重名先避**：装之前拿一次仓库里的 runner 名单，撞名就换成 `-2`、`-3`（`config.sh` 的同名注册会互相顶掉，所以不用 `--replace`）
4. 要注册令牌 → 下载（先看 `~/.cache/github-local-action` 里有没有包，有就不下）→ 解包 → `config.sh` 注册 → 装服务 → 启服务
5. 轮询 GitHub 等它变成 `online`（最多 90 秒），拿到 id 才算完

### 几个实现上的取舍

- **密码不进命令行**：sudo 密码从 stdin 第一行读（`read -r SUDOPW`），否则 `ps` 里一眼就能看到。注册令牌仍会出现在远程 `config.sh` 的参数里（几秒），这是 runner 自己设计的接口，躲不掉。
- **密码不落盘、也不长留**：只在内存里存 **30 分钟**，支撑这段时间内的「卸载」按钮；过期或进程重启就没了，想卸得重填（页面会明说）。
- **删除前先验身**：`rm -rf` 只在目录里存在 `config.sh` / `.runner` / `runsvc.sh` 之一时执行。同一条闸在入口挡了目录写法：`~/.ssh`、`~/.cache/*`（隐藏目录）、`~/..`（上跳）、`~`、`/`、带空格的名字全部 400。
- **不替用户删东西**：目录里有非 runner 内容就拒，只有「上次没配完的 runner 目录」才会被重置。
- **下载有进度心跳**：217MB 的包在弱网下能下十几分钟，`curl` 在后台跑、每 10 秒报一次已下载大小，否则看起来像卡死。
- **同机装第二个 runner 不用管目录**：`config.sh` 不允许一个目录配两次，所以第一个占了 `~/actions-runner` 之后，第二个自动落到 `~/actions-runner-2`，名字也会从 `主机名` 变成 `主机名-2`。

## 三个平台的差异（都是实测撞出来的）

| | Linux | macOS | Windows |
|---|---|---|---|
| 服务 | `svc.sh`（要 sudo） | `svc.sh`（**不能加 sudo**，装的是用户自己的 LaunchAgent） | 包里**没有** `svc.cmd`，只能用 `run.cmd` |
| 断连后还活着吗 | 服务托管，天然活着 | LaunchAgent 托管 | OpenSSH 的作业对象会随会话结束杀掉子进程，得用 `Invoke-CimMethod Win32_Process Create` 从 WMI 拉起 |
| 卸载 | 按服务名停 | 按 LaunchAgent 停 | 用 `Get-CimInstance Win32_Process` 按 `ExecutablePath` 前缀过滤，别按进程名一刀切 |
| 额外坑 | — | 只有 CLT 没有 Xcode 就出不了 arm 包、公证缺证书 | 没有 `pwsh`；执行策略默认 Restricted；脚本里的中文会乱码 |

Windows 上最容易踩的三个：

1. **没有 `pwsh`**。`shell: pwsh` 直接 `command not found`——PowerShell 7 是托管镜像预装的，自建机器上只有 Windows PowerShell 5.1，写成 `shell: powershell`。
2. **执行策略是 Restricted**。runner 用 `powershell -command ". 'script.ps1'"` 跑脚本，会被 `running scripts is disabled` 拦下。改机器级策略：`Set-ExecutionPolicy -Scope LocalMachine RemoteSigned`（需管理员）。
3. **脚本里的中文会变乱码**。PS 5.1 按系统代码页解析 `.ps1`，而 runner 写出来的是无 BOM 的 UTF-8，中文字符串会被打碎、整段脚本报语法错。要么装 PS 7，要么 Windows 那几步只用 ASCII。

另外：**Windows 机器默认没有 git**，`actions/checkout` 会直接失败。装一个 MinGit portable 解压到 `C:\tools\MinGit`，把它的 `cmd` 目录塞进 PATH。

还有一条容易忘的：**Windows 卸载只杀自己目录里的进程**。一台机器可以有好几个目录各跑一个 runner，早先按进程名一刀切 `Stop-Process`，卸 `actions-runner-2` 会把 `actions-runner` 那份也杀掉、且没人重启，那台直接变 offline。

## 安全前提

- **私有仓库才适合自建 runner**。公开仓库 + self-hosted 意味着任何人的 PR 都能在你的机器上执行代码，GitHub 自己也会警告。
- **runner 只是执行器，不是特权**：它以普通用户跑，`sudo` 要自己在机器上配；job 拿到的是仓库代码，不是你的登录态。
- **这台机器的 SSH 密码会经过面板**（只在本机内存里活 30 分钟、不落盘）。所以 `/runners/*` 和 `/github/*` 这些接口**只接受本机请求**，局域网设备访问会拿到 403。

## 按需模式（实验）

机器不是一直开着的时候，可以按需拉起：GitHub 上 job 排队 → 你的机器收到通知 → 临时注册一个 runner → 跑完自己消失。实验脚本在 [`docs/examples/jit-scheduler.py`](examples/jit-scheduler.py)，实测记录在 [实测笔记](lab-notes.md)。

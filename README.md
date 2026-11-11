# GitHub Local Action

把 GitHub 账户下所有仓库的事件，实时送到局域网里的设备；把 GitHub 的 job，派到你自己机器上跑。

```text
GitHub ──webhook──▶ remote-worker ──WebSocket──▶ local-client ──SSE──▶ 局域网设备
         (公网入口)     (Cloudflare)      (你要连出去的线)     (本机端口)
```

两个目录，互不依赖，各自能单独跑：

| 目录 | 跑在哪 | 干什么 |
|---|---|---|
| `remote-worker/` | Cloudflare（免费计划） | 收 GitHub 的 webhook，存进 D1，通过 WebSocket 推给本地 |
| `local-client/` | 你的机器 | 连远端 WebSocket，落本地 SQLite，用 SSE 推给局域网设备 |

## 给 agent 用

不想看文档、想让 AI 直接用这套东西？把这一行丢给它：

```
读 http://127.0.0.1:18091/agent.md，照里面的接口操作
```

那份 [`AGENT.md`](AGENT.md) 写了全部接口、参数、返回结构、权限边界（哪些只限本机）、
五个能直接粘贴的 curl 配方，以及「哪些操作得先问人」。服务自己也把它吐在
`/agent.md` 和 `/llms.txt` 上，所以 agent 只要拿到一个地址就能开工。

**有 MCP server**：`local-client/mcp_server.py`（stdio，只依赖标准库）。客户端配置里写一行就行：

```json
{"mcpServers": {"github-local-action": {
  "command": "python3",
  "args": ["/绝对路径/local-client/mcp_server.py"]}}}
```

7 个工具全只读：`health` `get_stats` `list_runs` `get_run_detail` `get_job_log` `recent_events`
`list_machines`。它不重写任何逻辑，就是把下面那些 HTTP 接口翻译一遍——不接 MCP 直接 curl
也一样能干活。

## 为什么要两层

GitHub 只能往公网地址发东西，你家/公司的网进不去。Cloudflare 替你当那个公网落脚点；你的机器主动往外连一条 WebSocket（NAT 不拦出站），远端有事件就顺着这条线推下来。整条链路没有轮询，延迟 1 秒级。

## 快速开始

**1. 远端**

```bash
cd remote-worker
npm install
npx wrangler d1 create github_local_action          # 记下输出的 database_id 填进 wrangler.toml
npx wrangler d1 execute github_local_action --file schema.sql --remote
npx wrangler secret put GITHUB_WEBHOOK_SECRET
npx wrangler secret put CLIENT_TOKEN
npx wrangler deploy
```

部署完会给你一个 `https://github-local-action.<你的账号>.workers.dev`。

**2. GitHub 侧**

建一个 GitHub App（比逐仓库配 webhook 省事），Webhook URL 填 `https://github-local-action.xxx.workers.dev/gh/webhook`，secret 用上一步那个，订阅你要的事件（push / pull_request / issues 等），然后安装到你的账户。

**3. 本地**

```bash
cd local-client
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp config.example.toml config.toml   # 填 WS 地址和 token
python3 relay.py
```

**4. 局域网设备**

浏览器打开 `http://<你的内网IP>:18091/` 就是状态面板；机器可读的接口是：

```bash
curl -N http://<你的内网IP>:18091/stream      # SSE 事件流
curl http://<你的内网IP>:18091/health         # 连接状态
curl http://<你的内网IP>:18091/config         # 当前配置（令牌只回掩码）
curl http://<你的内网IP>:18091/actions        # 工作流运行列表（?repo= 过滤）
```

中继地址、令牌和几个心跳参数可以在面板的「设置」页直接改，保存后会写回 `config.toml`（改前留 `.bak`），连接参数改完立即重连；监听地址、端口、库路径要重启进程才生效，页面上标了「改这些要重启进程」。

### 动作页能看到什么

侧栏「动作」把 `workflow_run` 事件按 run id 去重、拼成运行列表：工作流名、分支、状态图标、触发方式、用时、提交和发起人，点开一行能看细节并跳到 GitHub。

前提是 GitHub 的 webhook 里勾了 **Workflow runs** 事件，否则一条都不会来。

做得到和做不到的界线很明确：webhook 载荷只有**运行级**信息。单个运行里的 job、每个步骤、日志都不在载荷里，要调 GitHub REST API（`/repos/{owner}/{repo}/actions/runs/{id}/jobs`、`/logs`）才拿得到。

### 用本机的 gh 补齐 job 与步骤

不用另建 PAT：只要本机装了 `gh` 且已 `gh auth login`，接收端会直接复用它的登录态。

```bash
gh auth status        # 确认已登录，scope 里要有 repo（私有仓库）
```

- 设置页有一张「GitHub 凭据」卡片，显示 gh 版本、账号、令牌 scope，可重新检测
- 动作页底部的「直接问 GitHub」可以直接拉任意仓库的历史运行（不看 webhook）
- 点开任意一次运行，「加载 job 与步骤」显示每个 job 和每一步的状态，和 GitHub 页面上看到的一致

安全边界：令牌由 gh 自己（keyring）保管，接收端不读不写不落盘；`/github/*` 这几个接口**只接受本机请求**，局域网设备访问会拿到 403——没登录 GitHub 的设备不应该能通过这台机器读你的私有仓库。

日志仍然要单独下：`/logs` 返回的是 zip，体积大、不适合放进这个中转，暂时没做。

### 让它跑在自己的机器上（self-hosted runner）

默认的 `runs-on: ubuntu-latest` 是 GitHub 托管的机器，跑在微软的云上。想让 job 落在自己 LAN 里的机器上，装一个 runner 就行：不需要公网 IP，是 runner 主动连出去（轮询 + 长连接），跟中继的方向相反。

```bash
# 1) 拿注册令牌（1 小时有效）
gh api -X POST /repos/<owner>/<repo>/actions/runners/registration-token --jq .token

# 2) 在被控机器上装
mkdir -p ~/actions-runner && cd ~/actions-runner
curl -fL -o runner.tar.gz \
  https://github.com/actions/runner/releases/download/v2.338.2/actions-runner-linux-x64-2.338.2.tar.gz
tar xzf runner.tar.gz
./config.sh --url https://github.com/<owner>/<repo> --token <token> \
  --name linux-22 --labels self-hosted,linux,x64 --work _work --unattended

# 3) 装成系统服务，开机自启
sudo ./svc.sh install $USER && sudo ./svc.sh start
```

然后用标签挑机器：

```yaml
jobs:
  smoke:
    runs-on: [self-hosted, linux, x64]
```

几点实测结论：

- **私有仓库才适合自建 runner**。公开仓库 + self-hosted 意味着任何人的 PR 都能在你的机器上执行代码，GitHub 自己也会警告。
- **runner 只是执行器，不是特权**：它以普通用户跑，`sudo` 要自己在机器上配；job 拿到的是仓库代码，不是你的登录态。
- **结果两条路都能看**：GitHub 自己存一份日志；机器本地也留一份，`~/actions-runner/_diag/Worker_*.log` 是每次 job 的完整执行记录，断网时照样能翻。
- **不要再改 workflow 文件时要 `workflow` scope**。我们本机的 gh 令牌只有 `repo / read:org / gist / delete_repo`，写 `.github/workflows/*` 会被 API 以 404 拒绝（写普通文件正常）。绕法是走 SSH 推送——SSH 密钥不受 OAuth scope 限制。

这台 LAN 里三台机器都已装上 runner，并在同一次 `Three Platforms` run 里各跑一个 job、全部通过（run 42967265176）：

| 机器 | 系统 | 实测配置 | 已验证能跑 | 跑不起来的 |
|---|---|---|---|---|
| `:22` dev | Ubuntu 24.04 | 6 核 / 31G / 145G 空闲 / gcc 13.3 / node 24 / git 2.43 / docker 29 | 一切，含编译 | — |
| `:2222` dev | macOS 15.8（**Intel**） | 3 核 / 5G / 131G / clang + swiftc / node 24 / git 2.39 | 测试、x64 编译、checkout | `macos-14` 是 arm64，Intel 机器出不了 arm 包；没有 Xcode 只有 CLT，公证缺证书 |
| `:2223` Docker | Win11 LTSC 26100 | 3 核 / **3G** / 48G 空闲 / node 24 / git 2.56（portable） | 测试、checkout、脚本 | **没有 cl / MSBuild / makensis / docker / pwsh**，Electron + NSIS 打包现在做不了 |

### 装好之后：左侧「Runner」页

不用记上面那串命令，中继页面的 **Runner** 页就能日常操作：

- 填仓库名拉出该仓库下的所有 runner：在线/离线、在不在跑活、系统与架构、自定义标签，在线的排前面，每 8 秒自己刷一次
- **「添加一台机器」走 SSH 直接装**：只填地址 / 端口 / 用户名 / 密码，点一下就连过去把 runner 装好，日志实时往下走（细节见下一节）。**机器名和目录不用填**：名字读机器自己的主机名，目录默认 `~/actions-runner`，那儿已经有一个就自动顺延到 `-2`、`-3`
- 每台机器有两个按钮，意思不一样：**「卸载」**是连机器上把服务停了、服务卸载了、目录删了，再删 GitHub 注册；**「移除注册」**只删 GitHub 那一份，机器上不动
- 不想把密码给页面，就展开「只生成命令，自己到机器上敲」，拿一段现成的命令自己粘

注册令牌 1 小时有效；这一页的接口和 `/github/*` 一样只接受本机请求。

### 通过 SSH 装：密码只活一次

这个流程叫 `POST /runners/ssh/add`，用一个任务 id 轮询日志（`GET /runners/ssh/job/{id}`）。它做的事按顺序是：

1. **先探，不动 GitHub**：连上 → `uname -s` 认系统（Windows 没有 uname，退回去问 `$env:OS`）→ 读主机名定机器名 → 看目录里是什么
2. 目录分四种：`FRESH` 空、`STALE` 上次没配完的残留（清掉重来）、`CONFIGURED` 已经有个配好的 runner、`DIRTY` 有别的文件。后两种在「自己顺延」模式下不算错，直接试下一个目录名；只有调用方明确指定了目录才拒。**目录不可用的时候一个令牌都不会去要**——早期版本先要令牌再检查，目录不可用时白造一堆令牌
3. **重名先避**：装之前拿一次仓库里的 runner 名单，撞名就换成 `-2`、`-3`（`config.sh` 的同名注册会互相顶掉，所以不再用 `--replace`）
4. 要注册令牌 → 下载（**先看 `~/.cache/github-local-action` 里有没有包**，有就不下）→ 解包 → `config.sh` 注册 → 装服务 → 启服务
5. 轮询 GitHub 等它变成 `online`（最多 90 秒），拿到 id 才算完

几个实现上的取舍：

- **密码不进命令行**：sudo 密码从 stdin 第一行读（`read -r SUDOPW`），否则 `ps` 里一眼就能看到。注册令牌仍会出现在远程 `config.sh` 的参数里（几秒），这是 runner 自己设计的接口，躲不掉。
- **密码不落盘、也不长留**：只在内存里存 **30 分钟**，支撑这段时间内的「卸载」按钮；过期或进程重启就没了，想卸得重填地址/用户/密码（页面会明说）。
- **删除前先验身**：`rm -rf` 只在目录里存在 `config.sh` / `.runner` / `runsvc.sh` 之一时执行，否则直接报错不动手。同一条闸还在入口挡了目录写法：`~/.ssh`、`~/.cache/*`（隐藏目录）、`~/..`（上跳）、`~`、`/`、带空格的名字全部 400。
- **不替用户删东西**：目录里有非 runner 内容就拒，只有「上次没配完的 runner 目录」才会被重置。
- **下载有进度心跳**：217MB 的包在弱网下能下十几分钟，`curl` 在后台跑、每 10 秒报一次已下载大小，否则看起来像卡死。
- **同机装第二个 runner 不用管目录**：`config.sh` 不允许一个目录配两次，所以第一个占了 `~/actions-runner` 之后，第二个自动落到 `~/actions-runner-2`，名字也会从 `主机名` 变成 `主机名-2`。实测同一台 Linux 上跑通了：自动定名 `lab-linux-1`、自动落到 `~/actions-runner-2`，10 秒上线，卸载后目录和注册都清干净。
- **Windows 卸载只杀自己目录里的进程**：一台机器可以有好几个目录各跑一个 runner，而 Windows 没有 `svc.cmd`、服务管理器也不管它们。早先按进程名一刀切 `Stop-Process`，卸 `actions-runner-2` 会把 `actions-runner` 那份也杀掉、且没人重启，那台直接变 offline（实测踩到）。现在用 `Get-CimInstance Win32_Process` 按 `ExecutablePath` 前缀过滤。
- **Windows 走 `-EncodedCommand`**：PowerShell 脚本转 UTF-16LE + base64 传过去，避开 cmd.exe 那层吃引号；参数本身用单引号包，`'` 靠写两遍转义。
- **macOS 的 `svc.sh` 不能加 sudo**：它装的是用户自己的 LaunchAgent，脚本开头就看 EUID，root 一律 `Must not run with sudo` 退出。所以 macOS 那一支的 `svc.sh install/start` 和删目录都是裸跑。（这是真机上撞到的，Linux 那支必须 sudo，两边不一样。）
- **Windows 上 `√` 会变 `�`**：`config.cmd` 输出 UTF-8，PowerShell 默认按控制台代码页解。脚本开头加 `[Console]::OutputEncoding` 与 `$OutputEncoding` 两句 UTF8 就好了（实测对子进程也生效）。

实测（2026-11-11）：

| 平台 | 机器名（自动取的） | 做到哪一步 |
|---|---|---|
| Linux `:22` | `lab-linux-1` | **全流程跑通**：装 → online → 页面点「卸载」→ 服务停、目录删、注册删；同机装第二个自动落到 `~/actions-runner-2` |
| macOS `:2222`（iMac Pro） | `lab-mac-1` | **全流程跑通**：下包 → 解包 → 注册 → LaunchAgent 装好并启动 → 38 秒 online |
| Windows `:2223`（3 核 3G） | `lab-win-1` | **全流程跑通**：下包 → 注册 → WMI 拉起 `run.cmd` → 49 秒 online（Windows 的 runner 包里没有 `svc.cmd`，起不了服务，只能靠进程守着） |

三个平台都是同一个表单、只填地址/端口/用户/密码。

弱网实测：同一台机器第一次装 64 秒（下 217MB），第二次命中缓存后 **15 秒**装完。

### 一次真实 push 之后发生了什么（实测）

改一行 README 推到 `main`（commit `7777bde`），三个 workflow 全部自动起来，从推送到最后一个跑完共 2 分 39 秒：

| 时刻 | 发生了什么 |
|---|---|
| 14:00:23 | `git push` 成功 |
| 14:00:29 | 中继收到 push webhook（绕公网一圈回来，6 秒） |
| 14:00:31 | GitHub 建出 3 个运行，7 个 job 陆续排队 |
| 14:00:37 | 第一个 job 已在 `linux-22` 上开跑 |
| 14:01:02 | Self-hosted Smoke 完成（32 秒） |
| 14:02:39 | Three Platforms 完成（2 分 8 秒） |
| 14:03:02 | Syntax Check 完成（2 分 30 秒），三个全 success |

结果要在三处独立核对，只看到一处不算：

1. **GitHub 侧**：`gh run list` / `gh api .../runs/<id>/jobs`，每个 job 的 `runner_name` 与 `labels` 直接写着落在哪台机器（`linux-22` / `mac-2222` / `win-2223`）。
2. **机器侧**：`~/actions-runner/_diag/Worker_*.log` 里能搜到本次的完整 sha 与 run id；`_work/<repo>/<repo>/` 里的文件就是这次 checkout 下来的内容（改了哪行、有没有出现，一眼看得出）。
3. **中继侧**：动作页的 `workflow_run` 事件流，这一次从 `requested` 到 `completed` 一共 32 条事件实时推过来。

前提是仓库 webhook 里勾了 **Workflow runs**——没勾的表现是「CI 明明跑了，动作页一直空」，不是中继坏了。

这次验的是链路（推送能不能自动触发、job 是不是真落在自己机器上、结果能不能看到），**不是真构建**：macOS 那台只有 CLT 没有 Xcode，Windows 那台 3G 内存，Electron + NSIS 打包这两台现在都出不了包。

### 运行中的日志能实时看到吗（实测）

四条通道逐个试过，只有最后一条能拿到实时**文本**：

| 通道 | 运行中能不能拿 | 实测 |
|---|---|---|
| `/actions/runs/{id}/logs`、`/actions/jobs/{id}/logs` | **不能**，404 | 40 秒的运行期间一直 404，job 结束后约 6 秒才变 200；`gh run view --log` 直接报 `is still in progress; logs will be available when it is complete` |
| `runs/{id}/jobs` 里的步骤状态 | 能，但只有状态没有文字 | 4 秒轮询能看到「1/2 已完成，正在跑 = 每两秒一行」 |
| 机器上 `_diag/Worker_*.log` | runner 自己的事件有，**step 的 stdout 没有** | 拿探针打了 20 行标记，grep 命中 0 行 |
| 步骤自己 `tee` 到机器本地 | **能，延迟不到 2 秒** | 每 6 秒采样一次，每次都多出 3 行，与输出同步 |

结论：想在中继页上看「正在滚的日志」，只有一条路——让 workflow 自己落一份到机器上，中继去 tail 那台机器。GitHub 侧的文本日志天生是跑完才有；网页端点开能看到实时文字，那是浏览器登录态走的内部接口，REST 不开放。

步骤里这么写：

```yaml
- name: 一边输出一边落盘
  run: |
    for i in $(seq 1 30); do
      echo "[$i/30] $(date '+%H:%M:%S') 正在跑"
      sleep 2
    done 2>&1 | stdbuf -oL tee -a /tmp/gha-live.log
```

三个注意点：

- **必须 `stdbuf -oL`**。直接管道给 `tee`，stdout 会被块缓冲，几十秒才吐一次，看起来像没在跑。
- 文件留在机器上**不自动清**，多台机器要各自 tail；写 `/tmp` 重启就没了，要长期留就写到 `~/actions-runner/_logs/`。
- 步骤失败时 `tee` 照样写完了，但 `pipefail` 没开的话 `tee` 的退出码会盖掉失败——要保留失败语义就在 run 开头加 `set -o pipefail`。

#### 这不是我们踩的坑，是 GitHub 的已知限制

社区里有一个从 2023-11 挂到现在的大帖：`REST API .../attempts/{n}/logs returns 404 while workflow run is still running`，985 个赞、241 条回复、**官方一直没回答**（`isAnswered: false`），最后一次有人回帖是 2026-11-09。

- https://github.com/orgs/community/discussions/75518 （主帖，满屏 +1）
- https://github.com/orgs/community/discussions/154834 （2025-03：说以前能拿到半截日志，后来变成 404）
- https://github.com/actions/runner/issues/891 （self-hosted runner 能不能把 job 日志丢到 stdout）

帖子里的两个关键说法，我们都去验了：

1. **网页端能看到实时文字，是因为它走了未公开接口**。原帖的说法是「它调用了一个未公开的 API 去拿单个 step 的日志，而且是实时的」。这解释了为什么数据明明在服务器上、REST 却不给——要用这条路只能挂浏览器登录态去抓那个请求，GitHub 改一次前端就可能失效，不建议做进产品。
2. **runner 把 step 日志放在 `_diag/pages`，上传后删掉**（runner 维护者 TingluoHuang 的说法）。**实测在我们这台 runner 2.338.2 上不可用**：连续采了十几次样，`_diag/pages/*.log` 在整个步骤执行期间一直是 **0 字节**，job 结束后整个目录被清空。也就是说它是「步骤结束时才落盘、随即上传删除」，tail 不到中间过程。（另一个项目 hangar 的文档说这里是「live console output」，至少在我们这个版本对不上。）
3. 官方有个 `ACTIONS_RUNNER_PRINT_LOG_TO_STDOUT=1`（runner v2.300+），但它打的是 **runner 自己的日志**，不是步骤输出，而且很啰嗦（成段 JSON 折行），不适合当业务日志看。
4. `?tail_logs=true` 这个参数在帖子里被提到，我们实测：**不是实时流**。同一个 job，plain 一直 404，`tail_logs=true` 在结束前约 12 秒才开始返 200，只是个提前量。


### Windows 自建 runner 的三个坑（都是实测撞到的）

1. **没有 `pwsh`**。`shell: pwsh` 直接报 `pwsh: command not found` —— PowerShell 7 是托管镜像预装的，自建机器上只有 Windows PowerShell 5.1，写成 `shell: powershell`。
2. **执行策略是 Restricted**。runner 用 `powershell -command ". 'script.ps1'"` 跑脚本，策略为 Restricted 时直接 `running scripts is disabled`。改机器级策略：`Set-ExecutionPolicy -Scope LocalMachine RemoteSigned`（需管理员）。
3. **脚本里的中文会变乱码**。PS 5.1 按系统代码页解析 `.ps1`，而 runner 写出来的是无 BOM 的 UTF-8，中文字符串会被打碎、整段脚本报语法错。要么装 PS 7，要么 Windows 那几步只用 ASCII。

另外：Windows 的 runner 包里**没有 `svc.cmd`**（不像 Linux/macOS 有 `svc.sh`），所以没法装成服务。用 `run.cmd` 起后台进程时注意——Windows OpenSSH 的作业对象会随 SSH 会话结束一起杀掉子进程，得靠 `Invoke-CimMethod -ClassName Win32_Process -MethodName Create` 这样从 WMI 拉起才能活过断连。

还有 **Windows 机器默认没有 git**，`actions/checkout` 会直接失败；装一个 MinGit portable 解压到 `C:\tools\MinGit`，再把它的 `cmd` 目录塞进 PATH 就行。

## 成本

免费计划，实测模型见下方。20 个仓库、300 事件/天的量级，占用不到 4% 的免费额度，且不随时间增长（每天重置）。

| 维度 | 峰值占用 |
|---|---|
| Workers 请求 | 0.44% |
| Durable Object 请求 | 0.58% |
| Duration | 0.12% |
| SQL 存储 | 3.07%（唯一会累积的） |

关键纪律：**心跳必须由客户端发**。服务端只要用 `setInterval` 就永远无法休眠，duration 会从 0.12% 直接跳到 85%。

## 验收

`local-client/tests/e2e.py` 起一个假 Hub，验证 50 项：连接 → 续传 → 去重 → 断线重连后不重复 → 状态页 → 动作页 → runner 页 → 优雅退出。不需要 Cloudflare 账号就能跑。
`remote-worker/tests/smoke.py` 对着 `wrangler dev` 验 10 项：验签、幂等、token、补发、心跳。

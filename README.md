# gh-relay

把 GitHub 账户下所有仓库的事件，实时送到局域网里的设备。

```text
GitHub ──webhook──▶ remote-worker ──WebSocket──▶ local-client ──SSE──▶ 局域网设备
         (公网入口)     (Cloudflare)      (你要连出去的线)     (本机端口)
```

两个目录，互不依赖，各自能单独跑：

| 目录 | 跑在哪 | 干什么 |
|---|---|---|
| `remote-worker/` | Cloudflare（免费计划） | 收 GitHub 的 webhook，存进 D1，通过 WebSocket 推给本地 |
| `local-client/` | 你的机器 | 连远端 WebSocket，落本地 SQLite，用 SSE 推给局域网设备 |

## 为什么要两层

GitHub 只能往公网地址发东西，你家/公司的网进不去。Cloudflare 替你当那个公网落脚点；你的机器主动往外连一条 WebSocket（NAT 不拦出站），远端有事件就顺着这条线推下来。整条链路没有轮询，延迟 1 秒级。

## 快速开始

**1. 远端**

```bash
cd remote-worker
npm install
npx wrangler d1 create gh_relay          # 记下输出的 database_id 填进 wrangler.toml
npx wrangler d1 execute gh_relay --file schema.sql --remote
npx wrangler secret put GITHUB_WEBHOOK_SECRET
npx wrangler secret put CLIENT_TOKEN
npx wrangler deploy
```

部署完会给你一个 `https://gh-relay.<你的账号>.workers.dev`。

**2. GitHub 侧**

建一个 GitHub App（比逐仓库配 webhook 省事），Webhook URL 填 `https://gh-relay.xxx.workers.dev/gh/webhook`，secret 用上一步那个，订阅你要的事件（push / pull_request / issues 等），然后安装到你的账户。

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
- 「生成注册令牌」选系统（Linux / macOS / Windows）和机器名，把带令牌的完整命令拼好，点一下复制，粘到目标机器上执行就行
- 死掉的注册可以「移除」（机器上那份文件不会自动清）

注册令牌 1 小时有效，只当文本返回、页面不落盘；这一页的接口和 `/github/*` 一样只接受本机请求。

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

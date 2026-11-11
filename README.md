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

`local-client/tests/e2e.py` 起一个假 Hub，验证 17 项：连接 → 续传 → 去重 → 断线重连后不重复 → 状态页 → 优雅退出。不需要 Cloudflare 账号就能跑。
`remote-worker/tests/smoke.py` 对着 `wrangler dev` 验 10 项：验签、幂等、token、补发、心跳。

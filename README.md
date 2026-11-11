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
```

中继地址、令牌和几个心跳参数可以在面板的「设置」页直接改，保存后会写回 `config.toml`（改前留 `.bak`），连接参数改完立即重连；监听地址、端口、库路径要重启进程才生效，页面上标了「改这些要重启进程」。

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

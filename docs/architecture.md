# 架构

## 为什么必须两层

GitHub 只能往**公网地址**发 webhook，你家的网它进不去。反过来，你的机器可以随便往外连（出站 NAT 不做限制）。

所以整条链路的形状是「一个公网落脚点 + 一条你自己连出去的长连接」：

```text
GitHub ──webhook──▶ remote-worker ──WebSocket──▶ local-client ──SSE──▶ 局域网设备
         (公网入口)     (Cloudflare)      (你要连出去的线)     (本机端口)
```

- `remote-worker`：Cloudflare Worker + Durable Object。收 webhook、验签、写 D1、广播、秒回 202。它是无状态的，广播必须交给 DO（只有 DO 能持有长连接）。
- `local-client`：`relay.py`。主动连远端、落本地 SQLite、开 SSE。局域网设备只连它，不碰公网。

两层的好处不只是「穿透」：本地有库，所以断网、重启、远端重新部署之后，设备重连还能把缺的事件补齐。

## 数据怎么流

```text
POST /gh/webhook
  ├─ 验签（HMAC-SHA256，定时安全比较）
  ├─ 按 X-GitHub-Delivery 幂等落 D1（同一条重投不会存两次）
  ├─ 转给 DO 广播给所有在线的本地客户端
  └─ 202，不等客户端确认
```

本地客户端连上时先发 `{"type":"hello","cursor":N}`，服务端回 `ready` 再把 `id > N` 的事件补发一遍。所以：

- **断线期间的事件不会丢**：只要在 D1 里，回来就补
- **重复的事件不会重**：本地按 `id` 去重，SSE 侧按游标推进
- **没有轮询**：只有 DO 的广播和心跳

心跳方向是**客户端发、服务端回**。这是硬性纪律：服务端只要用 `setInterval` 就永远无法休眠，免费额度的 duration 会从 0.12% 直接跳到 85%。

## 协议

| 方向 | 消息 | 含义 |
|---|---|---|
| 客户端 → 服务端 | `{"type":"hello","cursor":N}` | 我从 N 之后要 |
| 服务端 → 客户端 | `{"type":"ready","cursor":N}` | 补发完毕，现在开始实时推 |
| 服务端 → 客户端 | `{"type":"event","id":N,"event":"push",…}` | 一条事件 |
| 客户端 → 服务端 | `{"type":"ping"}` / 服务端 `{"type":"pong"}` | 应用层心跳 |

两层心跳分开配：`heartbeat`（协议层 ping，防 NAT 回收空闲连接，30 秒是常规值）、`app_ping`（应用层，顺带确认对端真的还在处理消息）。

## 存储

**远端 D1**（`remote-worker/schema.sql`）：

- `gh_events`：`id` 自增就是续传游标；`delivery`（X-GitHub-Delivery）唯一索引做幂等
- `webhook_secrets`：每仓库一个 webhook 密钥，面板「自动配 webhook」往里写。不配就统一用全局 `GITHUB_WEBHOOK_SECRET`

**本地 SQLite**（`events.db`）：`events` 表存事件、`meta` 存游标，另有 `machines` 表存 runner 机器的连接信息。密码用 Fernet 加密，密钥单独放 `events.db.machines.key`（0600 权限）。两样分开存：光把 `events.db` 拷走解不出密码，密钥漏了也只是解不出来（明确为空），不会崩、更不会悄悄连错机器。

## 成本

免费计划，20 个仓库、300 事件/天的量级实测占用：

| 维度 | 峰值占用 |
|---|---|
| Workers 请求 | 0.44% |
| Durable Object 请求 | 0.58% |
| Duration | 0.12% |
| SQL 存储 | 3.07%（唯一会累积的） |

每天重置的部分不会随时间增长，只有 SQL 存储会慢慢涨——需要的话在 D1 上按 `created_at` 定期删旧行（`idx_gh_events_created` 就是给这个用的）。

## 为什么不用别的做法

| 方案 | 为什么没选 |
|---|---|
| 自己开个公网 VPS 收 webhook | 多一台要维护、要钱的机器，还得配证书、防扫描 |
| 内网穿透（frp / ngrok） | 要长期占一个公网端口或第三方域名，断线重连是黑盒 |
| 定时轮询 GitHub API | 有速率限制、延迟以分钟计、拿不到 webhook 才有的字段 |
| 浏览器直接连 GitHub | GitHub 的 webhook 不会推到浏览器；REST 又没有实时性 |

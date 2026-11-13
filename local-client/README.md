# local-client

本地 WS 接收端。主动往外连远端，把事件落到本地 SQLite，再在局域网用 SSE 推给设备。

```text
远端 WS ──▶ relay.py ──▶ events.db（去重 + 游标）
                 └─────▶ GET /stream（SSE）
```

## 跑起来

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

cp config.example.toml config.toml
# 填 ws_url 和 token

python3 relay.py
```

局域网设备：

```bash
curl -N http://<内网IP>:18091/stream
```

浏览器里 `new EventSource('http://<内网IP>:18091/stream')` 直接用，断线会自动带 `Last-Event-ID` 续传。

## 状态页

浏览器打开 `http://<内网IP>:18091/` 就能看当前状态，不用命令行：

- 顶部状态灯：绿 = 已连接，琥珀 = 连着但一小时没事件，红 = 断了（带错误原因）
- 账号胶囊：头像 + 登录名（从事件的 `repository.owner` 推导，头像取 `sender.avatar_url`）
- 六个卡片：游标、今日事件、总事件、距上一条、重连次数、SSE 客户端数
- 最近 24 小时每小时事件数柱状图
- 仓库列表：收到过事件的仓库，带事件总数、按类型分布、最近一条时间
- 事件流：页面自己挂一条 SSE，新事件实时插到顶部

页面是单文件、零外部依赖（不连 CDN），断网也能开。改了 `web/index.html` 需要重启进程 —— HTML 是启动时读进内存的。

## 端点

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/` | 状态面板（HTML） |
| GET | `/stream` | SSE 事件流，支持 `Last-Event-ID` 和 `?after=N` |
| GET | `/events?after=N&limit=200` | 按游标拉一段历史 |
| GET | `/stats` | 今日 / 总量 / 24 小时分布 / 按类型分布 / 账号 / 仓库列表 |
| GET | `/health` | 连接状态、游标、SSE 客户端数、距上一条事件的秒数 |
| GET | `/relay?force=1` | 中继自检：wrangler 装没装 / 登录没登录 / 有没有 workers 写权限 / worker 在不在 / 地址通不通 |
| POST | `/relay/deploy` | 跑 `wrangler deploy`，从输出里抬出 `*.workers.dev` 地址写回配置并重连（`{"apply":false}` 则只部署不改配置） |
| GET | `/relay/domains` | 账号里有哪些 zone、这个 worker 现在绑了哪些自定义域名、默认 workers.dev 地址 |
| POST | `/relay/domain` | 把一个子域名绑到中继（走 Workers 自定义域名接口，不改 wrangler.toml；等 HTTPS 通了才把中继地址切过去） |
| POST | `/relay/domain/remove` | 解绑；中继地址正指着它就退回 workers.dev |

`/relay` 和 `/relay/deploy` 只对本机开放。前者只读，一两秒就回来，缓存 20 秒；
后者是真的发布，会短时间摸一下 Cloudflare。为什么要它：地址以前靠人手抄，
抄错一个字母就是「连接失败」，日志里又看不出是抄错了还是网断了。

`/health` 里的 `seconds_since_last_event` 是判断「真的安静」还是「连接死了」的关键：配合 `connected` 看，连接为 true 但该值持续增大，说明链路活着但仓库确实没动静；连接为 false 则是真断了。

## 验收

```bash
python3 tests/e2e.py
```

起一个假 Hub，验证 22 项：连接、实时事件、SSE 推送、断线、自动重连、断线期间补发、全程无重复、游标不回退、历史接口排序、状态页、统计接口、账号与仓库识别、挂着 SSE 客户端时能快速退出。不需要 Cloudflare 账号。

想看看状态页长什么样，可以造一份 24 小时的历史数据：

```bash
python3 tests/seed_preview.py /tmp/preview.db
```

## 端口

默认 `18091`，避开 the desktop app 自己的 `3000`（Vite）和 `17890`（后端 HTTP）。改 `config.toml` 里的 `http_port`。

## 两个参数怎么调

`heartbeat`（默认 30 秒）是协议层 ping，防路由器/基站回收空闲连接。局域网直连可以调到 300，手机 4G 场景保持 30。

`app_ping`（默认 120 秒）是应用层 ping，让服务端有机会回一个 pong，确认它真的在处理消息而不只是 TCP 活着。这个开销极小（按 20:1 折算成计费请求）。

# 给 agent 的说明书（GitHub Local Action）

这份文件是给 AI agent / 脚本看的：读完就能操作本机这个服务，不用猜接口。
人看的说明在 [README.md](README.md)；按需 runner 的细节在
[docs/jit-on-demand-runner.md](../docs/jit-on-demand-runner.md)。

> 本机服务自己也会把这份文件原样吐出来：
> `curl -s http://127.0.0.1:18091/agent.md`（`/llms.txt` 同内容）。
> 英文版是 `curl -s http://127.0.0.1:18091/agent.en.md`（同一份内容的翻译，
> 网页切到 English 时展示的就是它）。
> 所以你把 agent 指向一个地址就够了，不用先塞文件。

---

## 它是什么

把 GitHub 账户下所有仓库的事件，实时送到局域网里的设备；并且能把 GitHub 的
job 派到自己的机器上跑（常驻 runner + 按需 JIT 临时 runner）。

```text
GitHub ──webhook──▶ remote-worker ──WebSocket──▶ local-client ──SSE──▶ 局域网设备
         (公网入口)   (Cloudflare)       (本机常驻进程)       (HTTP)
```

两层的原因：GitHub 只能往公网发，家里/公司的网进不去；本机主动往外连一条
WebSocket（NAT 不拦出站），远端有事件顺着这条线推下来。没有轮询，延迟 1 秒级。

对外就一个东西：`local-client/relay.py` 跑起来的一个 HTTP 服务。

## 地址

| 用途 | 地址 | 谁能连 |
|---|---|---|
| 本机（默认） | `http://127.0.0.1:18091` | 只有本机 |
| 局域网 | `http://<本机局域网IP>:18091` | 同网段设备 |

端口 18091 可用 `config.toml` 的 `http_port` 改（改了要重启）。先探活：

```bash
curl -s http://127.0.0.1:18091/health
# {"ok": true, "connected": true, "cursor": 797, "connects": 1,
#  "sse_clients": 2, "seconds_since_last_event": null, "last_error": ""}
```

`ok` / `connected` = 跟 Cloudflare 那条 WebSocket 通着没有。`ok:false` 时先看
`last_error`，不要重试刷接口。

## 权限边界（重要）

服务没有登录态，靠**来源 IP** 区分：

- **只对本机（127.0.0.1）开放**：全部 `/github/*`、`/webhook/*`、`/runners*`，以及 `/config` 的**写入**（`POST /config`）。
  这些会用到本机 `gh` 的 GitHub 凭据、机器 SSH 凭据、或者能改配置。
- **局域网也能读**：`/`（网页）、`/stream`、`/events`、`/stats`、`/actions`、`/config`（`GET`，令牌已掩码）、`/health`。

所以「从局域网另一台机器调 `/github/jobs`」会拿到
`403 {"error": "GitHub 凭据只对本机开放；局域网设备请用 webhook 那套数据"}` —— 这是设计，不是故障。

## 接口

全部返回 JSON（`/stream` 是 SSE，`/agent.md` 是 markdown）。`repo` 一律 `owner/name`。

| 方法 路径 | 做什么 | 参数 |
|---|---|---|
| `GET /health` | 连接状态、游标、SSE 客户端数 | — |
| `GET /stats` | 今日/总事件、今日/总运行、24 小时分桶、账号与仓库 | — |
| `GET /events` | 从本地库补历史事件 | `after`（id，默认 0）`limit`（默认 200，上限 1000） |
| `GET /stream` | SSE 实时流；带 `Last-Event-ID` 可续传 | `after` |
| `GET /actions` | 运行列表（webhook 拼的，不联网） | `repo` `limit`（默认 60） |
| `GET /github/auth` | 本机 gh 装没装、登没登 | — |
| `GET /github/runs` | 直接问 GitHub 要运行列表 | `repo` `limit` |
| `GET /github/jobs` | 一次运行的 job 与每步状态（跑完的走本地缓存） | `repo` `run` |
| `GET /github/joblog` | 单个 job 的日志纯文本（跑完才有，运行中 404） | `repo` `job` |
| `GET /github/repos` | 账号下仓库清单，按最近提交排序 | — |
| `GET /github/runners` | GitHub 侧 runner 列表 | `repo`（`owner/name` 或组织名） |
| `GET /runners` | 本机纳管的机器清单 | — |
| `GET /runners/jit` | 按需 runner 状态、池子、正在跑什么 | — |
| `POST /runners/jit/repo` | 开关某个仓库的按需接管 | `{"repo": "...", "on": true}` |
| `POST /runners/ssh/add` | 连一台机器并装 runner（异步任务） | `{"host","port","user","password","repo"?,"name"?}` |
| `GET /runners/ssh/job/{id}` | 上面那类任务的结果 | — |
| `GET /webhook/status` | 仓库 webhook 有没有指过来 | `repo` |
| `POST /webhook/setup` | 自动给仓库配 webhook | `{"repo": "..."}` |
| `POST /mcp` | MCP over HTTP 端点（JSON-RPC 2.0，只收本机） | 见下面 MCP 一节 |-

## 五个常用配方

```bash
B=http://127.0.0.1:18091

# 1. 最近 5 次运行（不需要联网、不需要 gh）
curl -s "$B/actions?limit=5" | jq -r '.runs[] | "\(.name) #\(.run_number) \(.status)/\(.conclusion) \(.sha)"'

# 2. 某次运行的 job 与每一步（成功收尾的会命中本地缓存，很快）
curl -s "$B/github/jobs?repo=example-org/relay-test&run=12345678901" \
  | jq '.jobs[] | {name, status, conclusion, steps: [.steps[] | "\(.number). \(.name) \(.conclusion // .status)"]}'

# 3. 某个 job 的日志（job 还在跑时 GitHub 返回 404，接口会给你 pending:true）
curl -s "$B/github/joblog?repo=example-org/relay-test&job=42000000001" | jq -r .text | tail -40

# 4. 跟 30 秒实时事件（Ctrl-C 退出；断线重连带 after=<最后一条 id> 不漏）
curl -N "$B/stream"

# 5. 机器清单（不返回密码，只有主机/用户/系统/架构）
curl -s "$B/runners" | jq '.machines[] | "\(.name) \(.os)/\(.arch) \(.host):\(.port)"'
```

## 数据结构

**事件**（`/events`、`/stream`）

```json
{"id": 797, "event": "workflow_run", "repo": "owner/name",
 "action": "completed", "at": 1794417917000, "payload": { /* GitHub 原样 */ }}
```

- `id` 是远端自增，本地按它去重 —— 断线重连、重复投递都不会重复入库。
- `at` 毫秒。`payload` 就是 GitHub webhook 的原始 body，字段按事件类型查 GitHub 文档。
- 续传：把这批最后一条的 `id` 当下次的 `after`。

**运行**（`/actions`）：`workflow_run` 事件按 `run.id` 去重，一条运行一行。
字段：`id repo name branch trigger path status conclusion run_number sha actor title started_at duration_s url`。

**动作数 vs 事件数**：一次运行会推 `requested` / `in_progress` / `completed` 多条事件，
所以 `stats.total`（事件条数）远大于 `stats.runs_total`（运行次数）。

## 干活时请守的规矩

1. **只读优先**。上面的 GET 全部安全，可以随便打。下面这些 POST（`/config`、`/runners/delete`、
   `/runners/ssh/add`、`/runners/ssh/remove`、`/runners/jit/*`、`/webhook/setup`）会改本机配置、
   动别人机器上的常驻进程、或者往 GitHub 写东西 —— 先跟人确认再发。
2. **不要把令牌、密码打进日志或回复**。`/config` 本来就只回 `token_hint`（`71a7••••6088`）；
   SSH 密码走请求体，别回显，别落盘。
3. **`/github/*` 调用的是本机 `gh` 的登录态**，消耗的是这个账号的 API 配额（未登录 60 次/小时）。
   看列表用 `/actions`（本地库，零成本），只有需要 job / 步骤 / 日志时才用 `/github/*`。
4. **不要高频轮询**。`/actions`、`/stats` 挂在 SSE 上按事件刷新就够了；运行中的 job 每 5 秒一次是上限。
5. 判断「跑完了没有」看 `status`（`queued` / `in_progress` / `completed`），
   结论看 `conclusion`（`success` / `failure` / `cancelled` / `skipped`）。

## 关于 MCP

**有两种接法，选一个就行**：

| 传输 | 怎么接 | 适用 |
|---|---|---|
| **HTTP**（Streamable HTTP） | 端点 `http://127.0.0.1:18091/mcp` | 客户端支持「远程/HTTP MCP」时首选，不额外起进程 |
| stdio | `python3 local-client/mcp_server.py` | 客户端只会拉起子进程时用（Claude Desktop 这种） |

两种后端是**同一份工具实现**（HTTP 那个就是把 `mcp_server.py` 的 handler 挂在 aiohttp 上），
行为、参数、返回完全一致。

### HTTP 接法

```json
{
  "mcpServers": {
    "github-local-action": {
      "type": "http",
      "url": "http://127.0.0.1:18091/mcp"
    }
  }
}
```

命令行客户端一般就是 `claude mcp add --transport http local-action http://127.0.0.1:18091/mcp`。
协议要点：`POST /mcp` 发 JSON-RPC，`initialize` 的响应头带 `Mcp-Session-Id`，后续请求把它带回去；
纯通知回 `202` 空体；`GET /mcp` 回 `405`（本服务没有「服务端主动推」的需求，不用 SSE 长流）。

**限制**：`/mcp` 只收本机（127.0.0.1）。因为工具里有 `/github/*`（吃本机 gh 的凭据），
要是对局域网开放，等于绕过了那道墙。要从别的机器用就起个隧道：
`ssh -L 18091:localhost:18091 you@10.0.0.5`。

### stdio 接法

```json
{
  "mcpServers": {
    "github-local-action": {
      "command": "python3",
      "args": ["/绝对路径/local-client/mcp_server.py"],
      "env": { "LOCAL_ACTION_URL": "http://127.0.0.1:18091" }
    }
  }
}
```

### 工具（两种传输一样）

| 工具 | 对应接口 | 干什么 |
|---|---|---|
| `health` | `/health` | 服务在不在、链路通不通（连不上时先调它） |
| `get_stats` | `/stats` | 今日/累计事件与运行数（24 小时分桶默认不带） |
| `list_runs` | `/actions` | 最近运行（本地库，零配额） |
| `get_run_detail` | `/github/jobs` | 一次运行的 job 与每步状态，可 `include_logs` 带日志尾巴 |
| `get_job_log` | `/github/joblog` | 单个 job 的日志，可 `max_chars` 限长 |
| `recent_events` | `/events` | 最近事件（payload 默认只给键名） |
| `list_machines` | `/runners` | 纳管的机器（不含密码） |

设计上的取舍：

- **一次运行的 job + 步骤 + 日志是一个 tool**（`get_run_detail`），不拆三个，
  否则 agent 要先看状态再回头取日志，多跑两轮。日志默认只给最后 60 行，
  要全文自己传 `log_tail_lines: 0` 或改用 `get_job_log`。
- **全部只读**。改配置、装/卸 runner、开按需池子这些会动别人机器或改本地配置的，
  没有做成 tool —— 让用户在网页上点，或者直接用 HTTP 接口（见上面表格）。
- `/stream` 那种长连接不适合做成 tool（MCP 的 tool 调用是请求-响应），
  所以做成 `recent_events(after=N)`，当游标用。

不想接 MCP 也没关系：它只是把下面这些 HTTP 接口翻译了一遍，直接 curl 一样能干活。

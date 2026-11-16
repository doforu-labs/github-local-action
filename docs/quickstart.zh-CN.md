# 快速开始

[English](quickstart.md)

从零到「面板上蹦出第一条事件」，大约 10 分钟。

## 需要什么

- 一个 GitHub 账户（能建 GitHub App、能装到自己的账户上）
- 一个 Cloudflare 账户（免费计划够用，不用绑卡）
- 一台常开的机器：macOS 10.15+ / Linux / Windows 都行，Python **3.11+**（读 TOML 用的是标准库 `tomllib`）
- **Node 22.12+**：只管部署那一半（`wrangler` 4 在更旧的 Node 上直接起不来；本地接收端完全不碰 Node）
- 可选：`gh`（GitHub CLI）并已 `gh auth login`。装了它才能看 job 与步骤、拉历史运行、在面板上装 runner

---

## 1. 部署远端中继（Cloudflare）

```bash
cd remote-worker
npm install
npx wrangler login          # 第一次用会开浏览器

# 建库。免费计划每账号 10 个 D1 库，给这个项目留一个。
npx wrangler d1 create github_local_action
# 输出里有一行 database_id = "xxxx"，把它填进 wrangler.toml 的同名项

# 建表（两张：事件日志 + 每仓库 webhook 密钥）
npx wrangler d1 execute github_local_action --file schema.sql --remote

# 两个密钥：一个给 GitHub 验签，一个给本地客户端连 WS
npx wrangler secret put GITHUB_WEBHOOK_SECRET   # 自己编一个长随机串，下一步要用同一个
npx wrangler secret put CLIENT_TOKEN            # 另一个长随机串，本地 config.toml 要用

npx wrangler deploy
```

部署完会打印一个地址：

```
https://github-local-action.<你的子域>.workers.dev
```

记下它。想绑自己的域名也可以——面板「设置 → 中继」里能一键绑，见 [后面](#5-换成自己的域名可选)。

验证一下：

```bash
curl https://github-local-action.<你的子域>.workers.dev/health
# {"ok":true,"events":0,"cursor":0}
```

## 2. 在 GitHub 上建一个 App

比逐个仓库配 webhook 省事：装一次，账户下所有仓库都覆盖。

1. 打开 <https://github.com/settings/apps> → **New GitHub App**
2. **Webhook URL** 填 `https://github-local-action.<你的子域>.workers.dev/gh/webhook`
3. **Webhook secret** 填上一步 `GITHUB_WEBHOOK_SECRET` 的同一个值
4. **Subscribe to events** 至少勾这几个：
   - `Push`、`Pull request`、`Issues`、`Issue comment`、`Release`、`Create`
   - **`Workflow run`** —— 不勾这一项，面板的「动作」页永远是空的（这是最常见的「配好了但没数据」）
5. **Repository permissions** 不需要额外授权，保持默认（`Metadata: read-only`）即可
6. 建好后 **Install App**，装到你的账户上，选 all repositories

> 只想给某几个仓库配、不想建 App 也行：仓库 Settings → Webhooks 逐个加，地址一样、secret 一样。

## 3. 起本地接收端

```bash
cd local-client
python3 -m venv .venv && source .venv/bin/activate     # Windows: .venv\Scripts\activate
pip install -r requirements.txt                        # 只有 aiohttp，SSH 装 runner 再加 paramiko

cp config.example.toml config.toml
```

编辑 `config.toml` 两行就够：

```toml
[relay]
ws_url = "wss://github-local-action.<你的子域>.workers.dev/ws"
token = "和 CLIENT_TOKEN 一模一样"
```

然后：

```bash
python3 relay.py
```

看到这几行就是通了：

```
INFO 本地 SSE 监听 http://0.0.0.0:18091/stream
INFO 连接 wss://…/ws（游标 0）
INFO 已连上，从游标 0 续传
```

想装成命令行工具而不是每次进目录跑：

```bash
pip install .          # 或 pipx install .
github-local-action -c ~/.config/github-local-action/config.toml
```

装成 wheel 之后，状态页和 `/agent.md` 会从 `<python前缀>/share/github-local-action/` 读，不用管。

## 4. 接上局域网设备

浏览器打开 `http://<这台机器的内网IP>:18091/` 就是面板。机器可读的接口：

```bash
curl -N http://<内网IP>:18091/stream      # SSE 事件流，支持 Last-Event-ID 与 ?after=N 续传
curl http://<内网IP>:18091/health         # 连接状态、游标、SSE 客户端数
curl http://<内网IP>:18091/stats          # 今日/总量/24 小时分布/仓库列表
curl http://<内网IP>:18091/actions        # 工作流运行列表（?repo= 过滤）
curl http://<内网IP>:18091/config         # 当前配置（令牌只回掩码）；改配置只允许在跑接收端的那台机器上做
```

网页里直接用 `EventSource`，断线浏览器会自己带 `Last-Event-ID` 重连，本地库负责补：

```js
const es = new EventSource('http://10.0.0.5:18091/stream');
es.onmessage = e => console.log(JSON.parse(e.data));
```

只想本机用，就把 `http_host` 改成 `127.0.0.1`。

## 5. 换成自己的域名（可选）

`*.workers.dev` 在国内不走 CDN，也可能被整段拦。Cloudflare 支持「Workers 自定义域名」：面板「设置 → 中继」里能看到账号里有哪些 zone、当前这个 worker 绑了哪些域名，选一个子域绑上去，等 HTTPS 通了它会把 `ws_url` 自动切过去并重连。

同一张卡片还能：

- **一键 `wrangler deploy`**：改完 worker 代码不用回命令行，面板会从输出里抬出 `*.workers.dev` 地址写回配置
- **看有没有部署权限**：调 `wrangler whoami`，把账号、令牌 scope 和「能不能写 workers」都列出来

## 排错

| 现象 | 先看这里 |
|---|---|
| 日志反复「连接失败」 | `ws_url` 是不是 `wss://…/ws`（少了 `/ws` 路径会 404）；`token` 和 `CLIENT_TOKEN` 是否一致 |
| 面板能开但没有事件 | GitHub App 里勾了 `Workflow run` 吗？只有在勾了之后推的事件才会来 |
| 事件来了但动作页空 | 同上，动作页只认 `workflow_run` 类型 |
| 局域网别的设备打不开 | `http_host` 是 `0.0.0.0` 吗？这台机器的防火墙放行 18091 了吗？ |
| `wrangler deploy` 报 D1 不存在 | `wrangler.toml` 里的 `database_id` 还是占位符 |
| 动作页点「加载 job」报错 | 本机没装 `gh` 或没 `gh auth login`；`gh auth status` 里要有 `repo` scope |
| 装了 `gh`、终端里也能用，面板仍报「本机没装 gh」 | 开机自启的服务是 launchd / systemd 拉起来的，没有登录 shell 的 PATH，找不到 `/opt/homebrew/bin` 里的 `gh`。plist 的 `EnvironmentVariables` 要有 `PATH`（unit 里是 `Environment=PATH=`）；把开机自启关掉再打开，面板会按当前代码重写一份带 PATH 的配置 |

端口默认 18091，改 `http_port` 需要重启进程；改 `ws_url` / `token` / 心跳参数不需要——面板里保存就重连。

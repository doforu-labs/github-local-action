# 安全策略

## 报告漏洞

请用 GitHub 的私下报告：仓库 **Security → Report a vulnerability**（Security Advisories）。
不要在公开 issue 里贴可利用细节、令牌、内网地址。

一般会在一周内回，修好之后在 advisory 里公开致谢（除非你想匿名）。

## 威胁模型

这个项目会经手三类敏感信息，分别说清楚它存在哪、谁能碰：

| 信息 | 存在哪 | 谁碰得到 |
|---|---|---|
| `CLIENT_TOKEN`（本地连远端的口令） | `local-client/config.toml` 明文；Cloudflare 侧是加密 secret | 本机文件系统；worker 的 secret 存储 |
| webhook 密钥 | `GITHUB_WEBHOOK_SECRET`（全局）或 D1 的 `webhook_secrets`（按仓库） | Cloudflare 侧 |
| runner 机器的 SSH 密码 | **只在本机内存里，30 分钟后消失**；要留过夜则用 Fernet 加密进本地 SQLite，密钥单独存 `events.db.machines.key`（0600） | 本机 |

`CLIENT_TOKEN` 会出现在 WebSocket 的查询串里（`?token=…`）。这意味着如果中间有反向代理，访问日志可能记下它——本项目不经过任何第三方代理，自己加代理的话记得关掉 query 日志。

## 设计上的边界

- **敏感接口只认本机**：`/github/*`（会用本机 `gh` 的登录态读私有仓库）、`/runners/*`（会 SSH 到别的机器、会返回机器清单）、`/relay/deploy`（会真的发布）、**`POST /config`**（会重写 `ws_url` 与令牌）都只在 `127.0.0.1` / `::1` 上可用，局域网访问一律 403。
- **配置可以读、不能改**：`GET /config` 对局域网开放（令牌已掩码），`POST /config` 只认本机——不然局域网里任何人都能把整条事件流指到自己的服务器上。
- **局域网接口不认证**：`/stream`、`/stats`、`/actions`、状态页假设「可信内网」。它们只暴露仓库名、分支、提交标题这类元数据，不暴露令牌。跨网段请走 SSH 隧道，别把端口暴露到公网。
- **不落明文密码**：SSH 密码不进命令行（`ps` 看不到）、不写日志、不进 SQLite 明文；`rm -rf` 之前先验目录里确实有 runner 的痕迹。
- **工作流文件不代写**：`gh` 令牌一般没有 `workflow` scope，写 `.github/workflows/*` 会被 GitHub 以 404 拒绝——这是有意的，不绕过。

## 部署时请务必

1. `GITHUB_WEBHOOK_SECRET` 和 `CLIENT_TOKEN` 都用长随机串（`openssl rand -hex 32`），别复用别处的密码。
2. `config.toml` 权限设成 `600`（`chmod 600 local-client/config.toml`）。
3. 不要把 `events.db`、`events.db.machines.key`、`config.toml` 提交进任何仓库——`.gitignore` 已经挡了，但别 `git add -f`。
4. **公开仓库不要挂 self-hosted runner**：任何人的 PR 都能在你的机器上执行代码。
5. 只给私有仓库用自带的面板 SSH 功能；装完 runner 记得把密码对应的机器账号换成密钥登录。

## 支持范围

项目处于早期（0.x），只对 `main` 分支的最新提交提供修复。发现历史版本的问题也欢迎报告，但不会单独发补丁版本。

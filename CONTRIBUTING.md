# 参与贡献

欢迎 issue 和 PR。默认 README 是英文（`README.zh-CN.md` 是中文版），`docs/` 下的指南和代码注释、commit message 以中文为主；中文和英文 PR 都欢迎。

## 先跑起来

```bash
git clone https://github.com/OWNER/github-local-action   # 换成你自己的 fork / 仓库地址
cd github-local-action

# 本地接收端（不需要 Cloudflare 账号就能跑测试）
cd local-client
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 远端中继（只在改 worker 时才需要）
cd ../remote-worker && npm ci
```

## 改完必须过的三关

```bash
make test          # 18 个单测 + e2e，约 1 分钟
make typecheck     # worker 的 TS 类型检查
make pii           # 仓库里不该有真实邮箱 / 内网 IP / 令牌 / 真机主机名
```

如果 `python3` 指向的不是装了依赖的那个（venv 目录被挪过、或同时有 homebrew / pyenv python），显式传：`make test PYTHON=.venv/bin/python`。

`make test` 不需要网络、不需要 Cloudflare、不需要 GitHub 登录：e2e 起的是 `local-client/tests/mock_hub.py` 这个假 Hub，行为对齐真 Hub（token 校验、游标补发、心跳）。

只改前端（`local-client/web/index.html`）也请跑 `make test-fast`：那 4 个 JS 测试直接抽页面里的纯函数跑，不启浏览器。

## 代码约定

- **测试是脚本，不是 pytest**：`tests/unit_*.py` 每个文件自己打印 PASS/FAIL、用退出码定生死。加测试就照抄旁边那个文件的骨架，别引入新框架。
- **注释写「为什么」，不写「做了什么」**：读代码就知道做了什么；难的是半年后忘了当初为什么这么选。比如「心跳必须客户端发，否则 DO 永远不休眠，duration 从 0.12% 跳到 85%」。
- **状态页零外部依赖**：`web/index.html` 是单文件，不连 CDN、不引构建工具。断网也要能开。
- **新增 HTTP 端点要在 `local-client/AGENT.md` 里登记**，那份文件同时是给 agent 的说明书，`/agent.md` 直接吐它。英文版 `AGENT.en.md` 同步。
- **敏感端点记得加 `_local_only()`**：会用本机凭据的一律只认 `127.0.0.1`。

## PR 要求

1. 说清楚**动机**：遇到什么现象、为什么现有行为不对。
2. 附上**验证方式**：跑了哪个测试、贴关键输出；改界面附截图/录屏。
3. 一次 PR 只做一件事。顺手重构请单独开。
4. 别把 `.venv/`、`events.db`、`config.toml`、`node_modules/` 带进来（`.gitignore` 挡着，`git add -A` 之后看一眼 `git status`）。

## Commit message

中文，第一行说结论（≤ 50 字），空一行，正文说为什么和踩到了什么。举例：

```
中继被删时：不再把整页 Cloudflare HTML 当错误消息显示

worker 被删掉时 workers.dev 返回的是一整页 HTML（里面藏着 error code: 1042），
以前直接塞进「连接失败：…」，前端只能看到一坨标签。现在先认错误码，
认出 1042 就翻成「worker 不在了，重新部署一次」。
```

## 许可

提交 PR 即表示同意以 [MIT](LICENSE) 授权你的贡献。

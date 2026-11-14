# 变更记录

格式参照 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)，版本号遵循
[语义化版本](https://semver.org/lang/zh-CN/)。

## [未发布]

### 加入
- MIT 许可证、`SECURITY.md`、`CONTRIBUTING.md`、`CODE_OF_CONDUCT.md`
- CI：Python 3.11/3.13、Node 20/22、Windows 前端测试、e2e、worker 类型检查、脱敏扫描
- `scripts/run_tests.py`：一条命令跑完 19 个单测 + e2e；`Makefile` 提供 `make test` / `make test-fast` / `make pii` / `make typecheck`
- `scripts/check_pii.py`：拦住真实邮箱、内网 IP、令牌、真机主机名、真实资源 id
- `local-client/pyproject.toml`：可 `pip install .`，装完有 `github-local-action` 命令，状态页与 `/agent.md` 随包安装
- `remote-worker/package-lock.json`：`npm ci` 可复现
- 文档拆分：`docs/quickstart.md`、`architecture.md`、`self-hosted-runner.md`、`known-limitations.md`、`lab-notes.md`、`publishing.md`，配三张真界面截图
- 默认 README 改成英文（`README.md`），中文版是 `README.zh-CN.md`；两版章节一一对应

### 更改
- 自动启动服务的 launchd label 去掉了组织/品牌前缀，改成 `local.github-local-action`（systemd unit 名 `github-local-action.service` 没变）。**装过改名前的版本**：先用旧版面板里的「卸载服务」卸一次再装新版——新旧两个进程会抢同一个端口
- `platform-*.yml` 的 self-hosted 闸从写死仓库名改成仓库变量 `RUN_SELF_HOSTED`，谁 fork 都不会往自己的机器上派 job
- `LICENSE` 版权方、`pyproject.toml` 的 authors / Homepage 不再写具体组织，改成 `The github-local-action contributors` 与 `OWNER` 占位
- 测试 fixture 里的 Cloudflare 账号子域（`*.demo-sub.workers.dev`）换成 `demo-sub`
- `scripts/check_pii.py` 新增两条规则：`*.workers.dev` / `*.pages.dev` 的账号子域、品牌/组织名（`LEGACY_SERVICE_LABELS` 那一行例外，因为它是用来清旧安装的）

### 修复
- `remote-worker/schema.sql` 缺少 `webhook_secrets` 表：按文档新建的部署在「自动配 webhook」时会 500
- `README` 里 `wrangler d1 create` 的库名与该文件里的 `database_name` 不一致，新人部署必卡
- `AGENT.md` / `AGENT.en.md` 移到 `local-client/`，这样 `pip install` 之后 `/agent.md` 也能读到

### 变更
- `platform-check.yml` / `platform-queue.yml` 的每个 job 加仓库名闸，避免在 fork 里往 self-hosted 机器派活
- `POST /config` 现在只接受本机请求：原来局域网里任何人都能改 `ws_url` 与令牌（等于把整条事件流指向自己的服务器）。`GET /config` 仍对局域网开放，新增 `tests/unit_local_only.py` 锁住这道闸（13 项）

## [0.1.0] - 2026-11-11

首个可用版本：远端 Worker（webhook 验签、D1 幂等、DO 广播）+ 本地接收端（WS 续传、SQLite 去重、SSE 推送、状态页/动作页/机器页）+ 只读 MCP server。

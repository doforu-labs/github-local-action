# 变更记录

格式参照 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)，版本号遵循
[语义化版本](https://semver.org/lang/zh-CN/)。

## [0.1.0] - 2026-11-14

首个公开版本。接口还会变，尤其是 MCP 工具签名和面板接口。

### 加入
- Docker：`Dockerfile` + `docker-compose.yml`，只容器化常驻端（Worker 仍用宿主机的 wrangler 部署）；`docs/docker.md` / `docs/docker.zh-CN.md`
- 本机闸新增 `trusted_peers`：容器里宿主机浏览器从网桥网关过来，不认它会 403。写 `"gateway"` 让程序自己从 `/proc/net/route` 算默认网关；环境变量 `GLA_TRUSTED_PEERS` 也能给。名单只从文件读，不开放给 HTTP 接口
- 首次公开发布
- README 顶部加 CI 徽章与许可证徽章。徽章地址必须硬写「组织/仓库」，与门禁的「组织名不许进仓库」正面冲突，解法不是放宽门禁而是给它加一条**从 `git remote` 现读**的窄例外（`check_pii.py` 的 `redact_self()`）：只放行完整的 `<组织>/<仓库>`，`com.<组织>.xxx` 与裸组织名照样拦；组织名本身仍不写进源码，fork 后自动指向 fork 自己的地址。两个方向都被 `unit_pii_self_url.py` 钉住
- MIT 许可证、`SECURITY.md`、`CONTRIBUTING.md`、`CODE_OF_CONDUCT.md`
- CI：Python 3.11/3.13、Node 20/22、Windows 前端测试、e2e、worker 类型检查、脱敏扫描
- `scripts/run_tests.py`：一条命令跑完 20 个单测 + e2e；`Makefile` 提供 `make test` / `make test-fast` / `make pii` / `make typecheck`
- `scripts/check_pii.py`：拦住真实邮箱、内网 IP、令牌、真机主机名、真实资源 id
- `local-client/pyproject.toml`：可 `pip install .`，装完有 `github-local-action` 命令，状态页与 `/agent.md` 随包安装
- `remote-worker/package-lock.json`：`npm ci` 可复现
- 文档拆分：`docs/quickstart.md`、`architecture.md`、`self-hosted-runner.md`、`known-limitations.md`、`lab-notes.md`、`publishing.md`，配三张真界面截图
- 默认 README 改成英文（`README.md`），中文版是 `README.zh-CN.md`；两版章节一一对应
- 六份指南全部双语：`docs/*.md` 英文、`docs/*.zh-CN.md` 中文，每页顶部带语言切换，中文版内容一行没少（`jit-on-demand-runner.md` 是实验附录，暂只中文）

### 更改
- 自动启动服务的 launchd label 去掉了组织/品牌前缀，改成 `local.github-local-action`（systemd unit 名 `github-local-action.service` 没变）。**装过改名前的版本**：先用旧版面板里的「卸载服务」卸一次再装新版——新旧两个进程会抢同一个端口
- `platform-*.yml` 的 self-hosted 闸从写死仓库名改成仓库变量 `RUN_SELF_HOSTED`，谁 fork 都不会往自己的机器上派 job
- `LICENSE` 版权方、`pyproject.toml` 的 authors / Homepage 不再写具体组织，改成 `The github-local-action contributors` 与 `OWNER` 占位
- 测试 fixture 里的 Cloudflare 账号子域（`*.demo-sub.workers.dev`）换成 `demo-sub`
- `scripts/check_pii.py` 新增两条规则：`*.workers.dev` / `*.pages.dev` 的账号子域、品牌/组织名（`LEGACY_SERVICE_LABELS` 那一行例外，因为它是用来清旧安装的）

### 修复
- `Config` 的 `ws_url` / `token` 补上默认值：没有配置文件时日志写着「使用默认值（不会连上任何地方）」，下一行却 `TypeError` 直接退出。容器第一次 `up`（data 目录是空的）必踩；现在能起来，`ws_loop` 也不再拿空地址去连（改为等待，提示只打一次）
- `scripts/run_tests.py`：仓库第一次在 GitHub Actions 上真跑时 `windows-latest` 连红两次。一是子进程输出用默认 cp1252 解码（测试输出是中文）→ 读线程抛异常、`proc.stdout` 变 `None`、接着 `proc.stdout + proc.stderr` 抛 `TypeError` 把真实失败盖住；二是修完解码后，汇总器自己 `print()` 中文测试名在 cp1252 管道上 `UnicodeEncodeError`。两处都已修（`encoding="utf-8"` + `errors="replace"`、`or ""` 兜底、`_force_utf8_output()`），新增 `unit_run_tests.py` 钉住（含反向对照：不修就必须炸）
- `remote-worker/schema.sql` 缺少 `webhook_secrets` 表：按文档新建的部署在「自动配 webhook」时会 500
- `README` 里 `wrangler d1 create` 的库名与该文件里的 `database_name` 不一致，新人部署必卡
- `AGENT.md` / `AGENT.en.md` 移到 `local-client/`，这样 `pip install` 之后 `/agent.md` 也能读到

### 变更
- `platform-check.yml` / `platform-queue.yml` 的每个 job 加仓库名闸，避免在 fork 里往 self-hosted 机器派活
- `POST /config` 现在只接受本机请求：原来局域网里任何人都能改 `ws_url` 与令牌（等于把整条事件流指向自己的服务器）。`GET /config` 仍对局域网开放，新增 `tests/unit_local_only.py` 锁住这道闸（13 项）

## [0.1.0] - 2026-11-11

首个可用版本：远端 Worker（webhook 验签、D1 幂等、DO 广播）+ 本地接收端（WS 续传、SQLite 去重、SSE 推送、状态页/动作页/机器页）+ 只读 MCP server。

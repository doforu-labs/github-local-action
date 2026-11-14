# 发布清单（维护者用）

把仓库从私有翻成公开之前过一遍。前两节是「已经做好的」，第三、四节是「只有仓库主人能做的」。

## 一、代码侧已经处理好的

| 项 | 状态 |
|---|---|
| 许可证 | `LICENSE`（MIT），依赖里 aiohttp 是 Apache-2.0、paramiko（可选）是 LGPL-2.1，不冲突 |
| 安全策略 / 贡献指南 / 行为准则 | `SECURITY.md`、`CONTRIBUTING.md`、`CODE_OF_CONDUCT.md` |
| CI | `.github/workflows/test.yml`：Python 3.11/3.13、Node 20/22、Windows 前端测试、e2e、worker 类型检查、脱敏扫描 |
| 统一测试入口 | `make test` / `scripts/run_tests.py` |
| 依赖锁定 | `remote-worker/package-lock.json`（`npm ci` 可复现） |
| 可安装 | `local-client/pyproject.toml`：`pip install .` 后 `github-local-action` 命令可用，状态页与 `/agent.md` 随包安装 |
| 脱敏 | 真实邮箱、内网 IP、真机主机名、Cloudflare 账号 id、D1 资源 id 已换成示例值；`make pii` 会把它们再抓出来 |
| 陌生人能不能跑起来 | `remote-worker/schema.sql` 补上了 `webhook_secrets` 表（原来自动配 webhook 会 500）；README 里 `d1 create` 的库名与 `wrangler.toml` 对齐 |
| 安全边界 | `POST /config` 补上本机闸（原来局域网设备能改 `ws_url` 与令牌），新增 `tests/unit_local_only.py` 锁住这道闸 |
| self-hosted workflow 的闸 | `platform-*.yml` 每个 job 加了 `if: github.repository == 'OWNER/github-local-action'`，在 fork 里不会往你的机器上派活 |
| 文档 | README 拆成首页 + `docs/quickstart` / `architecture` / `self-hosted-runner` / `known-limitations` / `lab-notes`，配三张真界面截图 |

## 二、翻转前必须先想清楚：git 历史

**现状**：历史里有一处真实的机器凭据——早期的 `local-client/tests/unit_machines.py` 用过实验室真机的地址、用户名、密码，后来（commits 里那条「测试数据换成假值」）改掉了，但**旧的那几条 commit 还在历史里**。翻 public 等于把它们永久公开。

三个处置方式，选一个：

**A. 重写历史（推荐，仓库从没公开过，代价为零）**

```bash
# 1) 先备份，这一步别省
git bundle create ~/gla-backup-$(date +%Y%m%d).bundle --all
git clone ~/gla-backup-*.bundle /tmp/gla-restore-check   # 顺手验证备份能用

# 2) 装 filter-repo
pip install git-filter-repo

# 3) 写一份替换清单（不要提交进仓库！）
cat > /tmp/redact.txt <<'EOF'
原地址==>10.0.0.21
原用户名==>tester
原密码==>P@ssw0rd-not-real
原主机名==>lab-linux-1
EOF

# 4) 重写
git filter-repo --replace-text /tmp/redact.txt

# 5) 验收：三条都应该是空的
git log --all -S'原地址' --oneline
git log --all -S'原密码' --oneline
git grep -n '原地址' $(git rev-list --all) | head
```

重写之后所有 commit hash 都会变，远端要 `git push --force`。推之前确认没有别的协作者/别的克隆。

**B. 不重写，但先改那台机器的密码**

最省事，前提是那台机器还归你管、而且密码确实换了。翻 public 之后历史就永久公开了，改不回来。

**C. 压成一条初始提交**

```bash
git checkout --orphan clean-main
git add -A && git commit -m "初始提交：GitHub 事件中继 + 本地接收端"
git branch -M clean-main main && git push --force origin main
```

最干净，代价是 78 条 commit message 全丢——那批 message 里其实写了不少「为什么这么改」的结论，倾向保留（选 A）。

## 三、翻转仓库可见性

1. **确认组织的可见性**：匿名访问 `api.github.com/orgs/<org>` 与 `api.github.com/repos/<org>/<repo>` 现在都返回 404，说明两者都不是公开状态。私有组织下的仓库能不能直接转 public，各计划策略不同，先到 **Settings → General → Danger Zone → Change visibility** 试一次，被拒会有明确报错。
2. **建议先复制一份试水**，而不是直接翻转：
   ```bash
   git push --mirror git@github.com:OWNER/github-local-action-public.git
   ```
   在副本上把 README、issue 模板、CI 全跑一遍，看过两天再决定主仓库翻不翻。
3. 翻转是不可逆的：历史会被永久公开、会被立刻 fork 抓走，后悔只能整仓删除。

## 四、公开之后建议打开

- **Settings → Code security**：Secret scanning + Push protection（防止以后有人手滑提交令牌）
- **Settings → General → Features**：Discussions（问答不用堆在 issue 里）
- **Settings → Branches**：保护 `main`，要求 CI 通过 + 至少一次 review
- **Settings → Actions → General**：Fork PR 的 workflow 需要 maintainer 批准
- **About**：一句话描述 + topics：
  `github-webhook` `cloudflare-workers` `self-hosted-runner` `sse` `sqlite` `mcp` `local-first`
- **Releases**：打 `v0.1.0`，写清「这是第一个公开版本，接口还会变」
- **Pin 一条 issue**：写明能力边界（运行中日志拿不到、局域网接口不认证）

## 五、公开之后每次改动

```bash
make test && make pii && make typecheck
```

`make pii` 是硬闸：真实邮箱、内网 IP、令牌、真机主机名一旦混进来，CI 直接红。

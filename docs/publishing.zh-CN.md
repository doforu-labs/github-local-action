# 发布清单（维护者用）

[English](publishing.md)

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
| self-hosted workflow 的闸 | `platform-*.yml` 每个 job 用仓库变量当开关：`if: vars.RUN_SELF_HOSTED == 'true'`，没设就不跑（fork 里天然是关的，也不跟组织/仓库名绑定） |
| 文档 | README 拆成首页 + `docs/quickstart` / `architecture` / `self-hosted-runner` / `known-limitations` / `lab-notes`，配三张真界面截图 |

## 二、git 历史：已经重写过一次

**状态：已完成**（2026-11-14）。原来的历史里带着实验室真机的地址 / 用户名 / 密码、账号邮箱、Cloudflare 账号 id 与账号子域、真机主机名、私有仓库名，以及组织名——翻 public 等于全部永久公开。已经用 `git filter-repo` 逐条替换掉，86 条 commit 一条没丢，commit message 里的旧值也一起换掉了。

**重写前打了完整备份**：`~/Desktop/github-local-action-backup-20261114.bundle`（git bundle，含重写前的全部历史）。要还原就 `git clone <那个 bundle> <目录>`。

以后再用同样的手法清理（比如又手滑提交了别的凭据）：

```bash
# 1) 备份，这一步别省
git bundle create ~/gla-backup-$(date +%Y%m%d).bundle --all

# 2) 替换清单放仓库外面，逐行「原值==>新值」
#    注意：filter-repo 不认 # 注释，整行都会被当成表达式——
#    一行裸 # 会被当成「匹配所有井号」的规则，把全仓库的注释符换成删除标记（踩过这个坑）
printf '%s\n' '原地址==>10.0.0.21' '原用户名==>tester' > /tmp/redact.txt

# 3) 文件内容和 commit message 都要替换
git filter-repo --replace-text /tmp/redact.txt --replace-message /tmp/redact.txt

# 4) 验收：应该是空的
git log -p | grep -nE '原地址|原用户名'
```

重写之后所有 commit hash 都变了，远端要 `git push --force`。推之前确认没有别的协作者、没有别的克隆。

### 还剩一件：commit 作者

86 条 commit 的作者都是同一个组织账号（noreply 地址里带组织名）。这在 `git log` 里看得见，但它属于「你打算用哪个账号发布」，不是泄漏——换成个人账号是一条命令：

```bash
printf '%s\n' 'Your Name <123456+you@users.noreply.github.com> 旧组织名 <旧的 noreply>' > /tmp/mailmap
git filter-repo --mailmap /tmp/mailmap
```

代价是所有 commit 不再关联到原组织账号（那正是目的）。改作者不可逆，所以留给你按发布账号决定。

## 三、把仓库公开（实际走过的路）

原仓库的历史里出现过真实凭据。`git filter-repo` 之后本地是干净的，但远端那份旧历史在 GitHub 上留过，翻它等于赌重写一定完美。实测更稳的办法是：**老仓库留着当私有备份，另起一个干净历史的新仓库对外**。

```bash
# 1) 先把名字腾出来（老仓库保持 private）
gh api -X PATCH repos/<owner>/github-local-action -f name=github-local-action-pre-rewrite

# 2) 建公开仓库并推
gh repo create <owner>/github-local-action --public --description "一句话介绍"
git remote set-url origin git@github.com:<owner>/github-local-action.git
git push -u origin main
```

**用 gh 的 OAuth token 推会被拒。** 那个 token 没有 `workflow` scope，而这个仓库带 `.github/workflows/*`：

```
! [remote rejected] main -> main (refusing to allow an OAuth App to create or
  update workflow `.github/workflows/platform-check.yml` without `workflow` scope)
```

两条路：`gh auth refresh -s workflow`，或者直接换成 SSH 地址推——SSH 密钥没有 scope 限制，一条命令就完事。

推完做匿名验收（不带任何凭据）：

```bash
curl -o /dev/null -w '%{http_code}\n' https://api.github.com/repos/<owner>/<repo>
curl -o /dev/null -w '%{http_code}\n' https://raw.githubusercontent.com/<owner>/<repo>/main/README.md
curl -sL -o /tmp/t.tar.gz -w '%{http_code} %{size_download}\n' \
  https://codeload.github.com/<owner>/<repo>/tar.gz/refs/heads/main
tar -tzf /tmp/t.tar.gz | head    # 截图和文档都得在包里
```

**第一次真跑 CI 一定会红**——本地能过不代表能过。本项目第一次在 `windows-latest` 上连红两次，两次都在 `scripts/run_tests.py` 里（先是 cp1252 locale 解不了测试输出，再是打印中文测试名失败）。用 `gh run view <id> --log-failed` 看日志，改完再推。另外仓库一转公开 Dependabot 就会开工（公共仓库默认开 security updates），一次冒出几个 major 版本 PR 是正常的，不是你改坏了什么。

反过来说，直接把原仓库翻成 public 是不可逆的：历史永久公开、会被立刻 fork 抓走，后悔只能整仓删除。

## 四、公开之后建议打开

- **Settings → Code security**：Secret scanning + Push protection（防止以后有人手滑提交令牌）
- **Settings → General → Features**：Discussions（问答不用堆在 issue 里）
- **Settings → Branches**：保护 `main`，要求 CI 通过 + 至少一次 review。本仓库自己只禁了强推和删除——维护者的令牌没有 `workflow` scope，Dependabot 那种动 `.github/workflows/*` 的 PR 经 API 根本合不了，只能推。等你手里有带该 scope 的令牌，再把「必须走 PR」打开。
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

# 实测笔记

这份文件记「真的跑起来之后看到了什么」。数字都是实测的，不是估算；机器名和地址已换成示例值。

## 一次真实 push 之后发生了什么

改一行 README 推到 `main`，三个 workflow 全部自动起来，从推送到最后一个跑完共 2 分 39 秒：

| 时刻 | 发生了什么 |
|---|---|
| 14:00:23 | `git push` 成功 |
| 14:00:29 | 中继收到 push webhook（绕公网一圈回来，6 秒） |
| 14:00:31 | GitHub 建出 3 个运行，7 个 job 陆续排队 |
| 14:00:37 | 第一个 job 已在 `lab-linux-1` 上开跑 |
| 14:01:02 | Self-hosted Smoke 完成（32 秒） |
| 14:02:39 | Three Platforms 完成（2 分 8 秒） |
| 14:03:02 | Syntax Check 完成（2 分 30 秒），三个全 success |

结果在三处独立核对，只看到一处不算：

1. **GitHub 侧**：`gh run list` / `gh api .../runs/<id>/jobs`，每个 job 的 `runner_name` 与 `labels` 直接写着落在哪台机器。
2. **机器侧**：`~/actions-runner/_diag/Worker_*.log` 里能搜到本次的完整 sha 与 run id；`_work/<repo>/<repo>/` 里的文件就是这次 checkout 下来的内容。
3. **中继侧**：动作页的 `workflow_run` 事件流，这一次从 `requested` 到 `completed` 一共 32 条事件实时推过来。

## 三个平台的机器

| 机器 | 系统 | 实测配置 | 能跑 | 跑不起来 |
|---|---|---|---|---|
| `lab-linux-1` | Ubuntu 24.04 | 6 核 / 31G / 145G 空闲 / gcc 13.3 / node 24 | 一切，含编译 | — |
| `lab-mac-1` | macOS 15.8（Intel） | 3 核 / 5G / 131G / clang + swiftc / node 24 | 测试、x64 编译、checkout | `macos-14` 是 arm64，Intel 机器出不了 arm 包；只有 CLT 没有 Xcode，公证缺证书 |
| `lab-win-1` | Win11 LTSC 26100 | 3 核 / **3G** / 48G / node 24 / MinGit | 测试、checkout、脚本 | 没有 `cl` / `MSBuild` / `makensis` / `docker` / `pwsh`，Electron + NSIS 打包做不了 |

三个平台都是同一张表单、只填地址/端口/用户/密码：

| 平台 | 做到哪一步 |
|---|---|
| Linux | 全流程：装 → online → 面板点「卸载」→ 服务停、目录删、注册删；同机装第二个自动落到 `~/actions-runner-2` |
| macOS | 全流程：下包 → 解包 → 注册 → LaunchAgent 装好并启动 → 38 秒 online |
| Windows | 全流程：下包 → 注册 → WMI 拉起 `run.cmd` → 49 秒 online（包里没有 `svc.cmd`，起不了服务，只能靠进程守着） |

弱网实测：同一台机器第一次装 64 秒（下 217MB 的包），第二次命中缓存后 **15 秒**装完。

## 免费额度

20 个仓库、300 事件/天，占用不到 4% 的免费额度，且不随时间增长（每天重置）：

| 维度 | 峰值占用 |
|---|---|
| Workers 请求 | 0.44% |
| Durable Object 请求 | 0.58% |
| Duration | 0.12% |
| SQL 存储 | 3.07%（唯一会累积的） |

关键纪律：**心跳必须由客户端发**。服务端只要用 `setInterval`，duration 就会从 0.12% 直接跳到 85%。

## 按需 runner（实验）

GitHub 上 job 排队时，由中继去把一台机器临时拉起来、注册成 runner、跑完自己消失。设计、时序和踩到的坑都在 [按需 runner 实验](jit-on-demand-runner.md)，脚本在 [examples/jit-scheduler.py](examples/jit-scheduler.py)。

要点：注册令牌 1 小时有效、runner 名字里的随机后缀用来区分同机多实例、跑完必须显式 unregister 否则列表会堆一串离线的僵尸 runner。

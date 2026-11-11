# 按需 runner（JIT）：不用组织，也能让一台机器给多个仓库干活

> 状态：已验证可行（2026-11-11，真机实测）
> 相关代码：`local-client/relay.py` 的 `_gh_scope()` / `_gh_registration_token()`
> 实验仓库：`example-org/demo-a`、`example-org/demo-b`

## 1. 要解决的问题

原来只有一种用法：`config.sh --url ... --token ...` 把 runner 注册成**常驻进程**，一个实例只认一个作用域（一个仓库，或一个组织）。由此带来三个不舒服的地方：

1. 一台机器想服务 N 个仓库，就得装 N 份，各占一个目录、一个系统服务。
2. 想「装一份给所有仓库用」，只能注册到**组织**，而组织级要求 `admin:org` 权限，还得先有组织。
3. 机器空转：没活的时候 runner 也一直在跑，一直占着 GitHub 的 runner 名额。

JIT（just-in-time，一次性 runner）解决的就是这三条：**不常驻、不绑作用域、按需现开**。

## 2. 两种模式的区别

| | 常驻（传统） | 按需（JIT） |
|---|---|---|
| 怎么进 GitHub | `config.sh` 注册，长期在线 | 服务端发一张一次性票，进程拿着票上线 |
| 绑定作用域 | 绑死一个（仓库/组织/企业） | 票里写死了是哪个仓库，但机器本身不绑 |
| 一个 runner 跑几个 job | 一直跑，不退出 | **只跑一个**，跑完自毁 |
| 权限要求 | 仓库级要 repo admin；组织级要 `admin:org` | **仓库级 `repo` 权限就够**，不需要 `admin:org` |
| 没活的时候 | 机器上一直有进程 | 机器上什么都没有 |
| 启动开销 | 一次（装完就完） | 每个 job 几秒 |

两条路可以并存，不是替换关系。

## 3. JIT 是怎么走的

```
                    ┌─────────────────────────────┐
  GitHub 上有人排队  │ POST /repos/{owner}/{repo}/  │
  （workflow_job     │      actions/runners/        │
    queued 事件）    │      generate-jitconfig      │
                    └──────────────┬──────────────┘
                                   │ 返回 encoded_jit_config（本次实测 4092 字符）
                                   ▼
                    ┌─────────────────────────────┐
                    │  SSH 到机器，落到独立目录      │
                    │  ./run.sh --jitconfig <票>   │
                    └──────────────┬──────────────┘
                                   │ 约 6 秒后 online
                                   ▼
                    ┌─────────────────────────────┐
                    │ 接一个 job → 跑完 →          │
                    │ 删 .runner / .credentials →  │
                    │ 退出，GitHub 上注册自动消失    │
                    └─────────────────────────────┘
```

票本身就是凭证，里面打包了 runner 身份、注册地址、仓库地址和一次性 token，所以**不要把它写进日志或提交到代码库**。

接口是**仓库级**的：

```
POST /repos/{owner}/{repo}/actions/runners/generate-jitconfig
```

这是关键——它不需要组织，只需要对那个仓库有权限。现有令牌（scopes: `repo`, `read:org`, `gist`, `delete_repo`）直接就调通了。

## 4. 实测数据

两个私有仓库 `demo-a` / `demo-b`，同一个 workflow（`runs-on: [self-hosted, jit-lab]`，里面 `sleep 30`），同一台 Linux（`lab-linux-1`，6 核 31G）。

### 4.1 只有一个名额（同时 1 个 JIT runner）

```
  3.4s 触发 demo-a
  6.5s 触发 demo-b
 30.2s 发票并拉起 jit-one-1  -> demo-a
 36.2s A:in_progress          B:queued          ← B 在排队
 75.6s A success（名额释放）
 89.0s 发票并拉起 jit-one-2  -> demo-b
 95.3s B:in_progress
132.9s B success
```

**总耗时 132.9 秒。B 从头到尾排在 A 后面。**

### 4.2 两个名额（同时 2 个 JIT runner）

```
  2.6s 触发 demo-a
  5.2s 触发 demo-b
 30.3s 发票并拉起 jit-two-1  -> demo-a
 44.8s 发票并拉起 jit-two-2  -> demo-b
 51.7s A:in_progress          B:in_progress     ← 同时跑
 76.0s A success
 88.7s B success
```

**总耗时 88.7 秒，两个 job 真正同时跑。**

### 4.3 换算下来

| 指标 | 数值 |
|---|---|
| 起进程 → 开始跑 job | 6.0 / 5.9 / 6.3 / 6.9 秒（四次平均 6.3 秒） |
| 单个 job 净耗时 | 约 39 秒（30 秒 sleep + 约 9 秒 setup/teardown） |
| 串行总耗时 | 132.9 秒 |
| 并行总耗时 | 88.7 秒 |
| 并行节省 | 44.2 秒（约 33%） |

### 4.4 「排队」到底是谁在排

**是 GitHub 在排队，不是调度器。** job 进了 `queued` 就躺在仓库的队列里，等着某个带匹配 label 的 runner 来认领；没有 runner 就一直是 `queued`，有就立刻派过去。调度器能做的只有一件事：**决定要不要再多开一个 runner**。

所以「能不能并行」这个问题，答案就一句话：**起几个进程就有几条并行线，不起就只有排队。**

### 4.5 五台机器同时跑（Linux + 3 台 macOS + Windows）

前面都是同一台机器开多个进程。这次是**真的跨机器**：

| 机器 | 入口 | 系统 |
|---|---|---|
| lab-linux-1 | `:22` | Ubuntu 24.04 / x86_64 / 6 核 |
| lab-mac-1（三台不同的机器，同名） | `:2222` `:3222` `:4222` | macOS 15.8 / Intel / 3 核 |
| lab-win-1 | `:2223` | Windows 11 26100 / 3 核 3G |

同一个 workflow 里写 5 个 job，各自挂不同的 label：

```yaml
jobs:
  linux-1:  runs-on: [self-hosted, jit-linux]
  mac-1:    runs-on: [self-hosted, jit-mac]
  mac-2:    runs-on: [self-hosted, jit-mac]
  mac-3:    runs-on: [self-hosted, jit-mac]
  win-1:    runs-on: [self-hosted, jit-win]
```

调度器看到哪个 job 在 `queued`，就按它的 label 找一台空闲机器、发一张票、起进程。

```
  8.8s 触发
 12.2s 发票 jit-mac-2222  -> mac-2
 14.9s 发票 jit-mac-3222  -> mac-3
 17.9s 发票 jit-mac-4222  -> mac-1
 20.9s 发票 jit-win-2223  -> win-1
 37.1s 发票 jit-linux-22  -> linux-1
 42.2s 五个全部 in_progress   ← 五台同时在跑
 86.5s mac-2、mac-3 完成
 91.4s linux-1 完成
 96.3s mac-1、win-1 完成
 96.3s 全部完成
```

**5 个 job、5 台机器、5 种系统组合，96 秒跑完。** 对比一下：同一台机器串行跑 2 个 job 要 132.9 秒。

一个现象值得记：三个 mac job 都挂 `jit-mac` label，最后落到哪台是 **GitHub 自己分配的**，不保证 `mac-1` 一定去 2222 那台——这一轮实际是 `mac-2`→2222、`mac-3`→3222、`mac-1`→4222。要固定归属就用独一无二的 label。

## 5. 调度器怎么设计

接在现有的事件流上，不需要新的入口：

```
webhook: workflow_job (action=queued)
   │
   ├─ 这个 job 属于哪个 repo？           ← 事件体里直接有
   ├─ 这个 repo 已经有空闲的 JIT runner 吗？ ← 查本地记账
   ├─ 当前并发数到上限了吗？              ← 可配置，比如 = 机器数 × 2
   │
   ├─ 不该开 → 什么都不做（让它在 GitHub 侧排队）
   └─ 该开   → generate-jitconfig → SSH 起进程 → 记一笔「这台机器在跑哪个 repo」
```

几个必须处理好的点：

1. **一票一 job**。票是一次性的，同一个 job 不能复用票；缓存票没用，每次都要新生成。
2. **名字要唯一**。同名会返回 `409 Already exists`（实测踩到过）。用 `jit-<repo>-<序号>` 这类带序号的命名。
3. **并发上限按机器算，不是按仓库算**。一台 6 核的机器同时跑两个 30 秒的任务很轻松，但跑两个编译任务就会互相拖慢。上限应该是「每台机器可承载数」，不是「每个仓库几个」。
4. **机器上要预置一份 runner 二进制**（解包后约 683MB）。每次只是起进程，不用重新下载。
5. **每个进程一个独立目录**。同一个目录并发两个进程会抢 `.runner` 文件。目录命名 `jitlab-1`、`jitlab-2`……
6. **进程自己会退出**，回收不需要额外逻辑；但要防「票发了、进程没起来」，得靠日志或超时兜底。

## 6. 已知限制与坑

### 6.1 接口层

| 现象 | 原因 | 处理 |
|---|---|---|
| `409 Already exists` | 票名重复 | 名字带序号 |
| `409 GitHub Actions is disabled on this repository` | 仓库关了 Actions | 先检查 `/repos/{r}/actions/permissions` 的 `enabled` |
| 通过 contents API 写 `.github/workflows/` 返回 404 | 令牌缺 `workflow` 权限 | 改用 SSH 推送（SSH key 不受 OAuth scope 限制） |
| `workflow_dispatch` 到 job 出现在 `queued` 有 20–40 秒延迟 | GitHub 侧建 run/job 慢 | 调度器要能容忍，别以为没触发成功就重复派 |
| 一台机器跑多个 JIT 进程 | 共享 CPU/内存/磁盘 | 设并发上限，别只按「有几个仓库」开 |
| 旧 run 的 job 一直 `queued`，新 run 的 job 抢不到 | **队列是全局 FIFO，跨 run 共享**。之前取消的 run 若留下排队 job，新起的 runner 会先去接它 | 跑之前先把历史队列清空（cancel 掉没跑完的 run） |
| 同一个 label 有多个 runner，job 落到哪台不确定 | GitHub 自己挑 | 要固定就用唯一 label |

### 6.2 各平台起 JIT 进程的坑（都是实测踩到的）

| 平台 | 现象 | 原因 | 办法 |
|---|---|---|---|
| macOS | `zsh: command not found: setsid` | `setsid` 是 Linux 的 util-linux，macOS 没有 | 用 `nohup ... &` 就够 |
| macOS/Linux | 进程秒退、无日志 | 上次被 kill 留下 `.runner` / `.credentials` | 启动前 `rm -f .runner .credentials*` |
| Windows | SSH 断开后进程被回收 | `Start-Process` 起的子进程跟着会话走 | 用 `Invoke-CimMethod Win32_Process Create` 脱会话 |
| Windows | `run.cmd` 起不来（进程秒退、无日志） | `run.cmd` 那层包装在非交互环境不工作 | 直接调 `bin\Runner.Listener.exe run --jitconfig` |
| Windows | `Access to the path '.runner' is denied` | 复制过来的 `.runner` 是**隐藏 + 只读**，`Remove-Item` 不加 `-Force` 会静默失败 | `Get-Item -Force` 读属性、置 `IsReadOnly=$false` 再 `Remove-Item -Force` |
| Windows | workflow 里 `shell: pwsh` 的步骤失败 | 该机器 pwsh 环境不完整 | 改用 `shell: cmd` + `ping -n 46 127.0.0.1 > nul` 当 sleep |

### 6.3 接进产品之后（已实现）

`relay.py` 里已经有这四个接口，页面上也有入口了：

| 接口 | 干什么 |
|---|---|
| `GET /runners/jit` | 看当前状态：开没开、池子里有哪几台、在跑什么、累计接了多少 |
| `POST /runners/jit/start` | `{repo, machines[], max_parallel}` 开始接活 |
| `POST /runners/jit/stop` | 停下（已经不排队的 job 下次再来就不会接了） |
| `POST /runners/jit/prepare` | `{name}` 给某台机器准备一份独立的 JIT 目录（从常驻那份复制二进制，不重新下载） |

调度器主体是 `jit_loop()`：跟着事件总线走，只看 `workflow_job` 的 `queued` 和 `completed` 两种。

**一条实测链路**（`example-org/demo-test`，真实机器）：

```
+ 0s  触发 jit-probe.yml（runs-on: [self-hosted, jit-lab]）
+ 8s  JIT 接上了：probe → lab-linux-1-2
+16s  在跑（running=1）
+32s  跑完，running 清空，stats={picked:1, done:1}
```

GitHub 那边对应的是：run `completed/success`，job 落在
`jit-lab-linux-1-2-464615` 上，跑完之后这个 runner 自己从列表里消失了。

### 用之前得知道的三件事

1. **仓库必须有 webhook**。调度器靠事件驱动，没配 webhook 的仓库永远不会有人给它发票。
   实测在 `demo-a`（没 webhook）上触发，等了 48 秒一条事件都不到；换成 `demo-test` 立刻就好了。
2. **机器凭据来自「添加一台机器」那次填的密码**，只在内存里活 30 分钟。过期了池子就是空的，
   页面会直接说——它不会偷偷用旧凭据。
3. **`prepare` 只做一次**。机器上得先有一份 `~/jitlab`，之后每次接活只是起进程；
   不 prepare 也能接，但进程会因为找不到 `run.sh` 失败。

## 7. 什么时候该用这个模式

**适合**：仓库多、任务少、机器少。比如我们这种「三台小机器服务一堆仓库」，常驻就是浪费，按需正合适。

**不适合**：高频 CI。每个 job 起一个进程、每次约 6 秒启动开销，跑得越密越亏；这种时候常驻反而更省。

**混合**：常驻一台当「底座」接常态任务，JIT 当「弹性扩容」在排队堆积时临时加。这是最实用的组合。

## 8. 现状

- [x] 验证 JIT 票能生成（仓库级，`repo` 权限够）
- [x] 验证进程上线、接活、跑完自毁
- [x] 验证同一台机器可以先后服务不同仓库
- [x] 验证并行/排队的实际行为（§4.1–4.4）
- [x] 验证跨 5 台机器（Linux / 3×macOS / Windows）同时并发（§4.5）
- [x] `relay.py` 支持组织级路径（填 `owner/repo` 走仓库级，只填组织名走组织级），行为对现有用法不变
- [x] 调度器接进 relay：`/runners/jit` 四个接口 + `jit_loop()` 跟事件总线（§6.3）
- [x] 前端 Runner 页加了「按需（JIT）」卡片：开关、仓库、并发数、机器多选、在跑的活、累计数
- [x] 真实端到端验收：排队事件 → 自动发票 → SSH 起进程 → job success → 自动消失
- [x] e2e 新增 13 条 JIT 用例，全量 78 项通过
- [ ] 并发上限、机器选择策略还是简单版（按顺序挑第一台闲的）
- [ ] 常驻 + 弹性混合模式未做（现在两者互不影响，但不共用名额）
- [ ] 常驻那台的 `actions-runner` 和按需的 `jitlab` 两份二进制（约 1.4GB），可以只留一份

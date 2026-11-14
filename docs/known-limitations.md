# 已知边界

这份文件写清楚「做不到」的部分，以及为什么做不到。大部分不是没实现，是上游不给。

## webhook 载荷只有运行级信息

`workflow_run` 事件的载荷里有：run id、工作流名、分支、状态、结论、触发方式、发起人、提交 sha。

没有：单个运行里的 job、每个步骤、日志。这些只能调 GitHub REST API（`/repos/{owner}/{repo}/actions/runs/{id}/jobs`、`/logs`）才拿得到，所以「加载 job 与步骤」是面板反过来用本机 `gh` 的登录态去问 GitHub，不是从事件里读的。

## 运行中的日志：只有一条路能拿到实时文本

四条通道逐个试过（2026-11 实测，runner 2.338.2）：

| 通道 | 运行中能不能拿 | 实测 |
|---|---|---|
| `/actions/runs/{id}/logs`、`/actions/jobs/{id}/logs` | **不能**，404 | 40 秒的运行期间一直 404，job 结束后约 6 秒才变 200；`gh run view --log` 直接报 `is still in progress` |
| `runs/{id}/jobs` 里的步骤状态 | 能，但只有状态没有文字 | 4 秒轮询能看到「1/2 已完成」 |
| 机器上 `_diag/Worker_*.log` | runner 自己的事件有，**step 的 stdout 没有** | 打 20 行标记，grep 命中 0 行 |
| 步骤自己 `tee` 到机器本地 | **能，延迟不到 2 秒** | 每 6 秒采样一次，每次都多出 3 行 |

结论：想在中继页上看「正在滚的日志」，只有让 workflow 自己落一份到机器上，中继去 tail 那台机器。GitHub 侧的文本日志天生是跑完才有。

```yaml
- name: 一边输出一边落盘
  run: |
    set -o pipefail            # 保留失败语义，否则 tee 的退出码会盖掉失败
    for i in $(seq 1 30); do
      echo "[$i/30] $(date '+%H:%M:%S') 正在跑"
      sleep 2
    done 2>&1 | stdbuf -oL tee -a /tmp/gha-live.log
```

- **必须 `stdbuf -oL`**。直接管道给 `tee`，stdout 会被块缓冲，几十秒才吐一次，看起来像没在跑。
- 文件留在机器上**不自动清**，多台机器要各自 tail；写 `/tmp` 重启就没了。
- 这是 GitHub 的已知限制，不是本项目的 bug：社区帖 [discussions#75518](https://github.com/orgs/community/discussions/75518) 从 2023-11 挂到 2026-11 官方一直没回；[#154834](https://github.com/orgs/community/discussions/154834) 说 2025-03 之后连半截日志都拿不到了。我们另外验了两个流传的说法：`_diag/pages/*.log` 在整个步骤执行期间一直是 0 字节（等步骤结束才落盘、随即上传删除），`?tail_logs=true` 也不是实时流，只是结束前约 12 秒提前返 200。

## 日志归档没做

`/logs` 返回的是 zip，体积大、不适合放进这个中转，所以面板只显示 job 与步骤状态，日志去 GitHub 页面看。

## 写 `.github/workflows/*` 需要 `workflow` scope

本机 `gh` 令牌一般只有 `repo / read:org / gist / delete_repo`，用 API 写 workflow 文件会被以 404 拒绝（写普通文件正常）。绕法是走 SSH 推送——SSH 密钥不受 OAuth scope 限制。

## 局域网接口没有认证

`/stream`、`/stats`、`/actions`、状态页本身都**不校验身份**，设计前提是「跑在可信局域网里，谁连上谁看」。它们只暴露你自己仓库的公开元数据（仓库名、分支、提交标题），不暴露令牌。

需要跨网段访问就别把端口暴露出去，用 SSH 隧道：

```bash
ssh -L 18091:localhost:18091 you@10.0.0.5
```

真正敏感的接口（`/github/*`、`/runners/*`、`/relay/deploy`，会用到本机 `gh` 凭据和 SSH 密码）**只接受本机请求**，局域网访问一律 403。

`GET /config` 局域网能读（令牌已掩码），但 **`POST /config` 只认本机**：改配置会重写 `ws_url` 与令牌，等于把整条事件流指向别处。所以在手机/平板上打开面板能看，改设置会拿到 403——回到跑接收端那台机器上改。

## self-hosted runner 在公开仓库上是危险的

公开仓库 + self-hosted 意味着任何人的 PR 都能在你机器上执行代码。GitHub 自己也会警告。要么只给私有仓库用，要么在 workflow 里加审批闸（`environment` + required reviewers）。

## Windows 上的测试没进 CI

`unit_machines` 会断言密钥文件权限是 `600`，这在 Windows 的权限模型里没有对应概念。所以 Windows 只跑前端那 4 个 JS 测试，Python 单测在 Linux / macOS 上跑。

## SQLite 单进程

本地库是 SQLite，写操作串行。几千条的规模完全够（`/stats` 对高频查询做了缓存 + 单飞），但别指望它当时序数据库存几十万行——真要长期留，定期把旧行导出成 JSONL 再删。

远端 D1 有自动清理：Durable Object 每天用 `setAlarm` 删掉超过 `RETENTION_DAYS`（默认 90 天）的事件，所以 SQL 存储不会无限长——实测 300 事件/天约占免费额度的 3%。

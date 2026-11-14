# 这个 PR 做了什么

<!-- 一句话说清动机：什么现象 / 为什么现有行为不对 -->

## 怎么验证的

<!--
跑了哪个测试、贴关键输出；改了界面就贴截图或录屏。
至少要有 `make test` 的末尾几行。
-->

```text
$ make test
...
```

## 检查清单

- [ ] `make test` 通过（没跑 e2e 的话说明原因）
- [ ] `make pii` 通过（没有带进真实邮箱 / 内网 IP / 令牌 / 真机主机名）
- [ ] 改了 worker 的话 `make typecheck` 通过
- [ ] 新增 HTTP 端点已在 `local-client/AGENT.md`（和 `AGENT.en.md`）里登记
- [ ] 新接口用到了本机凭据的话，加了 `_local_only()` 闸
- [ ] 这个 PR 只做一件事（顺手重构另开）

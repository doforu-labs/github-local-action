#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
e2e.py —— 端到端验收，不需要 Cloudflare 账号

它验证四件事：
  1. 本地端能连上 Hub 并收到实时事件
  2. SSE 端点能把事件推给「局域网设备」（这里用一个 SSE 客户端充当）
  3. 连接被掐断后能自动重连
  4. 断线期间产生的事件会被补发，且**不会重复**

跑法： python3 tests/e2e.py
"""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import aiohttp

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
MOCK = HERE / "mock_hub.py"
RELAY = ROOT / "relay.py"

MOCK_PORT = 17901
RELAY_PORT = 17902
TOKEN = "test-token"


class Checker:
    def __init__(self) -> None:
        self.failures: list[str] = []
        self.passes: list[str] = []

    def check(self, ok: bool, label: str, detail: str = "") -> None:
        if ok:
            self.passes.append(label)
            print(f"  PASS  {label}")
        else:
            self.failures.append(f"{label}: {detail}")
            print(f"  FAIL  {label}  {detail}")


async def wait_health(session: aiohttp.ClientSession, timeout: float = 15.0) -> dict:
    start = time.time()
    last = {}
    while time.time() - start < timeout:
        try:
            async with session.get(f"http://127.0.0.1:{RELAY_PORT}/health") as r:
                last = await r.json()
                if last.get("connected"):
                    return last
        except aiohttp.ClientError:
            pass
        await asyncio.sleep(0.2)
    return last


async def collect_sse(
    session: aiohttp.ClientSession, seconds: float, sink: list[int], after: int = 0
) -> None:
    """读 SSE 流，把 id 收集进 sink。after 模拟浏览器的 Last-Event-ID 续传。"""
    url = f"http://127.0.0.1:{RELAY_PORT}/stream"
    if after:
        url += f"?after={after}"
    async with session.get(url) as resp:
        assert resp.status == 200, f"SSE status {resp.status}"
        ctype = resp.headers.get("Content-Type", "")
        assert "text/event-stream" in ctype, ctype
        deadline = time.time() + seconds
        buf = ""
        while time.time() < deadline:
            try:
                chunk = await asyncio.wait_for(resp.content.read(1024), timeout=deadline - time.time())
            except asyncio.TimeoutError:
                break
            if not chunk:
                break
            buf += chunk.decode("utf-8", "replace")
            while "\n\n" in buf:
                frame, buf = buf.split("\n\n", 1)
                for line in frame.splitlines():
                    if line.startswith("id:"):
                        try:
                            sink.append(int(line[3:].strip()))
                        except ValueError:
                            pass


async def main() -> int:
    tmp = tempfile.mkdtemp(prefix="ghrelay-e2e-")
    db = str(Path(tmp) / "events.db")
    cfg = Path(tmp) / "config.toml"
    cfg.write_text(
        f"""[relay]
ws_url = "ws://127.0.0.1:{MOCK_PORT}/ws"
token = "{TOKEN}"
http_host = "127.0.0.1"
http_port = {RELAY_PORT}
db_path = "{db}"
heartbeat = 5
app_ping = 5
reconnect_max = 2
""",
        encoding="utf-8",
    )

    ck = Checker()
    mock = subprocess.Popen(
        [sys.executable, str(MOCK), "--port", str(MOCK_PORT), "--token", TOKEN, "--interval", "0.3"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.STDOUT,
    )
    relay = subprocess.Popen(
        [sys.executable, str(RELAY), "-c", str(cfg), "--log-level", "WARNING"],
        cwd=tmp,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.STDOUT,
    )

    try:
        await asyncio.sleep(1.0)
        async with aiohttp.ClientSession() as s:
            # ── 1. 连上 ──
            health = await wait_health(s)
            ck.check(bool(health.get("connected")), "本地端建立 WebSocket 连接", str(health))

            # ── 2. 收到实时事件 ──
            ids: list[int] = []
            await collect_sse(s, 2.5, ids)
            ck.check(len(ids) >= 4, "SSE 收到实时事件", f"只收到 {len(ids)} 条")
            ck.check(len(ids) == len(set(ids)), "实时阶段无重复", f"{len(ids)} 条里有重复")

            # ── 3. 掐断连接 ──
            before = max(ids) if ids else 0
            async with s.post(f"http://127.0.0.1:{MOCK_PORT}/control/drop") as r:
                dropped = await r.json()
            ck.check(dropped.get("closed", 0) >= 1, "已掐断连接", str(dropped))

            # 断线期间造 3 条事件
            async with s.post(f"http://127.0.0.1:{MOCK_PORT}/control/add?n=3") as r:
                added = (await r.json())["added"]
            print(f"        断线期间新增 id: {added}")

            # ── 4. 重连 + 补发 ──
            healed = await wait_health(s, timeout=15)
            ck.check(bool(healed.get("connected")), "断线后自动重连", str(healed))
            ck.check(healed.get("connects", 0) >= 2, "重连次数 >= 2", str(healed.get("connects")))

            ids2: list[int] = []
            await collect_sse(s, 3.0, ids2, after=before)
            got = set(ids2)
            missing = [i for i in added if i not in got]
            ck.check(not missing, "断线期间的事件被补发", f"缺 {missing}")

            all_ids = ids2
            ck.check(len(all_ids) == len(set(all_ids)), "补发阶段无重复", f"{len(all_ids)} 条里有重复")

            # 起点必须接在上次之后，不能倒回去重放
            if ids2:
                ck.check(min(ids2) > before, "补发从游标之后开始", f"min={min(ids2)} before={before}")

            # ── 5. 历史接口 ──
            async with s.get(f"http://127.0.0.1:{RELAY_PORT}/events?after=0&limit=5") as r:
                hist = await r.json()
            ck.check(len(hist.get("events", [])) == 5, "历史接口按 limit 返回", str(len(hist.get("events", []))))
            seq = [e["id"] for e in hist["events"]]
            ck.check(seq == sorted(seq), "历史按 id 升序", str(seq))

            # ── 6. 状态页 ──
            async with s.get(f"http://127.0.0.1:{RELAY_PORT}/") as r:
                html = await r.text()
                ctype = r.headers.get("Content-Type", "")
            ck.check(r.status == 200 and "text/html" in ctype, "状态页返回 HTML", ctype)
            ck.check("gh-relay" in html and "EventSource" in html, "状态页内容完整", f"{len(html)} 字节")

            async with s.get(f"http://127.0.0.1:{RELAY_PORT}/stats") as r:
                st = await r.json()
            ck.check(len(st.get("hourly", [])) == 24, "统计返回 24 个整点桶", str(len(st.get("hourly", []))))
            ck.check(st.get("total", 0) > 0, "统计有总事件数", str(st.get("total")))

            # ── 6b. 账号与仓库列表 ──
            acc = st.get("account") or {}
            ck.check(acc.get("login") == "demo", "从 payload 识别出账号", str(acc))
            ck.check(bool(acc.get("avatar")), "账号头像地址有值", str(acc.get("avatar")))
            repos = st.get("repos") or []
            ck.check(len(repos) == 1 and repos[0]["repo"] == "demo/repo", "仓库列表正确", str(repos))
            if repos:
                ck.check(repos[0]["events"] > 0 and repos[0]["owner"] == "demo", "仓库归属与计数正确", str(repos[0]))
                ck.check(set(repos[0]["types"]) == {"push", "pull_request"}, "仓库按类型分组", str(repos[0]["types"]))

            # ── 6c. 设置接口 ──
            BASE = f"http://127.0.0.1:{RELAY_PORT}"
            raw = await (await s.get(f"{BASE}/config")).text()
            conf = json.loads(raw)
            ck.check(conf.get("token_set") is True, "设置里报告已设置令牌", str(conf.get("token_set")))
            ck.check(TOKEN not in raw, "设置接口不回传令牌明文", raw[:120])
            ck.check("ws_url" in (conf.get("hot_fields") or []), "标记出可热改字段", str(conf.get("hot_fields")))

            bad = await s.post(f"{BASE}/config", json={"ws_url": "http://example.com"})
            ck.check(bad.status == 400, "非法地址被拒绝", str(bad.status))
            cold = await s.post(f"{BASE}/config", json={"http_port": 1234})
            ck.check(cold.status == 400, "需重启的字段不允许在线改", str(cold.status))

            saved = await s.post(f"{BASE}/config", json={"heartbeat": 9})
            body = await saved.json()
            ck.check(saved.status == 200 and body.get("changed") == ["heartbeat"],
                     "改心跳返回改动列表", str(body.get("changed")))
            text = cfg.read_text(encoding="utf-8")
            ck.check("heartbeat = 9" in text, "改动写回了配置文件", text.replace("\n", " ")[:80])
            ck.check(cfg.with_suffix(".toml.bak").exists(), "写回前留了 .bak", str(cfg))
            ck.check(TOKEN in text, "其余字段没被写丢", "")

            noop = await s.post(f"{BASE}/config", json={"heartbeat": 9})
            ck.check((await noop.json()).get("changed") == [], "重复保存时报告无改动", "")

            # ── 6d. 动作页（从 workflow_run 事件拼运行列表） ──
            empty = await (await s.get(f"{BASE}/actions")).json()
            ck.check(empty.get("total") == 0, "没有 workflow_run 时动作页是空的", str(empty.get("total")))

            await s.post(f"http://127.0.0.1:{MOCK_PORT}/control/workflow?run_id=9001")
            await asyncio.sleep(0.9)
            acts = await (await s.get(f"{BASE}/actions")).json()
            ck.check(acts.get("total") == 1, "两次状态推送去重成一次运行", str(acts.get("total")))
            if acts.get("runs"):
                run = acts["runs"][0]
                ck.check(run.get("status") == "completed" and run.get("conclusion") == "success",
                         "动作取到最新状态", f'{run.get("status")}/{run.get("conclusion")}')
                ck.check(run.get("branch") == "main" and run.get("sha") == "abc1234",
                         "动作带出分支和短 sha", f'{run.get("branch")} {run.get("sha")}')
                ck.check(bool(run.get("url")), "动作带出 GitHub 链接", str(run.get("url")))
            ck.check([r["repo"] for r in acts.get("repos", [])] == ["demo/repo"],
                     "动作按仓库归组", str(acts.get("repos")))
            filtered = await (await s.get(f"{BASE}/actions?repo=demo/repo")).json()
            ck.check(filtered.get("total") == 1, "动作支持按仓库过滤", str(filtered.get("total")))

            # ── 6e. 借本地 gh 的凭据（没装/未登录时走降级分支） ──
            auth = await (await s.get(f"{BASE}/github/auth")).json()
            ck.check({"installed", "logged_in"} <= set(auth), "github auth 返回合法结构", str(list(auth))[:80])
            bad_repo = await s.get(f"{BASE}/github/runs?repo=not-a-repo")
            ck.check(bad_repo.status == 400, "github 运行接口校验 repo 格式", str(bad_repo.status))
            bad_run = await s.get(f"{BASE}/github/jobs?repo=a/b&run=abc")
            ck.check(bad_run.status == 400, "github job 接口校验 run id", str(bad_run.status))
            if auth.get("logged_in"):
                missing = await s.get(f"{BASE}/github/runs?repo=OWNER/__no_such_repo__")
                ck.check(missing.status == 502, "仓库不存在时把 gh 的报错透出来", str(missing.status))
            else:
                refused = await s.get(f"{BASE}/github/runs?repo=a/b")
                ck.check(refused.status == 409, "gh 未登录时明确拒绝而不是假装空", str(refused.status))

            # ── 6f. 自建 runner 的接入页 ──
            bad = await s.get(f"{BASE}/github/runners?repo=not-a-repo")
            ck.check(bad.status == 400, "runner 列表校验 repo 格式", str(bad.status))
            # 「只生成命令」那条路已经删掉了，接口不能还留着
            gone = await s.post(f"{BASE}/github/runner-token",
                                json={"repo": "a/b", "platform": "linux"})
            ck.check(gone.status == 404, "runner 令牌接口已下线", str(gone.status))
            bad = await s.post(f"{BASE}/github/runners/remove",
                               json={"repo": "a/b", "id": "abc"})
            ck.check(bad.status == 400, "runner 移除校验 id", str(bad.status))
            if auth.get("logged_in"):
                got = await s.get(f"{BASE}/github/runners?repo=example-org/demo-test")
                # 网络被重置时给 502 可以接受，500 不行——那说明解析崩了
                ck.check(got.status in (200, 502), "runner 列表不会 500", str(got.status))
                if got.status == 200:
                    body = await got.json()
                    ck.check({"total", "online", "runners"} <= set(body),
                             "runner 列表结构完整", str(list(body))[:80])
                    rows = body.get("runners") or []
                    ck.check(all({"name", "status", "busy", "labels"} <= set(r) for r in rows),
                             "runner 行字段齐全", str(rows[:1])[:120])

            # ── 6f-2. SSH 装 runner：只做校验与边界，不真去连机器 ──
            # 这层测的是「不会因为参数不对就把密码发出去」和「重启后说清楚要重填」。
            bad = await s.post(f"{BASE}/runners/ssh/add",
                               json={"repo": "a/b", "host": "h", "user": "u", "name": "n"})
            ck.check(bad.status == 400, "SSH 添加：密码为空时拒绝", str(bad.status))
            bad = await s.post(f"{BASE}/runners/ssh/add",
                               json={"repo": "nope", "host": "h", "user": "u",
                                     "password": "p", "name": "n"})
            ck.check(bad.status == 400, "SSH 添加：repo 格式错就拒", str(bad.status))
            bad = await s.post(f"{BASE}/runners/ssh/add",
                               json={"repo": "a/b", "host": "h", "user": "u",
                                     "password": "p", "name": "有中文"})
            ck.check(bad.status == 400, "SSH 添加：机器名不允许奇怪字符", str(bad.status))
            bad = await s.post(f"{BASE}/runners/ssh/remove",
                               json={"repo": "a/b", "name": "ghost"})
            ck.check(bad.status == 400, "SSH 卸载：没有凭据时明确要重填", str(bad.status))
            gone = await s.get(f"{BASE}/runners/ssh/job/__nope__")
            ck.check(gone.status == 404, "SSH 任务：不存在的任务返回 404", str(gone.status))
            # 目录校验是删除路径的第一道闸：这几个必须被挡（否则 rm -rf 会删错东西）
            for dir_bad, label in (("~/.ssh", "隐藏目录"), ("~/..", "上跳"),
                                   ("~", "只有家目录"), ("/", "根"),
                                   ("~/a b", "带空格")):
                r0 = await s.post(f"{BASE}/runners/ssh/add",
                                  json={"repo": "a/b", "host": "h", "user": "u",
                                        "password": "p", "name": "n", "dir": dir_bad})
                ck.check(r0.status == 400, f"SSH 添加：目录栅栏挡 {label}",
                         f"{dir_bad} -> {r0.status}")
            r0 = await s.post(f"{BASE}/runners/ssh/add",
                              json={"repo": "a/b", "host": "h", "user": "u",
                                    "password": "p", "name": "n", "port": "abc"})
            ck.check(r0.status == 400, "SSH 添加：端口非数字被拒", str(r0.status))
            r0 = await s.post(f"{BASE}/runners/ssh/add",
                              json={"repo": "a/b", "host": "h", "user": "u",
                                    "password": "p", "name": "n", "os": "bsd"})
            ck.check(r0.status == 400, "SSH 添加：未知系统被拒", str(r0.status))

            # 页面现在只让填四样：地址/端口/用户/密码。机器名和目录都不传也得能交上去
            # （名字连上后读主机名，目录从 ~/actions-runner 顺延），所以不能报 400。
            r0 = await s.post(f"{BASE}/runners/ssh/add",
                              json={"repo": "a/b", "host": "127.0.0.1", "port": 1,
                                    "user": "u", "password": "p"})
            ck.check(r0.status == 200, "SSH 添加：不传机器名和目录也能提交", str(r0.status))
            if r0.status == 200:
                jd = await r0.json()
                ck.check(bool(jd.get("job")), "SSH 添加：返回任务号", str(jd)[:80])
                state = "running"
                for _ in range(24):
                    await asyncio.sleep(0.5)
                    q = await s.get(f"{BASE}/runners/ssh/job/{jd['job']}")
                    if q.status != 200:
                        break
                    state = (await q.json()).get("status") or ""
                    if state != "running":
                        break
                # 端口 1 上没人监听：必须在连接阶段就失败，不能卡住或假装成功
                ck.check(state == "failed", "SSH 添加：连不上的机器明确失败", state)

            # ── 7. 挂着 SSE 客户端时能否快速退出 ──
            # 回归点：不推哨兵的话 runner.cleanup() 会等活跃请求，最长 60 秒
            holder = asyncio.create_task(collect_sse(s, 30, []))
            await asyncio.sleep(1.5)
            t0 = time.time()
            relay.terminate()
            try:
                relay.wait(timeout=8)
                quick = True
            except subprocess.TimeoutExpired:
                quick = False
            elapsed = time.time() - t0
            ck.check(quick, "挂着 SSE 客户端时能快速退出", f"{elapsed:.1f}s 未退出")
            ck.check(quick and elapsed < 5, "退出耗时 <5s", f"{elapsed:.2f}s")
            holder.cancel()

    finally:
        for p in (relay, mock):
            p.terminate()
            try:
                p.wait(timeout=5)
            except subprocess.TimeoutExpired:
                p.kill()

    print()
    if ck.failures:
        print(f"结果：{len(ck.passes)} 通过 / {len(ck.failures)} 失败")
        for f in ck.failures:
            print(f"  - {f}")
        return 1
    print(f"结果：{len(ck.passes)} 项全部通过")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))

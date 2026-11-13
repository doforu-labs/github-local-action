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
import re
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
            ck.check("Local Action" in html and "EventSource" in html, "状态页内容完整", f"{len(html)} 字节")

            # ── 6b. 给 Agent 那一页：本来只是个页面，但它承担「把 agent 接进来」这一步，
            # 跑坏了用户会以为 MCP 不能用，所以这里钉死：入口在、复制块在、兜底在。
            ck.check('href="#/agent"' in html and 'data-view="agent"' in html,
                    "给 Agent 页：侧栏有入口", "")
            ck.check(all(f'id="{i}"' in html for i in ("view-agent", "agentBrief", "agentMcp", "agentStdio", "agentDoc")),
                    "给 Agent 页：四个块都在", "")
            ck.check(all(f'data-copy="{i}"' in html for i in ("agentBrief", "agentMcp", "agentStdio", "agentDoc")),
                    "给 Agent 页：每块都有复制按钮", "")
            ck.check("navigator.clipboard" in html and "execCommand('copy')" in html,
                    "给 Agent 页：clipboard 带非安全上下文兜底", "")
            ck.check("/agent.en.md" in html and "agentDocPath" in html,
                    "给 Agent 页：说明书跟着界面语言换", "")
            ck.check(all(f"{n}" in html for n in ("list_runs", "get_run_detail", "get_job_log", "list_machines")),
                    "给 Agent 页：开场白带上了全部工具名", "")

            async with s.get(f"http://127.0.0.1:{RELAY_PORT}/stats") as r:
                st = await r.json()
            ck.check(len(st.get("hourly", [])) == 24, "统计返回 24 个整点桶", str(len(st.get("hourly", []))))
            ck.check(st.get("total", 0) > 0, "统计有总事件数", str(st.get("total")))

            async with s.get(f"http://127.0.0.1:{RELAY_PORT}/agent.md") as r:
                doc = await r.text()
                dct = r.headers.get("Content-Type", "")
            ck.check(r.status == 200 and "markdown" in dct, "agent.md 以 markdown 返回", dct)
            ck.check("18091" in doc and "/github/joblog" in doc and "token_hint" in doc,
                     "agent.md 带上了地址和关键接口", f"{len(doc)} 字节")
            async with s.get(f"http://127.0.0.1:{RELAY_PORT}/llms.txt") as r2:
                ck.check(r2.status == 200 and (await r2.text()) == doc, "/llms.txt 与 agent.md 同内容", "")
            ck.check(TOKEN not in doc, "agent.md 不带令牌明文", "")

            # 英文版：页面切 English 时拉的这份。少一行、没翻完、还是中文，都算没做完。
            async with s.get(f"http://127.0.0.1:{RELAY_PORT}/agent.en.md") as r:
                en = await r.text()
                ect = r.headers.get("Content-Type", "")
            ck.check(r.status == 200 and "markdown" in ect, "agent.en.md 以 markdown 返回", ect)
            ck.check("18091" in en and "/github/joblog" in en and "token_hint" in en,
                     "agent.en.md 带上了地址和关键接口", f"{len(en)} 字节")
            ck.check(not re.search(r"[\u4e00-\u9fff]", en), "agent.en.md 里没有漏翻的中文",
                     str(re.findall(r"[\u4e00-\u9fff]+", en)[:5]))
            ck.check(len(en) > len(doc) // 2, "agent.en.md 不是空壳", f"{len(en)} vs {len(doc)}")
            ck.check(TOKEN not in en, "agent.en.md 不带令牌明文", "")
            async with s.get(f"http://127.0.0.1:{RELAY_PORT}/llms.txt") as r3:
                ck.check(r3.status == 200 and (await r3.text()) == doc, "llms.txt 仍跟中文版同内容", "")

            # ── 6a2. MCP over HTTP（Streamable HTTP）──
            MCP = f"http://127.0.0.1:{RELAY_PORT}/mcp"
            init = await s.post(f"{MCP}", json={
                "jsonrpc": "2.0", "id": 1, "method": "initialize",
                "params": {"protocolVersion": "2025-06-18", "capabilities": {},
                           "clientInfo": {"name": "e2e", "version": "0"}}})
            ijson = await init.json()
            sid = init.headers.get("Mcp-Session-Id", "")
            ck.check(init.status == 200 and ijson["result"]["protocolVersion"] == "2025-06-18",
                     "MCP HTTP 握手返回协议版本", str(ijson.get("result", {}))[:60])
            ck.check(bool(sid), "MCP HTTP 握手给了会话号", sid)
            tl = await (await s.post(f"{MCP}", json={
                "jsonrpc": "2.0", "id": 2, "method": "tools/list"})).json()
            ck.check(len(tl["result"]["tools"]) == 7, "MCP HTTP 列出 7 个工具",
                     str([t["name"] for t in tl["result"]["tools"]]))
            call = await (await s.post(f"{MCP}", json={
                "jsonrpc": "2.0", "id": 3, "method": "tools/call",
                "params": {"name": "health", "arguments": {}}})).json()
            ck.check(call["result"]["isError"] is False
                     and "connected" in call["result"]["content"][0]["text"],
                     "MCP HTTP 工具能回调自己的接口", call["result"]["content"][0]["text"][:60])
            note = await s.post(f"{MCP}", json={"jsonrpc": "2.0",
                                                       "method": "notifications/initialized"})
            ck.check(note.status == 202 and not (await note.text()), "MCP HTTP 通知回 202 空体",
                     str(note.status))
            got = await s.get(f"{MCP}")
            ck.check(got.status == 405, "MCP HTTP 的 GET 回 405（没开 SSE 长流）", str(got.status))
            bad_json = await s.post(f"{MCP}", data="not json",
                                    headers={"Content-Type": "application/json"})
            ck.check(bad_json.status == 400
                     and (await bad_json.json())["error"]["code"] == -32700,
                     "MCP HTTP 坏 JSON 回 -32700", str(bad_json.status))
            stale = await s.post(f"{MCP}", json={"jsonrpc": "2.0", "id": 4,
                                                      "method": "ping"},
                                 headers={"Mcp-Session-Id": "deadbeef"})
            ck.check(stale.status == 404, "MCP HTTP 不认得的会话号回 404", str(stale.status))
            good = await s.post(f"{MCP}", json={"jsonrpc": "2.0", "id": 5,
                                                     "method": "ping"},
                                headers={"Mcp-Session-Id": sid})
            ck.check(good.status == 200, "MCP HTTP 带对的会话号能用", str(good.status))
            bye = await s.delete(f"{MCP}", headers={"Mcp-Session-Id": sid})
            ck.check(bye.status == 200, "MCP HTTP 能正常收尾", str(bye.status))

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
            # stdio 那条 MCP 配置要绝对路径，页面靠这个字段推（config_path 可能是相对路径）
            ck.check(bool(conf.get("service_dir")) and "mcp_server.py" not in str(conf.get("service_dir")),
                     "设置里带上了装在哪（给 stdio 拼绝对路径）", str(conf.get("service_dir")))

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

            # 概览上的「今日运行 / 总运行」：口径得跟动作页对得上
            st2 = await (await s.get(f"{BASE}/stats")).json()
            ck.check(st2.get("runs_total") == acts.get("total"),
                     "统计的总运行数跟动作页一致",
                     f'{st2.get("runs_total")} vs {acts.get("total")}')
            ck.check(st2.get("runs_today") == 1, "今日运行数到刚跑的那一次",
                     str(st2.get("runs_today")))
            ck.check(isinstance(st2.get("runs_total"), int)
                     and isinstance(st2.get("runs_today"), int), "运行数是整数而不是字符串",
                     f'{type(st2.get("runs_total"))} {type(st2.get("runs_today"))}')

            # ── 6e. 借本地 gh 的凭据（没装/未登录时走降级分支） ──
            auth = await (await s.get(f"{BASE}/github/auth")).json()
            ck.check({"installed", "logged_in"} <= set(auth), "github auth 返回合法结构", str(list(auth))[:80])
            bad_repo = await s.get(f"{BASE}/github/runs?repo=not/a/repo")
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
            bad = await s.get(f"{BASE}/github/runners?repo=not/a/repo")
            ck.check(bad.status == 400, "runner 列表校验 repo 格式", str(bad.status))
            # 只填组织名是合法输入（走组织级），不该被当成格式错误
            orgish = await s.get(f"{BASE}/github/runners?repo=not-an-org-xyz")
            ck.check(orgish.status != 400, "runner 列表：只填组织名不当格式错误", str(orgish.status))
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
                               json={"repo": "a/b/c", "host": "h", "user": "u",
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

            # 机器不该被仓库绑住：仓库留空 = 纯按需机器，GitHub 那边一个字都不写。
            # 用 127.0.0.1:1 是因为那里没人监听，能在连接阶段就失败，不留下任何机器。
            r1 = await s.post(f"{BASE}/runners/ssh/add",
                              json={"host": "127.0.0.1", "port": 1,
                                    "user": "u", "password": "p"})
            ck.check(r1.status == 200, "SSH 添加：不填仓库也能提交", str(r1.status))
            if r1.status == 200:
                jd = await r1.json()
                st, lines = "running", []
                for _ in range(24):
                    await asyncio.sleep(0.5)
                    q = await s.get(f"{BASE}/runners/ssh/job/{jd['job']}")
                    if q.status != 200:
                        break
                    jb = await q.json()
                    st = jb.get("status") or ""
                    lines = jb.get("lines") or []
                    if st != "running":
                        break
                joined = "\n".join(lines)
                ck.check("纯按需" in joined, "SSH 添加：不填仓库走纯按需分支", joined[:120])
                ck.check("注册令牌" not in joined, "SSH 添加：不填仓库不去要 GitHub 令牌",
                         joined[:120])
                ck.check(st == "failed", "SSH 添加：纯按需连不上时也明确失败", st)

            # 机器清单：手上能连的机器，不按仓库分
            mach = await s.get(f"{BASE}/runners")
            md = await mach.json()
            ck.check(mach.status == 200, "机器清单：接口可用", str(mach.status))
            ck.check(isinstance(md.get("machines"), list), "机器清单：machines 是数组",
                     str(md)[:100])
            ck.check(md.get("count") == len(md.get("machines") or []),
                     "机器清单：count 跟数组对得上", str(md.get("count")))
            ck.check(all(set("name os arch host port pure added_at".split()) <= set(row)
                         for row in (md.get("machines") or [])),
                     "机器清单：每行字段齐全", str((md.get("machines") or [])[:1])[:120])
            ck.check("ttl" not in md, "机器清单：不再给凭据设过期时间", str(md)[:100])

            # 机器实时状态只依赖 SSH；不查询仓库或 GitHub 注册。
            async with s.get(f"{BASE}/machines/live") as lv:
                lvd = await lv.json()
            ck.check(lv.status == 200, "机器状态：接口可用", str(lv.status))
            ck.check(isinstance(lvd.get("machines"), dict), "机器状态：machines 是对象", str(lvd)[:120])
            ck.check(set(lvd.get("machines") or {}) == {r["name"] for r in (md.get("machines") or [])},
                     "机器状态：跟机器清单一一对应", str(list((lvd.get("machines") or {})))[:120])
            ck.check(all(v.get("state") in ("busy", "idle", "offline", "unprepared", "unknown")
                         for v in (lvd.get("machines") or {}).values()),
                     "机器状态：state 只有这几种", str(lvd.get("machines"))[:160])
            ck.check(lvd.get("repos") == [] and lvd.get("source") == "ssh" and "checked_at" in lvd,
                     "机器状态：通过 SSH 检测，不依赖仓库", str(lvd.get("repos"))[:120])
            ck.check(all((v.get("job") is None) or (v.get("state") == "busy")
                         for v in (lvd.get("machines") or {}).values()),
                     "机器状态：没在跑就不给 job", "")
            # 卡片上要显示「最后在线」，不是「接入时间」。这两个数就得能在 live 里拿到，
            # 而且必须是整数时间戳（0 = 从未在线过，前端会写「从未在线」）。
            ck.check(all(isinstance(v.get("last_online_at"), int)
                         for v in (lvd.get("machines") or {}).values()),
                     "机器状态：每台都带 last_online_at", str(lvd.get("machines"))[:160])
            ck.check(all(isinstance(r.get("last_ok"), int) for r in (md.get("machines") or [])),
                     "机器清单：每台都带 last_ok", str(md.get("machines"))[:160])

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

            # ── 6h. 按需 runner（JIT）──
            # 这个例子里池子是空的（没有真实 SSH 凭据），所以正好验校验层。
            jit = await s.get(f"{BASE}/runners/jit")
            jd = await jit.json()
            ck.check(jit.status == 200, "JIT：状态接口可用", str(jit.status))
            need = {"on", "repos", "watching", "max_parallel", "machines", "available",
                    "running", "history", "stats"}
            ck.check(need <= set(jd), "JIT：状态结构完整", str(sorted(set(jd)))[:120])
            ck.check(isinstance(jd.get("available_info"), list),
                     "JIT：状态带架构清单", str(jd.get("available_info"))[:80])
            ck.check(jd.get("on") is False, "JIT：默认是关的", str(jd.get("on")))
            ck.check("reconcile" in jd and isinstance(jd.get("reconcile"), dict),
                     "JIT：状态里带对账结果", str(jd.get("reconcile"))[:80])

            bad = await s.post(f"{BASE}/runners/jit/start", json={})
            ck.check(bad.status == 400, "JIT：没填仓库就拒", str(bad.status))
            bad = await s.post(f"{BASE}/runners/jit/start", json={"repo": "a/b/c"})
            ck.check(bad.status == 400, "JIT：仓库格式错就拒", str(bad.status))
            bad = await s.post(f"{BASE}/runners/jit/start",
                               json={"repo": "a/b", "max_parallel": 99})
            ck.check(bad.status == 400, "JIT：并发数越界就拒", str(bad.status))
            bad = await s.post(f"{BASE}/runners/jit/start",
                               json={"repo": "a/b", "max_parallel": "x"})
            ck.check(bad.status == 400, "JIT：并发数不是数字就拒", str(bad.status))
            bad = await s.post(f"{BASE}/runners/jit/start",
                               json={"repo": "a/b", "machines": ["不存在的机器"]})
            ck.check(bad.status == 400, "JIT：指定没凭据的机器就拒", str(bad.status))
            bad = await s.post(f"{BASE}/runners/jit/start", json={"repo": "a/b"})
            ck.check(bad.status == 400, "JIT：池子空时拒开", str(bad.status))
            if bad.status == 400:
                err = await bad.json()
                ck.check("池子" in err.get("error", ""), "JIT：空池子给的是人话",
                         err.get("error", "")[:60])

            bad = await s.post(f"{BASE}/runners/jit/prepare", json={"name": "nope"})
            ck.check(bad.status == 400, "JIT：准备目录要机器有凭据", str(bad.status))

            # ── 6h-2. 仓库开关：一个一个来，互不影响 ──
            # 这一步不需要机器，只改「接不接这个仓库」的状态
            on1 = await s.post(f"{BASE}/runners/jit/repo", json={"repo": "a/b", "on": True})
            ck.check(on1.status == 200, "JIT：能单独打开一个仓库", str(on1.status))
            d1 = await on1.json()
            ck.check(d1.get("repos", {}).get("a/b") is True, "JIT：打开后状态里是 true",
                     str(d1.get("repos"))[:80])

            on2 = await s.post(f"{BASE}/runners/jit/repo", json={"repo": "c/d", "on": True})
            d2 = await on2.json()
            ck.check(sorted(d2.get("watching") or []) == ["a/b", "c/d"],
                     "JIT：能同时接多个仓库", str(d2.get("watching")))

            off1 = await s.post(f"{BASE}/runners/jit/repo", json={"repo": "a/b", "on": False})
            d3 = await off1.json()
            ck.check(d3.get("watching") == ["c/d"], "JIT：关一个不影响另一个",
                     str(d3.get("watching")))

            bad = await s.post(f"{BASE}/runners/jit/repo", json={"repo": "a b", "on": True})
            ck.check(bad.status == 400, "JIT：开关也校验仓库名", str(bad.status))
            bad = await s.post(f"{BASE}/runners/jit/repo", json={"repo": "a/b", "on": "yes"})
            ck.check(bad.status == 400, "JIT：开关 on 必须是布尔", str(bad.status))

            bad = await s.post(f"{BASE}/runners/jit/pool", json={"machines": ["不存在的机器"]})
            ck.check(bad.status == 400, "JIT：池子只认有凭据的机器", str(bad.status))
            bad = await s.post(f"{BASE}/runners/jit/pool", json={"max_parallel": 99})
            ck.check(bad.status == 400, "JIT：池子并发也卡范围", str(bad.status))

            stop = await s.post(f"{BASE}/runners/jit/stop")
            ck.check(stop.status == 200, "JIT：能停下来", str(stop.status))
            sd = await stop.json()
            ck.check(sd.get("on") is False, "JIT：停完确实关了", str(sd.get("on")))
            ck.check(sd.get("repos") == {}, "JIT：停下会把仓库开关清空", str(sd.get("repos")))

            # 对账：没开任何仓库时应该什么都不问就直接返回（不连 GitHub）
            rec = await s.post(f"{BASE}/runners/jit/reconcile")
            ck.check(rec.status == 200, "JIT：对账接口可用", str(rec.status))
            rd0 = await rec.json()
            rc0 = rd0.get("reconcile") or {}
            ck.check(set(rc0) >= {"at", "repos", "queued", "launched", "waiting", "released"},
                     "JIT：对账结果结构完整", str(sorted(rc0))[:120])
            ck.check(rc0.get("repos") == [] and rc0.get("queued") == 0,
                     "JIT：没开仓库时对账不查 GitHub", str(rc0.get("repos"))[:80])

            # 自动启动：只验状态接口。真装/真卸由 tests/service_check.py 拿临时 label 跑
            # （e2e 如果直接 POST /service/install，会把服务文件写进用户真实目录）
            svc = await s.get(f"{BASE}/service")
            ck.check(svc.status == 200, "自动启动：状态接口可用", str(svc.status))
            svd = await svc.json()
            ck.check({"platform", "supported", "installed", "running", "managed",
                      "file", "log"} <= set(svd), "自动启动：状态结构完整", str(sorted(svd))[:120])
            ck.check(svd.get("platform") in ("macos", "linux"),
                     "自动启动：认出了平台", str(svd.get("platform")))
            ck.check(str(svd.get("file", "")).endswith((".plist", ".service")),
                     "自动启动：给出服务文件在哪", str(svd.get("file"))[:120])

            # ── 6i. 仓库全量清单（全部仓库 + 最近提交时间）──
            rl = await s.get(f"{BASE}/github/repos")
            ck.check(rl.status == 200, "仓库清单：接口可用", str(rl.status))
            rd = await rl.json()
            ck.check(isinstance(rd.get("repos"), list), "仓库清单：repos 是数组",
                     str(type(rd.get("repos"))))
            ck.check("count" in rd and "error" in rd, "仓库清单：带 count 和 error",
                     str(sorted(rd))[:80])
            rows = rd.get("repos") or []
            ck.check(rd.get("count") == len(rows), "仓库清单：count 跟数组对得上",
                     str(rd.get("count")))
            if rows:
                ck.check(all("repo" in x for x in rows[:20]),
                         "仓库清单：每行都有 repo 字段", str(rows[0])[:80])
                stamps = [x.get("pushed_at") or "" for x in rows[:20]]
                ck.check(stamps == sorted(stamps, reverse=True),
                         "仓库清单：按最近提交倒序", str(stamps[:3])[:80])
            else:
                # gh 没登录时拿不到名单，但接口本身不能挂
                ck.check(True, "仓库清单：拿不到名单也不报错", "")

            # ── 6j. webhook 能不能自动配 ──
            ws = await s.get(f"{BASE}/webhook/status",
                             params={"repo": "example-org/demo-test"})
            ck.check(ws.status == 200, "webhook 状态：接口可用", str(ws.status))
            wd = await ws.json()
            ck.check({"hook", "target", "ready"} <= set(wd),
                     "webhook 状态：带 hook/target/ready", str(sorted(wd))[:80])
            ck.check(str(wd.get("target", "")).endswith("/gh/webhook"),
                     "webhook 状态：目标地址是接收端", str(wd.get("target"))[:60])

            bad = await s.get(f"{BASE}/webhook/status", params={"repo": "a b"})
            ck.check(bad.status == 400, "webhook 状态：仓库名不对就拒", str(bad.status))
            bad = await s.post(f"{BASE}/webhook/setup", json={"repo": "a b"})
            ck.check(bad.status == 400, "webhook 配置：仓库名不对就拒", str(bad.status))
            bad = await s.post(f"{BASE}/webhook/setup", json={})
            ck.check(bad.status == 400, "webhook 配置：不填仓库就拒", str(bad.status))

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

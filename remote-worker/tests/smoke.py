#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
smoke.py —— 对着 `wrangler dev` 起本地 Worker 做真实请求验证

验证：
  1. 合法签名的 webhook → 202，且落库
  2. 错签名的 webhook → 401
  3. 重复 delivery → 202 但不重复落库（幂等）
  4. 错误 token 的 WebSocket → 401
  5. 正确 token + hello 游标 → 收到补发的事件

跑法（先起本地 worker）：
    npx wrangler dev --port 17903 --var GITHUB_WEBHOOK_SECRET:testsecret --var CLIENT_TOKEN:testtoken
    python3 tests/smoke.py --url http://127.0.0.1:17903 --token testtoken --secret testsecret
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import hmac
import json
import sys

import aiohttp

PASS, FAIL = [], []


def check(ok: bool, label: str, detail: str = "") -> None:
    (PASS if ok else FAIL).append(label)
    print(f"  {'PASS' if ok else 'FAIL'}  {label}  {detail if not ok else ''}".rstrip())


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="http://127.0.0.1:17903")
    ap.add_argument("--token", default="testtoken")
    ap.add_argument("--secret", default="testsecret")
    args = ap.parse_args()

    async with aiohttp.ClientSession() as s:
        # 先取基线：本地 D1 是持久的，重跑会累加
        async with s.get(f"{args.url}/health") as r:
            base = (await r.json()).get("events", 0)
        tag = str(int(asyncio.get_running_loop().time() * 1000))

        # ── 1. 合法签名 ──
        body = json.dumps(
            {
                "action": "opened",
                "repository": {"full_name": "demo/repo"},
                "pull_request": {"number": 42},
            }
        ).encode()
        sig = "sha256=" + hmac.new(args.secret.encode(), body, hashlib.sha256).hexdigest()
        headers = {
            "X-GitHub-Delivery": f"delivery-{tag}-1",
            "X-GitHub-Event": "pull_request",
            "X-Hub-Signature-256": sig,
            "Content-Type": "application/json",
        }
        async with s.post(f"{args.url}/gh/webhook", data=body, headers=headers) as r:
            text = await r.text()
        check(r.status == 202, "合法签名 webhook 返回 202", f"status={r.status} {text}")

        # ── 2. 错签名 ──
        bad = dict(headers, **{"X-Hub-Signature-256": "sha256=" + "0" * 64})
        async with s.post(f"{args.url}/gh/webhook", data=body, headers=bad) as r:
            check(r.status == 401, "错误签名被拒 401", f"status={r.status}")

        # ── 3. 重复投递 ──
        async with s.post(f"{args.url}/gh/webhook", data=body, headers=headers) as r:
            check(r.status == 202, "重复投递仍返回 202", f"status={r.status}")

        async with s.get(f"{args.url}/health") as r:
            health = await r.json()
        check(
            health.get("events") == base + 1,
            "幂等：重复投递只落库一条",
            f"base={base} now={health.get('events')}",
        )

        # 再补一条，方便验证补发
        body2 = json.dumps({"repository": {"full_name": "demo/repo"}, "ref": "refs/heads/main"}).encode()
        sig2 = "sha256=" + hmac.new(args.secret.encode(), body2, hashlib.sha256).hexdigest()
        async with s.post(
            f"{args.url}/gh/webhook",
            data=body2,
            headers={
                "X-GitHub-Delivery": f"delivery-{tag}-2",
                "X-GitHub-Event": "push",
                "X-Hub-Signature-256": sig2,
                "Content-Type": "application/json",
            },
        ) as r:
            check(r.status == 202, "第二条事件入库", f"status={r.status}")

        # ── 4. 错误 token ──
        ws_url = args.url.replace("http://", "ws://").replace("https://", "wss://")
        try:
            async with s.ws_connect(f"{ws_url}/ws?token=wrong") as ws:
                check(False, "错误 token 的 WebSocket 被拒", f"居然连上了 {ws}")
        except aiohttp.WSServerHandshakeError as e:
            check(e.status == 401, "错误 token 的 WebSocket 被拒 401", f"status={e.status}")

        # ── 5. 正确 token + 续传 ──
        got = []
        async with s.ws_connect(f"{ws_url}/ws?token={args.token}") as ws:
            await ws.send_json({"type": "hello", "cursor": 0})
            try:
                while len(got) < 2:
                    msg = await asyncio.wait_for(ws.receive(), timeout=8)
                    if msg.type != aiohttp.WSMsgType.TEXT:
                        break
                    data = json.loads(msg.data)
                    if data.get("type") == "event":
                        got.append(data)
            except asyncio.TimeoutError:
                pass
            await ws.send_json({"type": "ping"})
            pong = False
            try:
                while True:
                    msg = await asyncio.wait_for(ws.receive(), timeout=5)
                    if msg.type != aiohttp.WSMsgType.TEXT:
                        break
                    if json.loads(msg.data).get("type") == "pong":
                        pong = True
                        break
            except asyncio.TimeoutError:
                pass

        check(len(got) >= 2, "WebSocket 补发了事件（id>0 全量）", f"收到 {len(got)} 条")
        if len(got) >= 2:
            check(got[0]["id"] < got[1]["id"], "事件按 id 升序", str([g["id"] for g in got]))
            check(got[-1]["event"] == "push", "最后一条是 push", str(got[-1].get("event")))
        check(pong, "ping → pong 正常")

    print()
    if FAIL:
        print(f"结果：{len(PASS)} 通过 / {len(FAIL)} 失败")
        return 1
    print(f"结果：{len(PASS)} 项全部通过")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))

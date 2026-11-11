#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
mock_hub.py —— 假的远端 Hub，用来在没有 Cloudflare 账号的情况下验证本地端

行为对齐真 Hub：
  · token 不对就 401
  · 收到 {"type":"hello","cursor":N} → 回 ready，然后补发 id>N 的事件
  · 收到 {"type":"ping"} → 回 pong
  · 新事件广播给所有连接

额外加了两个控制端点，只给测试用：
  POST /control/add?n=3   凭空造几条事件
  POST /control/drop      掐断所有连接（模拟网络中断 / Worker 重新部署）
"""

from __future__ import annotations

import argparse
import asyncio
import json
import time
from typing import Any, Set

from aiohttp import web

hub: "Hub"


class Hub:
    def __init__(self) -> None:
        self.events: list[dict[str, Any]] = []
        self.next_id = 1
        self.sockets: Set[web.WebSocketResponse] = set()

    def make_event(self) -> dict[str, Any]:
        n = self.next_id
        ev = {
            "type": "event",
            "id": n,
            "event": "push" if n % 3 else "pull_request",
            "repo": "demo/repo",
            "action": None if n % 3 else "opened",
            "at": int(time.time() * 1000),
            "payload": {
                "seq": n,
                "ref": "refs/heads/main",
                # 跟真 webhook 一致的结构：账号和仓库都从这里取
                "repository": {
                    "full_name": "demo/repo",
                    "owner": {"login": "demo"},
                },
                "sender": {
                    "login": "demo",
                    "avatar_url": "https://avatars.githubusercontent.com/u/1?v=4",
                },
            },
        }
        self.next_id += 1
        self.events.append(ev)
        return ev

    async def broadcast(self, ev: dict[str, Any]) -> None:
        for ws in list(self.sockets):
            try:
                await ws.send_json(ev)
            except ConnectionError:
                self.sockets.discard(ws)


async def ws_handler(request: web.Request) -> web.WebSocketResponse:
    if request.query.get("token") != request.app["token"]:
        return web.Response(status=401, text="unauthorized")

    ws = web.WebSocketResponse(heartbeat=None)
    await ws.prepare(request)
    hub.sockets.add(ws)

    try:
        async for msg in ws:
            if msg.type != web.WSMsgType.TEXT:
                continue
            try:
                data = json.loads(msg.data)
            except json.JSONDecodeError:
                continue

            if data.get("type") == "hello":
                cursor = int(data.get("cursor") or 0)
                await ws.send_json({"type": "ready", "cursor": cursor})
                backlog = [e for e in hub.events if e["id"] > cursor]
                for ev in backlog:
                    await ws.send_json(ev)
                if backlog:
                    await ws.send_json(
                        {"type": "ready", "cursor": backlog[-1]["id"], "replayed": len(backlog)}
                    )
            elif data.get("type") == "ping":
                await ws.send_json({"type": "pong", "at": int(time.time() * 1000)})
    finally:
        hub.sockets.discard(ws)
    return ws


async def control_add(request: web.Request) -> web.Response:
    n = int(request.query.get("n", "1"))
    made = []
    for _ in range(n):
        ev = hub.make_event()
        made.append(ev["id"])
        await hub.broadcast(ev)
    return web.json_response({"added": made, "total": hub.next_id - 1})


async def control_drop(request: web.Request) -> web.Response:
    n = len(hub.sockets)
    for ws in list(hub.sockets):
        await ws.close(code=1012, message=b"server restart".ljust(123, b" "))
    hub.sockets.clear()
    return web.json_response({"closed": n})


async def control_state(request: web.Request) -> web.Response:
    return web.json_response(
        {"events": hub.next_id - 1, "sockets": len(hub.sockets)}
    )


async def emitter(interval: float) -> None:
    while True:
        await asyncio.sleep(interval)
        await hub.broadcast(hub.make_event())


async def main() -> None:
    global hub
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=17901)
    parser.add_argument("--token", default="test-token")
    parser.add_argument("--interval", type=float, default=0.3, help="自动发事件的间隔（秒）")
    parser.add_argument("--start-id", type=int, default=1, help="起始 id（预置历史数据时接着往下编）")
    args = parser.parse_args()

    hub = Hub()
    hub.next_id = args.start_id

    app = web.Application()
    app["token"] = args.token
    app.router.add_get("/ws", ws_handler)
    app.router.add_post("/control/add", control_add)
    app.router.add_post("/control/drop", control_drop)
    app.router.add_get("/control/state", control_state)

    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    await web.TCPSite(runner, args.host, args.port).start()

    task = asyncio.create_task(emitter(args.interval))
    print(f"mock hub on ws://{args.host}:{args.port}/ws token={args.token}", flush=True)
    try:
        await asyncio.Event().wait()
    finally:
        task.cancel()
        await runner.cleanup()


if __name__ == "__main__":
    asyncio.run(main())

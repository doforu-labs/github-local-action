#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""用真实 Chrome（headless）按指定宽度截一张整页图，给 README 用。

为什么不用 command 行的 --screenshot：这个页面挂着 SSE 长连接，--screenshot
会一直等 load 完成，实测在 macOS 上直接挂住。这里自己走 CDP，拿到稳定帧再截。

用法：
    python3 scripts/shot.py http://127.0.0.1:18291/#/actions docs/images/actions.png --height 1200
"""

from __future__ import annotations

import asyncio
import base64
import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import aiohttp

CHROME = os.environ.get("CHROME", "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome")


async def shot(url: str, out: Path, width: int, height: int, port: int) -> None:
    profile = tempfile.mkdtemp(prefix="chrome-shot-")
    proc = subprocess.Popen(
        [CHROME, "--headless=new", "--disable-gpu", "--no-first-run",
         "--no-default-browser-check", "--disable-extensions",
         "--hide-scrollbars", "--mute-audio",
         f"--user-data-dir={profile}", f"--remote-debugging-port={port}", "about:blank"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    try:
        async with aiohttp.ClientSession() as s:
            ws_url = ""
            for _ in range(50):                      # 等调试端口起来
                try:
                    async with s.get(f"http://127.0.0.1:{port}/json/version") as r:
                        ws_url = (await r.json())["webSocketDebuggerUrl"]
                    break
                except Exception:
                    await asyncio.sleep(0.2)
            if not ws_url:
                raise SystemExit("Chrome 调试端口没起来")

            # 新建一个渲染进程页；新版 Chrome 只接受 PUT
            async with s.put(f"http://127.0.0.1:{port}/json/new?about:blank") as r:
                page = await r.json()
            async with s.ws_connect(page["webSocketDebuggerUrl"], max_msg_size=0) as ws:
                seq = 0

                async def call(method: str, params: dict) -> dict:
                    nonlocal seq
                    seq += 1
                    await ws.send_json({"id": seq, "method": method, "params": params})
                    while True:
                        msg = json.loads((await ws.receive()).data)
                        if msg.get("id") == seq:
                            return msg.get("result", {})

                await call("Page.enable", {})
                await call("Emulation.setDeviceMetricsOverride",
                           {"width": width, "height": height, "deviceScaleFactor": 2, "mobile": False})
                await call("Page.navigate", {"url": url})
                await asyncio.sleep(3.0)             # 让接口数据落进 DOM
                res = await call("Page.captureScreenshot",
                                 {"format": "png", "captureBeyondViewport": True,
                                  "clip": {"x": 0, "y": 0, "width": width,
                                           "height": height, "scale": 1}})
                out.parent.mkdir(parents=True, exist_ok=True)
                out.write_bytes(base64.b64decode(res["data"]))
                print(f"{out}  {out.stat().st_size // 1024}KB")
    finally:
        proc.terminate()


def main() -> int:
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    if len(args) < 2:
        print(__doc__)
        return 2
    url, out = args[0], Path(args[1])
    width = int(os.environ.get("SHOT_WIDTH", "1440"))
    height = int(os.environ.get("SHOT_HEIGHT", "1000"))
    for a in sys.argv[1:]:
        if a.startswith("--height"):
            height = int(a.split("=", 1)[1])
        elif a.startswith("--width"):
            width = int(a.split("=", 1)[1])
    asyncio.run(shot(url, out, width, height, 9400 + int(time.time()) % 400))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

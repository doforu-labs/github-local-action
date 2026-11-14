#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""本机闸：哪些接口只认 127.0.0.1，改配置算不算其中一个。

为什么值得单独测：这道闸是「局域网里有人能改你的中继地址吗」的唯一防线，
而它只体现为每个 handler 开头的一行 `_local_only(request)`。删掉那一行代码
照样能跑、页面照样能开，只是别人能把整条事件流指到自己的服务器上——没有
测试的话，重构时最容易被顺手删。

跑法： python3 tests/unit_local_only.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import relay  # noqa: E402

FAILS: list[str] = []
PASSES: list[str] = []


def ck(ok: bool, label: str, detail: str = "") -> None:
    if ok:
        PASSES.append(label)
        print(f"  PASS  {label}")
    else:
        FAILS.append(label)
        print(f"  FAIL  {label}  {detail}")


class FakeReq:
    """只带 handler 在过闸前后会碰到的字段。"""

    def __init__(self, remote: str, body: dict | None = None):
        self.remote = remote
        self._body = body or {}
        self.app: dict = {}
        self.query: dict = {}
        self.match_info: dict = {}

    async def json(self):
        if self._body is _BAD_JSON:
            raise ValueError("not json")
        return self._body


_BAD_JSON = object()


def main() -> int:
    print("本机闸：回环地址的四种写法")
    for addr in ("127.0.0.1", "::1", "::ffff:127.0.0.1"):
        ck(relay.is_local(FakeReq(addr)), f"{addr} 判定为本机")
    for addr in ("10.0.0.21", "192.168.1.9", "::ffff:10.0.0.21", ""):
        ck(not relay.is_local(FakeReq(addr)), f"{addr or '(空)'} 判定为局域网")

    print("本机闸：放行与拒绝的形态")
    ck(relay._local_only(FakeReq("127.0.0.1")) is None, "本机放行（返回 None）")
    blocked = relay._local_only(FakeReq("10.0.0.21"))
    ck(blocked is not None and blocked.status == 403, "局域网被 403 拦下",
       str(getattr(blocked, "status", None)))
    ck(blocked is not None and "error" in json.loads(blocked.body.decode()), "拒绝时给出 error 文案")

    print("改配置：只允许本机")
    import asyncio

    remote = FakeReq("10.0.0.21", {"ws_url": "wss://evil.example/ws"})
    resp = asyncio.run(relay.handle_config_save(remote))
    ck(getattr(resp, "status", None) == 403, "局域网改配置被拒", str(getattr(resp, "status", None)))
    body = json.loads(resp.body.decode())
    ck("ws_url" in body.get("error", ""), "拒绝理由说清是哪几个字段", body.get("error", "")[:40])

    # 本机请求应当越过这道闸，往下走到「请求体不是合法 JSON」那一步
    # （真正写盘要完整的 cfg/app，这里只确认它没在闸上被拦）
    local_bad = FakeReq("127.0.0.1", _BAD_JSON)
    local_bad.app = {"cfg": relay.Config("wss://example.test/ws", "token"), "state": object()}
    resp2 = asyncio.run(relay.handle_config_save(local_bad))
    ck(getattr(resp2, "status", None) == 400, "本机请求越过闸（走到了解析体那步）",
       str(getattr(resp2, "status", None)))

    print()
    if FAILS:
        print(f"结果：{len(PASSES)} 通过 / {len(FAILS)} 失败")
        return 1
    print(f"结果：{len(PASSES)} 项全部通过")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

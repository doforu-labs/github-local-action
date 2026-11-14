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

    def __init__(self, remote: str, body: dict | None = None, headers: dict | None = None):
        self.remote = remote
        self._body = body or {}
        self.headers = headers or {}
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
    for addr in ("10.0.0.21", "203.0.113.7", "::ffff:10.0.0.21", ""):
        ck(not relay.is_local(FakeReq(addr)), f"{addr or '(空)'} 判定为局域网")

    print("本机闸：放行与拒绝的形态")
    ck(relay._local_only(FakeReq("127.0.0.1")) is None, "本机放行（返回 None）")
    blocked = relay._local_only(FakeReq("10.0.0.21"))
    ck(blocked is not None and blocked.status == 403, "局域网被 403 拦下",
       str(getattr(blocked, "status", None)))
    ck(blocked is not None and "error" in json.loads(blocked.body.decode()), "拒绝时给出 error 文案")

    print("跨源闸：/mcp 只认本机页面")
    # 浏览器发的请求本身就来自 127.0.0.1，来源 IP 拦不住它，所以得看 Origin。
    for origin in ("", "http://127.0.0.1:18091", "http://localhost:5173",
                   "https://127.0.0.1", "http://[::1]:8080"):
        ck(relay.mcp_origin_ok(origin), f"放行：{origin or '(没带 Origin，命令行/IDE)'}")
    for origin in ("https://evil.example", "http://10.0.0.21:8080", "null",
                   "https://127.0.0.1.evil.example", "http://localhost.attacker.test",
                   "ftp://x", "不是个 URL"):
        ck(not relay.mcp_origin_ok(origin), f"拦下：{origin}")

    gate = relay._mcp_origin_gate(
        FakeReq("127.0.0.1", headers={"Origin": "https://evil.example"}))
    ck(gate is not None and gate.status == 403, "外来页面过闸被 403",
       str(getattr(gate, "status", None)))
    ck(relay._mcp_origin_gate(FakeReq("127.0.0.1")) is None,
       "没带 Origin 的本机客户端照旧放行")
    ck(relay._mcp_origin_gate(
        FakeReq("127.0.0.1", headers={"Origin": "http://127.0.0.1:5173"})) is None,
       "本机页面放行")

    cors = relay.mcp_cors(FakeReq("127.0.0.1"))
    ck(cors["Access-Control-Allow-Origin"] == "*", "没带 Origin 时不回声通配")
    cors2 = relay.mcp_cors(FakeReq("127.0.0.1", headers={"Origin": "http://127.0.0.1:5173"}))
    ck(cors2["Access-Control-Allow-Origin"] == "http://127.0.0.1:5173",
       "带 Origin 时原样回声，不再是 *", str(cors2["Access-Control-Allow-Origin"]))
    ck(cors2.get("Vary") == "Origin", "回 Vary: Origin，别让缓存混用")

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

    print("本机闸：容器里跑时，宿主机的地址算不算本机")
    # 真实 /proc/net/route 的一段（默认路由的网关是 172.17.0.1）
    route = (
        "Iface\tDestination\tGateway\t\tFlags\tRefCnt\tUse\tMetric\tMask\t\tMTU\tWindow\tIRTT\n"
        "eth0\t00000000\t010011AC\t\t0003\t0\t0\t0\t00000000\t0\t0\t0\n"
        "eth0\t000011AC\t00000000\t\t0001\t0\t0\t0\t0000FFFF\t0\t0\t0\n"
    )
    ck(relay._default_gateway(route) == "172.17.0.1", "从 /proc/net/route 算出网关",
       repr(relay._default_gateway(route)))
    ck(relay._default_gateway("Iface\tDestination\tGateway\n") == "", "没有默认路由就返回空")
    ck(relay._default_gateway("乱七八糟\n不是路由表") == "", "解析不了不该抛异常")

    def req_with_peers(remote: str, peers: tuple[str, ...]) -> FakeReq:
        r = FakeReq(remote)
        r.app = {"cfg": relay.Config("wss://example.test/ws", "t", trusted_peers=peers)}
        return r

    saved_gateway = relay._gateway_cache["ip"]
    relay._gateway_cache["ip"] = "172.17.0.1"      # 假装在容器里
    try:
        ck(not relay.is_local(req_with_peers("172.17.0.1", ())),
           "默认不列：网桥网关仍算外人（行为没变）")
        ck(relay.is_local(req_with_peers("172.17.0.1", ("172.17.0.1",))),
           "显式列 IP：放行")
        ck(relay.is_local(req_with_peers("172.17.0.1", ("gateway",))),
           "写 gateway 令牌：自动认网关")
        ck(relay.is_local(req_with_peers("10.0.0.21", ("10.0.0.21", "gateway"))),
           "列了别的宿主机地址也放行")
        ck(not relay.is_local(req_with_peers("10.0.0.22", ("gateway",))),
           "同网段的其他设备仍被拦")
    finally:
        relay._gateway_cache["ip"] = saved_gateway

    print("容器布置：环境变量也能列可信地址")
    import os
    from pathlib import Path as _P
    import tempfile

    tmp = tempfile.TemporaryDirectory()
    cfg_path = _P(tmp.name) / "config.toml"
    cfg_path.write_text('[relay]\nws_url = "wss://x.test/ws"\ntoken = "t"\n'
                       'trusted_peers = ["10.0.0.21"]\n', encoding="utf-8")
    old_env = os.environ.get("GLA_TRUSTED_PEERS")
    os.environ["GLA_TRUSTED_PEERS"] = "gateway, 10.0.0.21"
    try:
        loaded = relay.Config.load(cfg_path)
        ck(loaded.trusted_peers == ("10.0.0.21", "gateway"),
           "文件 + 环境变量合并去重", str(loaded.trusted_peers))
        ck(relay._toml_value(list(loaded.trusted_peers)) == '["10.0.0.21", "gateway"]',
           "写回 TOML 时列值格式对", relay._toml_value(list(loaded.trusted_peers)))
    finally:
        if old_env is None:
            os.environ.pop("GLA_TRUSTED_PEERS", None)
        else:
            os.environ["GLA_TRUSTED_PEERS"] = old_env
        tmp.cleanup()

    print()
    if FAILS:
        print(f"结果：{len(PASSES)} 通过 / {len(FAILS)} 失败")
        return 1
    print(f"结果：{len(PASSES)} 项全部通过")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
unit_jit_persist.py —— 「按需开关重启后还在」这条的验收（不连网、不起服务）

修的是这个毛病：JIT_POOL 以前是纯内存，进程一退，「仓库」页 65 个开关全变回关闭。
看着像「没人开」，其实是「忘了」——开关是配置，得落盘。

单测要盯住的几件事（顺序就是出错的顺序）：

  · 打开一个仓库 → meta 表里真有这条记录，机器池、并发也一起
  · 关掉的仓库不能跟着存进去（否则下次重启自己又开了）
  · 重启（重建 JIT_POOL 再 restore）→ 这个仓库还开着
  · 存的机器已经删了/密码过期了 → 按「现在还能连的」重算，别恢复出个点不亮的池子
  · 主动停掉（repos 全空）→ 记录也清空，重启不能自说自话开回来
  · 脏数据（不是 owner/name、JSON 坏了、库写失败）→ 一律不抛，当没开过
  · 运行时状态（running/history）绝不跟着恢复 —— 不然重启后满屏「正在跑」

跑法： python3 tests/unit_jit_persist.py
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import relay  # noqa: E402

FAILS: list[str] = []
PASSES: list[str] = []


def ck(ok: bool, label: str, detail: str = "") -> None:
    if ok:
        PASSES.append(label)
    else:
        FAILS.append(f"{label}  {detail}".strip())


def _machine(name: str, os_: str = "macos", arch: str = "arm64") -> dict:
    return {"name": name, "os": os_, "arch": arch, "host": "10.0.0.21",
            "port": (2222 if os_ == "windows" else 22), "user": "dev", "dir": "/Users/dev/actions-runner"}


def _fresh_pool() -> None:
    """回到刚启动的样子：什么都没开。"""
    relay.JIT_POOL.update(
        repos={}, machines=[], max_parallel=2, running={}, history=[],
        stats={"picked": 0, "failed": 0, "done": 0, "skipped": 0, "last_error": ""},
    )


class FakeReq:
    """只喂 handler 真正用到的那几样：本机来源、app['store']、JSON body。"""

    def __init__(self, store, body: dict | None = None) -> None:
        self.remote = "127.0.0.1"
        self.app = {"store": store}
        self._body = body if body is not None else {}

    async def json(self):
        return self._body


def _raw_meta(store, key: str = relay.JIT_META_KEY) -> str | None:
    """绕过 Store 直接看库里到底写了什么（别让被测代码自己给自己作证）。"""
    conn = sqlite3.connect(store.path)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
        return str(row["value"]) if row else None
    finally:
        conn.close()


def main() -> None:
    tmp = Path(tempfile.mkdtemp(prefix="jit-persist-"))
    store = relay.Store(str(tmp / "events.db"))
    real_machines = relay.SSH_MACHINES

    try:
        # ── 1. meta 读写本身 ───────────────────────────────────────────
        ck(store.meta_get("nope", "缺省") == "缺省", "meta：没有的键给缺省值")
        store.meta_set("probe", "v1")
        ck(store.meta_get("probe") == "v1", "meta：写进去能读回来")
        store.meta_set("probe", "v2")
        ck(store.meta_get("probe") == "v2", "meta：同一个键覆盖而不是插两条")
        ck(_raw_meta(store, "probe") == "v2", "meta：确实落在 meta 表里")

        # ── 2. 打开一个仓库 → 落库 ────────────────────────────────────
        _fresh_pool()
        relay.SSH_MACHINES = {"Mac": _machine("Mac"), "win": _machine("win", "windows", "x64")}
        r = asyncio.run(relay.handle_runner_jit_repo(FakeReq(store, {"repo": "example-org/a", "on": True})))
        ck(r.status == 200, "开关：打开一个仓库返回 200", f"status={r.status}")
        body = json.loads(_raw_meta(store) or "{}")
        ck(body.get("repos") == ["example-org/a"], "开关：开着的仓库写进了库", json.dumps(body, ensure_ascii=False))
        ck(sorted(body.get("machines") or []) == ["Mac", "win"],
           "开关：机器池跟着一起写（不填就按当前能连的算）", str(body.get("machines")))
        ck(body.get("max_parallel") == 2, "开关：并发数跟着写", str(body.get("max_parallel")))
        ck("running" not in body and "history" not in body,
           "开关：运行时状态没有混进去", list(body.keys()))

        # ── 3. 关掉一个仓库 → 记录里不能再有它 ────────────────────────
        asyncio.run(relay.handle_runner_jit_repo(FakeReq(store, {"repo": "example-org/a", "on": False})))
        body = json.loads(_raw_meta(store) or "{}")
        ck(body.get("repos") == [], "开关：关掉的仓库不留在记录里", json.dumps(body.get("repos"), ensure_ascii=False))

        # ── 4. 重启：换一批机器，仓库开关得回来 ───────────────────────
        asyncio.run(relay.handle_runner_jit_repo(FakeReq(store, {"repo": "example-org/a", "on": True})))
        asyncio.run(relay.handle_runner_jit_repo(FakeReq(store, {"repo": "example-org/b", "on": True})))
        _fresh_pool()                       # 进程重启：内存全空
        relay.SSH_MACHINES = {"Mac": _machine("Mac")}   # win 那台已经删了
        relay._jit_restore(store)
        ck(sorted(relay.JIT_POOL["repos"]) == ["example-org/a", "example-org/b"],
           "重启：开着的仓库都回来了", str(relay.JIT_POOL["repos"]))
        ck(all(relay.JIT_POOL["repos"].values()), "重启：回来的都是开着的（不是开着却算关）")
        ck(relay.JIT_POOL["machines"] == ["Mac"],
           "重启：机器池按现在能连的重算，删掉的机器不复活", str(relay.JIT_POOL["machines"]))
        ck(relay.JIT_POOL["running"] == {} and relay.JIT_POOL["history"] == [],
           "重启：不会复活一批早就不存在的「正在跑」")

        # ── 5. 主动停掉 → 重启不能自己开回来 ──────────────────────────
        asyncio.run(relay.handle_runner_jit_stop(FakeReq(store)))
        ck(json.loads(_raw_meta(store) or "{}").get("repos") == [], "停止：记录清空")
        _fresh_pool()
        relay._jit_restore(store)
        ck(relay.JIT_POOL["repos"] == {}, "停止：重启之后仍然是关的（不会自说自话开回来）")

        # ── 6. 脏数据一律不炸 ────────────────────────────────────────
        store.meta_set(relay.JIT_META_KEY, json.dumps({"repos": ["example-org/a", "../etc/passwd", "", 42]}))
        _fresh_pool()
        try:
            relay._jit_restore(store)
            ok = relay.JIT_POOL["repos"] == {"example-org/a": True}
        except Exception as exc:                        # noqa: BLE001
            ok, exc  # noqa: B018
            ck(False, "脏数据：不像 owner/name 的被丢掉", repr(exc))
        else:
            ck(ok, "脏数据：不像 owner/name 的被丢掉", str(relay.JIT_POOL["repos"]))

        store.meta_set(relay.JIT_META_KEY, "{这不是 JSON")
        _fresh_pool()
        try:
            relay._jit_restore(store)
            ck(relay.JIT_POOL["repos"] == {}, "脏数据：JSON 坏了当没开过，不抛")
        except Exception as exc:                        # noqa: BLE001
            ck(False, "脏数据：JSON 坏了当没开过，不抛", repr(exc))

        # ── 7. 库写失败不能把开关接口打挂 ─────────────────────────────
        class BrokenStore:
            path = store.path

            def meta_set(self, *_a, **_k):
                raise sqlite3.OperationalError("disk I/O error")

            def meta_get(self, *_a, **_k):
                return ""

        _fresh_pool()
        relay.SSH_MACHINES = {"Mac": _machine("Mac")}
        try:
            r = asyncio.run(relay.handle_runner_jit_repo(
                FakeReq(BrokenStore(), {"repo": "example-org/a", "on": True})))
            ck(r.status == 200 and relay.JIT_POOL["repos"] == {"example-org/a": True},
               "写库失败：这一轮照样开，只是没记住")
        except Exception as exc:                        # noqa: BLE001
            ck(False, "写库失败：这一轮照样开，只是没记住", repr(exc))
    finally:
        relay.SSH_MACHINES = real_machines
        _fresh_pool()
        store.close()

    print()
    for p in PASSES:
        print(f"  PASS  {p}")
    for f in FAILS:
        print(f"  FAIL  {f}")
    print(f"\n结果：{len(PASSES)} 通过 / {len(FAILS)} 失败")
    sys.exit(1 if FAILS else 0)


if __name__ == "__main__":
    main()

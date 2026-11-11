#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
unit_machines.py —— 机器凭据落盘的单元验收（不连网、不起服务）

为什么要单独一个：凭据以前只在内存里，进程一重启就没了，机器第二天就用不了。
现在改成落 sqlite，而"落盘"这件事出错的方式很隐蔽——
  · 密码写成明文了，光看接口是看不出来的
  · 重启后读回来是空密码，但要等到真去连机器才发现
  · 密钥文件丢了，库里那堆密文永远解不开
前两条这里直接断言，第三条验证密钥缺失时的行为是"明确失败"而不是崩。

跑法： python3 tests/unit_machines.py
"""

from __future__ import annotations

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


def main() -> int:
    tmp = Path(tempfile.mkdtemp(prefix="mstore-"))
    db = tmp / "events.db"
    key = tmp / "events.db.machines.key"

    store = relay.MachineStore(str(db), str(key))

    ck(key.exists(), "首次启动会生成密钥文件")
    ck(oct(key.stat().st_mode)[-3:] == "600", "密钥文件权限是 600",
       oct(key.stat().st_mode)[-3:])

    store["mac-9"] = {
        "host": "10.0.0.9", "port": 22, "user": "tester",
        "password": "P@ssw0rd-not-real", "os": "macos", "arch": "arm64",
        "dir": "", "repo": "", "pure": True, "at": 1794405842,
    }
    ck("mac-9" in store, "写入后能查到")
    ck(len(store) == 1, "长度对得上", str(len(store)))

    back = store["mac-9"]
    ck(back["host"] == "10.0.0.9", "主机读回来了", str(back.get("host")))
    ck(back["user"] == "tester", "用户名读回来了", str(back.get("user")))
    ck(back["arch"] == "arm64", "架构读回来了", str(back.get("arch")))
    ck(back["pure"] is True, "纯按需标记读回来了", str(back.get("pure")))
    ck(back["password"] == "P@ssw0rd-not-real", "密码解密后跟原文一致", repr(back.get("password")))

    # 最关键的一条：库里不能出现明文密码
    raw = db.read_bytes()
    ck(b"P@ssw0rd-not-real" not in raw, "数据库文件里没有明文密码")
    conn = sqlite3.connect(str(db))
    secret = conn.execute("SELECT secret FROM machines WHERE name='mac-9'").fetchone()[0]
    conn.close()
    ck(secret and secret != "P@ssw0rd-not-real", "secret 列存的是密文", (secret or "")[:20])
    ck(secret.startswith("gAAAAA"), "secret 是 Fernet 格式", (secret or "")[:20])

    # 换个实例 = 进程重启：数据还在、密码还能解
    again = relay.MachineStore(str(db), str(key))
    ck(len(again) == 1, "重启后机器还在", str(len(again)))
    ck(again["mac-9"]["password"] == "P@ssw0rd-not-real", "重启后密码还能解出来",
       repr(again["mac-9"].get("password")))
    ck(again.keys() == ["mac-9"], "keys() 给出机器名", str(again.keys()))
    ck([n for n, _ in again.items()] == ["mac-9"], "items() 能遍历")

    # 同名再写一次 = 更新，不是插两条
    store["mac-9"] = {**back, "port": 2222}
    ck(len(store) == 1, "同名覆盖不会变两条", str(len(store)))
    ck(store["mac-9"]["port"] == 2222, "覆盖后的端口生效", str(store["mac-9"]["port"]))

    # 取不到时是 None，不是抛异常（上层到处用 .get）
    ck(store.get("没有这台") is None, "get 取不到给 None")
    ck(store.pop("没有这台", None) is None, "pop 不存在的给默认值")

    got = store.pop("mac-9")
    ck(got is not None and got["host"] == "10.0.0.9", "pop 返回被删的那条")
    ck(len(store) == 0, "pop 之后没了", str(len(store)))

    # 密钥丢了：不能让服务崩，但也不能假装能连——密码解出来是空串
    store["ghost"] = {"host": "h", "user": "u", "password": "p", "os": "linux"}
    key.unlink()
    broken = relay.MachineStore(str(db), str(key))
    ck(len(broken) == 1, "密钥丢了机器记录还在（不崩）", str(len(broken)))
    ck(broken["ghost"]["password"] == "", "密钥换了以后密码解不出来（明确为空，不是乱码）",
       repr(broken["ghost"]["password"]))

    print()
    for label in PASSES:
        print(f"  PASS  {label}")
    for item in FAILS:
        print(f"  FAIL  {item}")
    print(f"\n结果：{len(PASSES)} 通过 / {len(FAILS)} 失败")
    return 0 if not FAILS else 1


if __name__ == "__main__":
    raise SystemExit(main())

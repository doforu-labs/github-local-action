#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
unit_arch.py —— 架构相关逻辑的单元验收（不连网、不起服务）

为什么要单独一个：arm 机器是后来才加的，而「下错包」「标签打错」
这两件事在装之前都看不出来——装完 exec format error 才发现。
这几个函数是纯逻辑，直接断言比跑一整套 e2e 便宜得多。

跑法： python3 tests/unit_arch.py
"""

from __future__ import annotations

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
        FAILS.append(f"{label}: {detail}")
        print(f"  FAIL  {label}  {detail}")


def main() -> int:
    print("=== uname -m 的写法 → 架构 ===")
    cases = {
        "x86_64": "x64", "amd64": "x64", "x64": "x64",
        "aarch64": "arm64", "arm64": "arm64", "arm64ec": "arm64",
        "armv7l": "arm", "armv6l": "arm",
    }
    for raw, want in cases.items():
        got = relay._ARCH_ALIAS.get(raw)
        ck(got == want, f"{raw} → {want}", f"实际 {got}")
    ck(relay._ARCH_ALIAS.get("sparc64", "x64") == "x64",
       "认不出来的架构退回 x64", "退回值不对")

    print()
    print("=== 每个平台都得有 x64 的包 ===")
    for os_name in ("linux", "macos", "windows"):
        packs = relay._RUNNER_ARCHIVE.get(os_name) or {}
        ck("x64" in packs, f"{os_name} 有 x64 包", str(sorted(packs)))
        ck(all("{v}" in tpl for tpl in packs.values()),
           f"{os_name} 包名模板都带版本占位", str(list(packs.values())))

    print()
    print("=== arm 机器不能拿到 x64 包 ===")
    for os_name, arch in (("linux", "arm64"), ("linux", "arm"),
                          ("macos", "arm64"), ("windows", "arm64")):
        packs = relay._RUNNER_ARCHIVE.get(os_name) or {}
        tpl = packs.get(arch) or ""
        ck(bool(tpl) and "x64" not in tpl,
           f"{os_name}/{arch} 用的是 arm 包", tpl or "没有这个架构的包")

    print()
    print("=== 标签 ===")
    for arch, want in (("x64", "X64"), ("arm64", "ARM64"), ("arm", "ARM")):
        ck(relay._RUNNER_ARCH_LABEL.get(arch) == want, f"{arch} → {want}",
           str(relay._RUNNER_ARCH_LABEL.get(arch)))

    print()
    print("=== JIT 调度：架构对不对得上 ===")
    x64_box = {"os": "linux", "arch": "x64"}
    arm_box = {"os": "linux", "arch": "arm64"}
    old_box = {"os": "linux"}          # 升级前存下来的老记录，没有 arch 字段
    checks = [
        ("arm 活 → arm 机", arm_box, ["self-hosted", "linux", "arm64"], True),
        ("arm 活 → x64 机", x64_box, ["self-hosted", "linux", "arm64"], False),
        ("x64 活 → arm 机", arm_box, ["self-hosted", "linux", "x64"], False),
        ("没写架构 → x64 机", x64_box, ["self-hosted", "linux"], True),
        ("没写架构 → arm 机", arm_box, ["self-hosted", "linux"], True),
        ("老记录没 arch → 当 x64", old_box, ["self-hosted", "linux", "x64"], True),
        ("不是自建 runner", arm_box, ["ubuntu-latest"], False),
        ("平台不对", arm_box, ["self-hosted", "macos"], False),
    ]
    for label, box, labels, want in checks:
        got = relay._jit_can_run(box, labels)
        ck(got == want, label, f"实际 {'可以' if got else '不行'}")

    print()
    total = len(PASSES) + len(FAILS)
    if FAILS:
        print(f"结果：{len(PASSES)} 通过 / {len(FAILS)} 失败（共 {total}）")
        for f in FAILS:
            print("  -", f)
        return 1
    print(f"结果：{total} 项全部通过")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

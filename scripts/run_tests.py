#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""一条命令跑完所有测试，CI 和本地都用它。

    python3 scripts/run_tests.py              # 全跑（单测 + e2e）
    python3 scripts/run_tests.py --no-e2e     # 只跑单测（e2e 要占两个端口）
    python3 scripts/run_tests.py --only js    # 只跑前端单测

为什么不是 pytest：这些测试从第一天就是「自己打印 PASS/FAIL、退出码定生死」的
独立脚本，没依赖 pytest 的 fixture，迁移只会带来风险，不带来信息。这里只负责
把 19 个脚本（15 个 Python + 4 个 JS）串起来、汇总失败数、失败时给出文件名。
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CLIENT = ROOT / "local-client"
TESTS = CLIENT / "tests"


def _force_utf8_output() -> None:
    """把汇总器自己的输出流改成 UTF-8。

    Windows runner 上的 sys.stdout 是 cp1252（管道 + 本地化 locale），测试名里有中文，
    `print()` 直接抛 UnicodeEncodeError —— 这是修完解码之后暴露出来的另一半，同一个病。
    """
    for stream in (sys.stdout, sys.stderr):
        enc = (getattr(stream, "encoding", "") or "").lower().replace("-", "")
        if enc != "utf8" and hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8", errors="replace")


def _run(cmd: list[str]) -> tuple[bool, str]:
    # encoding 必须写死 utf-8：测试输出都是中文，Windows 默认拿 cp1252 解码会在读线程里
    # 抛 UnicodeDecodeError，那个线程一死 stdout 就变成 None（见下面那次真实翻车）。
    # 子进程也统一给 PYTHONIOENCODING，免得它自己往管道里写中文时先崩。
    env = {**os.environ, "PYTHONIOENCODING": "utf-8"}
    proc = subprocess.run(cmd, cwd=CLIENT, capture_output=True, text=True,
                          encoding="utf-8", errors="replace", env=env)
    # proc.stdout 可能是 None（读线程崩了、或者输出被吞），别拿它直接做字符串加法
    out = ((proc.stdout or "") + (proc.stderr or "")).strip()
    return proc.returncode == 0, out.splitlines()[-1] if out else "（无输出）"


def main() -> int:
    _force_utf8_output()
    only = ""
    for i, arg in enumerate(sys.argv):
        if arg == "--only" and i + 1 < len(sys.argv):
            only = sys.argv[i + 1]
    no_e2e = "--no-e2e" in sys.argv

    failed: list[str] = []
    py_tests = sorted(TESTS.glob("unit_*.py"))
    js_tests = sorted(TESTS.glob("unit_*.js"))

    if only in ("", "py", "python"):
        for path in py_tests:
            ok, last = _run([sys.executable, str(path)])
            print(f"  {'PASS' if ok else 'FAIL'}  {path.name:<28} {last[:70]}")
            if not ok:
                failed.append(path.name)

    if only in ("", "js"):
        for path in js_tests:
            ok, last = _run(["node", str(path)])
            print(f"  {'PASS' if ok else 'FAIL'}  {path.name:<28} {last[:70]}")
            if not ok:
                failed.append(path.name)

    if only in ("", "e2e") and not no_e2e:
        print("  … e2e（起假 Hub，约 40 秒）")
        ok, last = _run([sys.executable, str(TESTS / "e2e.py")])
        print(f"  {'PASS' if ok else 'FAIL'}  {'e2e.py':<28} {last[:70]}")
        if not ok:
            failed.append("e2e.py")

    print()
    if failed:
        print(f"FAIL：{len(failed)} 个测试文件没过 —— {', '.join(failed)}")
        return 1
    print("全部通过。")
    return 0


if __name__ == "__main__":
    _force_utf8_output()
    raise SystemExit(main())

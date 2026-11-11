#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
unit_runner_env.py —— 装 runner 时写 .env 的单元验收（不连网、不起服务）

为什么要单独一个：runner 服务是 launchd / systemd 拉起来的，继承到的 PATH
只有系统默认那几条，用户装在自己家目录里的 node / pyenv / cargo 在 job 里
一概看不见。实测三台 mac 都装了 node 在 ~/.local/node24/bin，job 里
`node -v` 却是 command not found —— 这种坑装完看日志才发现，太贵。

修法是装的那一刻把 PATH 写进 <runner 目录>/.env（actions/runner 的
Program.cs → LoadAndSetEnv 启动时读它，job 全继承）。这里断言三件事：

  1. 生成的脚本里确实有写 .env 的步骤，而且排在起服务之前
     （写在起服务之后等于这次装完不生效）
  2. 那段 shell 真的能跑，跑完 .env 里的 PATH 包含机器上真实存在的工具目录
     （只断言"字符串里有 .env"是骗人的，要真跑）
  3. .env 里用户自己加的别的变量不能被我们覆盖掉

跑法： python3 tests/unit_runner_env.py
"""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import relay  # noqa: E402


FAILS: list[str] = []
PASSES: list[str] = []


def ok(name: str) -> None:
    PASSES.append(name)
    print(f"  PASS  {name}")


def bad(name: str, detail: str = "") -> None:
    FAILS.append(f"{name}  {detail}")
    print(f"  FAIL  {name}  {detail}")


def check(name: str, cond: bool, detail: str = "") -> None:
    ok(name) if cond else bad(name, detail)


def _posix_script() -> str:
    p = {"dir": "~/actions-runner", "os": "linux", "user": "dev",
         "repo": "OWNER/x", "name": "t1"}
    return relay._posix_install_script(
        p, "TOKEN", "https://example.invalid/x.tar.gz", "self-hosted,Linux,X64")


def test_posix_has_env_step_before_service() -> None:
    s = _posix_script()
    check("POSIX：脚本里写了 .env", '.env' in s)
    exists = [l for l in s.splitlines() if 'ENVPATH' in l or 'PATH=%s' in l]
    check("POSIX：.env 里写的是 PATH", any('printf "PATH=' in l for l in s.splitlines()),
          str(exists[:2]))
    i_env = s.find('$DIR/.env')
    i_start = s.find("svc.sh start")
    check("POSIX：.env 写在起服务之前", 0 < i_env < i_start,
          f".env@{i_env} start@{i_start}")


def test_posix_snippet_syntax() -> None:
    s = _posix_script()
    with tempfile.NamedTemporaryFile("w", suffix=".sh", delete=False) as f:
        f.write(s)
        path = f.name
    try:
        r = subprocess.run(["bash", "-n", path], capture_output=True, text=True)
        check("POSIX：整段脚本 bash -n 通过", r.returncode == 0, r.stderr.strip()[:120])
    finally:
        os.unlink(path)


def _run_env_lines(home: Path, runner_dir: Path, preset: str = "") -> str:
    """真跑那段写 .env 的 shell，返回 .env 内容。"""
    script = "set -e\nDIR=%s\n%s\n" % (
        str(runner_dir), "\n".join(relay._posix_env_lines()))
    if preset:
        (runner_dir / ".env").write_text(preset)
    env = dict(os.environ)
    env["HOME"] = str(home)
    r = subprocess.run(["bash", "-c", script], capture_output=True, text=True, env=env)
    if r.returncode != 0:
        return "RUN_FAILED: " + (r.stderr.strip()[:200] or str(r.returncode))
    return (runner_dir / ".env").read_text()


def test_env_really_contains_tool_dirs() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        home = Path(tmp) / "home"
        runner = Path(tmp) / "runner"
        node_bin = home / ".local" / "node24" / "bin"
        node_bin.mkdir(parents=True)
        runner.mkdir()
        text = _run_env_lines(home, runner)
        check("真实执行：.env 里出现了 PATH=", text.startswith("PATH=") or "\nPATH=" in text,
              text[:80])
        line = next((l for l in text.splitlines() if l.startswith("PATH=")), "")
        segments = line[len("PATH="):].split(":")
        check("真实执行：家目录里装好的 node 进了 PATH",
              str(node_bin) in segments, line[:160])
        check("真实执行：系统默认目录没被丢掉",
              all(x in segments for x in ("/usr/bin", "/bin")), line[:160])


def test_env_keeps_other_vars() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        home = Path(tmp) / "home"
        runner = Path(tmp) / "runner"
        home.mkdir()
        runner.mkdir()
        text = _run_env_lines(home, runner, preset="LANG=en_US.UTF-8\nMY_FLAG=1\n")
        check("保留：用户自己加的变量还在",
              "LANG=en_US.UTF-8" in text and "MY_FLAG=1" in text, text[:120])
        check("保留：PATH 只有一行",
              sum(1 for l in text.splitlines() if l.startswith("PATH=")) == 1, text[:120])


def test_win_has_env_step() -> None:
    p = {"dir": "~/actions-runner", "os": "windows", "user": "Docker",
         "repo": "OWNER/x", "name": "w1"}
    s = relay._win_install_script(p, "T", "https://example.invalid/x.zip",
                                 "self-hosted,Windows,X64")
    check("Windows：脚本里写了 .env", "'.env'" in s or '.env' in s)
    check("Windows：合并了机器级和用户级 PATH",
          "'Machine'" in s and "'User'" in s)
    check("Windows：.env 写在做 run.cmd 之前",
          0 < s.find("'.env'") < s.find("run.cmd"))


def main() -> int:
    print("unit_runner_env —— 装 runner 时把工具链写进 .env")
    test_posix_has_env_step_before_service()
    test_posix_snippet_syntax()
    test_env_really_contains_tool_dirs()
    test_env_keeps_other_vars()
    test_win_has_env_step()
    print()
    if FAILS:
        print(f"结果：{len(PASSES)} 通过 / {len(FAILS)} 失败")
        for f in FAILS:
            print("  -", f)
        return 1
    print(f"结果：{len(PASSES)} 项全部通过")
    return 0


if __name__ == "__main__":
    sys.exit(main())

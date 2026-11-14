#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
service_check.py —— 「设置里那个自动启动开关」的验收

两部分：

A. 内容生成（任何平台都跑）
   · macOS：plist 能被 plistlib 解析，Label / ProgramArguments / RunAtLoad /
     KeepAlive / WorkingDirectory / 日志路径都对得上；路径里的 & < > 要转义。
   · Linux：unit 有 [Unit]/[Service]/[Install] 三段，ExecStart 里带空格的路径
     要加引号，Restart 策略和日志重定向都在。
   · 命令拼装：有配置文件就带 -c，没有就不带。

B. 真实起停（只有本机平台能跑）
   · macOS：确实 bootstrap 一个临时 label 的 LaunchAgent 起来（用本项目的生成器，
     命令换成一个小 HTTP 服务），等它监听、curl 通，再 bootout 掉确认停了。
   · Linux：同样思路用 systemctl --user 跑一遍临时 unit。

跑法： python3 tests/service_check.py
"""

from __future__ import annotations

import asyncio
import json
import os
import plistlib
import shlex
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request
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
        FAILS.append(f"{label}  {detail}".strip())
        print(f"  FAIL  {label}  {detail}")


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def _wait_http(port: int, timeout: float = 12.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{port}/", timeout=1) as r:
                if r.status == 200:
                    return True
        except Exception:  # noqa: BLE001
            time.sleep(0.3)
    return False


# ── A. 内容生成 ────────────────────────────────────────────

def check_command() -> None:
    print("A1. 命令拼装")
    with_cfg = relay.Config(ws_url="wss://x/ws", token="t", path="/tmp/config.toml")
    cmd = relay._service_command(with_cfg)
    ck(cmd[0] == sys.executable, "命令用当前解释器", cmd[0])
    ck(cmd[1].endswith("relay.py"), "命令指向 relay.py", cmd[1])
    ck(cmd[2:] == ["-c", "/tmp/config.toml"], "带上了配置文件路径", str(cmd[2:]))

    no_cfg = relay.Config(ws_url="wss://x/ws", token="t", path=None)
    ck(relay._service_command(no_cfg)[2:] == [], "没有配置文件就不带 -c",
       str(relay._service_command(no_cfg)))


def check_macos_text() -> None:
    print("A2. launchd plist")
    log = "/Users/dev/Library/Logs/github-local-action.log"
    cmd = ["/usr/bin/python3", "/path/relay & <x>.py", "-c", "/tmp/c.toml"]
    text = relay._service_plist("com.example.test", cmd, "/work dir", log)
    try:
        data = plistlib.loads(text.encode("utf-8"))
    except Exception as exc:  # noqa: BLE001
        ck(False, "plist 能被 plistlib 解析", f"{type(exc).__name__}: {exc}")
        return
    ck(True, "plist 能被 plistlib 解析")
    ck(data.get("Label") == "com.example.test", "Label 对", str(data.get("Label")))
    ck(data.get("ProgramArguments") == cmd, "ProgramArguments 逐项对（含转义）",
       str(data.get("ProgramArguments")))
    ck(data.get("RunAtLoad") is True, "登录/加载即启动")
    ck(data.get("KeepAlive") is True, "掉了会被拉起来")
    ck(data.get("WorkingDirectory") == "/work dir", "工作目录对", str(data.get("WorkingDirectory")))
    ck(data.get("StandardOutPath") == log and data.get("StandardErrorPath") == log,
       "日志输出到一个文件", str(data.get("StandardOutPath")))
    ck("PATH" in (data.get("EnvironmentVariables") or {}), "显式给了 PATH（launchd 没有登录 shell 的 PATH）")


def check_linux_text() -> None:
    print("A3. systemd unit")
    log = "/home/a b/.local/state/github-local-action.log"
    cmd = ["/usr/bin/python3", "/path/with space/relay.py", "-c", "/tmp/c.toml"]
    text = relay._service_unit(cmd, "/work dir", log)
    lines = text.splitlines()
    ck("[Unit]" in lines and "[Service]" in lines and "[Install]" in lines,
       "三段齐全", " / ".join(lines[:3]))
    ck("WantedBy=default.target" in lines, "挂在 default.target 上（用户会话）")
    ck("Restart=on-failure" in lines, "失败才重启，正常退出不拉起来")
    ck(f"StandardOutput=append:{log}" in lines and f"StandardError=append:{log}" in lines,
       "日志追加到同一个文件")
    exec_line = next((x for x in lines if x.startswith("ExecStart=")), "")
    ck(exec_line == "ExecStart=" + " ".join(shlex.quote(x) for x in cmd),
       "ExecStart 引号正确（带空格的路径）", exec_line)
    ck("PATH=" in text, "显式给了 PATH")
    ck("WorkingDirectory=/work dir" in lines or "WorkingDirectory='/work dir'" in lines
       or 'WorkingDirectory="/work dir"' in lines or "WorkingDirectory=/work dir" in lines,
       "工作目录写进去了", str([x for x in lines if "WorkingDirectory" in x]))


def check_status_shape() -> None:
    print("A4. 状态接口")
    st = asyncio.run(relay._service_status())
    for key in ("platform", "supported", "installed", "running", "managed", "file", "log", "hint"):
        ck(key in st, f"状态里有 {key}", str(sorted(st)))
    ck(st["platform"] in ("macos", "linux"), "认出了平台", str(st["platform"]))
    ck(st["file"].endswith(".plist") if st["platform"] == "macos" else st["file"].endswith(".service"),
       "服务文件放在该放的地方", st["file"])


# ── B. 真实起停 ────────────────────────────────────────────

def serve_script(port: int) -> list[str]:
    code = ("import http.server,socketserver;"
            "socketserver.TCPServer.allow_reuse_address=True;"
            f"http.server.HTTPServer(('127.0.0.1',{port}),http.server.SimpleHTTPRequestHandler)"
            ".serve_forever()")
    return [sys.executable, "-c", code]


def check_macos_lifecycle() -> None:
    print("B1. 真的用 launchd 起一个临时服务")
    if not Path("/bin/launchctl").exists():
        ck(False, "本机有 launchctl")
        return
    label = "local.servicecheck"
    port = _free_port()
    tmp = Path(tempfile.mkdtemp(prefix="svc-check-"))
    plist = tmp / f"{label}.plist"
    log = tmp / "out.log"
    text = relay._service_plist(label, serve_script(port), str(tmp), str(log))
    plist.write_text(text, encoding="utf-8")

    domain = f"gui/{os.getuid()}"
    try:
        subprocess.run(["launchctl", "bootout", f"{domain}/{label}"],
                       capture_output=True, timeout=15)
        r = subprocess.run(["launchctl", "bootstrap", domain, str(plist)],
                           capture_output=True, text=True, timeout=20)
        ck(r.returncode == 0, "bootstrap 成功", (r.stderr or r.stdout).strip()[:200])
        up = _wait_http(port)
        ck(up, f"服务真的在 :{port} 上应答了（{text.count('RunAtLoad') and 'RunAtLoad' }）",
           str(log.read_text()[:200] if log.exists() else "没日志"))
        r = subprocess.run(["launchctl", "print", f"{domain}/{label}"],
                           capture_output=True, text=True, timeout=15)
        ck(r.returncode == 0 and "state = running" in r.stdout,
           "launchctl 认为它在跑", r.stdout[:120])
    finally:
        subprocess.run(["launchctl", "bootout", f"{domain}/{label}"],
                       capture_output=True, timeout=15)
        time.sleep(1.5)
        down = not _wait_http(port, timeout=3)
        ck(down, "bootout 之后端口不再应答")
        import shutil
        shutil.rmtree(tmp, ignore_errors=True)


def check_linux_lifecycle() -> None:
    print("B1'. 真的用 systemd --user 起一个临时服务")
    label = "servicecheck"
    port = _free_port()
    unit_dir = Path.home() / ".config/systemd/user"
    unit_dir.mkdir(parents=True, exist_ok=True)
    unit = unit_dir / f"{label}.service"
    tmp = Path(tempfile.mkdtemp(prefix="svc-check-"))
    log = tmp / "out.log"
    unit.write_text(relay._service_unit(serve_script(port), str(tmp), str(log)), encoding="utf-8")
    try:
        r = subprocess.run(["systemctl", "--user", "daemon-reload"],
                           capture_output=True, text=True, timeout=20)
        ck(r.returncode == 0, "daemon-reload 成功", (r.stderr or "").strip()[:200])
        r = subprocess.run(["systemctl", "--user", "enable", "--now", f"{label}.service"],
                           capture_output=True, text=True, timeout=30)
        ck(r.returncode == 0, "enable --now 成功", (r.stderr or "").strip()[:200])
        ck(_wait_http(port), f"服务真的在 :{port} 上应答了",
           str(log.read_text()[:200] if log.exists() else "没日志"))
        r = subprocess.run(["systemctl", "--user", "is-active", f"{label}.service"],
                           capture_output=True, text=True, timeout=15)
        ck(r.stdout.strip() == "active", "systemd 认为它在跑", r.stdout.strip())
    finally:
        subprocess.run(["systemctl", "--user", "disable", "--now", f"{label}.service"],
                       capture_output=True, timeout=30)
        time.sleep(1.0)
        ck(not _wait_http(port, timeout=3), "停掉之后端口不再应答")
        unit.unlink(missing_ok=True)
        subprocess.run(["systemctl", "--user", "daemon-reload"], capture_output=True, timeout=20)
        import shutil
        shutil.rmtree(tmp, ignore_errors=True)


def main() -> int:
    print(f"平台：{relay._service_platform()}   python：{sys.executable}")
    check_command()
    check_macos_text()
    check_linux_text()
    check_status_shape()
    if relay._service_platform() == "macos":
        check_macos_lifecycle()
    elif relay._service_platform() == "linux":
        check_linux_lifecycle()
    else:
        print("B. 本机平台既不是 macOS 也不是 Linux，跳过真实起停")

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

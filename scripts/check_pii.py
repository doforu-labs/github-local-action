#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""扫全仓库（只扫 git 跟踪的文件），找出不该公开的东西。

为什么要有这个脚本：这个项目从私人实验室仓库长出来，README 和测试里天然会带
真实机器地址、账号邮箱、内网 IP。靠人肉 review 迟早漏一个，所以把「什么算泄漏」
写成断言，跟单测一起跑。

退出码 0 = 干净；1 = 有命中（CI 会红）。
自查用：python3 scripts/check_pii.py --verbose
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# 允许出现的地址：文档里的占位示例、回环、广播、CIDR 的基地址
SAFE_IPS = {
    "0.0.0.0", "0.0.0.1", "127.0.0.1",
    "10.0.0.5", "10.0.0.9", "10.0.0.21", "10.0.0.22",
    "192.168.0.0", "172.16.0.0",
}
# 允许出现的邮箱域名：示例域 + GitHub 的 noreply
SAFE_EMAIL_DOMAINS = {"example.com", "example.org", "users.noreply.github.com", "example.net"}
# 明显是假值的 32 位十六进制（账号 id 的占位符）
FAKE_HEX = {"0123456789abcdef0123456789abcdef"}
# 允许出现的 workers.dev 账号子域（Cloudflare 账号子域是账号级标识，能反查到人）
SAFE_WORKER_SUBDOMAINS = {"x", "demo-sub", "your-subdomain"}
# /Users/<name> 里允许的用户名（示例名，不代表真人）
SAFE_HOME_USERS = {"dev", "you", "tester", "user", "runner", "me"}

PRIVATE_IP = re.compile(
    r"\b(?:"
    r"10\.\d{1,3}\.\d{1,3}\.\d{1,3}"
    r"|192\.168\.\d{1,3}\.\d{1,3}"
    r"|172\.(?:1[6-9]|2\d|3[01])\.\d{1,3}\.\d{1,3}"
    r")\b"
)
EMAIL = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
HOME_DIR = re.compile(r"/Users/([A-Za-z0-9._-]+)")
UUID = re.compile(r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b")
# 完整 URL 里的 workers.dev / pages.dev 账号子域：<worker>.<账号子域>.workers.dev
WORKER_URL = re.compile(
    r"(?:https?|wss?)://[a-z0-9.-]*?([a-z0-9][a-z0-9-]*)\.(?:workers|pages)\.dev", re.I
)

HARD_RULES: list[tuple[str, re.Pattern[str], str]] = [
    ("GitHub token", re.compile(r"gh[pousr]_[A-Za-z0-9]{20,}"), "把令牌删掉，改用 secret put"),
    ("GitHub PAT", re.compile(r"github_pat_[A-Za-z0-9_]{20,}"), "把令牌删掉"),
    ("AWS key", re.compile(r"AKIA[0-9A-Z]{16}"), "把密钥删掉"),
    ("私钥", re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----"), "私钥不能进仓库"),
    ("Slack token", re.compile(r"xox[baprs]-[A-Za-z0-9-]{10,}"), "把令牌删掉"),
]

# 那些「自己环境里的专有名词」（真机主机名、私有仓库名、品牌词）不能写在仓库里：
# 词本身就是不该公开的东西，写进规则等于把它们又公开一遍。改成读本地的
# `scripts/pii-words.local`（已 gitignore），一行一个词，大小写不敏感。
# 没有这个文件时只跑上面的通用规则——公开仓库里别人 clone 下来也能跑。
LOCAL_WORDS = Path(__file__).with_name("pii-words.local")


def local_rules() -> list[tuple[str, re.Pattern[str], str]]:
    try:
        raw = LOCAL_WORDS.read_text(encoding="utf-8").splitlines()
    except OSError:
        return []
    words = [w.strip() for w in raw if w.strip() and not w.lstrip().startswith("#")]
    if not words:
        return []
    return [("本机专有名词", re.compile("|".join(re.escape(w) for w in words), re.I),
             "这些是你自己环境里的名字（真机名/私有仓库/品牌），不该进仓库")]


_RULES: list[tuple[str, re.Pattern[str], str]] | None = None


def rules() -> list[tuple[str, re.Pattern[str], str]]:
    global _RULES
    if _RULES is None:
        _RULES = HARD_RULES + local_rules()
    return _RULES


def tracked_files() -> list[Path]:
    out = subprocess.run(
        ["git", "ls-files", "-z"], cwd=ROOT, capture_output=True, check=True
    ).stdout
    return [ROOT / p for p in out.decode("utf-8").split("\0") if p]


def scan(path: Path) -> list[tuple[int, str, str, str]]:
    """返回 [(行号, 规则名, 命中文本, 建议)]。"""
    if path.resolve() == Path(__file__).resolve():
        return []          # 这个文件里的正则是在「讲」敏感词，扫自己只会报自己
    try:
        text = path.read_text(encoding="utf-8")
    except (UnicodeDecodeError, OSError):
        return []          # 二进制文件不管
    hits: list[tuple[int, str, str, str]] = []
    for lineno, line in enumerate(text.splitlines(), 1):
        for name, pattern, hint in rules():
            m = pattern.search(line)
            if m:
                hits.append((lineno, name, m.group(0), hint))
        for m in PRIVATE_IP.finditer(line):
            ip = m.group(0)
            # 10.0.0.0/8 这种网段写法是配置默认值，不是某台机器
            if ip in SAFE_IPS or line[m.end():m.end() + 1] == "/":
                continue
            hits.append((lineno, "内网 IP", ip, "换成 10.0.0.5 这类文档占位地址"))
        for m in EMAIL.finditer(line):
            found = m.group(0)
            domain = found.rsplit("@", 1)[1].lower()
            # git@github.com:owner/repo.git 是 SSH 写法，不是邮箱
            if domain in SAFE_EMAIL_DOMAINS or found.startswith("git@"):
                continue
            hits.append((lineno, "邮箱", found, "换成 demo@example.com"))
        for m in HOME_DIR.finditer(line):
            if m.group(1) in SAFE_HOME_USERS:
                continue
            hits.append((lineno, "家目录用户名", m.group(0), "换成 /Users/dev"))
        for m in UUID.finditer(line):
            # 只关心写进配置的 database_id：占位符必须是 REPLACE_WITH_*
            if "database_id" in line and "REPLACE_WITH" not in line:
                hits.append((lineno, "真实资源 id", m.group(0), "换成 REPLACE_WITH_YOUR_D1_DATABASE_ID"))
        for m in re.finditer(r"\b[0-9a-f]{32}\b", line):
            if m.group(0) not in FAKE_HEX:
                hits.append((lineno, "Cloudflare 账号 id", m.group(0), "换成 0123456789abcdef0123456789abcdef"))
        for m in WORKER_URL.finditer(line):
            if m.group(1).lower() not in SAFE_WORKER_SUBDOMAINS:
                hits.append((lineno, "Workers 账号子域", m.group(1),
                             "换成 demo-sub，别留自己账号的子域"))
    return hits


def main() -> int:
    verbose = "--verbose" in sys.argv or "-v" in sys.argv
    total = 0
    for path in tracked_files():
        rel = path.relative_to(ROOT)
        hits = scan(path)
        if not hits:
            continue
        total += len(hits)
        print(f"\n{rel}")
        for lineno, name, text, hint in hits:
            print(f"  {rel}:{lineno}  [{name}] {text}  →  {hint}")
    if total:
        print(f"\nFAIL：{total} 处不该公开的内容，逐条改掉再提交。")
        return 1
    if verbose:
        print("OK：仓库里没有邮箱 / 内网 IP / 令牌 / 真实资源 id。")
        if not LOCAL_WORDS.exists():
            print(f"提示：没有 {LOCAL_WORDS.name}，只跑了通用规则——"
                  "你自己机器名/私有仓库/品牌词这类专有名词不会被扫出来")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

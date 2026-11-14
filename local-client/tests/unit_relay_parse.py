#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""中继自检里那些「从 wrangler 输出抠东西」的纯函数。

这些都是对着真实输出写的（wrangler 4.156.2），改 wrangler 版本后输出格式一变
就会红——那时候登录态、权限、地址都会解析错，而错法很安静：页面上只是显示
「未登录」或地址空着。所以把它们钉住。

    .venv/bin/python tests/unit_relay_parse.py
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import relay  # noqa: E402

PASS = 0
FAIL = 0


def check(ok: bool, label: str, detail: str = "") -> None:
    global PASS, FAIL
    if ok:
        PASS += 1
        print(f"  PASS  {label}")
    else:
        FAIL += 1
        print(f"  FAIL  {label}  {detail}")


# 真实 `wrangler whoami` 输出（已去 ANSI）
WHOAMI = """
 ⛅️ wrangler 4.156.2
────────────────────
Getting User settings...
👋 You are logged in with an OAuth Token, associated with the email demo@example.com.
🔐 Credentials are stored in: /Users/you/.wrangler/config/default.toml
┌──────────────────────────────┬──────────────────────────────────┐
│ Account Name                 │ Account ID                       │
├──────────────────────────────┼──────────────────────────────────┤
│ Demo@example.com's Account    │ 0123456789abcdef0123456789abcdef │
└──────────────────────────────┴──────────────────────────────────┘
🔓 Token Permissions:
Scope (Access)
- user (read)
- offline_access 
- account (read)
- workers (write)
- workers_kv (write)
- workers_routes (write)
- workers_scripts (write)
- workers_tail (read)
- d1 (write)

▲ [WARNING] Wrangler is missing some expected Oauth scopes. To fix this, run `wrangler login` to refresh your token. The missing scopes are:

  - k2.read
  - challenge-widgets.write
"""

# 没登录时 wrangler 的样子
WHOAMI_ANON = """
 ⛅️ wrangler 4.156.2
────────────────────
✘ [ERROR] You are not authenticated. Please run `wrangler login`.
"""

# 只有读权限的令牌（部署会 403）
WHOAMI_READONLY = """
 ⛅️ wrangler 4.156.2
────────────────────
👋 You are logged in with an OAuth Token, associated with the email a@example.com.
│ Someone │ 0123456789abcdef0123456789abcdef │
🔓 Token Permissions:
Scope (Access)
- workers (read)
- account (read)
"""

DEPLOY_OK = """
 ⛅️ wrangler 4.156.2
────────────────────
Total Upload: 9.57 KiB / gzip: 3.37 KiB
Your Worker has access to the following bindings:
Binding                 Resource            
env.HUB (Hub)           Durable Object     
env.DB (gh_relay)       D1 Database         
Uploaded github-local-action (4.01 sec)
Deployed github-local-action triggers (0.99 sec)
  https://github-local-action.demo-sub.workers.dev
Current Version ID: 2664f76d-c1ca-4e51-abde-f39f2cf897ad
"""

DEPLOY_FAIL = """
 ⛅️ wrangler 4.156.2
────────────────────
✘ [ERROR] A request to the Cloudflare API (/accounts/xxx/workers/scripts/y) failed.
  Authentication error [code: 10000]
"""

DEPLOYMENTS = """
Showing last 3 deployments for github-local-action
Created: 2026-11-11T16:55:37.913Z
Author: demo@example.com
Source: Secret Change
Version(s):  (100%) aaaa
                 Created:  2026-11-10T09:00:00.000Z
Created: 2026-11-11T16:55:44.396Z
Author: demo@example.com
Version(s):  (100%) bbbb
Created: 2026-11-12T03:10:00.000Z
Author: demo@example.com
Version(s):  (100%) cccc
"""


def test_whoami_logged_in() -> None:
    print("wrangler whoami：已登录")
    info = relay._parse_whoami(WHOAMI)
    check(info["logged_in"] is True, "认出已登录")
    check(info["email"] == "demo@example.com", "抠出邮箱", info["email"])
    check(info["account_id"] == "0123456789abcdef0123456789abcdef", "抠出账号 id", info["account_id"])
    check(info["account_name"] == "Demo@example.com's Account", "抠出账号名", info["account_name"])
    check(info["can_deploy"] is True, "workers(write) 判定为可部署")
    names = {s["name"] for s in info["scopes"]}
    check({"workers", "workers_scripts", "d1"} <= names, "scope 全抓到了", str(sorted(names))[:120])
    check(info["missing_scopes"] == ["k2.read", "challenge-widgets.write"],
          "无关的缺 scope 也单独列出来", str(info["missing_scopes"]))
    check(relay._parse_wrangler_version(WHOAMI) == "4.156.2", "抠出 wrangler 版本")


def test_whoami_anon() -> None:
    print("wrangler whoami：未登录")
    info = relay._parse_whoami(WHOAMI_ANON)
    check(info["logged_in"] is False, "判定未登录")
    check(info["can_deploy"] is False, "未登录时不可部署")
    check(info["email"] == "", "没有假的邮箱", info["email"])


def test_whoami_readonly() -> None:
    print("wrangler whoami：只有读权限")
    info = relay._parse_whoami(WHOAMI_READONLY)
    check(info["logged_in"] is True, "已登录")
    check(info["can_deploy"] is False, "workers(read) 不算可部署")
    check(info["account_id"] == "0123456789abcdef0123456789abcdef", "抠出账号 id", info["account_id"])


def test_deploy_url() -> None:
    print("wrangler deploy：地址解析")
    m = relay._WORKER_URL_RE.search(DEPLOY_OK)
    check(bool(m), "从输出里找到 workers.dev 地址")
    url = "https://" + m.group(1) if m else ""
    check(url == "https://github-local-action.demo-sub.workers.dev", "地址正确", url)
    check(relay._deploy_url_to_ws(url) == "wss://github-local-action.demo-sub.workers.dev/ws",
          "https 转 wss 并补 /ws", relay._deploy_url_to_ws(url))
    check(relay._deploy_url_to_ws("https://x.workers.dev/ws") == "wss://x.workers.dev/ws",
          "已经是 /ws 的不会变成 /ws/ws")
    check(relay._deploy_url_to_ws("https://x.workers.dev/") == "wss://x.workers.dev/ws",
          "末尾斜杠不会拼出双斜杠")
    check(relay._deploy_url_to_ws("") == "", "空地址还是空")

    m2 = relay._WORKER_URL_RE.search(DEPLOY_FAIL)
    check(m2 is None, "失败输出里不会误抓地址")


def test_last_error() -> None:
    print("wrangler 报错：挑一句话")
    err = relay._relay_last_error(DEPLOY_FAIL)
    check(err.startswith("A request to the Cloudflare API"), "挑 ERROR 那行", err)
    check("✘" not in err, "去掉 ✘ 符号", err)
    check(relay._relay_last_error("") == "", "空输入不报错")
    check(relay._relay_last_error("只装了旧版\n升级一下\n") == "升级一下", "没 ERROR 时取最后一行")


def test_deployments() -> None:
    print("wrangler deployments list：部署次数与最近时间")
    import re
    count = len(re.findall(r"^Version\(s\):", DEPLOYMENTS, re.M))
    check(count == 3, "数出 3 次部署", str(count))
    stamps = re.findall(r"^Created:\s+(\S+)", DEPLOYMENTS, re.M)
    check(max(stamps) == "2026-11-12T03:10:00.000Z", "取最大值当最近一次", str(max(stamps)))


def test_worker_name() -> None:
    print("worker 名：从 wrangler.toml 读")
    name = relay._worker_name()
    check(name == "github-local-action", "读到 worker 名", name)
    check(relay._relay_dir().name == "remote-worker", "找到 worker 工程目录",
          str(relay._relay_dir()))


def test_http_error() -> None:
    print("HTTP 错误：变成一句人话")
    html = ('<!DOCTYPE html>\n<!--[if lt IE 7]> <html class="no-js ie6 oldie"> <![endif]-->\n'
            '<body><h2>Error 1042</h2><div>error code: 1042</div></body>')
    msg = relay._relay_http_error(404, "text/html; charset=UTF-8", html)
    check(msg == "HTTP 404（Cloudflare error code 1042）", "HTML 错误页只给状态码和 error code", msg)
    check("<" not in msg, "不再把 HTML 原样贴出来", msg)

    plain = relay._relay_http_error(404, "text/plain", "error code: 1042")
    check(plain == "HTTP 404 error code: 1042", "纯文本错误照样显示", plain)

    jsonish = relay._relay_http_error(500, "application/json", '{"error":"boom"}')
    check(jsonish == 'HTTP 500 {"error":"boom"}', "JSON 错误不丢内容", jsonish)

    nohint = relay._relay_http_error(503, "text/html", "<html><body>oops</body></html>")
    check(nohint == "HTTP 503（返回了一个网页，不是接口）", "HTML 里没 error code 时也给个说法", nohint)


def test_domain_helpers() -> None:
    print("自定义域名：域名校验与 zone 匹配")
    check(relay._valid_host("relay.example.com"), "普通子域合法")
    check(relay._valid_host("Relay.Example.COM") is True, "大小写不敏感")
    check(not relay._valid_host("example.com/path"), "带路径的不合法")
    check(not relay._valid_host("no-dot"), "没点不合法")
    check(not relay._valid_host(""), "空不合法")
    check(not relay._valid_host("-bad.example.com"), "开头横线不合法")

    zones = [{"name": "example.com", "id": "z1"},
             {"name": "api.example.com", "id": "z2"},
             {"name": "other.net", "id": "z3"}]
    check(relay._zone_for("relay.example.com", zones)["id"] == "z1", "子域归到父 zone")
    check(relay._zone_for("x.api.example.com", zones)["id"] == "z2", "有重叠时取最长匹配")
    check(relay._zone_for("example.com", zones)["id"] == "z1", "根域名自己也能匹配")
    check(relay._zone_for("notexample.com", zones) is None, "不同 zone 不误匹配")
    check(relay._zone_for("a.unknown.org", zones) is None, "不在列表里就返回 None")

    check(relay._workers_dev_url("github-local-action", "demo-sub")
          == "wss://github-local-action.demo-sub.workers.dev/ws", "拼出默认 ws 地址")
    check(relay._workers_dev_url("", "demo-sub") == "", "没 worker 名就不拼")


def main() -> int:
    print("中继自检的解析函数")
    test_whoami_logged_in()
    test_whoami_anon()
    test_whoami_readonly()
    test_deploy_url()
    test_last_error()
    test_deployments()
    test_worker_name()
    test_http_error()
    test_domain_helpers()
    print(f"\n结果：{PASS} 项通过，{FAIL} 项失败")
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())

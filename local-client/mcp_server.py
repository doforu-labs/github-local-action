#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""mcp_server.py —— 把本地 relay 的 HTTP 接口包成 MCP 工具（stdio）。

为什么是「薄壳」：relay 本来就是一个本地 HTTP + JSON 服务，agent 缺的不是
能力，是「有哪些接口、参数怎么填」。所以这里不重写任何逻辑，只做三件事：

  1. 说 MCP 协议（JSON-RPC 2.0 over stdio，一行一条消息）
  2. 把 7 个接口翻译成 tool，参数用 JSON Schema 描述清楚
  3. 顺手把一次运行的 job / 步骤 / 日志合成一个 tool 的结果——
     拆成三个 tool 会让 agent 多跑两轮

用法（给 MCP 客户端配置的就是这一行）：

    python3 local-client/mcp_server.py

    # 想让服务指到别的地址
    LOCAL_ACTION_URL=http://10.0.0.5:18091 python3 local-client/mcp_server.py

只依赖标准库。stdout 只能出协议消息，日志一律走 stderr。
"""

from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Callable

BASE = os.environ.get("LOCAL_ACTION_URL", "http://127.0.0.1:18091").rstrip("/")
TIMEOUT = float(os.environ.get("LOCAL_ACTION_TIMEOUT", "25"))

# 支持的协议版本，从新到旧。客户端报什么就回什么（只要认识），
# 不认识就回最新的，由客户端决定要不要继续。
PROTOCOLS = ("2025-06-18", "2025-03-26", "2024-11-05")
DEFAULT_PROTOCOL = PROTOCOLS[0]


def log(msg: str) -> None:
    print(f"[mcp] {msg}", file=sys.stderr, flush=True)


class ToolError(Exception):
    """工具本身跑失败了（网络、HTTP 4xx/5xx、参数不合法）。"""


def http_json(path: str, params: dict[str, Any] | None = None) -> Any:
    url = BASE + path
    if params:
        clean = {k: v for k, v in params.items() if v is not None and v != ""}
        if clean:
            url += "?" + urllib.parse.urlencode(clean, doseq=True)
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
            body = resp.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", "replace")[:400]
        raise ToolError(f"HTTP {exc.code} {_safe_url(url)}\n{detail}") from None
    except urllib.error.URLError as exc:
        raise ToolError(
            f"连不上 {_safe_url(BASE)}（{exc.reason}）。relay 起来了吗？端口对不对？"
        ) from None
    except TimeoutError:
        raise ToolError(f"{_safe_url(url)} 超过 {TIMEOUT:.0f}s 没响应") from None
    try:
        return json.loads(body or "null")
    except ValueError:
        raise ToolError(f"{_safe_url(url)} 返回的不是 JSON：{body[:200]}") from None


def _require(args: dict[str, Any], key: str) -> str:
    value = args.get(key)
    if value is None or str(value).strip() == "":
        raise ToolError(f"缺少参数 {key}")
    return str(value).strip()


def _as_int(args: dict[str, Any], key: str, default: int,
            minimum: int | None = None, maximum: int | None = None) -> int:
    """模型给的参数什么形状都有，这里统一收成能用的整数。

    字符串 "abc" 直接报人话，不要把 ValueError 的栈文扃回给模型。
    """
    raw = args.get(key)
    if raw is None or raw == "":
        value = default
    elif isinstance(raw, bool):
        value = int(raw)
    else:
        try:
            value = int(float(raw)) if isinstance(raw, str) and "." in raw else int(raw)
        except (TypeError, ValueError):
            raise ToolError(f"{key} 要是个整数，收到的是 {raw!r}") from None
    if minimum is not None and value < minimum:
        value = minimum
    if maximum is not None and value > maximum:
        value = maximum
    return value


def _as_bool(value: Any) -> bool:
    """模型经常把布尔写成字符串（"false" / "0"），按字面意思当 true 就错了。"""
    if isinstance(value, str):
        return value.strip().lower() not in ("", "0", "false", "no", "off", "null")
    return bool(value)


def _check_repo(repo: str) -> str:
    """只填一个词（缺 owner）时上游会当成「没这个仓库」返回空集，
    模型会误以为“真没跑过”。这里提前抦下来。"""
    owner, _, name = repo.partition("/")
    if not owner or not name:
        raise ToolError(f"repo 要写成 owner/name，你现在只给了 {repo!r}")
    if "/" in name or any(ch.isspace() for ch in repo):
        raise ToolError(f"repo 格式不对：{repo!r}，应该是 owner/name")
    return repo


def _safe_url(url: str) -> str:
    """报错里回显地址时把 user:pass@ 抹掉——LOCAL_ACTION_URL 是手工配的，
    真有人塞个带凭据的地址进去，别让它在错误文本里漏出去。"""
    parts = urllib.parse.urlsplit(url)
    if parts.username or parts.password:
        host = parts.hostname or ""
        if parts.port:
            host = f"{host}:{parts.port}"
        parts = parts._replace(netloc=f"***@{host}")
    return urllib.parse.urlunsplit(parts)


def _clip(text: str, limit: int) -> tuple[str, bool]:
    if limit and len(text) > limit:
        return text[:limit], True
    return text, False


def _tail(text: str, lines: int) -> str:
    if not lines:
        return text
    rows = text.splitlines()
    if len(rows) <= lines:
        return text
    return "\n".join(rows[-lines:])


# ───────────────────────────── 工具实现 ─────────────────────────────


def tool_health(args: dict[str, Any]) -> Any:
    return http_json("/health")


def tool_stats(args: dict[str, Any]) -> Any:
    data = http_json("/stats")
    # 24 小时分桶对 agent 没什么用，摘掉省 token，需要时自己带 raw=true
    if not _as_bool(args.get("raw")):
        data.pop("hourly", None)
    return data


def tool_list_runs(args: dict[str, Any]) -> Any:
    limit = _as_int(args, "limit", 10, 1, 100)
    repo = args.get("repo")
    repo = _check_repo(str(repo).strip()) if repo else None
    data = http_json("/actions", {"repo": repo, "limit": limit})
    runs = data.get("runs") or []
    out: dict[str, Any] = {
        "scope": repo or "全部仓库",
        # total 是「当前过滤条件下」的条数。带了 repo 时补一个全库总数，
        # 不然 agent 看到 total=81 会以为系统里就只有这一个仓库。
        "total": data.get("total"),
        "repos": data.get("repos"),
        "runs": [
            {
                k: r.get(k)
                for k in ("id", "repo", "name", "run_number", "status", "conclusion",
                          "branch", "sha", "trigger", "actor", "started_at", "duration_s",
                          "title", "url")
            }
            for r in runs
        ],
    }
    if repo:
        try:
            out["total_all"] = http_json("/stats").get("runs_total")
        except ToolError:
            pass  # 拿不到就不给，不影响主结果
    return out


def tool_recent_events(args: dict[str, Any]) -> Any:
    limit = _as_int(args, "limit", 20, 1, 200)
    # after 以前直接透传：传错值时 relay 会静默当成 0，游标被悄悄重置，
    # agent 会以为「这已经是全部了」。这里必须当面报错。
    after = _as_int(args, "after", 0, 0)
    data = http_json("/events", {"after": after, "limit": limit})
    full = _as_bool(args.get("full"))
    events = data.get("events") or []
    shallow = []
    for ev in events:
        payload = ev.get("payload")
        shallow.append({
            "id": ev.get("id"),
            "event": ev.get("event"),
            "action": ev.get("action"),
            "repo": ev.get("repo"),
            "at": ev.get("at"),
            # 原始 payload 可能几十 KB，只给个大小和顶层键名，要全量就传 full=true
            "payload_keys": sorted(payload.keys())[:20] if isinstance(payload, dict) else None,
            **({"payload": payload} if full else {}),
        })
    return {"after": data.get("after"), "count": len(shallow), "events": shallow}


def tool_get_run_detail(args: dict[str, Any]) -> Any:
    repo = _check_repo(_require(args, "repo"))
    run_id = _require(args, "run")
    data = http_json("/github/jobs", {"repo": repo, "run": run_id})
    jobs = data.get("jobs") or []
    include_logs = _as_bool(args.get("include_logs"))
    tail_lines = _as_int(args, "log_tail_lines", 60, 0)
    out_jobs = []
    for job in jobs:
        steps = [
            {
                "number": s.get("number"),
                "name": s.get("name"),
                "status": s.get("status"),
                "conclusion": s.get("conclusion"),
            }
            for s in (job.get("steps") or [])
        ]
        item: dict[str, Any] = {
            "id": job.get("id"),
            "name": job.get("name"),
            "status": job.get("status"),
            "conclusion": job.get("conclusion"),
            "runner": job.get("runner_name"),
            "html_url": job.get("html_url"),
            "steps": steps,
        }
        if include_logs:
            if job.get("status") != "completed":
                item["log"] = None
                item["log_note"] = "job 还没跑完，GitHub 此时不生成日志"
            else:
                try:
                    log = http_json("/github/joblog", {"repo": repo, "job": job.get("id")})
                    text = log.get("text") or ""
                    item["log"] = _tail(text, tail_lines)
                    item["log_truncated"] = bool(log.get("truncated"))
                    item["log_tail_lines"] = tail_lines
                except ToolError as exc:
                    item["log"] = None
                    item["log_note"] = str(exc)[:300]
        out_jobs.append(item)
    return {
        "repo": repo,
        "run": run_id,
        "cached": data.get("cached", False),
        "jobs": out_jobs,
    }


def tool_get_job_log(args: dict[str, Any]) -> Any:
    repo = _check_repo(_require(args, "repo"))
    job_id = _require(args, "job")
    max_chars = _as_int(args, "max_chars", 20000, 100)
    data = http_json("/github/joblog", {"repo": repo, "job": job_id})
    text, cut = _clip(data.get("text") or "", max_chars)
    return {
        "job": job_id,
        "bytes": data.get("bytes"),
        "truncated": bool(data.get("truncated")) or cut,
        "text": text,
    }


def tool_list_machines(args: dict[str, Any]) -> Any:
    data = http_json("/runners")
    machines = [
        {
            "name": m.get("name"),
            "os": m.get("os"),
            "arch": m.get("arch"),
            "host": m.get("host"),
            "port": m.get("port"),
            "user": m.get("user"),
            "jit_dir": m.get("jit_dir"),
        }
        for m in (data.get("machines") or [])
    ]
    return {"count": len(machines), "machines": machines}


TOOLS: list[dict[str, Any]] = [
    {
        "name": "health",
        "description": "relay 是否在跑、跟云端那条 WebSocket 通不通、当前游标。"
                       "调不通任何接口时先调它。",
        "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False},
        "handler": tool_health,
    },
    {
        "name": "get_stats",
        "description": "今日/累计的事件条数与运行次数、账号与仓库概览。"
                       "回答「今天跑了多少次 CI」这类问题用它，不消耗 GitHub 配额。",
        "inputSchema": {
            "type": "object",
            "properties": {
                "raw": {
                    "type": "boolean",
                    "description": "带上最近 24 小时的分桶数据（默认去掉，省 token）",
                },
            },
            "additionalProperties": False,
        },
        "handler": tool_stats,
    },
    {
        "name": "list_runs",
        "description": "最近的工作流运行列表（来自本地库，不联网、不消耗 GitHub 配额）。"
                       "每条含 id / 名称 / 状态 / 结论 / 分支 / 短 sha / 用时 / 链接。"
                       "传了 repo 时，total 是「这个仓库」的条数，total_all 是全库总数。",
        "inputSchema": {
            "type": "object",
            "properties": {
                "repo": {"type": "string", "description": "只看某个仓库，格式 owner/name；不填就是全部仓库"},
                "limit": {"type": "integer", "description": "最多几条，默认 10，上限 100", "minimum": 1, "maximum": 100},
            },
            "additionalProperties": False,
        },
        "handler": tool_list_runs,
    },
    {
        "name": "get_run_detail",
        "description": "一次运行的 job 与每一步的状态；可选把每个 job 的日志尾部一起带回来。"
                       "这是「这次为什么失败」的主入口——先不带日志看哪一步红了，再决定要不要带日志。"
                       "结果来自 GitHub API，跑完的运行会命中本地缓存。",
        "inputSchema": {
            "type": "object",
            "properties": {
                "repo": {"type": "string", "description": "owner/name"},
                "run": {"type": "string", "description": "运行 id（数字），从 list_runs 拿"},
                "include_logs": {"type": "boolean", "description": "连日志一起取（默认 false；job 没跑完时 GitHub 还没有日志）"},
                "log_tail_lines": {"type": "integer", "description": "日志只取最后多少行，默认 60；0 表示全文", "minimum": 0},
            },
            "required": ["repo", "run"],
            "additionalProperties": False,
        },
        "handler": tool_get_run_detail,
    },
    {
        "name": "get_job_log",
        "description": "单个 job 的完整日志（纯文本，已去掉终端颜色码）。"
                       "job 还在跑时 GitHub 返回 404，这里会给明确说明。",
        "inputSchema": {
            "type": "object",
            "properties": {
                "repo": {"type": "string", "description": "owner/name"},
                "job": {"type": "string", "description": "job id（数字），从 get_run_detail 拿"},
                "max_chars": {"type": "integer", "description": "最多回多少字符，默认 20000", "minimum": 100},
            },
            "required": ["repo", "job"],
            "additionalProperties": False,
        },
        "handler": tool_get_job_log,
    },
    {
        "name": "recent_events",
        "description": "最近的 webhook 事件（幂等 id 递增，可当游标续传）。"
                       "只给事件类型、动作、仓库和时间，payload 只列顶层键名；要全量传 full=true。",
        "inputSchema": {
            "type": "object",
            "properties": {
                "after": {"type": "integer", "description": "只要 id 大于它的（续传用），默认 0"},
                "limit": {"type": "integer", "description": "最多几条，默认 20，上限 200", "minimum": 1, "maximum": 200},
                "full": {"type": "boolean", "description": "连原始 payload 一起回（可能很大）"},
            },
            "additionalProperties": False,
        },
        "handler": tool_recent_events,
    },
    {
        "name": "list_machines",
        "description": "本机纳管的机器（能装 runner 的那些）：名字、系统、架构、SSH 地址。不返回密码。",
        "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False},
        "handler": tool_list_machines,
    },
]

TOOL_MAP: dict[str, dict[str, Any]] = {t["name"]: t for t in TOOLS}


def tool_specs() -> list[dict[str, Any]]:
    return [{k: t[k] for k in ("name", "description", "inputSchema")}
            | {"annotations": {"readOnlyHint": True, "openWorldHint": True}}
            for t in TOOLS]


# ───────────────────────────── 协议层 ─────────────────────────────


def call_tool(name: str, args: dict[str, Any]) -> dict[str, Any]:
    tool = TOOL_MAP.get(name)
    if tool is None:
        return {
            "content": [{"type": "text", "text": f"没有这个工具：{name}。可用的有："
                                                 + "、".join(TOOL_MAP)}],
            "isError": True,
        }
    try:
        data = tool["handler"](args or {})
    except ToolError as exc:
        return {"content": [{"type": "text", "text": str(exc)}], "isError": True}
    except Exception as exc:  # noqa: BLE001 - 任何意外都别把进程搞崩
        return {"content": [{"type": "text", "text": f"{type(exc).__name__}: {exc}"}],
                "isError": True}
    text = json.dumps(data, ensure_ascii=False, indent=2)
    return {"content": [{"type": "text", "text": text}], "isError": False}


def handle(msg: dict[str, Any]) -> dict[str, Any] | None:
    method = msg.get("method")
    msg_id = msg.get("id")
    params = msg.get("params") or {}

    if msg_id is None:
        # 通知（notifications/initialized 之类）：不需要回
        return None

    def ok(result: Any) -> dict[str, Any]:
        return {"jsonrpc": "2.0", "id": msg_id, "result": result}

    def err(code: int, message: str) -> dict[str, Any]:
        return {"jsonrpc": "2.0", "id": msg_id, "error": {"code": code, "message": message}}

    if method == "initialize":
        asked = params.get("protocolVersion")
        version = asked if asked in PROTOCOLS else DEFAULT_PROTOCOL
        return ok({
            "protocolVersion": version,
            "capabilities": {"tools": {"listChanged": False}},
            "serverInfo": {"name": "github-local-action", "version": "1.0.0"},
            "instructions": (
                "这是 GitHub Local Action 的本地服务（默认 " + BASE + "）。"
                "查运行：list_runs → get_run_detail（可带日志）。"
                "事件与统计：recent_events / get_stats。机器：list_machines。"
                "所有工具都是只读的；改配置、装/卸 runner 请让用户在网页上操作。"
            ),
        })
    if method == "ping":
        return ok({})
    if method == "tools/list":
        return ok({"tools": tool_specs()})
    if method == "tools/call":
        name = params.get("name") or ""
        return ok(call_tool(name, params.get("arguments") or {}))
    if method in ("resources/list", "prompts/list"):
        return ok({"resources": []} if method.startswith("resources") else {"prompts": []})
    return err(-32601, f"不认识的方法：{method}")


def main() -> int:
    # Windows 上 stdout 默认是 cp936 / cp1252，中文日志和技术消息会直接抛
    # UnicodeEncodeError，客户端只看到连接断开。钉成 UTF-8。
    for stream in (sys.stdin, sys.stdout):
        try:
            stream.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
        except (AttributeError, ValueError):
            pass
    log(f"启动，转发到 {BASE}")
    for raw in sys.stdin:
        line = raw.strip()
        if not line:
            continue
        try:
            msg = json.loads(line)
        except ValueError:
            out = {"jsonrpc": "2.0", "id": None,
                   "error": {"code": -32700, "message": "不是合法 JSON"}}
        else:
            if isinstance(msg, list):  # 批量请求
                replies = [r for r in (handle(m) for m in msg if isinstance(m, dict)) if r]
                out = replies or None
            elif isinstance(msg, dict):
                out = handle(msg)
            else:
                out = {"jsonrpc": "2.0", "id": None,
                       "error": {"code": -32600, "message": "请求必须是对象"}}
        if out is None:
            continue
        sys.stdout.write(json.dumps(out, ensure_ascii=False) + "\n")
        sys.stdout.flush()
    log("stdin 关了，退出")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit(0)

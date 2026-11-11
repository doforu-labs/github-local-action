#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
relay.py —— 本地 WS 接收端

干三件事：
  1. 主动往外连远端的 WebSocket（出站，NAT 不拦）
  2. 收到的事件落本地 SQLite，按 id 去重（断线重连不会重复）
  3. 在局域网开一个 SSE 端点，把事件推给本机/同网段的设备

为什么要本地再转一手：远端是公网，局域网设备不该直接连它；
而且本地有库，断网期间设备重连还能从库里补。

端口默认 18091，避开 the desktop app 的 3000 / 17890~17892。
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import logging
import os
import re
import shlex
import signal
import sqlite3
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional, Set

import aiohttp
from aiohttp import web

try:  # Python 3.11+
    import tomllib
except ModuleNotFoundError:  # pragma: no cover
    tomllib = None  # type: ignore

log = logging.getLogger("relay")

# 状态页：按 mtime 缓存，改完 HTML 刷新浏览器即可生效，不用重启进程
WEB_DIR = Path(__file__).resolve().parent / "web"
INDEX_FILE = WEB_DIR / "index.html"
_index_cache: tuple[float, str] = (-1.0, "")


def read_index() -> str:
    """返回状态页 HTML；文件没变就复用缓存。"""
    global _index_cache
    try:
        mtime = INDEX_FILE.stat().st_mtime
    except OSError:
        return ""
    cached_mtime, cached_html = _index_cache
    if mtime != cached_mtime:
        _index_cache = (mtime, INDEX_FILE.read_text(encoding="utf-8"))
    return _index_cache[1]

# ─────────────────────────────────────────────────────────────
# 配置
# ─────────────────────────────────────────────────────────────


@dataclass
class Config:
    ws_url: str
    token: str
    http_host: str = "0.0.0.0"
    http_port: int = 18091
    db_path: str = "events.db"
    heartbeat: int = 30       # 协议层 ping：防 NAT / 代理回收空闲连接
    app_ping: int = 120       # 应用层 ping：顺带确认对端活着
    reconnect_max: int = 60   # 重连退避上限（秒）
    path: Optional[Path] = None   # 配置文件位置，设置页保存时写回这里

    @classmethod
    def load(cls, path: Optional[Path]) -> "Config":
        raw: dict[str, Any] = {}
        if path and path.exists():
            if tomllib is None:
                raise SystemExit("需要 Python 3.11+ 才能读 TOML，或者改用环境变量")
            raw = tomllib.loads(path.read_text(encoding="utf-8")).get("relay", {})
            log.info("配置来自 %s", path)
        else:
            log.warning("没有找到配置文件，使用默认值（不会连上任何地方）")
        raw.pop("path", None)
        cfg = cls(**raw)
        cfg.path = path
        return cfg


# ─────────────────────────────────────────────────────────────
# 本地库
# ─────────────────────────────────────────────────────────────


class Store:
    """SQLite 落地。id 用远端的自增 id 当主键，天然幂等。"""

    def __init__(self, path: str):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS events (
              id      INTEGER PRIMARY KEY,
              event   TEXT NOT NULL,
              repo    TEXT,
              action  TEXT,
              at      INTEGER,
              payload TEXT
            );
            CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT);
            """
        )
        self.conn.commit()

    def cursor(self) -> int:
        row = self.conn.execute("SELECT value FROM meta WHERE key='cursor'").fetchone()
        if row:
            return int(row["value"])
        row = self.conn.execute("SELECT COALESCE(MAX(id), 0) AS m FROM events").fetchone()
        return int(row["m"])

    def set_cursor(self, value: int) -> None:
        self.conn.execute(
            "INSERT INTO meta(key, value) VALUES('cursor', ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (str(value),),
        )
        self.conn.commit()

    def insert(self, ev: dict[str, Any]) -> bool:
        """返回 True 表示是新事件（False = 重复，直接丢掉）"""
        try:
            cur = self.conn.execute(
                "INSERT OR IGNORE INTO events (id, event, repo, action, at, payload) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (
                    int(ev["id"]),
                    ev.get("event") or "",
                    ev.get("repo"),
                    ev.get("action"),
                    int(ev.get("at") or 0),
                    json.dumps(ev.get("payload"), ensure_ascii=False),
                ),
            )
        except (KeyError, TypeError, ValueError):
            return False
        if cur.rowcount:
            self.conn.commit()
            return True
        return False

    # ── 给状态页用的统计 ──

    def count_since(self, ts_ms: int) -> int:
        row = self.conn.execute(
            "SELECT COUNT(*) AS n FROM events WHERE at >= ?", (ts_ms,)
        ).fetchone()
        return int(row["n"])

    def count_all(self) -> int:
        row = self.conn.execute("SELECT COUNT(*) AS n FROM events").fetchone()
        return int(row["n"])

    def hourly(self, start_ms: int) -> dict[str, int]:
        rows = self.conn.execute(
            "SELECT strftime('%Y-%m-%d %H', at/1000, 'unixepoch', 'localtime') AS h, "
            "COUNT(*) AS n FROM events WHERE at >= ? GROUP BY h",
            (start_ms,),
        ).fetchall()
        return {r["h"]: int(r["n"]) for r in rows if r["h"]}

    def type_counts(self, limit: int = 8) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT event AS k, COUNT(*) AS n FROM events GROUP BY event ORDER BY n DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return [{"type": r["k"], "count": int(r["n"])} for r in rows]

    # ── 账号与仓库：直接从 payload 里取，不额外存字段 ──

    def repos(self) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT repo,
                   COUNT(*) AS events,
                   MAX(at) AS last_at,
                   CASE WHEN json_valid(payload)
                        THEN json_extract(payload, '$.repository.owner.login') END AS owner
              FROM events
             WHERE repo IS NOT NULL AND repo <> ''
             GROUP BY repo
             ORDER BY last_at DESC
            """
        ).fetchall()
        type_rows = self.conn.execute(
            """
            SELECT repo, event, COUNT(*) AS n
              FROM events WHERE repo IS NOT NULL AND repo <> ''
             GROUP BY repo, event
            """
        ).fetchall()
        by_repo: dict[str, dict[str, int]] = {}
        for t in type_rows:
            by_repo.setdefault(t["repo"], {})[t["event"]] = int(t["n"])

        return [
            {
                "repo": r["repo"],
                "owner": r["owner"],
                "events": int(r["events"]),
                "last_at": int(r["last_at"] or 0),
                "types": by_repo.get(r["repo"], {}),
            }
            for r in rows
        ]

    def account(self) -> dict[str, Any] | None:
        """事件最多的那个仓库归属账号；头像用最近一条事件的 sender"""
        row = self.conn.execute(
            """
            SELECT CASE WHEN json_valid(payload)
                        THEN json_extract(payload, '$.repository.owner.login') END AS login,
                   COUNT(*) AS n,
                   MAX(at) AS last_at
              FROM events
             WHERE repo IS NOT NULL AND repo <> ''
             GROUP BY login
            HAVING login IS NOT NULL
             ORDER BY n DESC, last_at DESC
             LIMIT 1
            """
        ).fetchone()
        if not row or not row["login"]:
            return None

        av = self.conn.execute(
            """
            SELECT CASE WHEN json_valid(payload)
                        THEN json_extract(payload, '$.sender.avatar_url') END AS avatar
              FROM events
             WHERE repo IS NOT NULL AND repo <> ''
             ORDER BY id DESC LIMIT 1
            """
        ).fetchone()

        return {
            "login": row["login"],
            "avatar": av["avatar"] if av else None,
            "events": int(row["n"]),
            "last_at": int(row["last_at"] or 0),
        }

    def replay(self, after: int, limit: int = 500) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT id, event, repo, action, at, payload FROM events "
            "WHERE id > ? ORDER BY id LIMIT ?",
            (after, limit),
        ).fetchall()
        out = []
        for r in rows:
            try:
                payload = json.loads(r["payload"]) if r["payload"] else None
            except json.JSONDecodeError:
                payload = r["payload"]
            out.append(
                {
                    "id": r["id"],
                    "event": r["event"],
                    "repo": r["repo"],
                    "action": r["action"],
                    "at": r["at"],
                    "payload": payload,
                }
            )
        return out

    def close(self) -> None:
        self.conn.close()


# ─────────────────────────────────────────────────────────────
# 扇出
# ─────────────────────────────────────────────────────────────


class Bus:
    """一个极简的进程内广播：SSE 订阅者各拿一个队列。"""

    def __init__(self) -> None:
        self._subs: Set[asyncio.Queue] = set()

    def subscribe(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=2000)
        self._subs.add(q)
        return q

    def unsubscribe(self, q: asyncio.Queue) -> None:
        self._subs.discard(q)

    def publish(self, item: dict[str, Any]) -> None:
        for q in list(self._subs):
            try:
                q.put_nowait(item)
            except asyncio.QueueFull:
                # 消费端太慢就丢，宁可丢也不能把内存吃光
                log.warning("SSE 队列满，丢弃事件 id=%s", item.get("id"))

    def close_all(self) -> None:
        """推一个哨兵让所有 SSE 循环自己退出。

        不这么做的话，关闭时 runner.cleanup() 要等活跃请求结束，
        浏览器开着页面就会卡满 shutdown_timeout。
        """
        for q in list(self._subs):
            try:
                q.put_nowait(None)
            except asyncio.QueueFull:
                pass


# ─────────────────────────────────────────────────────────────
# 状态（给 /health 看）
# ─────────────────────────────────────────────────────────────


@dataclass
class State:
    connected: bool = False
    connects: int = 0
    last_event_at: float = 0.0
    last_error: str = ""
    ws: Any = None        # 当前连接，设置页改地址/令牌时用它立刻断开重连
    kick: bool = False    # 置位后下一轮不等退避，直接重连


# ─────────────────────────────────────────────────────────────
# WebSocket 主循环
# ─────────────────────────────────────────────────────────────


async def app_pinger(ws: aiohttp.ClientWebSocketResponse, interval: int) -> None:
    """应用层心跳。必须在客户端发 —— 服务端用定时器就永远无法休眠。"""
    try:
        while not ws.closed:
            await asyncio.sleep(interval)
            if ws.closed:
                break
            await ws.send_json({"type": "ping"})
    except (asyncio.CancelledError, ConnectionResetError):
        pass


async def ws_loop(cfg: Config, store: Store, bus: Bus, state: State, stop: asyncio.Event) -> None:
    url = f"{cfg.ws_url}{'&' if '?' in cfg.ws_url else '?'}token={cfg.token}"
    delay = 1

    timeout = aiohttp.ClientTimeout(total=None, sock_connect=15)
    connector = aiohttp.TCPConnector(force_close=False, enable_cleanup_closed=True)

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        while not stop.is_set():
            try:
                log.info("连接 %s（游标 %s）", cfg.ws_url, store.cursor())
                async with session.ws_connect(
                    url,
                    heartbeat=cfg.heartbeat,   # 协议层 ping，检测死连接 + 保 NAT
                    autoping=True,
                    max_msg_size=8 * 1024 * 1024,
                ) as ws:
                    state.connected = True
                    state.connects += 1
                    state.ws = ws
                    delay = 1
                    cursor = store.cursor()
                    await ws.send_json({"type": "hello", "cursor": cursor})
                    log.info("已连上，从游标 %s 续传", cursor)

                    pinger = asyncio.create_task(app_pinger(ws, cfg.app_ping))
                    try:
                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                _handle_text(msg.data, store, bus, state)
                            elif msg.type in (
                                aiohttp.WSMsgType.ERROR,
                                aiohttp.WSMsgType.CLOSED,
                                aiohttp.WSMsgType.CLOSE,
                                aiohttp.WSMsgType.CLOSING,
                            ):
                                break
                    finally:
                        pinger.cancel()

            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                state.last_error = f"{type(exc).__name__}: {exc}"
                log.warning("连接结束：%s", state.last_error)
            finally:
                state.connected = False
                state.ws = None

            if stop.is_set():
                break
            if state.kick:  # 改了连接参数：不等退避
                state.kick = False
                delay = 1
            log.info("%.0f 秒后重连", delay)
            try:
                await asyncio.wait_for(stop.wait(), timeout=delay)
            except asyncio.TimeoutError:
                pass
            delay = min(delay * 2, cfg.reconnect_max)


def _handle_text(raw: str, store: Store, bus: Bus, state: State) -> None:
    try:
        msg = json.loads(raw)
    except json.JSONDecodeError:
        log.warning("收到非 JSON 消息，忽略")
        return

    kind = msg.get("type")
    if kind == "event":
        if store.insert(msg):
            cursor = int(msg["id"])
            if cursor > store.cursor():
                store.set_cursor(cursor)
            state.last_event_at = time.time()
            bus.publish(msg)
            log.info("事件 #%s %s %s", msg["id"], msg.get("event"), msg.get("repo"))
        return

    if kind == "ready":
        log.info(
            "服务端确认游标 %s%s",
            msg.get("cursor"),
            f"（补发 {msg['replayed']} 条）" if msg.get("replayed") else "",
        )
    elif kind == "pong":
        return
    else:
        log.debug("未知消息类型：%s", kind)


# ─────────────────────────────────────────────────────────────
# HTTP / SSE 端点
# ─────────────────────────────────────────────────────────────


def _sse_frame(ev: dict[str, Any]) -> bytes:
    data = json.dumps(ev, ensure_ascii=False, separators=(",", ":"))
    return f"id: {ev['id']}\nevent: gh\ndata: {data}\n\n".encode("utf-8")


def _after_from(request: web.Request) -> int:
    """优先用 SSE 标准的 Last-Event-ID，其次 ?after="""
    raw = request.headers.get("Last-Event-ID") or request.query.get("after") or "0"
    try:
        return max(0, int(raw))
    except ValueError:
        return 0


async def handle_stream(request: web.Request) -> web.StreamResponse:
    store: Store = request.app["store"]
    bus: Bus = request.app["bus"]

    resp = web.StreamResponse(
        status=200,
        headers={
            "Content-Type": "text/event-stream; charset=utf-8",
            "Cache-Control": "no-cache, no-transform",
            "X-Accel-Buffering": "no",
            "Access-Control-Allow-Origin": "*",
        },
    )
    await resp.prepare(request)

    # 先订阅再回放，避免「回放完到订阅之间」漏事件
    queue = bus.subscribe()
    sent_max = _after_from(request)
    log.info("SSE 连接建立，从 id>%s 开始", sent_max)

    try:
        await resp.write(b"retry: 3000\n\n")
        for ev in store.replay(sent_max):
            await resp.write(_sse_frame(ev))
            sent_max = max(sent_max, int(ev["id"]))

        while True:
            try:
                item = await asyncio.wait_for(queue.get(), timeout=20)
            except asyncio.TimeoutError:
                # 心跳注释行，防止中间层掐断空闲连接
                await resp.write(b": ping\n\n")
                continue
            if item is None:
                break  # 服务正在关闭
            if int(item["id"]) <= sent_max:
                continue
            await resp.write(_sse_frame(item))
            sent_max = int(item["id"])
    except (asyncio.CancelledError, ConnectionResetError, RuntimeError):
        pass
    finally:
        bus.unsubscribe(queue)
        log.info("SSE 连接关闭（最后 id=%s）", sent_max)
    return resp


async def handle_events(request: web.Request) -> web.Response:
    store: Store = request.app["store"]
    after = _after_from(request)
    try:
        limit = min(1000, max(1, int(request.query.get("limit", "200"))))
    except ValueError:
        limit = 200
    return web.json_response({"events": store.replay(after, limit), "after": after})


async def handle_health(request: web.Request) -> web.Response:
    state: State = request.app["state"]
    store: Store = request.app["store"]
    bus: Bus = request.app["bus"]
    gap = int(time.time() - state.last_event_at) if state.last_event_at else None
    return web.json_response(
        {
            "ok": state.connected,
            "connected": state.connected,
            "cursor": store.cursor(),
            "connects": state.connects,
            "sse_clients": len(bus._subs),  # noqa: SLF001
            "seconds_since_last_event": gap,
            "last_error": state.last_error,
        }
    )


def build_stats(store: Store) -> dict[str, Any]:
    """状态页用的聚合：今日、总量、最近 24 小时每小时、按类型分布。"""
    from datetime import datetime, timedelta

    now = datetime.now()
    start = now.replace(minute=0, second=0, microsecond=0) - timedelta(hours=23)
    counts = store.hourly(int(start.timestamp() * 1000))

    hourly = []
    for i in range(24):
        t = start + timedelta(hours=i)
        hourly.append({"label": t.strftime("%H:00"), "count": counts.get(t.strftime("%Y-%m-%d %H"), 0)})

    midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
    repos = store.repos()
    return {
        "today": store.count_since(int(midnight.timestamp() * 1000)),
        "total": store.count_all(),
        "hourly": hourly,
        "by_type": store.type_counts(),
        "account": store.account(),
        "repos": repos,
        "repo_count": len(repos),
    }


async def handle_stats(request: web.Request) -> web.Response:
    return web.json_response(build_stats(request.app["store"]))


# ─────────────────────────────────────────────────────────────
# GitHub Actions：从 workflow_run 事件拼运行列表
# ─────────────────────────────────────────────────────────────

# 一次运行会推 requested / in_progress / completed 多条，按 run id 去重只留最新
def _iso_ms(text: Any) -> int:
    if not text:
        return 0
    try:
        from datetime import datetime

        return int(datetime.fromisoformat(str(text).replace("Z", "+00:00")).timestamp() * 1000)
    except (ValueError, TypeError):
        return 0


def build_actions(store: Store, repo: str = "", limit: int = 60) -> dict[str, Any]:
    """把库里的 workflow_run 事件还原成「一次运行一行」。

    载荷里带 name / head_branch / status / conclusion / run_number / html_url /
    head_commit，足够拼出 GitHub 那种列表；jobs 和步骤不在载荷里。
    """
    sql = "SELECT id, repo, action, at, payload FROM events WHERE event = 'workflow_run'"
    params: list[Any] = []
    if repo:
        sql += " AND repo = ?"
        params.append(repo)
    sql += " ORDER BY id DESC LIMIT 4000"
    rows = store.conn.execute(sql, params).fetchall()

    runs: dict[int, dict[str, Any]] = {}
    for row in rows:
        try:
            payload = json.loads(row["payload"] or "{}")
        except ValueError:
            continue
        wr = payload.get("workflow_run") or {}
        run_id = wr.get("id")
        if run_id is None or run_id in runs:
            continue  # 已经拿到更新的状态了

        started = _iso_ms(wr.get("run_started_at") or wr.get("created_at"))
        updated = _iso_ms(wr.get("updated_at"))
        commit = wr.get("head_commit") or {}
        actor = wr.get("actor") or {}
        prs = wr.get("pull_requests") or []
        runs[run_id] = {
            "id": run_id,
            "repo": row["repo"] or "",
            "name": wr.get("name") or "",
            "branch": wr.get("head_branch") or "",
            "trigger": wr.get("event") or "",
            "path": wr.get("path") or "",
            "status": wr.get("status") or "",
            "conclusion": wr.get("conclusion") or "",
            "run_number": wr.get("run_number"),
            "run_attempt": wr.get("run_attempt") or 1,
            "title": (wr.get("display_title") or commit.get("message") or "").split("\n")[0],
            "sha": (wr.get("head_sha") or "")[:7],
            "author": ((commit.get("author") or {}).get("name") or ""),
            "actor": actor.get("login") or "",
            "actor_avatar": actor.get("avatar_url") or "",
            "pr": (prs[0] or {}).get("number") if prs else None,
            "url": wr.get("html_url") or "",
            "started_at": started,
            "updated_at": updated,
            "duration_s": max(0, int((updated - started) / 1000)) if started and updated else None,
            "seen_at": row["at"],
        }

    ordered = sorted(runs.values(), key=lambda r: r["started_at"] or r["seen_at"] or 0, reverse=True)

    # 侧边的仓库下拉：只列有运行记录的仓库
    repos = [
        {"repo": r["repo"], "runs": sum(1 for x in ordered if x["repo"] == r["repo"])}
        for r in {x["repo"]: x for x in ordered}.values()
    ]
    repos.sort(key=lambda r: r["runs"], reverse=True)

    return {"runs": ordered[:limit], "total": len(ordered), "repos": repos}


async def handle_actions(request: web.Request) -> web.Response:
    repo = request.query.get("repo", "")
    try:
        limit = min(200, max(1, int(request.query.get("limit", "60"))))
    except ValueError:
        limit = 60
    return web.json_response(build_actions(request.app["store"], repo, limit))


# ─────────────────────────────────────────────────────────────
# 借本地 gh 的登录态：拉运行、拉 job 与步骤
# ─────────────────────────────────────────────────────────────

# 这些接口会拿用户的 GitHub 凭据去读私有仓库，所以只对本机开放
GH_TIMEOUT = 25.0
# GitHub 里 runner 能挂在三处：仓库、组织、企业。这里只区分前两种，判法跟
# actions/runner 自己的 create-latest-svc.sh 一样——带斜杠是仓库级，不带是组织级。
_REPO_RE = re.compile(r"^[\w.-]+(/[\w.-]+)?$")


def _gh_scope(repo: str) -> str:
    """把用户填的 scope 翻成接口路径前缀。

    "owner/name" → "repos/owner/name"（只对这一个仓库可见）
    "org"        → "orgs/org"        （整个组织下所有仓库共用）
    """
    return f"repos/{repo}" if "/" in repo else f"orgs/{repo}"


def _gh_error_text(raw: str, repo: str) -> str:
    """GitHub 的原始报错经常就一句 'gh: Not Found (HTTP 404)'，看不出错在哪。

    两种最常见的踩坑：把仓库名当组织名填了（没有斜杠），或者查组织却没
    admin:org 权限。这两种都换成人话。
    """
    msg = (raw or "").strip()
    low = msg.lower()
    if "404" in msg or "not found" in low:
        kind = "组织" if "/" not in repo else "仓库"
        return f"GitHub 上找不到这个{kind}：{repo}。名字拼错了？还是没权限？"
    if "403" in msg and "admin:org" in low:
        return (f"{repo} 是组织级，读它要 admin:org 权限。"
                f"先跑：gh auth refresh -h github.com -s admin:org")
    return msg[:300]
_gh_auth: dict[str, Any] = {"at": 0.0, "value": None}


def is_local(request: web.Request) -> bool:
    return request.remote in ("127.0.0.1", "::1", "::ffff:127.0.0.1")


def _local_only(request: web.Request) -> Optional[web.Response]:
    if is_local(request):
        return None
    return web.json_response(
        {"error": "GitHub 凭据只对本机开放；局域网设备请用 webhook 那套数据"}, status=403
    )


# api.github.com 在有些网络里会时不时 TLS 直接断（Go 报 EOF / connection reset）。
# 这类失败重试往往就过了，所以只读调用默认重试 3 次。
_GH_TRANSIENT = (
    "eof", "connection reset", "connection closed", "broken pipe",
    "tls handshake", "i/o timeout", "temporarily unavailable",
    "unexpected eof", "http/2 frame", "no such host",
)


def _gh_transient(err: str) -> bool:
    low = (err or "").lower()
    return any(k in low for k in _GH_TRANSIENT)


async def _gh_once(args: list[str]) -> tuple[int, str, str]:
    """跑一条 gh 命令。用 asyncio 子进程，不堵事件循环。"""
    try:
        proc = await asyncio.create_subprocess_exec(
            "gh", *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
    except FileNotFoundError:
        return 127, "", "本机没装 gh"
    try:
        out, err = await asyncio.wait_for(proc.communicate(), timeout=GH_TIMEOUT)
    except asyncio.TimeoutError:
        proc.kill()
        return 124, "", f"gh 超过 {GH_TIMEOUT:.0f}s 没返回"
    return proc.returncode or 0, out.decode("utf-8", "replace"), err.decode("utf-8", "replace")


async def gh_run(args: list[str], attempts: int = 3) -> tuple[int, str, str]:
    """带重试的 gh。只给只读命令用——写操作重放会重复执行。"""
    rc, out, err = 1, "", ""
    for i in range(max(1, attempts)):
        rc, out, err = await _gh_once(args)
        if rc == 0 or not _gh_transient(err):
            return rc, out, err
        if i + 1 < attempts:
            await asyncio.sleep(0.4 * (i + 1))
    if _gh_transient(err):
        err = err.strip() + "\n（尝试 " + str(attempts) + " 次都连不上 api.github.com；" \
              "本机到 GitHub 的 TLS 连接被重置，可给 relay 进程设 HTTPS_PROXY 走代理）"
    return rc, out, err


async def github_auth(force: bool = False) -> dict[str, Any]:
    """探测本机 gh 登录态。结果缓存 60 秒，不会每个请求都起进程。"""
    now = time.time()
    cached = _gh_auth.get("value")
    if not force and cached and now - _gh_auth["at"] < 60:
        return cached

    state: dict[str, Any] = {
        "installed": False, "logged_in": False, "version": "",
        "account": "", "scopes": [], "error": "",
    }
    rc, out, err = await gh_run(["--version"])
    if rc != 0:
        state["error"] = (err or out).strip()[:200] or "本机没装 gh"
        _gh_auth.update(at=now, value=state)
        return state
    state["installed"] = True
    parts = out.split()
    state["version"] = parts[2] if len(parts) > 2 else ""

    rc, out, err = await gh_run(["api", "-i", "user"])
    if rc != 0:
        state["error"] = (err or out).strip()[:200] or "gh 未登录"
        _gh_auth.update(at=now, value=state)
        return state
    state["logged_in"] = True
    for line in out.splitlines():          # 响应头里的 scope 最准
        if line.lower().startswith("x-oauth-scopes:"):
            raw = line.split(":", 1)[1].strip()
            state["scopes"] = [s.strip() for s in raw.split(",") if s.strip()]
            break
    # 响应体就是 user 的 JSON，顺手把登录名拿出来，省一次网络往返
    body = out.split("\r\n\r\n", 1)[-1] if "\r\n\r\n" in out else out.split("\n\n", 1)[-1]
    try:
        state["account"] = (json.loads(body) or {}).get("login") or ""
    except (ValueError, AttributeError):
        pass

    _gh_auth.update(at=now, value=state)
    return state


def _api_row(repo: str, raw: dict[str, Any]) -> dict[str, Any]:
    """把 gh run list 的字段拼成和 build_actions 一样的行。"""
    started = _iso_ms(raw.get("startedAt") or raw.get("createdAt"))
    updated = _iso_ms(raw.get("updatedAt"))
    return {
        "id": raw.get("databaseId"),
        "repo": repo,
        "name": raw.get("workflowName") or raw.get("name") or "",
        "branch": raw.get("headBranch") or "",
        "trigger": raw.get("event") or "",
        "path": "",
        "status": raw.get("status") or "",
        "conclusion": raw.get("conclusion") or "",
        "run_number": raw.get("number"),
        "run_attempt": raw.get("attempt") or 1,
        "title": raw.get("displayTitle") or "",
        "sha": (raw.get("headSha") or "")[:7],
        "author": "",
        "actor": "",
        "actor_avatar": "",
        "pr": None,
        "url": raw.get("url") or "",
        "started_at": started,
        "updated_at": updated,
        "duration_s": max(0, int((updated - started) / 1000)) if started and updated else None,
        "seen_at": started,
        "origin": "api",
    }


async def handle_github_auth(request: web.Request) -> web.Response:
    blocked = _local_only(request)
    if blocked:
        return blocked
    return web.json_response(await github_auth(force=request.query.get("refresh") == "1"))


async def handle_github_runs(request: web.Request) -> web.Response:
    """直接向 GitHub 要某个仓库的最近运行（需要 gh 已登录）。"""
    blocked = _local_only(request)
    if blocked:
        return blocked
    repo = (request.query.get("repo") or "").strip()
    if not _REPO_RE.match(repo):
        return web.json_response({"error": "填 owner/name 走仓库级，只填组织名走组织级"}, status=400)
    auth = await github_auth()
    if not auth["logged_in"]:
        return web.json_response({"error": auth["error"] or "gh 未登录"}, status=409)
    try:
        limit = min(50, max(1, int(request.query.get("limit", "20"))))
    except ValueError:
        limit = 20

    rc, out, err = await gh_run([
        "run", "list", "-R", repo, "-L", str(limit), "--json",
        "databaseId,number,name,workflowName,displayTitle,status,conclusion,"
        "headBranch,headSha,event,startedAt,createdAt,updatedAt,attempt,url",
    ])
    if rc != 0:
        return web.json_response({"error": (err or out).strip()[:300]}, status=502)
    try:
        rows = json.loads(out or "[]")
    except ValueError:
        return web.json_response({"error": "gh 返回的不是 JSON"}, status=502)
    runs = [_api_row(repo, r) for r in rows]
    runs.sort(key=lambda r: r["started_at"] or 0, reverse=True)
    return web.json_response({"repo": repo, "runs": runs, "total": len(runs)})


async def handle_github_jobs(request: web.Request) -> web.Response:
    """一次运行里的 job 与每一步状态——这个只有 API 有，webhook 不给。"""
    blocked = _local_only(request)
    if blocked:
        return blocked
    repo = (request.query.get("repo") or "").strip()
    run_id = (request.query.get("run") or "").strip()
    if not _REPO_RE.match(repo):
        return web.json_response({"error": "填 owner/name 走仓库级，只填组织名走组织级"}, status=400)
    if not run_id.isdigit():
        return web.json_response({"error": "run 要是数字 id"}, status=400)
    auth = await github_auth()
    if not auth["logged_in"]:
        return web.json_response({"error": auth["error"] or "gh 未登录"}, status=409)

    rc, out, err = await gh_run([
        "api", f"repos/{repo}/actions/runs/{run_id}/jobs", "--jq",
        "{total: .total_count, jobs: [.jobs[] | {id, name, status, conclusion, "
        "started_at, completed_at, html_url, "
        "steps: [.steps[] | {number, name, status, conclusion}]}]}",
    ])
    if rc != 0:
        return web.json_response({"error": (err or out).strip()[:300]}, status=502)
    try:
        data = json.loads(out or "{}")
    except ValueError:
        return web.json_response({"error": "gh 返回的不是 JSON"}, status=502)
    return web.json_response(data)


# ─────────────────────────────────────────────────────────────
# 自建 runner：看在线状态、发注册令牌
# ─────────────────────────────────────────────────────────────

# 安装包版本。写死比每次请求都去问 GitHub 稳——
# 真要跟新时改这一行，前端从接口拿，不会各写一份。
RUNNER_VERSION = "2.338.2"
_RUNNER_ARCHIVE = {
    "linux": "actions-runner-linux-x64-{v}.tar.gz",
    "macos": "actions-runner-osx-x64-{v}.tar.gz",
    "windows": "actions-runner-win-x64-{v}.zip",
}
_RUNNER_OS_LABEL = {"linux": "Linux", "macos": "macOS", "windows": "Windows"}


def _runner_row(raw: dict[str, Any]) -> dict[str, Any]:
    """把 GitHub 的 runner 记录整理成前端好用的形状。"""
    # --jq 已经把 labels 拆成了字符串数组；别的调用方可能传对象，两种都兼容
    labels: list[str] = []
    for item in raw.get("labels") or []:
        if isinstance(item, dict):
            labels.append(str(item.get("name") or ""))
        else:
            labels.append(str(item or ""))
    labels = [x for x in labels if x]
    lowered = [x.lower() for x in labels]
    os_name = next((x for x in labels if x.lower() in ("linux", "macos", "windows")), "")
    arch = next((x for x in labels if x.lower() in ("x64", "arm64", "arm")), "")
    return {
        "id": raw.get("id"),
        "name": raw.get("name") or "",
        "os": os_name,
        "arch": arch,
        "status": raw.get("status") or "",
        "busy": bool(raw.get("busy")),
        "labels": [x for x in labels if x.lower() != "self-hosted"],
    }


async def handle_github_runners(request: web.Request) -> web.Response:
    """列出某个仓库下注册过的 runner 以及它们在不在线。"""
    blocked = _local_only(request)
    if blocked:
        return blocked
    repo = (request.query.get("repo") or "").strip()
    if not _REPO_RE.match(repo):
        return web.json_response({"error": "填 owner/name 走仓库级，只填组织名走组织级"}, status=400)
    auth = await github_auth()
    if not auth["logged_in"]:
        return web.json_response({"error": auth["error"] or "gh 未登录"}, status=409)

    rc, out, err = await gh_run([
        "api", f"{_gh_scope(repo)}/actions/runners", "--jq",
        "{total: .total_count, runners: [.runners[] | {id, name, status, busy, "
        "labels: [.labels[].name]}]}",
    ])
    if rc != 0:
        return web.json_response({"error": _gh_error_text(err or out, repo)}, status=502)
    try:
        data = json.loads(out or "{}")
    except ValueError:
        return web.json_response({"error": "gh 返回的不是 JSON"}, status=502)

    rows = [_runner_row(r) for r in (data.get("runners") or [])]
    rows.sort(key=lambda r: (r["status"] != "online", r["name"]))   # 在线的排前面
    return web.json_response({
        "repo": repo,
        "total": data.get("total", len(rows)),
        "online": sum(1 for r in rows if r["status"] == "online"),
        "busy": sum(1 for r in rows if r["busy"]),
        "runners": rows,
        # 这些名字的密码还在内存里（30 分钟内），能一键「连机器一起卸」
        "ssh_ready": sorted(_machines_live().keys()),
    })


async def handle_github_runner_remove(request: web.Request) -> web.Response:
    """从仓库里删掉一个 runner 注册（机器上那份文件不会自动清）。"""
    blocked = _local_only(request)
    if blocked:
        return blocked
    try:
        body = await request.json()
    except Exception:
        body = {}
    if not isinstance(body, dict):
        body = {}
    repo = str(body.get("repo") or "").strip()
    runner_id = str(body.get("id") or "").strip()
    if not _REPO_RE.match(repo):
        return web.json_response({"error": "填 owner/name 走仓库级，只填组织名走组织级"}, status=400)
    if not runner_id.isdigit():
        return web.json_response({"error": "runner id 要是数字"}, status=400)

    auth = await github_auth()
    if not auth["logged_in"]:
        return web.json_response({"error": auth["error"] or "gh 未登录"}, status=409)

    rc, out, err = await gh_run([
        "api", "-X", "DELETE", f"{_gh_scope(repo)}/actions/runners/{runner_id}",
    ], attempts=1)
    if rc != 0:
        return web.json_response({"error": _gh_error_text(err or out, repo)}, status=502)
    return web.json_response({"ok": True, "removed": int(runner_id)})


# ─────────────────────────────────────────────────────────────
# 通过 SSH 装 / 卸 runner
#
# 页面上的「添加一台机器」不是生成一段命令让你自己敲，而是直接连过去装好。
# 密码只在这一行活着：连着内存里的任务走完就丢，不落盘、不写日志、
# 不进 SQLite，重启进程就什么也不剩。
# ─────────────────────────────────────────────────────────────

try:  # 没装也能跑，只是这一块返回 501
    import paramiko
except Exception:  # pragma: no cover
    paramiko = None  # type: ignore

SSH_TIMEOUT = 15          # 连接/认证超时（秒）
SSH_JOB_LINES = 1200      # 单个任务最多留多少行日志
SSH_JOBS: dict[str, dict[str, Any]] = {}
# 机器名 -> 连接参数（含密码）。只在内存、**只在 30 分钟内有效**，
# 过期就丢，用来支撑「连机器一起卸」的按钮。
SSH_MACHINES: dict[str, dict[str, Any]] = {}
SSH_MACHINE_TTL = 30 * 60

# 机器名和目录都不让人填：名字取机器自己的主机名（连上看一眼就知道），
# 目录从 ~/actions-runner 开始，那儿已经有一个就顺延到 -2、-3。
# 只有直接打 API 的人才会传这两个参数——页面已经不给了。
DEFAULT_RUNNER_DIR = "~/actions-runner"
_AUTO_DIR_TRIES = 8


def _machines_live() -> dict[str, dict[str, Any]]:
    """还在有效期内的机器凭据。过期的顺手清掉。"""
    now = time.time()
    for name in [n for n, m in SSH_MACHINES.items()
                 if now - float(m.get("at") or 0) > SSH_MACHINE_TTL]:
        SSH_MACHINES.pop(name, None)
    return SSH_MACHINES


class SshBox:
    """一台机器的 SSH 会话。paramiko 是阻塞的，所有调用都丢线程里跑。"""

    def __init__(self, host: str, port: int, user: str, password: str, log_fn) -> None:
        self.host, self.port, self.user = host, port, user
        self.password = password
        self.log = log_fn
        self.client = None

    def connect(self) -> None:
        """连上去。连不上重试两次——内网机器偶尔会因为 sshd 的
        MaxStartups 把并发会话卡掉一下，一次失败不代表机器有问题。"""
        if paramiko is None:
            raise RuntimeError("本机没装 paramiko")
        last: Optional[Exception] = None
        for attempt in range(3):
            try:
                c = paramiko.SSHClient()
                # 内网机器，首次连接没法人工确认指纹；用完即弃的会话，接受新主机键。
                c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                c.connect(
                    self.host, port=self.port, username=self.user, password=self.password,
                    timeout=SSH_TIMEOUT, banner_timeout=SSH_TIMEOUT, auth_timeout=SSH_TIMEOUT,
                    look_for_keys=False, allow_agent=False,
                )
                self.client = c
                return
            except Exception as exc:            # noqa: BLE001
                last = exc
                if attempt < 2:
                    self.log(f"连不上（{type(exc).__name__}），{attempt + 1} 秒后重试")
                    time.sleep(attempt + 1)
        raise last if last else RuntimeError("连不上")

    def close(self) -> None:
        if self.client is not None:
            try:
                self.client.close()
            except Exception:
                pass
            self.client = None

    def run(self, cmd: str, stdin_text: Optional[str] = None, timeout: float = 1800,
            quiet: bool = False) -> tuple[int, str]:
        """跑一条命令，边跑边把输出按行喂给 log。返回 (退出码, 全部输出)。

        quiet=True 用于探测类命令（uname / hostname / 看目录）：它们的输出
        已经被上层另起一行讲清楚了，再原样打一遗只会让日志看着很乱。
        """
        if self.client is None:
            raise RuntimeError("SSH 还没连上")
        chan = self.client.get_transport().open_session()
        chan.set_combine_stderr(True)
        chan.exec_command(cmd)
        if stdin_text is not None:
            chan.send(stdin_text.encode("utf-8") + b"\n")
            chan.shutdown_write()

        chunks: list[str] = []
        buf = b""
        deadline = time.time() + timeout
        while True:
            if chan.recv_ready():
                data = chan.recv(65536)
                if data:
                    buf += data
                    while b"\n" in buf:
                        line, buf = buf.split(b"\n", 1)
                        text = line.decode("utf-8", "replace").replace("\r", "")
                        chunks.append(text)
                        if text.strip() and not quiet:
                            self.log(text)
                continue
            if chan.exit_status_ready():
                while chan.recv_ready():          # 把退出前剩下的读干
                    buf += chan.recv(65536)
                break
            if time.time() > deadline:
                chan.close()
                raise TimeoutError(f"命令超过 {timeout:.0f}s 没跑完")
            time.sleep(0.05)

        if buf.strip():
            text = buf.decode("utf-8", "replace").replace("\r", "")
            chunks.append(text)
            if not quiet:
                self.log(text)
        return chan.recv_exit_status(), "\n".join(chunks)


def _ssh_new_job(kind: str, title: str) -> dict[str, Any]:
    jid = uuid.uuid4().hex[:10]
    job = {
        "id": jid, "kind": kind, "title": title, "status": "running",
        "lines": [], "error": "", "result": None, "started_at": time.time(),
    }
    SSH_JOBS[jid] = job
    # 只留最近 20 个**已结束**的任务。正在跑的绝不能淘汰——
    # 淘汰后前端轮询会拿到 404，就再也看不到自己的日志了。
    if len(SSH_JOBS) > 20:
        finished = [j for j in SSH_JOBS.values() if j["status"] != "running"]
        for old in sorted(finished, key=lambda j: j["started_at"])[: max(0, len(SSH_JOBS) - 20)]:
            SSH_JOBS.pop(old["id"], None)
    return job


_PS_NOISE = ("#< CLIXML", "<Objs ", "Preparing modules for first use")


def _ssh_log(job: dict[str, Any], text: str) -> None:
    """一行一行记。Windows 的 PowerShell 会往 stderr 里塞 CLIXML 进度噪音，
    对排障没用、还糊屏，这类行直接丢掉。"""
    stamp = time.strftime("%H:%M:%S")
    for line in str(text).splitlines() or [""]:
        if any(token in line for token in _PS_NOISE):
            continue
        job["lines"].append(f"{stamp} {line}")
    if len(job["lines"]) > SSH_JOB_LINES:
        del job["lines"][: len(job["lines"]) - SSH_JOB_LINES]


def _dir_reject_reason(path: str) -> str:
    """目录能不能当 runner 目录用。空字符串表示可以用。

    这是「删除」路径的第一道闸：卸载时会 rm -rf $DIR，写错一个字就是灾难。
    所以宁严勿松：不许 ..、不许隐藏目录（.ssh / .cache / .config 全部挡掉）、
    至少要有一层名字（~/x 可以，~ 不行），字符只允许字母数字和 ._-/。
    """
    if not path or len(path) > 200:
        return "目录写得太长或者为空"
    norm = path.replace("\\", "/")
    parts = [x for x in norm.split("/") if x]
    if not parts:
        return "目录不能是根"
    if ".." in parts:
        return "目录里不许出现 .."
    if len(parts) < 2:
        return "目录要写成 ~/actions-runner 这样，至少带一层名字"
    for seg in parts:
        if seg.startswith("."):
            return "不许把 runner 装到隐藏目录里（.ssh、.cache 这类）"
        if not re.match(r"^[\w.+~-]+$", seg):
            return "目录名里有不支持的字符"
    if not (path.startswith("~") or path.startswith("/")):
        return "目录要写成 ~/xxx 或 /path/xxx"
    return ""


def _clean_runner_name(raw: str) -> str:
    """把主机名洗成能当 runner 名的样子。

    Linux 主机名可能长成 `lab-linux-1`，macOS 带 `.local`，
    Windows 是 `WIN-XXXX`。统一：不合法的字符换成横线，去掉首尾符号。
    """
    s = re.sub(r"[^A-Za-z0-9._-]+", "-", (raw or "").strip())
    s = re.sub(r"[-._]{2,}", "-", s).strip("-._")
    return s[:63]


def _remote_hostname(box: SshBox, plat: str) -> str:
    """读机器自己的名字。取不到就给空串，让调用方兜底。"""
    cmd = ('powershell -NoProfile -Command "$env:COMPUTERNAME"'
           if plat == "windows" else "hostname -s 2>/dev/null || hostname")
    try:
        rc, out = box.run(cmd, timeout=30, quiet=True)
    except Exception:
        return ""
    if rc != 0:
        return ""
    # 从下往上看：登录提示里可能夹着一堆东西，最后一行才是真正的输出
    for line in reversed((out or "").splitlines()):
        name = _clean_runner_name(line)
        if name:
            return name
    return ""


def _ssh_params(body: dict[str, Any]) -> tuple[dict[str, Any], str]:
    """校验连接参数。返回 (参数, 错误)。

    name / dir 都是可选的：不传就由连上之后自动定（主机名 + 顺延目录）。
    传了也照样校验——API 直接调用时这两个值会落到 rm -rf 的路径上。
    """
    repo = str(body.get("repo") or "").strip()
    host = str(body.get("host") or "").strip()
    user = str(body.get("user") or "").strip()
    password = str(body.get("password") or "")
    name = str(body.get("name") or "").strip()
    plat = str(body.get("os") or "linux").strip().lower()
    given_os = bool(str(body.get("os") or "").strip())
    given_dir = str(body.get("dir") or "").strip()
    directory = given_dir or DEFAULT_RUNNER_DIR
    try:
        port = int(body.get("port") or 22)
    except (TypeError, ValueError):
        return {}, "端口要写成数字"

    if not _REPO_RE.match(repo):
        return {}, "填 owner/name 走仓库级，只填组织名走组织级"
    if not host:
        return {}, "要填机器地址"
    if not user:
        return {}, "要填 SSH 用户名"
    if not password:
        return {}, "这里要走密码登录，密码不能空（密钥登录暂时不支持）"
    if name and not re.match(r"^[A-Za-z0-9._-]{1,63}$", name):
        return {}, "机器名只能用字母、数字、点、横线、下划线"
    if plat not in _RUNNER_ARCHIVE:
        return {}, "系统只支持 linux / macos / windows"
    if not (1 <= port <= 65535):
        return {}, "端口超出范围"
    bad = _dir_reject_reason(directory)
    if bad:
        return {}, bad
    return {
        "repo": repo, "host": host, "port": port, "user": user,
        "password": password, "name": name, "os": plat, "dir": directory,
        "auto_name": not name, "auto_dir": not given_dir, "auto_os": not given_os,
    }, ""


def _posix_install_script(p: dict[str, Any], token: str, archive: str, labels: str,
                          wipe: bool = False) -> str:
    """拼一个 bash 脚本。sudo 密码从 stdin 第一行读（read SUDOPW），
    不写在命令里——写进去的话 `ps` 一眼就能看到。

    安装包先落进 ~/.cache/gh-relay：同一台机器装第二个 runner 不用再下 200MB，
    弱网下这条能省十几分钟。
    """
    d = p["dir"].replace("~", "$HOME")
    name = archive.rsplit("/", 1)[-1]           # actions-runner-linux-x64-2.338.2.tar.gz
    lines = [
        "set -e",
        'read -r SUDOPW',
        f'DIR="{d}"',
        # 再查一道：探测和下载之间那段空档里可能被人插了一脚
        'if [ -f "$DIR/.runner" ]; then echo "这目录已经是一个配好的 runner，不盖它"; exit 3; fi',
        'CACHE="$HOME/.cache/gh-relay"',
        'mkdir -p "$CACHE"',
        f'TAR="$CACHE/{name}"',
        'if tar -tzf "$TAR" >/dev/null 2>&1; then',
        '  echo "[1/4] 用本地缓存的安装包（$(du -h "$TAR" | cut -f1)），不再下载"',
        'else',
        '  echo "[1/4] 下载 runner 约 200MB；慢的话每 10 秒报一次进度"',
        '  rm -f "$TAR.part"',
        f'  curl -fsSL -o "$TAR.part" {shlex.quote(archive)} &',
        '  CPID=$!',
        '  while kill -0 "$CPID" 2>/dev/null; do',
        '    sleep 10',
        '    kill -0 "$CPID" 2>/dev/null && echo "      已下载 $(du -h "$TAR.part" 2>/dev/null | cut -f1)"',
        '  done',
        '  if ! wait "$CPID"; then echo "下载失败：这台机器连 github.com 有问题"; exit 4; fi',
        '  mv "$TAR.part" "$TAR"',
        'fi',
    ]
    if wipe:
        lines.append('echo "清掉上次没配完的残留"')
        lines.append('if [ -f "$DIR/config.sh" ]; then rm -rf "$DIR"; else echo "残留目录里没有 config.sh，不敢删"; exit 5; fi')
    lines += [
        'mkdir -p "$DIR" && cd "$DIR"',
        'echo "[2/4] 解包"',
        'tar xzf "$TAR" -C "$DIR"',
        'echo "[3/4] 注册"',
        f'./config.sh --url {shlex.quote("https://github.com/" + p["repo"])} '
        f'--token {shlex.quote(token)} --name {shlex.quote(p["name"])} '
        f'--labels {shlex.quote(labels)} --work _work --unattended',
        'echo "[4/4] 装成服务并启动"',
    ]
    if p["os"] == "macos":
        # macOS 的 svc.sh 装的是 LaunchAgent（装在用户自己的 ~/Library），
        # 它开头就检查 EUID，看见 root 直接喊 "Must not run with sudo" 退出。
        # 这一支绝不能加 sudo，也不要 sudo 密码。
        lines += [
            './svc.sh install',
            './svc.sh start',
            './svc.sh status || true',
        ]
    else:
        lines += [
            f'printf "%s\\n" "$SUDOPW" | sudo -S ./svc.sh install {shlex.quote(p["user"])}',
            'printf "%s\\n" "$SUDOPW" | sudo -S ./svc.sh start',
            'printf "%s\\n" "$SUDOPW" | sudo -S ./svc.sh status || true',
        ]
    lines.append('echo "[ok] runner 已装到 $(hostname)：$(uname -srm)"')
    return "\n".join(lines)



def _posix_uninstall_script(p: dict[str, Any]) -> str:
    d = p["dir"].replace("~", "$HOME")
    # 同上：macOS 的 svc.sh 一看见 root 就退出，删目录也不需要 root
    sudo = "" if p["os"] == "macos" else 'printf "%s\\n" "$SUDOPW" | sudo -S '
    return "\n".join([
        "set +e",
        'read -r SUDOPW',
        f'DIR="{d}"',
        'if [ -d "$DIR" ]; then cd "$DIR"; fi',
        'if [ -x ./svc.sh ]; then',
        f'  {sudo}./svc.sh stop',
        f'  {sudo}./svc.sh uninstall',
        'fi',
        # 只删确实长得像 runner 的目录
        'if [ -f "$DIR/config.sh" ] || [ -f "$DIR/.runner" ] || [ -f "$DIR/runsvc.sh" ]; then',
        '  echo "删掉 $DIR"',
        f'  {sudo}rm -rf "$DIR"',
        'else',
        '  echo "$DIR 里看不到 runner 的痕迹，不动它"; exit 5',
        'fi',
        'echo "清理完成"',
    ])


def _psq(value: Any) -> str:
    """PowerShell 单引号字符串。单引号在 PS 里靠写两遍转义。"""
    return "'" + str(value).replace("'", "''") + "'"


def _win_install_script(p: dict[str, Any], token: str, archive: str, labels: str,
                        wipe: bool = False) -> str:
    d = p["dir"].replace("~", "$env:USERPROFILE").replace("/", "\\")
    zipname = archive.rsplit("/", 1)[-1]
    # 走 Windows PowerShell 5.1：只用 ASCII，脚本里一点中文都不能有（会按代码页拆坏）
    parts = [
        "$ErrorActionPreference='Stop'",
        # config.cmd 打印的 √ 是 UTF-8，PS 默认按控制台代码页解就成了 �。
        # 实测这两句能让子进程的 UTF-8 输出原样回来。
        '[Console]::OutputEncoding=[Text.Encoding]::UTF8',
        '$OutputEncoding=[Text.Encoding]::UTF8',
        f'$dir=\"{d}\"',
        'if (Test-Path \"$dir\\.runner\") { Write-Output \'dir already configured\'; exit 3 }',
        '$cache=\"$env:USERPROFILE\\.cache\\gh-relay\"',
        'New-Item -ItemType Directory -Force $cache | Out-Null',
        f'$zip=\"$cache\\{zipname}\"',
    ]
    if wipe:
        parts.append('if (Test-Path $dir) { Remove-Item -Recurse -Force $dir }')
    parts += [
        'if (Test-Path $zip) { Write-Output \'[1/4] using cached archive\' } '
        f"else {{ Write-Output \'[1/4] downloading ~200MB\'; "
        f"Invoke-WebRequest -Uri \'{archive}\' -OutFile (\"$zip.part\"); "
        'Move-Item \"$zip.part\" $zip -Force }',
        'New-Item -ItemType Directory -Force $dir | Out-Null',
        'Write-Output \'[2/4] extracting\'',
        'Expand-Archive $zip -DestinationPath $dir -Force',
        'Set-Location $dir',
        'Write-Output \'[3/4] configuring\'',
        f'.\\config.cmd --url {_psq("https://github.com/" + p["repo"])} '
        f'--token {_psq(token)} --name {_psq(p["name"])} '
        f'--labels {_psq(labels)} --work _work --unattended',
        'Write-Output \'[4/4] starting (runner has no svc.cmd on Windows)\'',
        '$p = Invoke-CimMethod -ClassName Win32_Process -MethodName Create '
        '-Arguments @{ CommandLine = \"`\"$dir\\run.cmd`\"\" }',
        'Write-Output \"[4/4] pid=$($p.ProcessId)\"',
        '(Get-Process -Name \'Runner.Listener\' -ErrorAction SilentlyContinue | '
        'Select-Object -First 1 | ForEach-Object { Write-Output \"listener pid=$($_.Id)\" })',
    ]
    return "; ".join(parts)



def _ps(script: str) -> str:
    """把 PowerShell 脚本包成一条命令行。

    不走 -Command "..."：cmd.exe 那层会吃掉一层引号、$ 和反斜杠，
    脚本一长就拼坏。转 base64 用 -EncodedCommand，远程解出来就是原文。
    """
    b64 = base64.b64encode(script.encode("utf-16-le")).decode("ascii")
    return f"powershell -NoProfile -ExecutionPolicy Bypass -EncodedCommand {b64}"


def _dir_state(box: SshBox, p: dict[str, Any]) -> str:
    """装之前先看目录里是什么：CONFIGURED / STALE / DIRTY / FRESH。

    已经配好的 runner 绝对不能盖（config.sh 会拒，盖了也会把别人弄坏）；
    目录里有别的东西也不能替用户删。
    """
    d = p["dir"]
    if p["os"] == "windows":
        wd = d.replace("~", "$env:USERPROFILE")
        ps = (
            '$d="' + wd + '"; '
            r'if (Test-Path "$d\.runner") { "CONFIGURED" } '
            r'elseif (Test-Path "$d\config.cmd") { "STALE" } '
            'elseif ((Test-Path "$d") -and (Get-ChildItem "$d" -Force | Select-Object -First 1)) { "DIRTY" } '
            'else { "FRESH" }'
        )
        cmd = _ps(ps)
    else:
        hd = d.replace("~", "$HOME")
        cmd = ('DIR="' + hd + '"; '
               'if [ -f "$DIR/.runner" ]; then echo CONFIGURED; '
               'elif [ -f "$DIR/config.sh" ]; then echo STALE; '
               'elif [ -d "$DIR" ] && [ -n "$(ls -A "$DIR" 2>/dev/null)" ]; then echo DIRTY; '
               'else echo FRESH; fi')
    _, out = box.run(cmd, timeout=60, quiet=True)
    # 只看最后几行、且要求整行等于标记：MOTD 里随便出现一个 FRESH
    # 就会让判断走错路，而这条判断后面跟着的是 rm -rf。
    tokens = ("CONFIGURED", "STALE", "DIRTY", "FRESH")
    for line in reversed((out or "").splitlines()):
        text = line.strip()
        if text in tokens:
            return text
    return "UNKNOWN"


def _win_uninstall_script(p: dict[str, Any]) -> str:
    d = p["dir"].replace("~", "$env:USERPROFILE").replace("/", "\\")
    return "; ".join([
        "$ErrorActionPreference='Continue'",
        f'$dir="{d}"',
        # 只杀**这个目录下**的 runner。Windows 上一台机器可以有好几个目录各跑一个，
        # 以前按进程名一刀切，卸一台会把别的目录里的 runner 一起杀掉（实测踩过：
        # 卸 actions-runner-2 把 actions-runner 那份也杀没了，那台直接变 offline，
        # 而 Windows 没有 svc.cmd，没人会把它拉起来）。
        '$procs = Get-CimInstance Win32_Process '
        '| Where-Object { ($_.Name -eq \'Runner.Listener.exe\' -or $_.Name -eq \'Runner.Worker.exe\') '
        '-and $_.ExecutablePath -and $_.ExecutablePath.StartsWith($dir, \'OrdinalIgnoreCase\') }',
        '$procs | ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }',
        "Start-Sleep -Seconds 2",
        # 只删确实长得像 runner 的目录，别的一律不碰
        "if (-not (Test-Path $dir)) { Write-Output 'dir already gone' } "
        "elseif ((Test-Path \"$dir\\config.cmd\") -or (Test-Path \"$dir\\.runner\")) "
        "{ Remove-Item -Recurse -Force $dir; Write-Output 'removed' } "
        "else { Write-Output 'not a runner dir, left untouched'; exit 5 }",
    ])


async def _gh_registration_token(repo: str) -> tuple[str, str]:
    rc, out, err = await gh_run([
        "api", "-X", "POST", f"{_gh_scope(repo)}/actions/runners/registration-token",
        "--jq", ".token",
    ], attempts=1)
    if rc != 0:
        return "", (err or out).strip()[:300]
    return (out or "").strip().strip('"'), ""


async def _gh_runner_by_name(repo: str, name: str) -> Optional[dict[str, Any]]:
    rc, out, err = await gh_run([
        "api", f"{_gh_scope(repo)}/actions/runners", "--jq",
        '{runners: [.runners[] | {id, name, status, busy}]}',
    ])
    if rc != 0:
        return None
    try:
        data = json.loads(out or "{}")
    except ValueError:
        return None
    for r in data.get("runners") or []:
        if r.get("name") == name:
            return r
    return None


async def _gh_runner_names(repo: str) -> list[str]:
    """仓库里已经存在哪些 runner 名（包括掉线的僵尸注册）。

    用来避名：同名注册会被 config.sh 顶掉，那样会把另一台机器的 runner 带走。
    """
    rc, out, err = await gh_run([
        "api", f"{_gh_scope(repo)}/actions/runners", "--jq", "[.runners[].name]",
    ])
    if rc != 0:
        return []
    try:
        data = json.loads(out or "[]")
    except ValueError:
        return []
    return [str(x) for x in data] if isinstance(data, list) else []


def _detect_os(box: SshBox, job: dict[str, Any]) -> str:
    """先按 POSIX 试一句 uname；不行就当 Windows，再用 PowerShell 确认。

    不要用 `||` 之类的语法去一次分辨——Windows PowerShell 5.1 里根本没有 `||`，
    探针本身会报错，反而分不清。
    """
    try:
        rc, out = box.run("uname -s", timeout=30, quiet=True)
        name = (out or "").strip().splitlines()[-1].strip() if (out or "").strip() else ""
        if rc == 0 and name in ("Linux", "Darwin"):
            return "macos" if name == "Darwin" else "linux"
    except Exception:
        pass
    rc, out = box.run('powershell -NoProfile -Command "$env:OS"', timeout=60, quiet=True)
    if "Windows" in (out or ""):
        return "windows"
    return ""


def _box_for(p: dict[str, Any], job: dict[str, Any]) -> SshBox:
    return SshBox(p["host"], p["port"], p["user"], p["password"],
                  lambda line: _ssh_log(job, line))


def _ssh_precheck(job: dict[str, Any], p: dict[str, Any]) -> tuple[bool, str]:
    """连上去看三件事：什么系统、机器叫什么、装到哪个目录。

    这一步放在申请注册令牌之前——目录不可用的时候**一个令牌都不该造**。
    名字和目录在页面里都不用填：名字读机器自己的主机名，目录从
    ~/actions-runner 开始顺延，直到找到一个能用的。
    """
    box = _box_for(p, job)
    try:
        _ssh_log(job, f"连接 {p['user']}@{p['host']}:{p['port']} …")
        box.connect()
        _ssh_log(job, "连上了，看看是什么系统")
        found = _detect_os(box, job)
        if not found:
            return False, "认不出这台机器是什么系统（uname 和 PowerShell 都没应答）"
        if found != p["os"]:
            # 页面已经不让人选系统了，os 默认值是内部分底，别说成「你选的」
            if p.get("auto_os"):
                _ssh_log(job, f"实际是 {found}，按这个来")
            else:
                _ssh_log(job, f"你选的是 {p['os']}，实际上是 {found}，按实际的来")
            p["os"] = found

        # 名字：机器自己叫什么就叫什么，不用人填
        if not p.get("name"):
            guess = _remote_hostname(box, p["os"])
            if guess:
                p["name"] = guess
                _ssh_log(job, f"机器名自动取成 {guess}（读的是它自己的主机名）")
            else:
                # 主机名读不出来时退回「用户名-地址」，至少两台机器不会撞在一起
                fallback = _clean_runner_name(f"{p['user']}-{p['host'].replace('.', '-')}")
                p["name"] = fallback or f"runner-{p['port']}"
                _ssh_log(job, f"主机名没读到，机器名用 {p['name']} 顶着")

        # 目录：默认 ~/actions-runner；那里已经有一个就顺延到 -2、-3
        if p.get("auto_dir"):
            picks = [DEFAULT_RUNNER_DIR if i == 1 else f"{DEFAULT_RUNNER_DIR}-{i}"
                     for i in range(1, _AUTO_DIR_TRIES + 1)]
        else:
            picks = [p["dir"]]

        chosen = ""
        for candidate in picks:
            p["dir"] = candidate
            state = _dir_state(box, p)
            _ssh_log(job, f"目录 {candidate} 状态：{state}")
            if state in ("FRESH", "STALE"):
                chosen = candidate
                p["_wipe"] = state == "STALE"
                break
            if state == "UNKNOWN" and not p.get("auto_dir"):
                # 指定的目录、状态读不出来：按原样继续试，安装脚本里还有一道检查
                _ssh_log(job, "目录状态没读出来，继续试（脚本里还有一道检查）")
                chosen = candidate
                p["_wipe"] = False
                break
            if not p.get("auto_dir"):
                if state == "CONFIGURED":
                    return False, (f"{candidate} 里已经有一个配好的 runner，不覆盖它。"
                                   "同一台机器装第二个得用别的目录；"
                                   "或者先去那台机器上卸掉旧的。")
                if state == "DIRTY":
                    return False, f"{candidate} 里已经有别的内容，不敢替你删。"
            # 自动模式下 CONFIGURED / DIRTY 都不算错，换个目录接着看
        if not chosen:
            return False, (f"从 {DEFAULT_RUNNER_DIR} 到 -{_AUTO_DIR_TRIES} 都占满了，"
                           "这台机器装不下新的了；先卸掉一个再装")
        p["dir"] = chosen
        _ssh_log(job, f"目录定在 {chosen}")
        return True, ""
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"[:300]
    finally:
        box.close()


def _ssh_install_blocking(job: dict[str, Any], p: dict[str, Any], token: str) -> tuple[bool, str]:
    """装。预检已经确认过系统和目录，这里只管下、解、注册、起服务。"""
    box = _box_for(p, job)
    try:
        box.connect()
        archive = ("https://github.com/actions/runner/releases/download/v{v}/"
                   + _RUNNER_ARCHIVE[p["os"]]).format(v=RUNNER_VERSION)
        labels = f"self-hosted,{_RUNNER_OS_LABEL[p['os']]},X64"
        _ssh_log(job, f"runner v{RUNNER_VERSION} · 标签 {labels} · 目录 {p['dir']}")
        wipe = bool(p.get("_wipe"))

        if p["os"] == "windows":
            cmd = _ps(_win_install_script(p, token, archive, labels, wipe=wipe))
            rc, out = box.run(cmd, timeout=1800)
        else:
            script = _posix_install_script(p, token, archive, labels, wipe=wipe)
            cmd = f"bash -c {shlex.quote(script)}"
            # 密码走 stdin，不进命令行
            rc, out = box.run(cmd, stdin_text=p["password"], timeout=1800)

        if rc != 0:
            tail = "\n".join((out or "").strip().splitlines()[-3:])
            if rc == 3:
                return False, "这目录已经是一个配好的 runner，没盖它（换个目录再装）"
            if rc == 5:
                return False, "目录内容对不上（没有 runner 的痕迹），为了安全没动它"
            return False, f"安装命令退出码 {rc}：{tail[:300]}"
        return True, ""
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"[:300]
    finally:
        box.close()


async def _run_ssh_add(job: dict[str, Any], p: dict[str, Any]) -> None:
    try:
        ok, err = await asyncio.to_thread(_ssh_precheck, job, p)
        if not ok:
            job.update(status="failed", error=err)
            _ssh_log(job, f"失败：{err}")
            return

        auth = await github_auth()
        if not auth["logged_in"]:
            job.update(status="failed", error=auth["error"] or "gh 未登录")
            _ssh_log(job, "gh 没登录，取不到注册令牌（机器这次什么都没改）")
            return

        # 重名先避掉：主杺名差不多、或同一台机器装第二个，都会撞名。
        # 没避的话 config.sh 会直接报名字已存在，报错信息还很难看懂。
        taken = {n.lower() for n in await _gh_runner_names(p["repo"])}
        if p["name"].lower() in taken:
            base = p["name"]
            for i in range(2, 10):
                candidate = f"{base}-{i}"[:63]
                if candidate.lower() not in taken:
                    p["name"] = candidate
                    _ssh_log(job, f"仓库里已经有叫 {base} 的 runner，这台改叫 {candidate}")
                    break
            else:
                job.update(status="failed", error=f"{base} 这个名字以及 -2..-9 都被占了")
                _ssh_log(job, "名字全被占了，没动 GitHub、也没上机器装")
                return

        _ssh_log(job, f"给 {p['repo']} 要一个注册令牌（1 小时有效）")
        token, terr = await _gh_registration_token(p["repo"])
        if not token:
            job.update(status="failed", error=terr or "GitHub 没给令牌")
            _ssh_log(job, "令牌拿不到，停了（机器这次什么都没改）")
            return
        _ssh_log(job, "令牌拿到了（这里不打印，只当参数传过去）")

        ok, err = await asyncio.to_thread(_ssh_install_blocking, job, p, token)
        if not ok:
            job.update(status="failed", error=err)
            _ssh_log(job, f"失败：{err}")
            return

        SSH_MACHINES[p["name"]] = {
            "host": p["host"], "port": p["port"], "user": p["user"],
            "password": p["password"], "os": p["os"], "dir": p["dir"],
            "repo": p["repo"], "at": time.time(),
        }
        _ssh_log(job, "装完了，等 GitHub 把它标成在线（最多 90 秒）")
        for _ in range(30):
            found = await _gh_runner_by_name(p["repo"], p["name"])
            if found and found.get("status") == "online":
                job.update(status="done", result={
                    "id": found.get("id"), "name": p["name"],
                    "os": p["os"], "status": "online",
                })
                _ssh_log(job, f"在线了：{p['name']}（id {found.get('id')}）")
                return
            await asyncio.sleep(3)
        job.update(status="done", result={"name": p["name"], "os": p["os"], "status": "pending"})
        _ssh_log(job, "注册成功了，但 90 秒内没等到在线——去机器看 _diag/Worker_*.log")
    except asyncio.CancelledError:
        job.update(status="failed", error="任务被取消（服务可能在关闭）")
        _ssh_log(job, "任务被取消")
        raise
    except Exception as exc:                       # noqa: BLE001
        job.update(status="failed", error=f"{type(exc).__name__}: {exc}"[:300])
        _ssh_log(job, f"意外出错：{exc}")



def _ssh_uninstall_blocking(job: dict[str, Any], p: dict[str, Any]) -> tuple[bool, str]:
    box = _box_for(p, job)
    try:
        _ssh_log(job, f"连接 {p['user']}@{p['host']}:{p['port']} …")
        box.connect()
        found = _detect_os(box, job)
        if found and found != p["os"]:
            _ssh_log(job, f"你记的是 {p['os']}，实际是 {found}，按实际的来")
            p["os"] = found
        if p["os"] == "windows":
            cmd = _ps(_win_uninstall_script(p))
            rc, out = box.run(cmd, timeout=300)
        else:
            cmd = "bash -c " + shlex.quote(_posix_uninstall_script(p))
            rc, out = box.run(cmd, stdin_text=p["password"], timeout=300)
        if rc != 0:
            return False, "清理命令退出码 %d：%s" % (rc, (out or "").strip()[-300:])
        return True, ""
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"[:300]
    finally:
        box.close()


async def _run_ssh_remove(job: dict[str, Any], p: dict[str, Any], runner_id: Optional[int]) -> None:
    try:
        ok, err = await asyncio.to_thread(_ssh_uninstall_blocking, job, p)
        if not ok:
            job.update(status="failed", error=err)
            _ssh_log(job, f"机器侧清理失败：{err}")
            return
        _ssh_log(job, "机器侧停服务、装服务、删目录都做完了")

        rid = runner_id
        if rid is None:
            found = await _gh_runner_by_name(p["repo"], p["name"])
            rid = int(found["id"]) if found and found.get("id") else None
        if rid is None:
            SSH_MACHINES.pop(p["name"], None)
            job.update(status="done", result={"name": p["name"], "github": "already-gone"})
            _ssh_log(job, "GitHub 上本来就没有这个注册了")
            return

        rc, out, err = await gh_run([
            "api", "-X", "DELETE", f"{_gh_scope(p['repo'])}/actions/runners/{rid}",
        ], attempts=1)
        if rc != 0:
            job.update(status="failed", error=(err or out).strip()[:300])
            _ssh_log(job, "机器清理了，但删 GitHub 注册失败：" + (err or out).strip()[:200])
            return
        SSH_MACHINES.pop(p["name"], None)
        job.update(status="done", result={"name": p["name"], "github": "removed", "id": rid})
        _ssh_log(job, f"完了：机器清理干净，GitHub 上的注册 {rid} 也删了")
    except asyncio.CancelledError:
        job.update(status="failed", error="任务被取消（服务可能在关闭）")
        _ssh_log(job, "任务被取消")
        raise
    except Exception as exc:                       # noqa: BLE001
        job.update(status="failed", error=f"{type(exc).__name__}: {exc}"[:300])
        _ssh_log(job, f"意外出错：{exc}")


async def handle_runner_ssh_add(request: web.Request) -> web.Response:
    """通过 SSH 把一台机器接进来：下载 runner、注册、装服务、等它上线。"""
    blocked = _local_only(request)
    if blocked:
        return blocked
    if paramiko is None:
        return web.json_response(
            {"error": "服务缺 paramiko，先在 venv 里 pip install paramiko"}, status=501)
    try:
        body = await request.json()
    except Exception:
        body = {}
    if not isinstance(body, dict):
        body = {}
    p, err = _ssh_params(body)
    if err:
        return web.json_response({"error": err}, status=400)
    job = _ssh_new_job("add", f"装机器 @ {p['host']}:{p['port']}")
    _ssh_log(job, f"目标仓库 {p['repo']}，机器名和目录连上去自己定"
                   if not p["name"] else f"目标仓库 {p['repo']}，机器名 {p['name']}")
    job["_task"] = asyncio.create_task(_run_ssh_add(job, p))
    return web.json_response({"job": job["id"], "name": p["name"]})


async def handle_runner_ssh_remove(request: web.Request) -> web.Response:
    """连上机器停服务删目录，再把 GitHub 上的注册删掉。"""
    blocked = _local_only(request)
    if blocked:
        return blocked
    if paramiko is None:
        return web.json_response({"error": "服务缺 paramiko"}, status=501)
    try:
        body = await request.json()
    except Exception:
        body = {}
    if not isinstance(body, dict):
        body = {}
    repo = str(body.get("repo") or "").strip()
    name = str(body.get("name") or "").strip()
    if not _REPO_RE.match(repo):
        return web.json_response({"error": "填 owner/name 走仓库级，只填组织名走组织级"}, status=400)
    if not name:
        return web.json_response({"error": "要说是哪一台"}, status=400)

    known = dict(_machines_live().get(name) or {})
    merged = {**known, **{k: v for k, v in body.items() if v not in (None, "")}}
    p, err = _ssh_params(merged)
    if err:
        return web.json_response(
            {"error": err, "need_creds": "这台机器的连接参数不在内存里（重启过），要重填地址/用户/密码"},
            status=400)
    rid = body.get("id")
    try:
        rid_int = int(rid) if rid not in (None, "") else None
    except (TypeError, ValueError):
        rid_int = None
    job = _ssh_new_job("remove", f"卸 {name} @ {p['host']}:{p['port']}")
    _ssh_log(job, f"从 {p['repo']} 里卸掉 {name}")
    job["_task"] = asyncio.create_task(_run_ssh_remove(job, p, rid_int))
    return web.json_response({"job": job["id"]})


async def handle_runner_ssh_job(request: web.Request) -> web.Response:
    """看一个安装/卸载任务跑到哪了。"""
    blocked = _local_only(request)
    if blocked:
        return blocked
    jid = (request.match_info.get("job") or "").strip()
    job = SSH_JOBS.get(jid)
    if not job:
        return web.json_response({"error": "没这个任务（可能重启过）"}, status=404)
    return web.json_response({
        "id": job["id"], "kind": job["kind"], "title": job["title"],
        "status": job["status"], "error": job["error"], "result": job["result"],
        "lines": job["lines"],
    })


# ─────────────────────────────────────────────────────────────
# 设置：读 / 改中继参数
# ─────────────────────────────────────────────────────────────

# 改完立即生效；其余要重启进程
HOT_FIELDS = ("ws_url", "token", "heartbeat", "app_ping", "reconnect_max")
COLD_FIELDS = ("http_host", "http_port", "db_path")
_LIMITS = {"heartbeat": (5, 600), "app_ping": (30, 3600), "reconnect_max": (5, 600)}


def mask_token(token: str) -> str:
    """只露头尾几位。令牌明文不出本机，也就不出 /config 的响应。"""
    if not token:
        return ""
    if len(token) <= 8:
        return "•" * len(token)
    return f"{token[:4]}••••{token[-4:]}"


def config_public(cfg: Config) -> dict[str, Any]:
    return {
        "ws_url": cfg.ws_url,
        "http_host": cfg.http_host,
        "http_port": cfg.http_port,
        "db_path": cfg.db_path,
        "heartbeat": cfg.heartbeat,
        "app_ping": cfg.app_ping,
        "reconnect_max": cfg.reconnect_max,
        "token_set": bool(cfg.token),
        "token_hint": mask_token(cfg.token),
        "config_path": str(cfg.path) if cfg.path else "",
        "hot_fields": list(HOT_FIELDS),
        "cold_fields": list(COLD_FIELDS),
    }


def validate_patch(patch: dict[str, Any]) -> dict[str, Any]:
    """校验并清洗改动；不合法就抛 ValueError，调用方转成 400。"""
    if not isinstance(patch, dict):
        raise ValueError("请求体要是一个 JSON 对象")
    unknown = set(patch) - set(HOT_FIELDS)
    if unknown:
        raise ValueError("这些字段不支持在线修改：" + "、".join(sorted(unknown)))

    clean: dict[str, Any] = {}
    if "ws_url" in patch:
        url = str(patch["ws_url"]).strip()
        if not url.startswith(("ws://", "wss://")) or len(url) < 10:
            raise ValueError("中继地址要写成 wss://主机/路径")
        clean["ws_url"] = url
    if "token" in patch:
        tok = str(patch["token"]).strip()
        if not tok:
            raise ValueError("令牌不能为空；不想改就别传这个字段")
        if len(tok) > 512:
            raise ValueError("令牌太长了")
        clean["token"] = tok
    for key, (lo, hi) in _LIMITS.items():
        if key not in patch:
            continue
        try:
            val = int(patch[key])
        except (TypeError, ValueError):
            raise ValueError(f"{key} 要填整数") from None
        if not lo <= val <= hi:
            raise ValueError(f"{key} 要在 {lo}~{hi} 之间")
        clean[key] = val
    return clean


def _toml_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    text = str(value).replace("\\", "\\\\").replace('"', '\\"')
    return f'"{text}"'


def save_config(cfg: Config) -> None:
    """把 [relay] 里的值写回文件，其它段落和注释原样保留。

    只动能识别的键，写完先落 .bak，再原子替换，写坏了还能手动恢复。
    """
    if not cfg.path:
        raise RuntimeError("启动时没带配置文件，改不了")
    path = cfg.path
    lines = path.read_text(encoding="utf-8").splitlines() if path.exists() else ["[relay]"]

    start = next((i for i, ln in enumerate(lines) if ln.strip() == "[relay]"), None)
    if start is None:
        lines.append("[relay]")
        start = len(lines) - 1
    end = len(lines)
    for i in range(start + 1, len(lines)):
        if lines[i].lstrip().startswith("["):
            end = i
            break

    wanted = {
        "ws_url": cfg.ws_url,
        "token": cfg.token,
        "http_host": cfg.http_host,
        "http_port": cfg.http_port,
        "db_path": cfg.db_path,
        "heartbeat": cfg.heartbeat,
        "app_ping": cfg.app_ping,
        "reconnect_max": cfg.reconnect_max,
    }
    body = lines[start + 1:end]
    seen: set[str] = set()
    for idx, line in enumerate(body):
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key = stripped.split("=", 1)[0].strip()
        if key in wanted:
            body[idx] = f"{key} = {_toml_value(wanted[key])}"
            seen.add(key)
    body.extend(f"{k} = {_toml_value(wanted[k])}" for k in wanted if k not in seen)
    while body and not body[-1].strip():   # 把空行挤到段末，别让新键跑到空行后面
        body.pop()
    if end < len(lines):
        body.append("")
    lines[start + 1:end] = body

    if path.exists():
        path.with_suffix(path.suffix + ".bak").write_text(
            path.read_text(encoding="utf-8"), encoding="utf-8"
        )
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text("\n".join(lines) + "\n", encoding="utf-8")
    os.replace(tmp, path)


async def handle_config(request: web.Request) -> web.Response:
    return web.json_response(config_public(request.app["cfg"]))


async def handle_config_save(request: web.Request) -> web.Response:
    cfg: Config = request.app["cfg"]
    state: State = request.app["state"]

    try:
        raw = await request.json()
    except Exception:  # noqa: BLE001
        return web.json_response({"error": "请求体不是合法 JSON"}, status=400)
    try:
        patch = validate_patch(raw)
    except ValueError as exc:
        return web.json_response({"error": str(exc)}, status=400)
    if not patch:
        return web.json_response({"error": "没有要改的内容"}, status=400)

    changed = [k for k, v in patch.items() if getattr(cfg, k) != v]
    if not changed:
        return web.json_response({**config_public(cfg), "changed": [], "reconnect": False})

    previous = {k: getattr(cfg, k) for k in changed}
    for key in changed:
        setattr(cfg, key, patch[key])
    try:
        save_config(cfg)
    except OSError as exc:
        for key, value in previous.items():  # 写不进去就回滚内存里的值
            setattr(cfg, key, value)
        return web.json_response({"error": f"写入配置失败：{exc}"}, status=500)

    # 地址或令牌变了就得断掉现有连接，让 ws_loop 用新参数重连
    reconnect = any(k in ("ws_url", "token") for k in changed)
    if reconnect and state.ws is not None:
        state.kick = True
        try:
            await state.ws.close()
        except Exception:  # noqa: BLE001
            pass
    log.info("配置已更新：%s", "、".join(changed))
    return web.json_response(
        {**config_public(cfg), "changed": changed, "reconnect": reconnect}
    )


async def handle_index(request: web.Request) -> web.Response:
    html = read_index()
    if html:
        return web.Response(
            text=html,
            content_type="text/html",
            charset="utf-8",
            headers={"Cache-Control": "no-store"},
        )
    return web.Response(
        text=(
            "gh-relay 本地端（web/index.html 没找到，退化为纯文本）\n\n"
            "  GET /                  状态面板\n"
            "  GET /stream            SSE 事件流（支持 Last-Event-ID 续传）\n"
            "  GET /events?after=N    按游标拉一段历史\n"
            "  GET /stats             统计\n"
            "  GET /health            连接状态与游标\n"
        ),
        content_type="text/plain",
    )


# ─────────────────────────────────────────────────────────────
# 入口
# ─────────────────────────────────────────────────────────────


# ─────────────────────────────────────────────────────────────
# 按需 runner（JIT）
#
# 常驻模式是一台机器绑一个仓库、一直在线；按需模式是「谁有活才起谁」：
#   workflow_job(queued) 事件 → 看它要哪些 label → 从池子里挑一台闲着的机器
#   → 向 GitHub 要一张一次性票 → SSH 过去把进程拉起来 → 跑完自己消失。
#
# 票是一次性的，用完即废，所以没有缓存、也没有「复用」这回事。
# 机器凭据来自 SSH_MACHINES（就是「添加一台机器」填密码那次留下的，30 分钟内有效）。
# ─────────────────────────────────────────────────────────────

JIT_DIRNAME = "jitlab"          # 跟常驻 runner 的 actions-runner 分开放，互不干扰
JIT_HISTORY_MAX = 30

JIT_POOL: dict[str, Any] = {
    "on": False,
    "repo": "",
    "machines": [],             # 参与调度的机器名（对应 SSH_MACHINES 的键）
    "max_parallel": 2,
    "running": {},              # job_id -> {job, machine, name, label, at}
    "history": [],              # 最近的接活记录，新的在前
    "stats": {"picked": 0, "failed": 0, "done": 0, "last_error": ""},
}

_JIT_TASKS: set[asyncio.Task] = set()


def _jit_os(machine: dict[str, Any]) -> str:
    return str(machine.get("os") or "").lower()


def _jit_home_dir(machine: dict[str, Any]) -> str:
    """机器上放 JIT 的那份 runner 的目录。

    Windows 没法用 ~，从常驻 runner 的安装目录推一层上去：
    C:\\Users\\Docker\\actions-runner → C:\\Users\\Docker\\jitlab
    """
    if _jit_os(machine) == "windows":
        base = str(machine.get("dir") or r"C:\actions-runner")
        parent = base.rsplit("\\", 1)[0] if "\\" in base else r"C:\Users"
        return parent.rstrip("\\") + "\\" + JIT_DIRNAME
    return f"~/{JIT_DIRNAME}"


def _jit_can_run(machine: dict[str, Any], labels: list[str]) -> bool:
    """这台机器能不能接这个 job。

    只看两件事：job 要 self-hosted，以及它点名的平台跟机器对得上。
    自定义 label（比如 jit-lab）不用管——票是我们自己发的，带上就是了。
    """
    want = {str(x).lower() for x in labels}
    if "self-hosted" not in want:
        return False
    plat = want & {"linux", "macos", "windows"}
    if plat and _jit_os(machine) not in plat:
        return False
    return True


def _jit_pick(labels: list[str]) -> Optional[dict[str, Any]]:
    """挑一台闲着又能跑这个 job 的机器。"""
    busy = {str(r.get("machine")) for r in JIT_POOL["running"].values()}
    for name in JIT_POOL["machines"]:
        machine = SSH_MACHINES.get(name)
        if not machine or name in busy:
            continue
        if _jit_can_run(machine, labels):
            return {"name": name, **machine}
    return None


def _jit_record(kind: str, **fields: Any) -> None:
    entry = {"kind": kind, "at": time.time(), **fields}
    JIT_POOL["history"].insert(0, entry)
    del JIT_POOL["history"][JIT_HISTORY_MAX:]


async def _jit_ticket(repo: str, name: str, labels: list[str]) -> tuple[str, str]:
    """向 GitHub 要一张一次性票。labels 原样带上 job 要的那些。"""
    args = ["api", "-X", "POST",
            f"{_gh_scope(repo)}/actions/runners/generate-jitconfig",
            "-f", f"name={name}", "-F", "runner_group_id=1",
            "-f", "work_folder=_work"]
    for label in labels:
        args += ["-f", f"labels[]={label}"]
    rc, out, err = await gh_run(args, attempts=2)
    if rc != 0:
        return "", (err or out).strip()[:200]
    try:
        data = json.loads(out or "{}")
    except ValueError:
        return "", "GitHub 返回的不是 JSON"
    cfg = data.get("encoded_jit_config")
    if not cfg:
        return "", "票是空的"
    return str(cfg), ""


async def _jit_launch(machine: dict[str, Any], cfg: str) -> tuple[bool, str]:
    """把票送过去、把进程拉起来。票走 stdin 写文件，不进命令行也不进日志。"""
    box = SshBox(machine["host"], int(machine["port"]), machine["user"],
                 machine["password"], log.info)
    try:
        await asyncio.to_thread(box.connect)
    except Exception as exc:  # noqa: BLE001
        return False, f"连不上：{exc}"

    directory = _jit_home_dir(machine)
    try:
        if _jit_os(machine) == "windows":
            ps = (
                f"$c=(Get-Content -Raw '{directory}\\jit.cfg').Trim(); "
                "foreach($n in @('.runner','.credentials','.credentials_rsaparams')){ "
                f"$p=Join-Path '{directory}' $n; "
                "if(Test-Path -LiteralPath $p){ $i=Get-Item -LiteralPath $p -Force; "
                "$i.IsReadOnly=$false; "
                "Remove-Item -LiteralPath $p -Force -ErrorAction SilentlyContinue } }; "
                f"$line='{directory}\\bin\\Runner.Listener.exe run --jitconfig ' + $c; "
                "Invoke-CimMethod -ClassName Win32_Process -MethodName Create "
                f"-Arguments @{{CommandLine=$line; CurrentDirectory='{directory}'}} | "
                "Select-Object -ExpandProperty ProcessId"
            )
            rc, out = await asyncio.to_thread(
                box.run, f'powershell -NoProfile -Command "{ps}"', None, 60)
            return rc == 0, "" if rc == 0 else (out or "").strip()[:200]

        # 票写进文件：base64 走 stdin 落地，省得处理引号和换行
        b64 = base64.b64encode(cfg.encode("utf-8")).decode("ascii")
        rc, out = await asyncio.to_thread(
            box.run,
            f'cd {directory} && printf %s {shlex.quote(b64)} | base64 -d > jit.cfg && '
            f'wc -c < jit.cfg',
            None, 60, True)
        if rc != 0:
            return False, (out or "").strip()[:200]
        # macOS 没有 setsid，nohup 就能活过 ssh 断开；顺手清掉上次被 kill 的残留
        cmd = (f'cd {directory} && rm -f .runner .credentials .credentials_rsaparams && '
               f'nohup ./run.sh --jitconfig "$(cat jit.cfg)" > run.log 2>&1 < /dev/null & '
               f'echo started')
        rc, out = await asyncio.to_thread(box.run, cmd, None, 60, True)
        return rc == 0, "" if rc == 0 else (out or "").strip()[:200]
    except Exception as exc:  # noqa: BLE001
        return False, f"拉起失败：{exc}"
    finally:
        box.close()


async def _jit_take_job(item: dict[str, Any]) -> None:
    """一个 job 排队了：挑机器、发票、起进程。"""
    payload = item.get("payload") or {}
    job = payload.get("workflow_job") or {}
    labels = [str(x) for x in (job.get("labels") or [])]
    repo = str(item.get("repo") or "")
    jid = str(job.get("id") or "")
    jname = str(job.get("name") or "")

    machine = _jit_pick(labels)
    if not machine:
        JIT_POOL["stats"]["skipped"] += 1
        return

    runner_name = f"jit-{machine['name']}-{jid[-6:]}" if jid else f"jit-{machine['name']}"
    cfg, err = await _jit_ticket(repo, runner_name, labels)
    if not cfg:
        JIT_POOL["stats"]["failed"] += 1
        JIT_POOL["stats"]["last_error"] = err
        _jit_record("failed", job=jname, machine=machine["name"], job_id=jid, error=err)
        return

    ok, err = await _jit_launch(machine, cfg)
    if not ok:
        JIT_POOL["stats"]["failed"] += 1
        JIT_POOL["stats"]["last_error"] = err
        _jit_record("failed", job=jname, machine=machine["name"], job_id=jid, error=err)
        return

    JIT_POOL["running"][jid or runner_name] = {
        "job": jname, "machine": machine["name"], "name": runner_name,
        "labels": labels, "at": time.time(),
    }
    JIT_POOL["stats"]["picked"] += 1
    _jit_record("picked", job=jname, machine=machine["name"], runner=runner_name, job_id=jid)
    log.info("JIT 接活：%s → %s（%s）", jname, machine["name"], runner_name)


async def _jit_on_event(item: dict[str, Any]) -> None:
    """按需模式的入口：只关心「有 job 排队」和「job 结束」。"""
    if not JIT_POOL["on"]:
        return
    if item.get("event") != "workflow_job":
        return
    action = item.get("action")
    payload = item.get("payload") or {}
    job = payload.get("workflow_job") or {}
    jid = str(job.get("id") or "")

    if action == "completed":
        if JIT_POOL["running"].pop(jid, None) is not None:
            JIT_POOL["stats"]["done"] += 1
        return

    if action != "queued":
        return
    if str(item.get("repo") or "") != JIT_POOL["repo"]:
        return
    if len(JIT_POOL["running"]) >= int(JIT_POOL["max_parallel"]):
        JIT_POOL["stats"]["skipped"] += 1
        return

    task = asyncio.create_task(_jit_take_job(item))
    _JIT_TASKS.add(task)
    task.add_done_callback(_JIT_TASKS.discard)


async def jit_loop(bus: Bus) -> None:
    """订阅事件总线，来一条看一条。"""
    queue = bus.subscribe()
    try:
        while True:
            item = await queue.get()
            if item is None:
                break
            try:
                await _jit_on_event(item)
            except Exception as exc:  # noqa: BLE001
                log.warning("JIT 处理事件出错：%s", exc)
                JIT_POOL["stats"]["last_error"] = str(exc)[:200]
    finally:
        bus.unsubscribe(queue)


# ── 接口 ──

def _jit_view() -> dict[str, Any]:
    now = time.time()
    machines = []
    for name in JIT_POOL["machines"]:
        machine = SSH_MACHINES.get(name)
        if not machine:
            continue
        busy = any(r.get("machine") == name for r in JIT_POOL["running"].values())
        machines.append({
            "name": name,
            "os": _jit_os(machine),
            "dir": _jit_home_dir(machine),
            "busy": busy,
        })
    return {
        "on": JIT_POOL["on"],
        "repo": JIT_POOL["repo"],
        "max_parallel": JIT_POOL["max_parallel"],
        "machines": machines,
        "available": sorted(_machines_live().keys()),
        "running": [
            {"job": r["job"], "machine": r["machine"], "name": r["name"],
             "seconds": round(now - float(r["at"]), 1)}
            for r in JIT_POOL["running"].values()
        ],
        "history": JIT_POOL["history"][:12],
        "stats": dict(JIT_POOL["stats"]),
        "pool_size": len(SSH_MACHINES),
    }


async def handle_runner_jit(request: web.Request) -> web.Response:
    blocked = _local_only(request)
    if blocked:
        return blocked
    return web.json_response(_jit_view())


async def handle_runner_jit_start(request: web.Request) -> web.Response:
    blocked = _local_only(request)
    if blocked:
        return blocked
    try:
        body = await request.json()
    except Exception:  # noqa: BLE001
        body = {}
    if not isinstance(body, dict):
        body = {}

    repo = str(body.get("repo") or "").strip()
    if not _REPO_RE.match(repo):
        return web.json_response(
            {"error": "填 owner/name 走仓库级，只填组织名走组织级"}, status=400)

    live = _machines_live()
    wanted = body.get("machines")
    if wanted is None:
        names = list(live.keys())
    else:
        if not isinstance(wanted, list):
            return web.json_response({"error": "machines 要是数组"}, status=400)
        names = [str(x) for x in wanted]
        missing = [n for n in names if n not in live]
        if missing:
            return web.json_response(
                {"error": f"这些机器没凭据或已过期：{', '.join(missing[:3])}"}, status=400)
    try:
        parallel = int(body.get("max_parallel") or 2)
    except (TypeError, ValueError):
        return web.json_response({"error": "max_parallel 要是数字"}, status=400)
    if parallel < 1 or parallel > 16:
        return web.json_response({"error": "max_parallel 要在 1~16 之间"}, status=400)

    if not names:
        return web.json_response(
            {"error": "池子是空的：先用「添加一台机器」把机器连一遍，密码只在 30 分钟内有效"},
            status=400)

    JIT_POOL.update(on=True, repo=repo, machines=names, max_parallel=parallel)
    JIT_POOL["stats"]["last_error"] = ""
    log.info("JIT 开启：仓库 %s，机器 %s，并发 %d", repo, names, parallel)
    return web.json_response(_jit_view())


async def handle_runner_jit_stop(request: web.Request) -> web.Response:
    blocked = _local_only(request)
    if blocked:
        return blocked
    JIT_POOL.update(on=False)
    log.info("JIT 关闭")
    return web.json_response(_jit_view())


async def handle_runner_jit_prepare(request: web.Request) -> web.Response:
    """给一台机器准备一份独立的 JIT 目录（从常驻那份复制二进制，不重新下载）。"""
    blocked = _local_only(request)
    if blocked:
        return blocked
    try:
        body = await request.json()
    except Exception:  # noqa: BLE001
        body = {}
    if not isinstance(body, dict):
        body = {}
    name = str(body.get("name") or "").strip()
    machine = _machines_live().get(name)
    if not machine:
        return web.json_response(
            {"error": "这台机器没有凭据（没连过，或者密码已过期）"}, status=400)

    job = _ssh_new_job("jit-prepare", f"准备 JIT 目录：{name}")

    async def work() -> None:
        directory = _jit_home_dir(machine)
        source = str(machine.get("dir") or "")
        if not source:
            job.update(status="failed", error="不知道常驻 runner 装在哪")
            return
        _ssh_log(job, f"从 {source} 复制一份到 {directory}（不重新下载）")
        box = SshBox(machine["host"], int(machine["port"]), machine["user"],
                     machine["password"], lambda t: _ssh_log(job, t))
        try:
            await asyncio.to_thread(box.connect)
            if _jit_os(machine) == "windows":
                cmd = (
                    f'powershell -NoProfile -Command "'
                    f'if (Test-Path \'{directory}\') {{ Remove-Item \'{directory}\' '
                    f'-Recurse -Force -ErrorAction SilentlyContinue }}; '
                    f'Copy-Item \'{source}\' \'{directory}\' -Recurse -Force; '
                    f'foreach($n in @(\'_work\',\'_diag\')) {{ '
                    f'Remove-Item (Join-Path \'{directory}\' $n) -Recurse -Force '
                    f'-ErrorAction SilentlyContinue }}; '
                    f'foreach($n in @(\'.runner\',\'.credentials\',\'._credentials_rsap\','
                    f'\'.credentials_rsaparams\',\'.service\')) {{ '
                    f'$p=Join-Path \'{directory}\' $n; '
                    f'if (Test-Path -LiteralPath $p) {{ $i=Get-Item -LiteralPath $p -Force; '
                    f'$i.IsReadOnly=$false; '
                    f'Remove-Item -LiteralPath $p -Force -ErrorAction SilentlyContinue }} }}; '
                    f'Write-Output (\'run.cmd: \' + (Test-Path (Join-Path \'{directory}\' '
                    f'\'bin\\Runner.Listener.exe\')))"'
                )
            else:
                keep = " ".join(f"--exclude={x}" for x in (
                    "_work", "_diag", ".runner", ".credentials",
                    ".credentials_rsaparams", ".service"))
                cmd = (f'rm -rf {directory} && mkdir -p {directory} && '
                       f'tar -C {source} {keep} -cf - . 2>/dev/null | '
                       f'tar -C {directory} -xf - && ls {directory}/run.sh')
            rc, out = await asyncio.to_thread(box.run, cmd, None, 600, True)
            if rc != 0:
                job.update(status="failed", error=(out or "").strip()[:300])
                _ssh_log(job, "失败：" + (out or "").strip()[:200])
                return
            job.update(status="done", result={"name": name, "dir": directory})
            _ssh_log(job, f"好了，以后这台机器按需起 runner 就用 {directory}")
        except Exception as exc:  # noqa: BLE001
            job.update(status="failed", error=str(exc)[:300])
            _ssh_log(job, f"失败：{exc}")
        finally:
            box.close()

    task = asyncio.create_task(work())
    _JIT_TASKS.add(task)
    task.add_done_callback(_JIT_TASKS.discard)
    return web.json_response({"job": job["id"], "kind": job["kind"]})


def build_app(cfg: Config, store: Store, bus: Bus, state: State) -> web.Application:
    app = web.Application()
    app["store"] = store
    app["bus"] = bus
    app["state"] = state
    app["cfg"] = cfg
    app.router.add_get("/", handle_index)
    app.router.add_get("/stream", handle_stream)
    app.router.add_get("/events", handle_events)
    app.router.add_get("/stats", handle_stats)
    app.router.add_get("/actions", handle_actions)
    app.router.add_get("/github/auth", handle_github_auth)
    app.router.add_get("/github/runs", handle_github_runs)
    app.router.add_get("/github/jobs", handle_github_jobs)
    app.router.add_get("/github/runners", handle_github_runners)
    app.router.add_post("/github/runners/remove", handle_github_runner_remove)
    app.router.add_post("/runners/ssh/add", handle_runner_ssh_add)
    app.router.add_post("/runners/ssh/remove", handle_runner_ssh_remove)
    app.router.add_get("/runners/ssh/job/{job}", handle_runner_ssh_job)
    app.router.add_get("/runners/jit", handle_runner_jit)
    app.router.add_post("/runners/jit/start", handle_runner_jit_start)
    app.router.add_post("/runners/jit/stop", handle_runner_jit_stop)
    app.router.add_post("/runners/jit/prepare", handle_runner_jit_prepare)
    app.router.add_get("/health", handle_health)
    app.router.add_get("/config", handle_config)
    app.router.add_post("/config", handle_config_save)
    return app


async def main_async(cfg: Config) -> None:
    store = Store(cfg.db_path)
    bus = Bus()
    state = State()
    stop = asyncio.Event()

    app = build_app(cfg, store, bus, state)
    # shutdown_timeout 放小：正常的退出路径是 bus.close_all() 把 SSE 推开，
    # 这里只是兵底，防着某个 handler 卡住把 Ctrl-C 拖成一分钟。
    runner = web.AppRunner(app, access_log=None, shutdown_timeout=3)
    await runner.setup()
    site = web.TCPSite(runner, cfg.http_host, cfg.http_port)
    await site.start()
    log.info("本地 SSE 监听 http://%s:%s/stream", cfg.http_host, cfg.http_port)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:  # Windows
            pass

    task = asyncio.create_task(ws_loop(cfg, store, bus, state, stop))
    jit_task = asyncio.create_task(jit_loop(bus))
    try:
        await stop.wait()
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        jit_task.cancel()
        try:
            await jit_task
        except asyncio.CancelledError:
            pass
        bus.close_all()
        await runner.cleanup()
        store.close()
        log.info("已退出")


def main() -> None:
    parser = argparse.ArgumentParser(description="gh-relay 本地接收端")
    parser.add_argument("-c", "--config", default="config.toml", help="配置文件路径")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S",
    )

    cfg = Config.load(Path(args.config))
    asyncio.run(main_async(cfg))


if __name__ == "__main__":
    main()

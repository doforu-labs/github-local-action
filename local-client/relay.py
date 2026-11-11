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
import json
import logging
import os
import signal
import sqlite3
import time
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
    try:
        await stop.wait()
    finally:
        task.cancel()
        try:
            await task
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

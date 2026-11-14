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

端口默认 18091。
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import glob
import json
import logging
import os
import re
import secrets
import shutil
import shlex
import signal
import sqlite3
import sys
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional, Set
from urllib.parse import urlparse
from collections import OrderedDict

import aiohttp
from aiohttp import web

try:  # Python 3.11+
    import tomllib
except ModuleNotFoundError:  # pragma: no cover
    tomllib = None  # type: ignore

log = logging.getLogger("relay")

# 状态页：按 mtime 缓存，改完 HTML 刷新浏览器即可生效，不用重启进程
#
# 为什么要在两个地方找：从源码目录跑时 web/ 就在旁边；`pip install` 之后
# 它被装到 <prefix>/share/github-local-action/web（见 pyproject.toml 的 data-files），
# 源码目录里根本没有这个子目录。
def _data_roots() -> tuple[Path, ...]:
    here = Path(__file__).resolve().parent
    roots = [here, here.parent]
    for prefix in (sys.prefix, sys.base_prefix):
        roots.append(Path(prefix) / "share" / "github-local-action")
    return tuple(roots)


def _web_dir() -> Path:
    for root in _data_roots():
        if (root / "web" / "index.html").is_file():
            return root / "web"
    return Path(__file__).resolve().parent / "web"


WEB_DIR = _web_dir()
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
    # 都有默认值：没配置文件时也得能起来（不然「使用默认值」那句日志就是谎话，
    # 实际会 TypeError 直接退出）。容器里第一次跑 data 目录是空的，全靠这个。
    ws_url: str = ""
    token: str = ""
    http_host: str = "0.0.0.0"
    http_port: int = 18091
    db_path: str = "events.db"
    heartbeat: int = 30       # 协议层 ping：防 NAT / 代理回收空闲连接
    app_ping: int = 120       # 应用层 ping：顺带确认对端活着
    reconnect_max: int = 60   # 重连退避上限（秒）
    # 装 runner 时要写进机器上 .env 的代理（空 = 直连）。自建机器常常连不上或极慢地
    # 连 github.com，这里填一次，以后每台新机器装 runner 都自动带上，不用逐台手改。
    proxy: str = ""
    # 容器里跑时，宿主机浏览器过来的连接源地址是 docker 网桥网关（如 172.17.0.1）或
    # 宿主在网桥里的地址，不是 127.0.0.1，于是「只许本机」的那些接口会把宿主机也当外
    # 人（403）。这里把可信的地址显式列出来；默认空表 = 只有真回环算本机，行为不变。
    # 只从配置文件读，不开给 /config 接口——闸的名单不该能在线改。
    trusted_peers: tuple[str, ...] = ()
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
        # 容器环境变量也能列可信地址：第一次跑时配置文件里还没有这一行，
        # 而这时又需要先从浏览器把 ws_url / 令牌填进去。两个来源合并。
        extra = [p.strip() for p in os.environ.get("GLA_TRUSTED_PEERS", "").split(",") if p.strip()]
        if extra:
            cfg.trusted_peers = tuple(dict.fromkeys((*cfg.trusted_peers, *extra)))
        return cfg


# ─────────────────────────────────────────────────────────────
# 本地库
# ─────────────────────────────────────────────────────────────


class Memo:
    """短命结果缓存 + 单飞：并发打同一个接口只算一次。

    为什么需要：这些查询是 CPU 活（解几千条事件的 JSON），开线程并不能
    并行——GIL 一次只给一个线程。实测在飞 ≥16 个 /stats 时，线程们互相争
    GIL，事件循环照样拿不到，/health 尖峰从 5ms 又回到 500ms。
    既然同样一个结果被重复算了很多次，那就只算一次。

    失效不靠时间：键里带 store.version（每条新事件 +1），所以新事件一入
    库下一次就是新的。TTL 只是兜底防内存长胖。
    """

    def __init__(self, ttl: float = 300.0, max_entries: int = 64):
        self.ttl = ttl
        self.max_entries = max_entries
        self._data: dict[tuple[Any, ...], tuple[float, Any]] = {}
        self._lock = threading.Lock()
        self.hits = 0
        self.misses = 0

    def get_or_compute(self, key: tuple[Any, ...], fn: Callable[[], Any]) -> Any:
        now = time.monotonic()
        hit = self._data.get(key)
        if hit and hit[0] > now:
            self.hits += 1
            return hit[1]
        with self._lock:  # 同一时刻只让一个线程真的去算
            hit = self._data.get(key)
            if hit and hit[0] > now:
                self.hits += 1
                return hit[1]
            self.misses += 1
            value = fn()
            if len(self._data) >= self.max_entries:
                # 清掉过期的；还满就清空（这个缓存不是关键路径）
                keep = {k: v for k, v in self._data.items() if v[0] > now}
                self._data = keep if len(keep) < self.max_entries else {}
            self._data[key] = (now + self.ttl, value)
            return value


MEMO = Memo()
# 事件回放的缓存单独一份：一条带 200 个事件的结果可能几百 KB，
# 条数得掐得比聚合结果更紧，不然库放不下。
MEMO_EVENTS = Memo(ttl=30.0, max_entries=8)
EVENTS_MEMO_MAX_LIMIT = 1000


class Store:
    """SQLite 落地。id 用远端的自增 id 当主键，天然幂等。"""

    def __init__(self, path: str):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self._local = threading.local()
        # 每插一条事件就 +1，用作聚合缓存的版本号
        self.version = 0
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
            CREATE TABLE IF NOT EXISTS gh_cache (
              key  TEXT PRIMARY KEY,
              body TEXT NOT NULL,
              at   INTEGER NOT NULL
            );
            """
        )
        self.conn.commit()
        # 游标放内存：/health 每两秒被轮一次，让它去读库没意义，
        # 高峰期它还得排在其它查询后面等（实测 1ms 变 140ms）。
        self._cursor = self._read_cursor()

    def _ro(self) -> sqlite3.Connection:
        """读用连接：每个线程一条。

        WAL 下多读本可以真并行，但共用一条连接会把并发读排成一队，
        12 个并发 /stats 就能把 /health 拖到 140ms。写入仍然走 self.conn。
        """
        conn = getattr(self._local, "ro", None)
        if conn is None:
            conn = sqlite3.connect(self.path, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA query_only = ON")
            self._local.ro = conn
        return conn

    def query(self, sql: str, params: tuple[Any, ...] = ()) -> list[sqlite3.Row]:
        """只读查询（可以放心在别的线程里调）。"""
        return self._ro().execute(sql, params).fetchall()

    def _read_cursor(self) -> int:
        row = self.conn.execute("SELECT value FROM meta WHERE key='cursor'").fetchone()
        if row:
            return int(row["value"])
        row = self.conn.execute("SELECT COALESCE(MAX(id), 0) AS m FROM events").fetchone()
        return int(row["m"])

    def cursor(self) -> int:
        return self._cursor

    # ── GitHub 那边取回来的东西（job 列表、日志）:存一份，第二次直接给 ──
    #
    # 只缓存「不会再变」的结果：job 全部完成的运行、能取到的日志（GitHub 在
    # job 没跑完时给 404，所以能取到就意味着跑完了）。还在跑的运行不读缓存，
    # 每次重取——那才是「同步」。
    def cache_get(self, key: str) -> Optional[tuple[str, int]]:
        row = self._ro().execute(
            "SELECT body, at FROM gh_cache WHERE key = ?", (key,)
        ).fetchone()
        if not row:
            return None
        return str(row["body"]), int(row["at"])

    def cache_put(self, key: str, body: str) -> None:
        self.conn.execute(
            "INSERT INTO gh_cache (key, body, at) VALUES (?, ?, ?) "
            "ON CONFLICT(key) DO UPDATE SET body=excluded.body, at=excluded.at",
            (key, body, int(time.time())),
        )
        # 别让库无限长：只留最近 400 条（日志一条最多 2MB，400 条封顶）
        self.conn.execute(
            "DELETE FROM gh_cache WHERE key NOT IN "
            "(SELECT key FROM gh_cache ORDER BY at DESC LIMIT 400)"
        )
        self.conn.commit()

    def set_cursor(self, value: int) -> None:
        self._cursor = int(value)
        self.conn.execute(
            "INSERT INTO meta(key, value) VALUES('cursor', ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (str(value),),
        )
        self.conn.commit()

    # ── 杂项配置：重启以后还得记住的东西（按需池子开在哪些仓库上）──
    def meta_get(self, key: str, default: str = "") -> str:
        row = self.conn.execute(
            "SELECT value FROM meta WHERE key = ?", (key,)
        ).fetchone()
        return str(row["value"]) if row else default

    def meta_set(self, key: str, value: str) -> None:
        self.conn.execute(
            "INSERT INTO meta(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
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
            self.version += 1
            if int(ev["id"]) > self._cursor:
                self._cursor = int(ev["id"])
            return True
        return False

    # ── 给状态页用的统计 ──

    def count_since(self, ts_ms: int) -> int:
        row = self._ro().execute(
            "SELECT COUNT(*) AS n FROM events WHERE at >= ?", (ts_ms,)
        ).fetchone()
        return int(row["n"])

    def count_all(self) -> int:
        row = self._ro().execute("SELECT COUNT(*) AS n FROM events").fetchone()
        return int(row["n"])

    def run_count(self, since_ms: int | None = None) -> int:
        """一共跑过多少次（workflow run）。

        一次运行会推 requested / in_progress / completed 好几条 event，按
        workflow_run.id 去重才是运行次数——跟动作页那个总数对得上。
        这里不用「构建」：workflow 里跑的不一定都是构建（探针、语法检查、
        发布都算），也不叫「动作」：Actions 里的 Action 指的是可复用的步骤。
        """
        sql = "SELECT COUNT(DISTINCT json_extract(payload, '$.workflow_run.id')) AS n " \
              "FROM events WHERE event = 'workflow_run' " \
              "AND json_extract(payload, '$.workflow_run.id') IS NOT NULL"
        params: tuple[Any, ...] = ()
        if since_ms is not None:
            sql += " AND at >= ?"
            params = (since_ms,)
        try:
            row = self._ro().execute(sql, params).fetchone()
            return int(row["n"])
        except sqlite3.OperationalError:
            # 很老的 SQLite 没有 json_extract：退回在 Python 里数
            q = "SELECT payload FROM events WHERE event = 'workflow_run'"
            if since_ms is not None:
                q += " AND at >= ?"
            ids = set()
            for row in self._ro().execute(q, params).fetchall():
                try:
                    rid = (json.loads(row["payload"] or "{}").get("workflow_run") or {}).get("id")
                except ValueError:
                    continue
                if rid is not None:
                    ids.add(rid)
            return len(ids)

    def hourly(self, start_ms: int) -> dict[str, int]:
        rows = self._ro().execute(
            "SELECT strftime('%Y-%m-%d %H', at/1000, 'unixepoch', 'localtime') AS h, "
            "COUNT(*) AS n FROM events WHERE at >= ? GROUP BY h",
            (start_ms,),
        ).fetchall()
        return {r["h"]: int(r["n"]) for r in rows if r["h"]}

    def type_counts(self, limit: int = 8) -> list[dict[str, Any]]:
        rows = self._ro().execute(
            "SELECT event AS k, COUNT(*) AS n FROM events GROUP BY event ORDER BY n DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return [{"type": r["k"], "count": int(r["n"])} for r in rows]

    # ── 账号与仓库：直接从 payload 里取，不额外存字段 ──

    def repos(self) -> list[dict[str, Any]]:
        rows = self._ro().execute(
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
        type_rows = self._ro().execute(
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
        row = self._ro().execute(
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

        av = self._ro().execute(
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

    def replay(self, after: int, limit: int = 500, *, latest: bool = False) -> list[dict[str, Any]]:
        # 游标续读用升序；页面首次打开取末尾 N 条，再按升序交给调用者。
        order = "DESC" if latest else "ASC"
        rows = self._ro().execute(
            "SELECT id, event, repo, action, at, payload FROM events "
            f"WHERE id > ? ORDER BY id {order} LIMIT ?",
            (after, limit),
        ).fetchall()
        if latest:
            rows.reverse()
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
    delay = 1
    warned_unconfigured = False

    timeout = aiohttp.ClientTimeout(total=None, sock_connect=15)
    connector = aiohttp.TCPConnector(force_close=False, enable_cleanup_closed=True)

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        while not stop.is_set():
            # 还没填地址：拿空串去连只会每轮报一次 InvalidURL。停下来等人在设置页填，
            # 提示只打一次——daemon 日志里每 15 秒刷一行没必要。
            if not cfg.ws_url:
                if not warned_unconfigured:
                    log.warning("还没有配置中继地址：打开 http://%s:%s 在设置页填 ws_url 与令牌",
                                cfg.http_host, cfg.http_port)
                    warned_unconfigured = True
                try:
                    await asyncio.wait_for(stop.wait(), timeout=15)
                except asyncio.TimeoutError:
                    pass
                continue
            # 地址和令牌每轮重算：它们可以热改（POST /config），算在循环外的话
            # 改了地址只有重启进程才生效，日志却已经显示新地址，排查时极度误导。
            url = f"{cfg.ws_url}{'&' if '?' in cfg.ws_url else '?'}token={cfg.token}"
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
        # 一页只有 500 条；必须补完所有页才能转入实时等待。
        while True:
            batch = await _offload(store.replay, sent_max)
            for ev in batch:
                await resp.write(_sse_frame(ev))
                sent_max = max(sent_max, int(ev["id"]))
            if len(batch) < 500:
                break

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


async def _offload(fn: Callable[..., Any], *args: Any) -> Any:
    """把同步的库查询丢到线程池。

    这些查询要读几千行事件并解 JSON（约 20~40ms）。直接在事件循环里做，
    一次并发就能把整个服务卡住：SSE 心跳发不出去、/health 从 1ms 变几百 ms。
    实测 12 个并发 /stats 能把 /health 拖到 477ms，就是这么来的。
    """
    return await asyncio.get_running_loop().run_in_executor(None, fn, *args)


async def handle_events(request: web.Request) -> web.Response:
    store: Store = request.app["store"]
    after = _after_from(request)
    try:
        limit = min(1000, max(1, int(request.query.get("limit", "200"))))
    except ValueError:
        limit = 200
    latest = request.query.get("latest") == "1"
    events = await _offload(
        MEMO_EVENTS.get_or_compute if limit <= EVENTS_MEMO_MAX_LIMIT else (lambda _k, fn: fn()),
        ("events", store.version, after, limit, latest),
        lambda: store.replay(after, limit, latest=latest))
    return web.json_response({"events": events, "after": after})


async def handle_health(request: web.Request) -> web.Response:
    state: State = request.app["state"]
    store: Store = request.app["store"]
    bus: Bus = request.app["bus"]
    gap = int(time.time() - state.last_event_at) if state.last_event_at else None
    # cursor 也是一次库查询。高峰期它要排在别的请求后面等连接锁，
    # 直接在循环里等会把「本应 1ms」的健康检查拖到上百 ms。
    cursor = await _offload(store.cursor)
    return web.json_response(
        {
            "ok": state.connected,
            "connected": state.connected,
            "cursor": cursor,
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
    midnight_ms = int(midnight.timestamp() * 1000)
    repos = store.repos()
    return {
        "today": store.count_since(midnight_ms),
        "total": store.count_all(),
        # 运行（workflow run）的个数，跟「动作」页的总数一个口径
        "runs_today": store.run_count(midnight_ms),
        "runs_total": store.run_count(),
        "hourly": hourly,
        "by_type": store.type_counts(),
        "account": store.account(),
        "repos": repos,
        "repo_count": len(repos),
    }


async def handle_stats(request: web.Request) -> web.Response:
    store: Store = request.app["store"]
    data = await _offload(
        MEMO.get_or_compute, ("stats", store.version), lambda: build_stats(store))
    return web.json_response(data)


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


_RUN_STATUS_RANK = {"queued": 1, "in_progress": 2, "completed": 3}


def _newer_run_state(old: dict[str, Any], wr: dict[str, Any]) -> bool:
    """同一个 run 来了第二条事件：哪条才是「更新的状态」？

    不能按到达顺序算。GitHub 的投递会重试、会乱序，实测过这种：先到
    completed/cancelled，后面又补了一条 in_progress，而且两条的 updated_at
    一模一样（同一个时刻推了两遍，状态字段退回去了）。按到达顺序取，
    这条运行就永远停在「进行中」。

    所以先看载荷里 GitHub 自己盖的 updated_at；时间戳一样时再看状态谁更靠后——
    completed 永远比 in_progress 可信，因为它不会自己倒退。
    """
    new_at = _iso_ms(wr.get("updated_at"))
    old_at = int(old.get("updated_at") or 0)
    if new_at != old_at:
        return new_at > old_at
    return _RUN_STATUS_RANK.get(str(wr.get("status") or ""), 0) >= \
        _RUN_STATUS_RANK.get(str(old.get("status") or ""), 0)


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
    rows = store.query(sql, tuple(params))

    runs: dict[int, dict[str, Any]] = {}
    for row in rows:
        try:
            payload = json.loads(row["payload"] or "{}")
        except ValueError:
            continue
        wr = payload.get("workflow_run") or {}
        run_id = wr.get("id")
        if run_id is None:
            continue
        prev = runs.get(run_id)
        if prev is not None and not _newer_run_state(prev, wr):
            continue  # 迟到的旧快照，不能盖掉手上这份

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
            # 提交本身的提交时间（≠ 运行开始时间）。workflow_dispatch / 重跑时
            # 两者能差好几天，卡片上只写运行时间会让人以为提交是刚提的。
            "commit_at": _iso_ms(commit.get("timestamp")),
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
    store: Store = request.app["store"]
    data = await _offload(
        MEMO.get_or_compute, ("actions", store.version, repo, limit),
        lambda: build_actions(store, repo, limit))
    return web.json_response(data)


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


def _default_gateway(text: Optional[str] = None) -> str:
    """容器里的默认网关 = 宿主机在 docker 网桥上的地址（一般是 172.17.0.1）。

    为什么要它：容器内看到的浏览器来源地址就是这个网关，不是 127.0.0.1。让人去
    手查这个 IP 太不友好，所以配置里可以直接写 `"gateway"`，启动时自己算。

    读 /proc/net/route（Linux 容器里一定有；macOS 上没这个文件，返回空串）：
    Iface Destination Gateway Flags RefCnt Use Metric Mask ...
    Destination 为 00000000 且 Flags 含 0x2 的就是默认路由，Gateway 是小端十六进制。
    """
    try:
        raw_lines = (text if text is not None else
                     Path("/proc/net/route").read_text(encoding="utf-8")).splitlines()[1:]
    except OSError:
        return ""
    for row in raw_lines:
        parts = row.split()
        if len(parts) < 4 or parts[1] != "00000000":
            continue
        try:
            flags = int(parts[3], 16)
            packed = int(parts[2], 16)
        except ValueError:
            continue
        if flags & 0x2:
            return ".".join(str((packed >> shift) & 0xFF) for shift in (0, 8, 16, 24))
    return ""


_gateway_cache: dict[str, str] = {"ip": ""}


def container_gateway() -> str:
    """默认网关算一次就够，失败也不重试（容器里路由不会中途变）。"""
    if not _gateway_cache["ip"]:
        _gateway_cache["ip"] = _default_gateway()
    return _gateway_cache["ip"]


def trusted_peers(request: web.Request) -> tuple[str, ...]:
    """配置里列的自家人地址；`"gateway"` 是一个令牌，不是 IP。"""
    try:
        return tuple(request.app["cfg"].trusted_peers or ())
    except (KeyError, AttributeError, TypeError):
        return ()              # 测试里的假 request 没有 app


def is_local(request: web.Request) -> bool:
    """这个请求算不算「跑接收端的那台机器上发起的」。

    除了真回环，配置里的 `trusted_peers` 也算——容器里跑时宿主机的浏览器从网桥
    网关过来，不认这个就整个设置页/机器管理都 403。
    """
    remote = request.remote
    if remote in ("127.0.0.1", "::1", "::ffff:127.0.0.1"):
        return True
    peers = trusted_peers(request)
    if not peers:
        return False
    if remote in peers:
        return True
    return "gateway" in peers and remote == container_gateway()


def _local_only(
    request: web.Request,
    reason: str = "GitHub 凭据只对本机开放；局域网设备请用 webhook 那套数据",
) -> Optional[web.Response]:
    if is_local(request):
        return None
    return web.json_response({"error": reason}, status=403)


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
        # gh run list 不返回提交时间，API 拉回来的行就没有这个值（卡片上不显示）
        "commit_at": 0,
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


# job 说自己还在跑、但那台 runner 早就空了 —— GitHub 的状态回写会滞后。
# 2026-11-11 webhooks 事故之后实测过两段：开跑时 queued→in_progress 晚 4~5 分钟，
# 跑完时 completed 也晚 1 分钟以上（机器日志 21:04:03 已经写着 Succeeded，
# 页面上那个 job 还挂着 in_progress）。界面上照 job 状态写「进行中」，人会以为卡死了。
# 所以拿 runner 的忙闲做一次交叉校验，对不上就单独标出来：卡的是 GitHub，不是任务。
_STALE_GRACE_MS = 45_000      # 刚跑完那一下 runner 也会先变空闲，留 45 秒缓冲别误报


async def _mark_stale_jobs(jobs: list[dict[str, Any]], repo: str) -> None:
    """给「说在跑但机器已经空了」的 job 打 stale 标记。就地改，失败就算了。

    只标 in_progress：queued 且已经派到某台 runner 上是很正常的一瞬（拿到活正要开干），
    标它等于天天喊狼来了。
    """
    pending = [j for j in jobs
               if j.get("status") == "in_progress" and j.get("runner_name")]
    if not pending:
        return
    try:
        # 这个接口本来每 8 秒就为机器页问一次，缓存命中时基本不要钱
        runners, _err = await _repo_runners(repo)
    except Exception as exc:                       # noqa: BLE001
        log.info("交叉校验 runner 忙闲失败（不影响看 job）：%s", exc)
        return
    known = {str(r.get("name") or "").lower(): r for r in runners if r.get("name")}
    now_ms = int(time.time() * 1000)
    for job in pending:
        runner = known.get(str(job.get("runner_name") or "").lower())
        if runner is None:
            continue
        started = _iso_ms(job.get("started_at"))
        if started and now_ms - started < _STALE_GRACE_MS:
            continue
        # busy=false = 机器上已经没有 worker 了；offline = runner 根本没连上。
        # 这两种情况都不该还有 job 挂在 in_progress。
        if not runner.get("busy"):
            job["stale"] = "runner_idle"
        elif str(runner.get("status") or "") != "online":
            job["stale"] = "runner_offline"


async def handle_github_jobs(request: web.Request) -> web.Response:
    """一次运行里的 job 与每一步状态——这个只有 API 有，webhook 不给。

    job 一旦全部跑完就不再变，所以落库；第二次问同一个运行直接给库里的，
    不再去 GitHub。还在跑的运行不读缓存，每次都重取，前端那 5 秒一轮的
    刷新才真的是「同步」。
    """
    blocked = _local_only(request)
    if blocked:
        return blocked
    repo = (request.query.get("repo") or "").strip()
    run_id = (request.query.get("run") or "").strip()
    if not _REPO_RE.match(repo):
        return web.json_response({"error": "填 owner/name 走仓库级，只填组织名走组织级"}, status=400)
    if not run_id.isdigit():
        return web.json_response({"error": "run 要是数字 id"}, status=400)

    store: Store = request.app["store"]
    key = f"jobs:{repo}:{run_id}"
    hit = store.cache_get(key)
    if hit:
        body, at = hit
        try:
            data = json.loads(body)
        except ValueError:
            data = None
        if isinstance(data, dict) and data.get("jobs") and all(
                j.get("status") == "completed" for j in data["jobs"]):
            data["cached"] = True
            data["cached_at"] = at
            return web.json_response(data)

    auth = await github_auth()
    if not auth["logged_in"]:
        return web.json_response({"error": auth["error"] or "gh 未登录"}, status=409)

    rc, out, err = await gh_run([
        "api", f"repos/{repo}/actions/runs/{run_id}/jobs", "--jq",
        "{total: .total_count, jobs: [.jobs[] | {id, name, status, conclusion, "
        "started_at, completed_at, html_url, runner_name, "
        "steps: [.steps[] | {number, name, status, conclusion, started_at, completed_at}]}]}",
    ])
    if rc != 0:
        return web.json_response({"error": (err or out).strip()[:300]}, status=502)
    try:
        data = json.loads(out or "{}")
    except ValueError:
        return web.json_response({"error": "gh 返回的不是 JSON"}, status=502)
    if data.get("jobs"):
        store.cache_put(key, json.dumps(data, ensure_ascii=False))
        # 标记放在落库之后：stale 是「此刻机器空没空」的判断，不能跟着 job 一起被
        # 缓存下来（缓存只在全都跑完时才读，那时候它已经是过去的真相了）
        await _mark_stale_jobs(data["jobs"], repo)
    return web.json_response(data)


# 日志正文带终端颜色码（\x1b[32m 这类），先剥掉再给前端，
# 否则 pre 里会显示成一堆乱码一样的符号。
_ANSI_RE = re.compile(r"\x1b\[[0-9;?]*[A-Za-z]|\x1b\][^\x07]*\x07|\r")


def _strip_ansi(text: str) -> str:
    return _ANSI_RE.sub("", text)


async def handle_github_joblog(request: web.Request) -> web.Response:
    """单个 job 的完整日志（纯文本）。

    日志不在 webhook 里，也不在 jobs 接口里：要 /actions/jobs/{id}/logs，
    它一般 302 到一个签名直链，直链上是纯文本。签名链不需要再带令牌，
    所以这里只把 location 拿出来自己去取——gh 的令牌始终不出 gh。
    job 还在跑时 GitHub 直接 404，日志压根还没生成。
    """
    blocked = _local_only(request)
    if blocked:
        return blocked
    repo = (request.query.get("repo") or "").strip()
    job_id = (request.query.get("job") or "").strip()
    if not _REPO_RE.match(repo):
        return web.json_response({"error": "填 owner/name 走仓库级，只填组织名走组织级"}, status=400)
    if not job_id.isdigit():
        return web.json_response({"error": "job 要是数字 id"}, status=400)

    store: Store = request.app["store"]
    key = f"log:{job_id}"
    hit = store.cache_get(key)
    if hit:
        body, at = hit
        try:
            data = json.loads(body)
        except ValueError:
            data = None
        if isinstance(data, dict) and "text" in data:
            data["cached"] = True
            data["cached_at"] = at
            return web.json_response(data)

    auth = await github_auth()
    if not auth["logged_in"]:
        return web.json_response({"error": auth["error"] or "gh 未登录"}, status=409)

    rc, out, err = await gh_run([
        "api", "-i", "--allow-escape-sequences",
        f"repos/{repo}/actions/jobs/{job_id}/logs"])
    if rc != 0:
        msg = (err or out).strip()
        if "404" in msg:
            return web.json_response(
                {"error": "GitHub 还没生成这个 job 的日志（要跑完才有）", "pending": True},
                status=404)
        return web.json_response({"error": msg[:300]}, status=502)

    # gh api -i 的输出是「状态行 + 头 + 空行 + 体」
    cut = out.find("\r\n\r\n")
    sep = 4
    if cut < 0:
        cut = out.find("\n\n")
        sep = 2
    head = out[:cut] if cut >= 0 else out
    body = out[cut + sep:] if cut >= 0 else ""
    location = ""
    for line in head.splitlines():
        if line.lower().startswith("location:"):
            location = line.split(":", 1)[1].strip()
            break

    limit = 2 * 1024 * 1024
    result: dict[str, Any]
    if not location:
        # gh 自己跟了 302（GET 默认跟随），体里就是日志正文
        raw = body.encode("utf-8", "replace")
        result = {
            "text": _strip_ansi(body[:limit]),
            "truncated": len(raw) > limit,
            "bytes": len(raw)}
        store.cache_put(key, json.dumps(result, ensure_ascii=False))
        return web.json_response(result)

    try:
        async with aiohttp.ClientSession() as sess:
            async with sess.get(location, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                if resp.status != 200:
                    return web.json_response(
                        {"error": f"下载日志失败 HTTP {resp.status}"}, status=502)
                raw = await resp.content.read(limit + 1)
    except Exception as exc:  # noqa: BLE001 - 网络问题原样告诉前端
        return web.json_response({"error": f"下载日志失败：{exc}"}, status=502)

    result = {
        "text": _strip_ansi(raw[:limit].decode("utf-8", "replace")),
        "truncated": len(raw) > limit,
        "bytes": len(raw),
    }
    # 能取到就说明 job 已经跑完，日志不会再变，存一份
    store.cache_put(key, json.dumps(result, ensure_ascii=False))
    return web.json_response(result)


# ─────────────────────────────────────────────────────────────
# 自建 runner：看在线状态、发注册令牌
# ─────────────────────────────────────────────────────────────

# 安装包版本。写死比每次请求都去问 GitHub 稳——
# 真要跟新时改这一行，前端从接口拿，不会各写一份。
RUNNER_VERSION = "2.338.2"
# 每个平台每个架构一个包。GitHub 官方发的就是这个命名，arm64 的机器要下 arm64 那份，
# 拿 x64 包在 arm 上跑会直接 exec format error。
_RUNNER_ARCHIVE = {
    "linux": {
        "x64": "actions-runner-linux-x64-{v}.tar.gz",
        "arm64": "actions-runner-linux-arm64-{v}.tar.gz",
        "arm": "actions-runner-linux-arm-{v}.tar.gz",
    },
    "macos": {
        "x64": "actions-runner-osx-x64-{v}.tar.gz",
        "arm64": "actions-runner-osx-arm64-{v}.tar.gz",
    },
    "windows": {
        "x64": "actions-runner-win-x64-{v}.zip",
        "arm64": "actions-runner-win-arm64-{v}.zip",
    },
}
_RUNNER_OS_LABEL = {"linux": "Linux", "macos": "macOS", "windows": "Windows"}
# GitHub 给 runner 打的架构标签，大写那个。自建 runner 默认只带 self-hosted/系统/X64，
# arm 机器以前会被硬写成 X64，job 里写 runs-on: [self-hosted, ARM64] 永远匹配不到。
_RUNNER_ARCH_LABEL = {"x64": "X64", "arm64": "ARM64", "arm": "ARM"}
# uname -m 和 PROCESSOR_ARCHITECTURE 的各种写法，归到三种
_ARCH_ALIAS = {
    "x86_64": "x64", "amd64": "x64", "x64": "x64",
    "aarch64": "arm64", "arm64": "arm64", "arm64ec": "arm64",
    "armv7l": "arm", "armv6l": "arm", "arm": "arm",
}


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


async def handle_github_repos(request: web.Request) -> web.Response:
    """账号下能看到的全部仓库（含组织），最近提交的在前。"""
    blocked = _local_only(request)
    if blocked:
        return blocked
    force = request.query.get("refresh") == "1"
    repos, err = await _gh_my_repos(force)
    return web.json_response({"repos": repos, "error": err, "count": len(repos)})


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

try:  # 密码加密用。没装也不崩，只是密码不落盘、用的时候让人重填
    from cryptography.fernet import Fernet
except Exception:  # pragma: no cover
    Fernet = None  # type: ignore

SSH_TIMEOUT = 15          # 连接/认证超时（秒）
SSH_JOB_LINES = 1200      # 单个任务最多留多少行日志
SSH_JOBS: dict[str, dict[str, Any]] = {}
# 机器名 -> 连接参数（含密码）。落 sqlite，重启和过夜都还在。
# 密码加密后存：密钥单独一个 0600 文件放在库旁边，光把 events.db 拷走解不开。
SSH_MACHINES: Any = {}      # 启动时换成 MachineStore，接口跟 dict 一样
SSH_MACHINE_TTL = 30 * 60   # 只用来算「多久没连过了」，不再删数据

# 机器名和目录都不让人填：名字取机器自己的主机名（连上看一眼就知道），
# 目录从 ~/actions-runner 开始，那儿已经有一个就顺延到 -2、-3。
# 只有直接打 API 的人才会传这两个参数——页面已经不给了。
DEFAULT_RUNNER_DIR = "~/actions-runner"
_AUTO_DIR_TRIES = 8


def _machines_live() -> Any:
    """手上所有机器的凭据。现在已经落盘，重启也还在，所以不再按时间清掉。"""
    return SSH_MACHINES


class MachineStore:
    """机器凭据落盘。接口照 dict 抄，上层代码一行不用改。

    之前是纯内存 dict，进程一重启就全没了；但机器明天还得用，每次重填密码
    不是个事，所以落到 events.db 里。密码用 Fernet 加密后存，密钥单独放一个
    0600 文件——两样东西分开，光拷走 events.db 解不出密码。
    """

    def __init__(self, path: str, key_path: str):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS machines (
              name     TEXT PRIMARY KEY,
              host     TEXT NOT NULL,
              port     INTEGER,
              user     TEXT,
              secret   TEXT,
              os       TEXT,
              arch     TEXT,
              dir      TEXT,
              repo     TEXT,
              pure     INTEGER DEFAULT 0,
              added_at INTEGER,
              last_ok  INTEGER
            );
            """
        )
        self.conn.commit()
        self._fernet = self._load_key(key_path)

    @staticmethod
    def _load_key(path: str):
        if Fernet is None:
            return None
        p = Path(path)
        if p.exists():
            raw = p.read_bytes().strip()
        else:
            raw = Fernet.generate_key()
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_bytes(raw)
            try:
                p.chmod(0o600)
            except OSError:
                pass
        try:
            return Fernet(raw)
        except Exception:  # noqa: BLE001
            return None

    def _enc(self, text: str) -> str:
        if not text or self._fernet is None:
            return ""
        return self._fernet.encrypt(text.encode()).decode()

    def _dec(self, blob: str) -> str:
        if not blob or self._fernet is None:
            return ""
        try:
            return self._fernet.decrypt(blob.encode()).decode()
        except Exception:  # noqa: BLE001
            return ""

    def _row(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "host": row["host"], "port": int(row["port"] or 22),
            "user": row["user"] or "", "password": self._dec(row["secret"] or ""),
            "os": row["os"] or "", "arch": row["arch"] or "x64",
            "dir": row["dir"] or "", "repo": row["repo"] or "",
            "pure": bool(row["pure"]), "at": row["added_at"] or 0,
            "last_ok": row["last_ok"] or 0,
        }

    def __getitem__(self, name: str) -> dict[str, Any]:
        row = self.conn.execute("SELECT * FROM machines WHERE name=?", (name,)).fetchone()
        if row is None:
            raise KeyError(name)
        return self._row(row)

    def get(self, name: str, default: Any = None) -> Any:
        try:
            return self[name]
        except KeyError:
            return default

    def __contains__(self, name: object) -> bool:
        row = self.conn.execute("SELECT 1 FROM machines WHERE name=?", (str(name),)).fetchone()
        return row is not None

    def __setitem__(self, name: str, m: dict[str, Any]) -> None:
        self.conn.execute(
            "INSERT INTO machines (name, host, port, user, secret, os, arch, dir, repo,"
            " pure, added_at, last_ok) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)"
            " ON CONFLICT(name) DO UPDATE SET host=excluded.host, port=excluded.port,"
            " user=excluded.user, secret=excluded.secret, os=excluded.os,"
            " arch=excluded.arch, dir=excluded.dir, repo=excluded.repo,"
            " pure=excluded.pure, added_at=excluded.added_at,"
            " last_ok=MAX(machines.last_ok, excluded.last_ok)",
            (name, str(m.get("host") or ""), int(m.get("port") or 22),
             str(m.get("user") or ""), self._enc(str(m.get("password") or "")),
             str(m.get("os") or ""), str(m.get("arch") or "x64"),
             str(m.get("dir") or ""), str(m.get("repo") or ""),
             1 if m.get("pure") else 0,
             int(m.get("at") or time.time()), int(m.get("last_ok") or 0)),
        )
        self.conn.commit()

    def pop(self, name: str, default: Any = None) -> Any:
        cur = self.get(name)
        if cur is None:
            return default
        self.conn.execute("DELETE FROM machines WHERE name=?", (name,))
        self.conn.commit()
        return cur

    def touch(self, name: str, at: float | None = None, min_gap: int = 20) -> None:
        """记下这台机器最近一次确认在线是几点。

        节流靠 SQL 自己的 WHERE 做：同一条记录 min_gap 秒内不会被重复写，
        页面 8 秒刷一次、live 缓存 4 秒，也不至于把磁盘写热。
        """
        now = int(at or time.time())
        self.conn.execute("UPDATE machines SET last_ok=? WHERE name=? AND last_ok < ?",
                          (now, name, now - int(min_gap)))
        self.conn.commit()

    def remove_many(self, names: list[str]) -> None:
        with self.conn:
            self.conn.executemany("DELETE FROM machines WHERE name=?", [(name,) for name in names])

    def keys(self) -> list[str]:
        return [r["name"] for r in
                self.conn.execute("SELECT name FROM machines ORDER BY name")]

    def values(self) -> list[dict[str, Any]]:
        return [self._row(r) for r in
                self.conn.execute("SELECT * FROM machines ORDER BY name")]

    def items(self) -> list[tuple[str, dict[str, Any]]]:
        return [(r["name"], self._row(r)) for r in
                self.conn.execute("SELECT * FROM machines ORDER BY name")]

    def __iter__(self):
        return iter(self.keys())

    def __len__(self) -> int:
        row = self.conn.execute("SELECT COUNT(*) AS c FROM machines").fetchone()
        return int(row["c"])


class SshBox:
    """一台机器的 SSH 会话。paramiko 是阻塞的，所有调用都丢线程里跑。"""

    def __init__(self, host: str, port: int, user: str, password: str, log_fn) -> None:
        self.host, self.port, self.user = host, port, user
        self.password = password
        self.log = log_fn
        self.client = None

    def connect(self, *, timeout: float = SSH_TIMEOUT, attempts: int = 3) -> None:
        """连上去。连不上重试两次——内网机器偶尔会因为 sshd 的
        MaxStartups 把并发会话卡掉一下，一次失败不代表机器有问题。"""
        if paramiko is None:
            raise RuntimeError("本机没装 paramiko")
        last: Optional[Exception] = None
        for attempt in range(attempts):
            try:
                c = paramiko.SSHClient()
                # 内网机器，首次连接没法人工确认指纹；用完即弃的会话，接受新主机键。
                c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                c.connect(
                    self.host, port=self.port, username=self.user, password=self.password,
                    timeout=timeout, banner_timeout=timeout, auth_timeout=timeout,
                    look_for_keys=False, allow_agent=False,
                )
                self.client = c
                return
            except Exception as exc:            # noqa: BLE001
                last = exc
                if attempt + 1 < attempts:
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

    def put(self, remote_path: str, data: bytes) -> None:
        """SFTP 上传一个文件。

        写 jit.cfg 要用它：那张票是几 KB 的 base64，塞命令行会被长度和引号
        两件事一起坑（Windows 的 PowerShell 尤其明显），走 SFTP 就没有这问题。
        Windows 的 OpenSSH 只认正斜杠，这里统一换掉。
        """
        if self.client is None:
            raise RuntimeError("SSH 还没连上")
        sftp = self.client.open_sftp()
        try:
            with sftp.file(remote_path.replace("\\", "/"), "wb") as fh:
                fh.write(data)
        finally:
            sftp.close()

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

    if repo and not _REPO_RE.match(repo):
        return {}, "填 owner/name 走仓库级，只填组织名走组织级；留空就是纯按需机器，不注册到任何仓库"
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


# 装 runner 时要写进机器上 .env 的代理；空字符串 = 不写（直连）。
# 由 main() 从 config.toml 的 [relay] proxy 读入，设置页改完同步更新。
PROXY_URL = ""

# 走代理时不该绕路的地址：内网与本机，加上 runner 自己连的 broker / 制品域。
# broker（*.actions.githubusercontent.com）走代理反而连不上（实测 .NET 报
# HostUnreachable），它直连本来是好的，所以必须排除。
DEFAULT_NO_PROXY = ("localhost,127.0.0.1,::1,192.168.0.0/16,10.0.0.0/8,172.16.0.0/12,.local,"
                    ".actions.githubusercontent.com,.blob.core.windows.net,.azure.com,"
                    # runner 自己（.NET HttpClient）经代理下载 action 包会报 No route to host，
                    # 这条直连本来就通，所以排除掉；job 里的 git / electron 下载照旧走代理。
                    ".codeload.github.com")

# 生成 .env 代理行的模板。用占位符替换而不是拼引号，少一层转义少一个坑。
_PROXY_POSIX_TMPL = [
    'grep -v -E "^(HTTP_PROXY|HTTPS_PROXY|ALL_PROXY|NO_PROXY)=" "@ENV@" > "@TMP@" || true',
    'printf "HTTP_PROXY=%s\\nHTTPS_PROXY=%s\\nALL_PROXY=%s\\nNO_PROXY=%s\\n" '
    '@URL@ @URL@ @URL@ @NO@ >> "@TMP@"',
    'mv "@TMP@" "@ENV@"',
    'echo "  代理已写入 .env：@URL@"',
]


def _proxy_reject_reason(value: str) -> str:
    """代理地址能不能用。空字符串表示「直连」，也算合法。"""
    v = (value or "").strip()
    if not v:
        return ""
    if len(v) > 200:
        return "代理地址太长了"
    if not v.startswith(("http://", "https://", "socks5://", "socks5h://")):
        return "代理要写成 http://主机:端口 或 socks5://主机:端口"
    rest = v.split("://", 1)[1]
    if "/" in rest:
        return "只要 主机:端口，别带路径"
    host, _, port = rest.rpartition(":")
    if not host or not port.isdigit() or not 0 < int(port) < 65536:
        return "代理地址要带端口，比如 http://127.0.0.1:7890"
    return ""


def _posix_proxy_env_lines(dirvar: str = "DIR") -> list[str]:
    """把代理写进 .env（已有就覆盖）。

    和 PATH 那几行同一个道理：runner 服务是 launchd / systemd 拉起来的，
    继承不到登录 shell 里的变量，只能靠 .env。
    """
    if not PROXY_URL:
        return []
    subs = {"@ENV@": '$' + dirvar + '/.env',
            "@TMP@": '$' + dirvar + '/.env.tmp',
            "@URL@": shlex.quote(PROXY_URL),
            "@NO@": shlex.quote(DEFAULT_NO_PROXY)}
    out = []
    for line in _PROXY_POSIX_TMPL:
        for key, val in subs.items():
            line = line.replace(key, val)
        out.append(line)
    return out


def _win_proxy_env_lines(dirvar: str = "dir") -> list[str]:
    """Windows 版本：同样覆盖写。PATH 那几行已经保证 .env 存在。"""
    if not PROXY_URL:
        return []
    pairs = [f"HTTP_PROXY={PROXY_URL}", f"HTTPS_PROXY={PROXY_URL}",
             f"ALL_PROXY={PROXY_URL}", f"NO_PROXY={DEFAULT_NO_PROXY}"]
    joined = ",".join("'" + x + "'" for x in pairs)
    return [
        "$envPath=Join-Path $" + dirvar + " '.env'",
        "$old=@(); if (Test-Path $envPath) { $old=@(Get-Content $envPath | Where-Object "
        "{ $_ -notmatch '^(HTTP_PROXY|HTTPS_PROXY|ALL_PROXY|NO_PROXY)=' }) }",
        "(@($old)+@(" + joined + ")) | Set-Content -Path $envPath -Encoding ASCII",
        "Write-Output ('[5/5] proxy -> .env: " + PROXY_URL + "')",
    ]


def _posix_env_lines(dirvar: str = "DIR") -> list[str]:
    """把机器上已有的工具链写进 runner 目录的 .env。

    为什么必须有这一步：runner 服务是 launchd / systemd 拉起来的，它继承到的 PATH
    只有系统默认那几条（/usr/bin:/bin:/usr/sbin:/sbin），登录 shell 里
    ~/.local/node24/bin、~/.nvm/... 这些一概不会出现在 job 里。
    实测：三台 mac 都装了 node 在 /Users/dev/.local/node24/bin，job 里
    `node -v` 却是 command not found，就是这条链断了。

    官方机制（actions/runner src/Runner.Listener/Program.cs → LoadAndSetEnv）：
    启动时把 <runner 根目录>/.env 逐行 KEY=VALUE 塞进进程环境，job 全部继承。
    所以路径拼对了，重启服务即生效，不必给每台机器装一遍 node。

    取的是「登录 shell 的 PATH」+「机器上真实存在的常见工具目录」：
    前者覆盖 nvm / pyenv 这类写在 rc 文件里的配置，后者兜住 rc 没写但装了的。
    """
    return [
        'echo "  把已有的工具链写进 .env（否则 job 里只有系统默认 PATH）"',
        'LOGINPATH=$(bash -lc \'printf %s "$PATH"\' 2>/dev/null || true)',
        '[ -n "$LOGINPATH" ] || LOGINPATH="/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"',
        'EXTRA=""',
        'for c in "$HOME"/.local/node*/bin "$HOME"/.nvm/versions/node/*/bin "$HOME"/.volta/bin '
        '"$HOME"/.cargo/bin "$HOME"/.pyenv/shims "$HOME"/.asdf/shims "$HOME"/.local/bin '
        '"$HOME"/.deno/bin /opt/homebrew/bin /opt/homebrew/sbin /usr/local/bin /snap/bin; do',
        '  if [ -d "$c" ]; then EXTRA="$EXTRA:$c"; fi',
        'done',
        '# 去重，保持先出现的优先（登录 PATH 排在前面）',
        'ENVPATH=""',
        'for s in $(printf %s "$LOGINPATH$EXTRA" | tr ":" " "); do',
        '  case ":$ENVPATH:" in',
        '    *":$s:"*) ;;',
        '    *) ENVPATH="${ENVPATH:+$ENVPATH:}$s";;',
        '  esac',
        'done',
        'touch "$' + dirvar + '/.env"',
        'grep -v "^PATH=" "$' + dirvar + '/.env" > "$' + dirvar + '/.env.tmp" || true',
        'printf "PATH=%s\\n" "$ENVPATH" >> "$' + dirvar + '/.env.tmp"',
        'mv "$' + dirvar + '/.env.tmp" "$' + dirvar + '/.env"',
        'echo "  PATH 已写入 $' + dirvar + '/.env"',
    ] + _posix_proxy_env_lines(dirvar)


def _win_env_parts(dirvar: str = "dir") -> list[str]:
    """Windows 同理，但 PATH 用分号，而且要合并机器级和用户级两份。

    Windows 服务账户（LocalSystem / NetworkService）拿不到用户的 PATH，
    所以 rustup / node 这类装在 %USERPROFILE% 下的工具在 job 里会找不到
    （actions/runner#2540 就是这个问题）。合并去重后写进 .env，一次解决。
    """
    return [
        "$mp=[Environment]::GetEnvironmentVariable('Path','Machine')",
        "$up=[Environment]::GetEnvironmentVariable('Path','User')",
        '$seen=@{}; $keep=@()',
        "foreach ($x in ((@($mp,$up) -join ';') -split ';')) { "
        "if ($x -and -not $seen.ContainsKey($x.ToLower())) { $seen[$x.ToLower()]=1; $keep+=$x } }",
        "$envPath=Join-Path $" + dirvar + " '.env'",
        "$old=@(); if (Test-Path $envPath) { "
        "$old=@(Get-Content $envPath | Where-Object { $_ -notmatch '^PATH=' }) }",
        "(@($old)+@('PATH=' + ($keep -join ';'))) | Set-Content -Path $envPath -Encoding ASCII",
        "Write-Output ('[4/4] PATH -> .env: ' + ($keep -join ';'))",
    ] + _win_proxy_env_lines(dirvar)


def _posix_install_script(p: dict[str, Any], token: str, archive: str, labels: str,
                          wipe: bool = False) -> str:
    """拼一个 bash 脚本。sudo 密码从 stdin 第一行读（read SUDOPW），
    不写在命令里——写进去的话 `ps` 一眼就能看到。

    安装包先落进 ~/.cache/github-local-action：同一台机器装第二个 runner 不用再下 200MB，
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
        'CACHE="$HOME/.cache/github-local-action"',
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
        *_posix_env_lines(),
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
        '$cache=\"$env:USERPROFILE\\.cache\\github-local-action\"',
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
        *_win_env_parts(),
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


# 账号下能看到的全部仓库（含组织），带最近提交时间。60 秒缓存，别每次都打 GitHub。
_REPOS_CACHE: dict[str, Any] = {"at": 0.0, "repos": [], "error": ""}
_REPOS_TTL = 60.0


async def _gh_my_repos(force: bool = False) -> tuple[list[dict[str, Any]], str]:
    now = time.time()
    if not force and _REPOS_CACHE["repos"] and now - float(_REPOS_CACHE["at"]) < _REPOS_TTL:
        return list(_REPOS_CACHE["repos"]), str(_REPOS_CACHE["error"])
    rc, out, err = await gh_run([
        "api", "/user/repos?per_page=100&sort=pushed&direction=desc",
        "--jq",
        "[.[] | {repo: .full_name, private: .private, archived: .archived,"
        " pushed_at: .pushed_at, lang: .language}]",
    ])
    if rc != 0:
        msg = (err or out).strip()[:300]
        # 拉不到就保留上一次的名单，别让整页突然变空
        _REPOS_CACHE.update(at=now, error=msg)
        return list(_REPOS_CACHE["repos"]), msg
    try:
        data = json.loads(out or "[]")
    except ValueError:
        data = []
    data = [r for r in data if isinstance(r, dict) and r.get("repo")]
    # 自己再排一次。GitHub 的 ?sort=pushed 在刚推完的几秒里，排序结果和 pushed_at
    # 字段会短暂对不上（e2e 那条「按最近提交倒序」就这么闪红过一次，差 61 秒）。
    # 按字段排最稳；没有时间的用空串，降序时自然排到最后。
    data.sort(key=lambda r: str(r.get("pushed_at") or ""), reverse=True)
    _REPOS_CACHE.update(at=now, repos=data, error="")
    return data, ""


# ─────────────────────────────────────────────────────────────
# 给仓库自动配 webhook
#
# GitHub 那边要三样东西：接收地址、content-type、密钥。地址和 content-type 是死的，
# 密钥以前只有 Cloudflare 上有（secret_text 读不出来），所以只能手工去网页点五步。
# 现在接收端支持「一个仓库一个密钥」（存 D1）：这里生成一个、注册上去、
# 再拿它把 hook 建好——点一下开关就完事，不用再找人要密钥。
# ─────────────────────────────────────────────────────────────

HOOK_EVENTS = ["workflow_job", "workflow_run", "push", "issues",
               "issue_comment", "pull_request"]


def _worker_base(cfg: Config) -> str:
    """从 ws_url 反推接收端的 HTTP 根地址（wss://x/ws → https://x）。"""
    url = str(cfg.ws_url or "").strip()
    url = url.replace("wss://", "https://").replace("ws://", "http://")
    if url.endswith("/ws"):
        url = url[:-3]
    return url.rstrip("/")


def _hook_target(cfg: Config) -> str:
    base = _worker_base(cfg)
    return f"{base}/gh/webhook" if base else ""


async def _worker_put_secret(cfg: Config, repo: str, secret: str) -> tuple[bool, str]:
    """把仓库自己的密钥登记到接收端，之后它就用这个密钥验签。"""
    base = _worker_base(cfg)
    if not base:
        return False, "配置里没有 ws_url，不知道接收端在哪"
    try:
        timeout = aiohttp.ClientTimeout(total=15)
        async with aiohttp.ClientSession(timeout=timeout) as sess:
            async with sess.post(f"{base}/gh/secret",
                                 json={"repo": repo, "secret": secret},
                                 headers={"Authorization": f"Bearer {cfg.token}"}) as resp:
                if resp.status != 200:
                    return False, f"接收端拒绝：HTTP {resp.status} {(await resp.text())[:120]}"
                return True, ""
    except Exception as exc:                       # noqa: BLE001
        return False, f"{type(exc).__name__}: {exc}"[:200]


def _hook_args(target: str, secret: str) -> list[str]:
    args = ["-f", f"config[url]={target}",
            "-f", "config[content_type]=json",
            "-f", f"config[secret]={secret}",
            "-f", "config[insecure_ssl]=0",
            "-F", "active=true"]
    for ev in HOOK_EVENTS:
        args += ["-f", f"events[]={ev}"]
    return args


async def _gh_setup_hook(cfg: Config, repo: str,
                         secret: str) -> tuple[bool, str, dict[str, Any]]:
    """在仓库上建（或更新）指向我们的 webhook。幂等：已存在就改，不重复建。"""
    target = _hook_target(cfg)
    if not target:
        return False, "配置里没有 ws_url，不知道接收端在哪", {}

    rc, out, err = await gh_run([
        "api", f"repos/{repo}/hooks",
        "--jq", '[.[] | {id, url: .config.url}]',
    ])
    if rc != 0:
        return False, _gh_error_text(err or out, repo), {}

    hooks: list[dict[str, Any]] = []
    try:
        parsed = json.loads(out or "[]")
        if isinstance(parsed, list):
            hooks = [h for h in parsed if isinstance(h, dict)]
    except ValueError:
        hooks = []

    mine = [h for h in hooks if str(h.get("url") or "") == target]
    if mine:
        hid = mine[0]["id"]
        rc, out, err = await gh_run(
            ["api", "-X", "PATCH", f"repos/{repo}/hooks/{hid}"] + _hook_args(target, secret))
        if rc != 0:
            return False, _gh_error_text(err or out, repo), {}
        return True, "", {"id": hid, "created": False, "url": target}

    rc, out, err = await gh_run(
        ["api", "-X", "POST", f"repos/{repo}/hooks", "-f", "name=web"]
        + _hook_args(target, secret))
    if rc != 0:
        return False, _gh_error_text(err or out, repo), {}

    info: dict[str, Any] = {}
    try:
        got = json.loads(out or "{}")
        if isinstance(got, dict):
            info = got
    except ValueError:
        pass
    return True, "", {"id": info.get("id"), "created": True, "url": target}


async def handle_webhook_setup(request: web.Request) -> web.Response:
    """一次把 webhook 配好：生成密钥 → 登记到接收端 → 建/更新 hook。"""
    blocked = _local_only(request)
    if blocked:
        return blocked
    try:
        body = await request.json()
    except Exception:                              # noqa: BLE001
        body = {}
    if not isinstance(body, dict):
        body = {}

    repo = str(body.get("repo") or "").strip()
    if not _REPO_RE.match(repo) or "/" not in repo:
        return web.json_response(
            {"error": "要写成 owner/name"}, status=400)

    cfg: Config = request.app["cfg"]
    secret = secrets.token_hex(32)

    ok, err = await _worker_put_secret(cfg, repo, secret)
    if not ok:
        return web.json_response({"error": err}, status=400)

    ok, err, info = await _gh_setup_hook(cfg, repo, secret)
    if not ok:
        return web.json_response({"error": err}, status=400)

    log.info("webhook %s：%s（hook %s）",
             "新建" if info.get("created") else "更新", repo, info.get("id"))
    return web.json_response({"ok": True, "repo": repo, **info})


async def handle_webhook_status(request: web.Request) -> web.Response:
    """看一个仓库配没配：我们这边有没有它的密钥 + GitHub 上有没有指向我们的 hook。"""
    blocked = _local_only(request)
    if blocked:
        return blocked
    repo = (request.query.get("repo") or "").strip()
    if not _REPO_RE.match(repo) or "/" not in repo:
        return web.json_response({"error": "要写成 owner/name"}, status=400)

    cfg: Config = request.app["cfg"]
    target = _hook_target(cfg)
    has_hook = False
    hook_id = None
    rc, out, err = await gh_run([
        "api", f"repos/{repo}/hooks", "--jq", '[.[] | {id, url: .config.url}]',
    ])
    if rc == 0:
        try:
            for h in json.loads(out or "[]"):
                if isinstance(h, dict) and str(h.get("url") or "") == target:
                    has_hook, hook_id = True, h.get("id")
                    break
        except ValueError:
            pass
    return web.json_response({
        "repo": repo, "target": target,
        "hook": has_hook, "hook_id": hook_id,
        "ready": has_hook,
        "error": "" if rc == 0 else _gh_error_text(err or out, repo),
    })


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


def _detect_arch(box: SshBox, os_name: str) -> str:
    """看这台机器是 x64 还是 arm64。认不出来就当 x64——
    绝大多数机器都是，猜错的话装完会立刻在解包/执行那步报出来，不会静静装错。
    """
    raw = ""
    try:
        if os_name == "windows":
            rc, out = box.run(
                'powershell -NoProfile -Command "$env:PROCESSOR_ARCHITECTURE"',
                timeout=30, quiet=True)
        else:
            rc, out = box.run("uname -m", timeout=30, quiet=True)
        if rc == 0:
            lines = [x.strip() for x in (out or "").splitlines() if x.strip()]
            raw = lines[-1].lower() if lines else ""
    except Exception:  # noqa: BLE001
        pass
    return _ARCH_ALIAS.get(raw, "x64")


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

        # 架构：arm 机器得下 arm 包，标签也得是 ARM64
        p["arch"] = _detect_arch(box, p["os"])
        _ssh_log(job, f"架构 {p['arch']}（{_RUNNER_ARCH_LABEL[p['arch']]}），标签会带上它")

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


def _ssh_precheck_pure(job: dict[str, Any], p: dict[str, Any]) -> tuple[bool, str]:
    """纯按需机器的预检：只认三件事——什么系统、什么架构、机器叫什么。

    跟装常驻那份的区别：不碰 ~/actions-runner（那台机器上可能一台常驻都没有），
    也不去 GitHub 申请注册令牌。这台机器从头到尾不会在 GitHub 上留下任何记录。
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
            _ssh_log(job, f"实际是 {found}，按这个来")
            p["os"] = found

        p["arch"] = _detect_arch(box, p["os"])
        _ssh_log(job, f"架构 {p['arch']}（{_RUNNER_ARCH_LABEL[p['arch']]}）")

        if not p.get("name"):
            guess = _remote_hostname(box, p["os"])
            if guess:
                p["name"] = guess
                _ssh_log(job, f"机器名自动取成 {guess}（读的是它自己的主机名）")
            else:
                fallback = _clean_runner_name(f"{p['user']}-{p['host'].replace('.', '-')}")
                p["name"] = fallback or f"runner-{p['port']}"
                _ssh_log(job, f"主机名没读到，机器名用 {p['name']} 顶着")
        return True, ""
    except Exception as exc:  # noqa: BLE001
        return False, f"{type(exc).__name__}: {exc}"[:300]
    finally:
        box.close()


def _runner_pkg_name(os_name: str, arch: str) -> str:
    """这台机器该用哪个安装包（不含版本号的模板已经填好）。"""
    packs = _RUNNER_ARCHIVE.get(os_name) or _RUNNER_ARCHIVE["linux"]
    return (packs.get(arch) or packs["x64"]).format(v=RUNNER_VERSION)


def _jit_env_fn_posix() -> str:
    """给 JIT 脚本用的 ensure_env：目录可能还没建，先 mkdir。"""
    body = "\n".join("  " + line for line in _posix_env_lines("D"))
    return 'ensure_env() {\n  mkdir -p "$D"\n' + body + "\n}"


def _jit_env_fn_win() -> str:
    body = "\n".join(_win_env_parts("$D"))
    return "function Ensure-Env {\n" + body + "\n}"


def _posix_jit_bootstrap(directory: str, pkg: str, url: str) -> str:
    """让一台 POSIX 机器有一份能用的 JIT 目录。三种来源，优先不联网的那种。

    每条出口都顺手写一次 .env：按需 runner 也是从 $D 起 listener，同样读
    $D/.env。不写的话，纯按需的机器上 job 里照样看不到 node / nvm。
    """
    return f"""set -e
D={directory}
{_jit_env_fn_posix()}
if [ -x "$D/run.sh" ]; then ensure_env; echo "已经有一份能用的，不用动"; exit 0; fi
SRC="$HOME/actions-runner"
if [ -x "$SRC/run.sh" ]; then
  echo "从机器上那份常驻 runner 复制（不联网）"
  rm -rf "$D"; mkdir -p "$D"
  tar -C "$SRC" --exclude=_work --exclude=_diag --exclude=.runner \\
      --exclude=.credentials --exclude=.credentials_rsaparams --exclude=.service \\
      -cf - . 2>/dev/null | tar -C "$D" -xf -
  test -x "$D/run.sh" && {{ ensure_env; echo "复制完成"; exit 0; }}
fi
PKG="$HOME/.cache/github-local-action/{pkg}"
mkdir -p "$HOME/.cache/github-local-action"
if tar -tzf "$PKG" >/dev/null 2>&1; then
  echo "用机器上已经存着的安装包"
else
  echo "机器上没有安装包，得下一份（这一步看它连 GitHub 快不快）"
  rm -f "$PKG.part"
  curl -fsSL --max-time 3600 -o "$PKG.part" {url}
  mv "$PKG.part" "$PKG"
  echo "下载完成"
fi
rm -rf "$D"; mkdir -p "$D"
tar xzf "$PKG" -C "$D"
ensure_env
test -x "$D/run.sh" && echo "解包完成"
"""


def _win_jit_bootstrap(directory: str, url: str) -> str:
    """Windows 版本：有常驻就本地复制，没有就下 zip 解开。同样写 .env。"""
    return f"""$ErrorActionPreference='Stop'
$D='{directory}'
{_jit_env_fn_win()}
if (Test-Path (Join-Path $D 'bin\\Runner.Listener.exe')) {{ '已经有一份能用的，不用动'; Ensure-Env; exit 0 }}
$SRC="$env:USERPROFILE\\actions-runner"
if (Test-Path (Join-Path $SRC 'bin\\Runner.Listener.exe')) {{
  '从机器上那份常驻 runner 复制（不联网）'
  Remove-Item $D -Recurse -Force -ErrorAction SilentlyContinue
  Copy-Item $SRC $D -Recurse -Force
  foreach($n in @('_work','_diag')) {{ Remove-Item (Join-Path $D $n) -Recurse -Force -ErrorAction SilentlyContinue }}
  foreach($n in @('.runner','.credentials','.credentials_rsaparams','.service')) {{
    $p=Join-Path $D $n
    if (Test-Path -LiteralPath $p) {{ (Get-Item -LiteralPath $p -Force).IsReadOnly=$false; Remove-Item -LiteralPath $p -Force -ErrorAction SilentlyContinue }}
  }}
  '复制完成'
  Ensure-Env
  exit 0
}}
$CACHE="$env:USERPROFILE\\.cache\\github-local-action"
New-Item -ItemType Directory -Force -Path $CACHE | Out-Null
$ZIP=Join-Path $CACHE 'runner.zip'
'机器上没有安装包，得下一份'
Invoke-WebRequest -Uri '{url}' -OutFile $ZIP -UseBasicParsing
Remove-Item $D -Recurse -Force -ErrorAction SilentlyContinue
Expand-Archive -Path $ZIP -DestinationPath $D -Force
Ensure-Env
'解包完成'
"""


def _jit_bootstrap_blocking(job: dict[str, Any], machine: dict[str, Any]) -> tuple[bool, str]:
    """让这台机器有一份能用的 JIT 目录，不用人再点一次「准备目录」。

    顺序：已经就绪就不动 → 有常驻 runner 就本地复制（最快）→ 机器上有旧包就解
    → 都没有才下载。所以「机器连 GitHub 慢」只在最后一档才会咬人。
    """
    directory = _jit_home_dir(machine)
    pkg = _runner_pkg_name(_jit_os(machine), _jit_arch(machine))
    url = (f"https://github.com/actions/runner/releases/download/v{RUNNER_VERSION}/{pkg}")
    box = SshBox(machine["host"], int(machine["port"]), machine["user"],
                 machine["password"], lambda t: _ssh_log(job, t))
    try:
        box.connect()
        if _jit_os(machine) == "windows":
            script = _win_jit_bootstrap(directory, url)
            rc, out = box.run(_ps(script), None, 3600, True)
        else:
            script = _posix_jit_bootstrap(directory, pkg, url)
            rc, out = box.run(f"bash -c {shlex.quote(script)}", None, 3600, True)
        tail = "\n".join([x for x in (out or "").strip().splitlines() if x.strip()][-3:])
        if rc != 0:
            return False, f"准备目录失败（退出码 {rc}）：{tail[:300]}"
        return True, tail[:300]
    except Exception as exc:  # noqa: BLE001
        return False, f"{type(exc).__name__}: {exc}"[:300]
    finally:
        box.close()


def _ssh_install_blocking(job: dict[str, Any], p: dict[str, Any], token: str) -> tuple[bool, str]:
    """装。预检已经确认过系统和目录，这里只管下、解、注册、起服务。"""
    box = _box_for(p, job)
    try:
        box.connect()
        # 架构在预检里已经探测过了；没探测到就按 x64 走
        arch = str(p.get("arch") or "x64")
        packs = _RUNNER_ARCHIVE[p["os"]]
        pack = packs.get(arch) or packs["x64"]
        if pack is not packs.get("x64"):
            _ssh_log(job, f"下 {arch} 那份包（{pack}）")
        archive = ("https://github.com/actions/runner/releases/download/v{v}/"
                   + pack).format(v=RUNNER_VERSION)
        labels = (f"self-hosted,{_RUNNER_OS_LABEL[p['os']]},"
                  f"{_RUNNER_ARCH_LABEL.get(arch, 'X64')}")
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


async def _ssh_add_pure(job: dict[str, Any], p: dict[str, Any]) -> None:
    """不挂任何仓库地把一台机器接进来。"""
    ok, err = await asyncio.to_thread(_ssh_precheck_pure, job, p)
    if not ok:
        job.update(status="failed", error=err)
        _ssh_log(job, f"失败：{err}")
        return

    # 纯按需模式没有 GitHub 帮忙查重，同名得在本地避掉（两台都叫 Mac 很常见）
    base = p["name"]
    name = base
    for i in range(2, 10):
        if name not in SSH_MACHINES:
            break
        name = f"{base}-{i}"[:63]
    if name != base:
        _ssh_log(job, f"已经有一台叫 {base} 的，这台记成 {name}")
    p["name"] = name

    machine = {
        "host": p["host"], "port": p["port"], "user": p["user"],
        "password": p["password"], "os": p["os"], "arch": p.get("arch") or "x64",
        "dir": "", "repo": "", "pure": True, "at": time.time(),
    }
    SSH_MACHINES[name] = machine
    _ssh_log(job, "不挂任何仓库：GitHub 上不会多出记录，机器上也不留常驻 runner")

    ok, info = await asyncio.to_thread(_jit_bootstrap_blocking, job, machine)
    if not ok:
        # 凭据留着：机器已经连上了，只是目录没备好，下次直接重试就行
        job.update(status="failed", error=info)
        _ssh_log(job, f"失败：{info}")
        return
    _ssh_log(job, f"按需目录好了：{_jit_home_dir(machine)}")
    job.update(status="done", result={
        "name": name, "os": p["os"], "arch": p.get("arch") or "x64",
        "status": "ready", "pure": True,
    })


async def _run_ssh_add(job: dict[str, Any], p: dict[str, Any]) -> None:
    try:
        # 没填仓库 = 纯按需机器：GitHub 那边一个字都不写，机器上也不装常驻 runner。
        # 只确认能连、能跑，把按需目录备好，真排上 job 了再按仓库现签一张票。
        if not p.get("repo"):
            await _ssh_add_pure(job, p)
            return

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
            "arch": p.get("arch") or "x64",
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


async def handle_machines(request: web.Request) -> web.Response:
    """手上这几台机器：不按仓库分，就是“我能连的那些”。"""
    blocked = _local_only(request)
    if blocked:
        return blocked
    rows = await _offload(_machine_rows)
    return web.json_response({
        "machines": rows, "count": len(rows),
    })


def _machine_rows() -> list[dict[str, Any]]:
    live = _machines_live()
    now = time.time()
    rows: list[dict[str, Any]] = []
    for name, m in live.items():
        at = float(m.get("at") or now)
        rows.append({
            "name": name,
            "os": m.get("os") or "",
            "arch": m.get("arch") or "x64",
            "host": m.get("host") or "",
            "port": m.get("port") or 22,
            "user": m.get("user") or "",
            "repo": m.get("repo") or "",
            "pure": bool(m.get("pure")),
            "dir": m.get("dir") or "",
            "jit_dir": _jit_home_dir(m),
            "added_at": int(at),
            "last_ok": int(m.get("last_ok") or 0),
        })
    rows.sort(key=lambda x: str(x["name"]).lower())
    return rows


# ── GitHub runner/job 查询缓存（用于动作详情，与机器连通性无关）──
_RUNNER_CACHE: dict[str, dict[str, Any]] = {}
_RUNNER_TTL = 15.0
_JOB_CACHE: dict[str, dict[str, Any]] = {}
_JOB_TTL = 12.0


async def _repo_runners(repo: str) -> tuple[list[dict[str, Any]], str]:
    """某个仓库下所有 runner 的在线/忙闲。"""
    now = time.time()
    hit = _RUNNER_CACHE.get(repo)
    if hit and now - float(hit["at"]) < float(hit.get("ttl") or _RUNNER_TTL):
        return hit["runners"], hit["error"]
    rc, out, err = await gh_run([
        "api", f"{_gh_scope(repo)}/actions/runners", "--jq",
        "{runners: [.runners[] | {id, name, status, busy, labels: [.labels[].name]}]}",
    ])
    rows: list[dict[str, Any]] = []
    text = ""
    if rc != 0:
        text = _gh_error_text(err or out, repo)
    else:
        try:
            data = json.loads(out or "{}")
            rows = [_runner_row(r) for r in (data.get("runners") or [])]
        except ValueError:
            text = "gh 返回的不是 JSON"
    # 名单是空的（这仓库没挂 runner）就少问几次：那种仓库问多少次都是空
    ttl = _RUNNER_TTL if rows else max(_RUNNER_TTL, 120.0)
    _RUNNER_CACHE[repo] = {"at": now, "runners": rows, "error": text, "ttl": ttl}
    return rows, text


async def _repo_running_jobs(repo: str) -> tuple[list[dict[str, Any]], str]:
    """仓库里正在跑的 job，带 runner 名——回答「这台机器在跑哪个 action」。

    没有「一条命令拿到全部在跑的 job」的接口，只能先拿进行中的 run 再逐个问
    job 列表；所以最多看 5 个 run，凑够了就提前收工。
    """
    now = time.time()
    hit = _JOB_CACHE.get(repo)
    if hit and now - float(hit["at"]) < _JOB_TTL:
        return hit["jobs"], hit["error"]
    rc, out, err = await gh_run([
        "api", f"{_gh_scope(repo)}/actions/runs?status=in_progress&per_page=20", "--jq",
        "[.workflow_runs[] | {id, name, run_number, head_branch}]",
    ])
    if rc != 0:
        text = _gh_error_text(err or out, repo)
        _JOB_CACHE[repo] = {"at": now, "jobs": [], "error": text}
        return [], text
    try:
        runs = json.loads(out or "[]")[:5]
    except ValueError:
        _JOB_CACHE[repo] = {"at": now, "jobs": [], "error": "gh 返回的不是 JSON"}
        return [], "gh 返回的不是 JSON"
    found: list[dict[str, Any]] = []
    for run in runs:
        rc, out, err = await gh_run([
            "api", f"{_gh_scope(repo)}/actions/runs/{run['id']}/jobs", "--jq",
            '[.jobs[] | select(.status == "in_progress") | {name, runner_name, started_at, step: (.steps[]? | select(.status == "in_progress") | .name)}]',
        ], attempts=1)
        if rc != 0:
            continue
        try:
            jobs = json.loads(out or "[]")
        except ValueError:
            continue
        for j in jobs:
            if not j.get("runner_name"):
                continue
            found.append({
                "job_name": j.get("name") or "",
                "runner_name": j.get("runner_name") or "",
                "step": j.get("step") or "",
                "started_at": j.get("started_at") or "",
                "run_id": run.get("id"),
                "run_name": run.get("name") or "",
                "run_number": run.get("run_number"),
                "branch": run.get("head_branch") or "",
                "repo": repo,
            })
        if len(found) >= 8:
            break
    _JOB_CACHE[repo] = {"at": now, "jobs": found, "error": ""}
    return found, ""


def _machine_endpoint(machine: dict[str, Any]) -> tuple[str, int]:
    return (str(machine.get("host") or "").lower(), int(machine.get("port") or 22))


def _machine_groups() -> dict[tuple[str, int], list[tuple[str, dict[str, Any]]]]:
    groups: dict[tuple[str, int], list[tuple[str, dict[str, Any]]]] = {}
    for name, machine in sorted(_machines_live().items(), key=lambda pair: pair[0].lower()):
        groups.setdefault(_machine_endpoint(machine), []).append((name, machine))
    return groups


_MACHINE_PROBE_CACHE: dict[tuple[str, int], dict[str, Any]] = {}
_MACHINE_PROBE_TASKS: dict[tuple[Any, ...], asyncio.Task] = {}
_MACHINE_PROBE_SLOTS = asyncio.Semaphore(4)
_MACHINE_PROBE_TTL = 20.0


def _probe_machine_ssh(machine: dict[str, Any]) -> dict[str, Any]:
    """只读探测：SSH、独立按需目录和工作进程，与 GitHub 登录/仓库无关。"""
    box = SshBox(machine["host"], int(machine.get("port") or 22), machine["user"],
                 machine.get("password") or "", lambda _: None)
    connected = False
    try:
        box.connect(timeout=5, attempts=1)
        connected = True
        if _jit_os(machine) == "windows":
            # 以远端真实用户目录为准；兼容之前准备在 C:\jitlab 等位置的安装。
            legacy = _jit_home_dir(machine).replace("'", "''")
            script = (
                "$ErrorActionPreference='Stop'; $ProgressPreference='SilentlyContinue'; "
                "$candidates=@((Join-Path $env:USERPROFILE 'jitlab'), "
                f"'{legacy}', 'C:\\jitlab'); "
                "$directory=$candidates[0]; $ready=$false; "
                "foreach($d in $candidates){ "
                "if((Test-Path (Join-Path $d 'bin\\Runner.Listener.exe')) -and "
                "(Test-Path (Join-Path $d 'run.cmd'))){$directory=$d; $ready=$true; break} }; "
                "$busy=@(Get-Process -Name 'Runner.Worker' -ErrorAction SilentlyContinue).Count -gt 0; "
                "@{ready=$ready; busy=$busy; jit_dir=$directory} | ConvertTo-Json -Compress"
            )
            rc, out = box.run(_ps(script), timeout=8, quiet=True)
            if rc != 0:
                raise RuntimeError("probe failed")
            # Windows PowerShell 可能把模块初始化的 CLIXML 进度混进合并输出。
            line = next(line for line in out.splitlines() if line.strip().startswith('{'))
            details = json.loads(line)
        else:
            sftp = box.client.open_sftp()
            try:
                directory = sftp.normalize('.') + '/' + JIT_DIRNAME
                try:
                    listener = sftp.stat(directory + '/bin/Runner.Listener')
                    run = sftp.stat(directory + '/run.sh')
                    ready = bool(listener.st_mode & 0o111) and bool(run.st_mode & 0o111)
                except FileNotFoundError:
                    ready = False
            finally:
                sftp.close()
            rc, out = box.run('ps -axo comm=', timeout=8, quiet=True)
            if rc != 0:
                raise RuntimeError("probe failed")
            details = {"ready": ready, "busy": any(
                line.strip().rsplit('/', 1)[-1] == 'Runner.Worker' for line in out.splitlines()),
                "jit_dir": directory}
        return {"connected": True, "ready": bool(details.get("ready")),
                "busy": bool(details.get("busy")), "jit_dir": str(details.get("jit_dir") or ""),
                "error": ""}
    except Exception as exc:
        if paramiko is not None and isinstance(exc, paramiko.AuthenticationException):
            reason = "SSH 认证失败，请更新连接凭据"
        elif connected:
            reason = "SSH 已连接，但运行环境检测失败"
        else:
            reason = "SSH 无法连接，请检查机器、网络和 SSH 服务"
        return {"connected": connected, "ready": False, "busy": False,
                "error": reason, "error_type": type(exc).__name__}
    finally:
        box.close()


def _group_signature(aliases) -> tuple[Any, ...]:
    """一组机器的凭据指纹：凭据一变，旧探测结果就不再可信。"""
    return tuple((name, m.get("user"), m.get("password"), m.get("os"), m.get("dir"))
                 for name, m in aliases)


async def _probe_machine_group(endpoint, aliases) -> dict[str, Any]:
    # 凭据变更立即失效；同端点并发请求只发起一次 SSH 探测。
    signature = _group_signature(aliases)
    hit = _MACHINE_PROBE_CACHE.get(endpoint)
    if hit and hit["signature"] == signature and time.time() - hit["at"] < _MACHINE_PROBE_TTL:
        return hit["result"]
    key = (endpoint, signature)

    async def run():
        result = {"connected": False, "ready": False, "busy": False, "error": "没有可用的 SSH 凭据"}
        async with _MACHINE_PROBE_SLOTS:
            for name, machine in aliases:
                current = dict(await asyncio.to_thread(_probe_machine_ssh, machine))
                current["credential_name"] = name
                if current.get("connected") or not result.get("connected"):
                    result = current
                if current.get("connected") and not current.get("error"):
                    break
        _MACHINE_PROBE_CACHE[endpoint] = {"signature": signature, "at": time.time(), "result": result}
        return result

    task = _MACHINE_PROBE_TASKS.get(key)
    if task is None:
        task = asyncio.create_task(run())
        _MACHINE_PROBE_TASKS[key] = task
        task.add_done_callback(lambda done: _MACHINE_PROBE_TASKS.pop(key, None))
    return await asyncio.shield(task)


_MACHINE_LIVE_WAIT = 8.0      # 页面 8 秒轮一次，接口不能比它更久


def _probe_outcome(task: Any) -> dict[str, Any]:
    """一个端点的探测结果：炸了、被取消、超时都折成「未知」，不往上抛。"""
    if not task.done():
        return {"connected": False, "ready": False, "busy": False, "timed_out": True,
                "error": f"探测超过 {int(_MACHINE_LIVE_WAIT)} 秒没回来，下一轮自动重试"}
    if task.cancelled():
        return {"connected": False, "ready": False, "busy": False, "failed": True,
                "error": "探测被取消"}
    exc = task.exception()
    if exc is not None:
        # 一个端点炸了只能影响它自己，不能把整页拖成 502
        log.info("机器探测失败（不影响其它机器）：%s: %s", type(exc).__name__, exc)
        return {"connected": False, "ready": False, "busy": False, "failed": True,
                "error": f"探测出错：{type(exc).__name__}"}
    result = task.result()
    if not isinstance(result, dict):
        return {"connected": False, "ready": False, "busy": False, "failed": True,
                "error": "探测没有返回结果"}
    return result


async def _machines_live_view() -> dict[str, Any]:
    """机器在线状态由 SSH 决定，不扫描仓库或按 runner 名称猜机器。

    单个端点探测抛异常或者太慢，只让那台机器变「状态未知」：一次请求最多等
    _MACHINE_LIVE_WAIT 秒，等不到的端点先按未知返回，探测本身继续跑，下一轮
    直接命中缓存。以前这里是裸 gather，一个端点抛异常整个接口就 502。
    """
    groups = _machine_groups()
    tasks = {}
    for endpoint, aliases in groups.items():
        task = asyncio.ensure_future(_probe_machine_group(endpoint, aliases))
        # 超时被放弃的探测任务后面还会跑完：这里先把异常吃掉，免得到 GC 时
        # 刷一堆「Task exception was never retrieved」。
        task.add_done_callback(lambda done: done.cancelled() or done.exception())
        tasks[endpoint] = task
    if tasks:
        await asyncio.wait(list(tasks.values()), timeout=_MACHINE_LIVE_WAIT)
    out: dict[str, dict[str, Any]] = {}
    for endpoint, aliases in groups.items():
        probe = _probe_outcome(tasks[endpoint])
        names = {name for name, _ in aliases}
        running = next((r for r in JIT_POOL["running"].values() if r.get("machine") in names), None)
        if probe.get("timed_out") or probe.get("failed"):
            state = "unknown"
        elif not probe.get("connected"):
            state = "offline"
        elif probe.get("error"):
            state = "unknown"
        elif probe.get("busy") or running:
            state = "busy"
        else:
            state = "idle" if probe.get("ready") else "unprepared"
        checked_at = _MACHINE_PROBE_CACHE.get(endpoint, {}).get("at", time.time())
        # 「在线」= 环境就绪（idle/busy）。仅仅 SSH 连得上、或者探测出错，都不算——
        # 否则一台环境坏掉的机器会永远显示「最后在线 · 刚刚」。
        usable = state in ("idle", "busy")
        for name, machine in aliases:
            if usable and hasattr(_machines_live(), "touch"):
                _machines_live().touch(name, at=checked_at, min_gap=20)
            last_online = int(_machines_live().get(name, {}).get("last_ok") or 0)
            if usable:
                last_online = max(last_online, int(checked_at))
            out[name] = {
                "state": state, "connected": bool(probe.get("connected")),
                "ready": bool(probe.get("ready")), "runner": running.get("name", "") if running else "",
                "labels": [], "repo": "", "error": probe.get("error") or "",
                "job": ({"job_name": running.get("job"), "run_id": running.get("run_id"),
                         "repo": running.get("repo"), "run_name": running.get("workflow_name"),
                         "started_at": int(float(running.get("at") or 0) * 1000)}
                        if state == "busy" and running else None),
                "last_online_at": last_online, "checked_at": checked_at,
                "jit_dir": probe.get("jit_dir") or "", "source": "ssh",
            }
    # repos/errors 保留字段兼容旧客户端；机器探测不需要仓库列表。
    return {"machines": out, "repos": [], "errors": {}, "source": "ssh", "checked_at": time.time()}


async def handle_machines_live(request: web.Request) -> web.Response:
    blocked = _local_only(request)
    if blocked:
        return blocked
    try:
        return web.json_response(await _machines_live_view())
    except Exception as exc:
        return web.json_response({"error": f"机器状态检测失败：{type(exc).__name__}"}, status=502)


async def _prime_machine_live(app: web.Application) -> None:
    try:
        await asyncio.sleep(2.0)
        data = await _machines_live_view()
        log.info("机器 SSH 状态已预热（%s 个端点）", len(_machine_groups()))
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        log.info("机器状态预热失败（不影响用）：%s", type(exc).__name__)


async def handle_machine_delete(request: web.Request) -> web.Response:
    """断开一台机器：只删本地凭据，远端主机和 GitHub 上的东西一概不碰。"""
    blocked = _local_only(request)
    if blocked is not None:
        return blocked
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"error": "请求格式不正确"}, status=400)
    if not isinstance(body, dict) or not isinstance(body.get("name"), str):
        return web.json_response({"error": "请指定机器"}, status=400)
    name = body["name"]
    machine = SSH_MACHINES.get(name)
    if machine is None:
        return web.json_response({"error": "机器已不存在，请刷新列表"}, status=404)
    # 页面按 SSH 端点归组，所以删也删整组：同一台机器重复接入过几次就一次清干净
    names = [n for n, m in SSH_MACHINES.items()
             if str(m["host"]).lower() == str(machine["host"]).lower()
             and int(m.get("port") or 22) == int(machine.get("port") or 22)]
    busy = any(r.get("machine") in names for r in JIT_POOL["running"].values())
    preparing = any(not task.done() for task in _JIT_TASKS) or any(
        job.get("status") == "running" for job in SSH_JOBS.values())
    if busy or preparing:
        return web.json_response({"error": "有任务正在运行或准备，请完成后再删除机器"}, status=409)
    SSH_MACHINES.remove_many(names)
    _jit_auto_pool()
    return web.json_response({"ok": True})


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
    if p["repo"]:
        _ssh_log(job, f"目标仓库 {p['repo']}，机器名和目录连上去自己定"
                 if not p["name"] else f"目标仓库 {p['repo']}，机器名 {p['name']}")
    else:
        _ssh_log(job, "没填仓库，按纯按需机器接进来（不往 GitHub 注册任何东西）")
    job["_task"] = asyncio.create_task(_run_ssh_add(job, p))
    return web.json_response({"job": job["id"], "name": p["name"]})

# ─────────────────────────────────────────────────────────────
# 代理：装 runner 时自动写进 .env，也能一键同步到已有机器
# ─────────────────────────────────────────────────────────────


def _posix_proxy_sync_script(directory: str, plat: str) -> str:
    """把 .env 里的代理刷一遍，然后重启服务 —— .env 只在 listener 启动时读。"""
    d = directory.replace("~", "$HOME")
    lines = ["set -e", f"DIR={shlex.quote(d)}", 'cd "$DIR"']
    if plat != "macos":
        lines.append("IFS= read -r SUDOPW || true")
    lines += _posix_env_lines("DIR")
    if plat == "macos":
        lines.append(
            'if [ -x ./svc.sh ]; then ./svc.sh stop >/dev/null 2>&1 || true; '
            './svc.sh start >/dev/null 2>&1 || true; echo "  服务已重启（launchd）"; '
            'else echo "  没有 svc.sh，重启后 .env 才生效"; fi')
    else:
        lines.append(
            'if [ -x ./svc.sh ]; then '
            # 先确认这个密码真能 sudo：不是 sudoer、或者 sudo 密码跟登录密码不一样的
            # 机器，.env 写了也不会生效，得直接报失败，不能回一句「已同步」。
            'if echo "$SUDOPW" | sudo -S -k true >/dev/null 2>&1; then '
            'echo "$SUDOPW" | sudo -S ./svc.sh stop >/dev/null 2>&1 || true; '
            'echo "$SUDOPW" | sudo -S ./svc.sh start >/dev/null 2>&1 || true; '
            'echo "  服务已重启（systemd）"; '
            'else echo "  .env 已写，但登录密码不能 sudo：服务没重启，重启后才生效"; exit 3; fi; '
            'else echo "  没有 svc.sh，重启后 .env 才生效"; fi')
    return "\n".join(lines)


def _proxy_sync_blocking(job: dict[str, Any]) -> list[dict[str, Any]]:
    """逐台刷 .env。一台失败不影响其它台。"""
    rows: list[dict[str, Any]] = []
    for name, machine in dict(_machines_live()).items():
        if not machine.get("password"):
            _ssh_log(job, f"{name}：没存连接密码，跳过（重填一次凭据再同步）")
            rows.append({"name": name, "ok": False, "error": "没有凭据"})
            continue
        box = SshBox(machine["host"], int(machine["port"]), machine["user"],
                     machine["password"], lambda t: _ssh_log(job, t))
        try:
            box.connect()
            directory = str(machine.get("dir") or "~/actions-runner")
            plat = str(machine.get("os") or "linux").lower()
            if plat == "windows":
                script = "\n".join([f"$dir = '{directory}'"] + _win_proxy_env_lines("dir"))
                rc, out = box.run(_ps(script), None, 300, True)
                _ssh_log(job, f"{name}：.env 已写，Windows 要等 runner 下次重启才生效")
            else:
                script = _posix_proxy_sync_script(directory, plat)
                rc, out = box.run(f"bash -c {shlex.quote(script)}",
                                  stdin_text=machine["password"], timeout=300)
            if rc != 0:
                rows.append({"name": name, "ok": False, "error": f"退出码 {rc}"})
                _ssh_log(job, f"{name}：失败（退出码 {rc}）")
                continue
            rows.append({"name": name, "ok": True, "error": ""})
            _ssh_log(job, f"{name}：已同步")
        except Exception as exc:  # noqa: BLE001
            rows.append({"name": name, "ok": False,
                         "error": f"{type(exc).__name__}: {exc}"[:200]})
            _ssh_log(job, f"{name}：{type(exc).__name__}: {exc}")
        finally:
            box.close()
    return rows


async def _run_proxy_sync(job: dict[str, Any]) -> None:
    try:
        rows = await asyncio.to_thread(_proxy_sync_blocking, job)
        ok = [r for r in rows if r["ok"]]
        job.update(status="done",
                   result={"total": len(rows), "ok": len(ok), "machines": rows})
        _ssh_log(job, f"完成：{len(ok)}/{len(rows)} 台已同步")
    except Exception as exc:  # noqa: BLE001
        job.update(status="failed", error=f"{type(exc).__name__}: {exc}"[:300])
        _ssh_log(job, f"失败：{exc}")


async def handle_runner_proxy_get(request: web.Request) -> web.Response:
    blocked = _local_only(request)
    if blocked:
        return blocked
    cfg: Config = request.app["cfg"]
    return web.json_response({"proxy": cfg.proxy, "no_proxy": DEFAULT_NO_PROXY,
                              "machines": sorted(dict(_machines_live()))})


async def handle_runner_proxy(request: web.Request) -> web.Response:
    """设置 runner 代理：写回配置，并同步到所有已知机器（.env + 重启服务）。"""
    blocked = _local_only(request)
    if blocked:
        return blocked
    try:
        body = await request.json()
    except Exception:  # noqa: BLE001
        body = {}
    if not isinstance(body, dict):
        body = {}
    # 「没传这个字段」和「传了空串」是两件事：前者是请求写错了，后者才是「直连」。
    # 以前用 body.get("proxy") or ""，一个拼错的请求体就能静默清掉全机代理。
    if "proxy" not in body:
        return web.json_response(
            {"error": "要说清楚 proxy：直连请传空字符串", "proxy": None}, status=400)
    raw = body.get("proxy")
    if not isinstance(raw, str):
        return web.json_response(
            {"error": "proxy 要是字符串（空字符串 = 直连）"}, status=400)
    value = raw.strip()
    reason = _proxy_reject_reason(value)
    if reason:
        return web.json_response({"error": reason}, status=400)

    cfg: Config = request.app["cfg"]
    previous = cfg.proxy
    cfg.proxy = value
    try:
        save_config(cfg)
    except OSError as exc:
        cfg.proxy = previous
        return web.json_response({"error": f"写入配置失败：{exc}"}, status=500)
    global PROXY_URL
    PROXY_URL = value

    job = _ssh_new_job("proxy", f"同步代理到各台机器（{value or '直连'}）")
    _ssh_log(job, f"代理设为 {value or '（空，直连）'}，已写回 {cfg.path}")
    job["_task"] = asyncio.create_task(_run_proxy_sync(job))
    return web.json_response({"proxy": value, "job": job["id"]})



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
        "proxy": cfg.proxy,
        "trusted_peers": list(cfg.trusted_peers),
        "token_set": bool(cfg.token),
        "token_hint": mask_token(cfg.token),
        "config_path": str(cfg.path) if cfg.path else "",
        # 装在哪：stdio 那条 MCP 配置要绝对路径，前端从相对路径推不出来
        "service_dir": str(Path(__file__).resolve().parent),
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
    if isinstance(value, (list, tuple)):
        return "[" + ", ".join(_toml_value(v) for v in value) + "]"
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
        "proxy": cfg.proxy,
        "trusted_peers": list(cfg.trusted_peers),
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
    # 读配置无所谓（令牌已掩码），改配置就是另一回事：ws_url 一改，整条事件流
    # 就被指到别人的服务器上了。所以保存只允许在本机做，跟 /github/*、/runners/* 同一道闸。
    blocked = _local_only(
        request,
        "改配置会重写 ws_url 与令牌，只允许在跑接收端的那台机器上保存；"
        "局域网设备能看（/config），不能改",
    )
    if blocked:
        return blocked
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


# 给 agent 的说明书：仓库根放一份 AGENT.md，服务自己也吐一份。
# 网址比文件好使——把 agent 指向 http://127.0.0.1:18091/agent.md 就行。
# 英文版是 AGENT.en.md，走 /agent.en.md；网页切到 English 时拉的就是它。
_AGENT_NAMES = {"zh": "AGENT.md", "en": "AGENT.en.md"}
_agent_doc_cache: dict[str, dict[str, Any]] = {"zh": {"mtime": 0.0, "text": ""},
                                             "en": {"mtime": 0.0, "text": ""}}


def agent_doc(lang: str = "zh") -> str:
    """读仓库里那份说明书。英文版没写/丢了就退回中文——总比空白强。"""
    for want in ([lang, "zh"] if lang != "zh" else ["zh"]):
        name = _AGENT_NAMES.get(want, "AGENT.md")
        cache = _agent_doc_cache.setdefault(want, {"mtime": 0.0, "text": ""})
        for root in _data_roots():
            path = root / name
            try:
                stamp = path.stat().st_mtime
            except OSError:
                continue
            if cache["mtime"] != stamp:
                try:
                    cache["text"] = path.read_text(encoding="utf-8")
                    cache["mtime"] = stamp
                except OSError:
                    continue
            if cache["text"]:
                return cache["text"]
    return ""


async def handle_agent_doc(request: web.Request) -> web.Response:
    """`/agent.md`、`/agent.en.md` 和 `/llms.txt` 都指这里。"""
    lang = "en" if request.path.endswith(".en.md") else "zh"
    text = agent_doc(lang)
    if not text:
        return web.Response(
            text="AGENT.md 没找到；local-client/ 目录里应该有一份。\n",
            content_type="text/plain",
            charset="utf-8",
            status=404,
        )
    return web.Response(
        text=text,
        content_type="text/markdown",
        charset="utf-8",
        headers={"Cache-Control": "no-cache"},
    )


# ═════════════════ MCP over HTTP（Streamable HTTP 传输）══════════════════
#
# MCP 除了 stdio 还有 HTTP 传输（2025-03-26 版起叫 Streamable HTTP）：
# 就一个端点，POST 发 JSON-RPC，回 JSON（或 SSE）。服务本来就是 aiohttp，
# 直接在它身上挂 /mcp，好处是不用为每个客户端各起一个子进程，
# 而且 18091 这个端口本来就在那儿。
#
# 工具实现跟 mcp_server.py 共用一份（import 过来），不重写第二遍。
#
# 两个坑已处理：
#   1. mcp_server 里的 http_json 是同步 urllib，而工具要回调本服务的
#      /actions、/github/*。在事件循环里直接调会把循环卡死（自己等自己），
#      所以丢到线程池里跑。
#   2. 工具里有些只能本机看（/github/* 用的是本机 gh 的凭据），/mcp 要是
#      对局域网开就等于绕过了那道墙，所以这里一律只收 127.0.0.1。

MCP_SESSIONS: "OrderedDict[str, float]" = OrderedDict()
MCP_SESSION_MAX = 64


def mcp_touch_session(sid: str) -> None:
    """记一笔会话最近用过的时间。

    淘汰必须按「最久没用」——用 set.pop() 随机弹的时候，实测会把**正在
    使用中**的会话挤掉，客户端下一次请求就莫名 404。
    """
    if sid in MCP_SESSIONS:
        MCP_SESSIONS.move_to_end(sid)
        MCP_SESSIONS[sid] = time.time()
        return
    MCP_SESSIONS[sid] = time.time()
    while len(MCP_SESSIONS) > MCP_SESSION_MAX:
        MCP_SESSIONS.popitem(last=False)

try:  # 文件不在就当没这个功能，别把整个服务拖死
    import mcp_server as _mcp
    if not getattr(_mcp, "TOOLS", None):
        raise RuntimeError("mcp_server 里没有 TOOLS")
    _MCP_ERR = ""
except Exception as _exc:  # noqa: BLE001
    _mcp = None  # type: ignore[assignment]
    _MCP_ERR = f"{type(_exc).__name__}: {_exc}"
    log.warning("MCP over HTTP 不可用：%s", _MCP_ERR)


def mcp_base(request: web.Request) -> str:
    """工具回调本服务时的基地址。必须走本机回环——/github/* 只认 127.0.0.1，
    自报局域网 IP 会被自己 403。"""
    cfg: Config = request.app["cfg"]
    return f"http://127.0.0.1:{cfg.http_port}"


def mcp_handle(msg: dict[str, Any], base: str) -> dict[str, Any] | None:
    """在别的线程里跑。BASE 是模块级变量，并发请求写同一个值不影响结果——
    它们指向的是同一个本机地址。"""
    if _mcp is None:
        return None
    _mcp.BASE = base
    return _mcp.handle(msg)


async def handle_mcp_post(request: web.Request) -> web.Response:
    """POST /mcp：收一条（或一批）JSON-RPC。"""
    blocked = _local_only(request)
    if blocked:
        return blocked
    if _mcp is None:
        return web.json_response(
            {"jsonrpc": "2.0", "id": None,
             "error": {"code": -32603, "message": f"MCP 模块没加载：{_MCP_ERR}"}},
            status=501)

    raw = await request.text()
    try:
        payload = json.loads(raw or "null")
    except ValueError:
        return web.json_response(
            {"jsonrpc": "2.0", "id": None,
             "error": {"code": -32700, "message": "不是合法 JSON"}}, status=400)

    if isinstance(payload, dict):
        msgs = [payload]
        single = True
    elif isinstance(payload, list):
        msgs = [m for m in payload if isinstance(m, dict)]
        single = False
    else:
        return web.json_response(
            {"jsonrpc": "2.0", "id": None,
             "error": {"code": -32600, "message": "请求必须是对象"}}, status=400)

    # 客户端报的会话号认得就行；报了不认识的（比如服务重启过）就让它重新初始化
    sid = request.headers.get("Mcp-Session-Id", "")
    if sid and sid not in MCP_SESSIONS:
        return web.json_response(
            {"jsonrpc": "2.0", "id": None,
             "error": {"code": -32001, "message": "会话不在了，重新 initialize"}},
            status=404)
    if sid:
        mcp_touch_session(sid)

    base = mcp_base(request)

    # 全是通知（没有 id）时按规范回 202 空体
    if not any(m.get("id") is not None for m in msgs):
        await asyncio.get_running_loop().run_in_executor(
            None, lambda: [mcp_handle(m, base) for m in msgs])
        return web.Response(status=202, headers=mcp_cors())

    loop = asyncio.get_running_loop()
    if single:
        out = await loop.run_in_executor(None, mcp_handle, msgs[0], base)
    else:
        replies = await loop.run_in_executor(
            None, lambda: [r for r in (mcp_handle(m, base) for m in msgs) if r is not None])
        out = replies or None

    if out is None:
        return web.Response(status=202, headers=mcp_cors())

    headers = mcp_cors()
    if single and msgs[0].get("method") == "initialize":
        new_sid = uuid.uuid4().hex
        mcp_touch_session(new_sid)
        headers["Mcp-Session-Id"] = new_sid
    return web.json_response(out, headers=headers)


def mcp_cors() -> dict[str, str]:
    return {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "Content-Type, Mcp-Session-Id, MCP-Protocol-Version",
        "Access-Control-Expose-Headers": "Mcp-Session-Id",
    }


async def handle_mcp_other(request: web.Request) -> web.Response:
    """GET 会想开 SSE 长流；本服务没有「主动推给客户端」的需求，回 405 让
    客户端回到 POST。DELETE 是客户端收尾，直接受。"""
    if request.method == "DELETE":
        sid = request.headers.get("Mcp-Session-Id", "")
        MCP_SESSIONS.pop(sid, None)
        return web.Response(status=200, headers=mcp_cors())
    return web.Response(status=405, headers={**mcp_cors(), "Allow": "POST, DELETE"})


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
            "GitHub Local Action 本地端（web/index.html 没找到，退化为纯文本）\n\n"
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
# 机器凭据来自 SSH_MACHINES；SSH 实测决定是否可用，机器不绑定仓库。
# ─────────────────────────────────────────────────────────────

JIT_DIRNAME = "jitlab"          # 跟常驻 runner 的 actions-runner 分开放，互不干扰
JIT_HISTORY_MAX = 30

JIT_POOL: dict[str, Any] = {
    # 一个仓库一个开关。开着的仓库才接活，没开的一条都不看。
    "repos": {},
    "machines": [],             # 参与调度的机器名（对应 SSH_MACHINES 的键）
    "max_parallel": 2,
    "running": {},              # job_id -> {job, machine, name, label, at}
    "history": [],              # 最近的接活记录，新的在前
    "stats": {"picked": 0, "failed": 0, "done": 0, "skipped": 0,
              "reconciled": 0, "released": 0, "last_error": ""},
}

_JIT_TASKS: set[asyncio.Task] = set()

# ── 对账：GitHub 的队列才是真源，事件只是加速 ──────────────────────────
# 只在 webhook 到达那一刻挑机器有个结构性缺口：那一刻机器全忙、或者事件压根没
# 送到，这个 job 就没人管了（GitHub 那边一直排到 24 小时上限才判失败）。所以
# 再定时问一次 GitHub「还有谁在排队」，把漏掉的补上，顺手收回泄漏的名额。
JIT_RECONCILE_INTERVAL = 30.0      # 多久对一次账
JIT_RECONCILE_GRACE = 20.0         # 排这么久的队还没人接才补，先给 webhook 那条路机会
JIT_RECONCILE_MAX_TRIES = 6        # 同一个 job 最多补发几次
JIT_RECONCILE_GAP = 45.0           # 两次补发之间至少隔这么久
JIT_RECONCILE_FORGET = 1800.0      # 从队列里消失这么久之后忘掉它的补发次数
JIT_RUNNING_CHECK_AFTER = 900.0    # running 里的条目挂这么久就去 GitHub 核一次
JIT_RUNNING_CHECK_PER_PASS = 5     # 一轮最多核几条，别把 gh 配额吃光

_JIT_RETRY: dict[str, dict[str, Any]] = {}      # job_id -> {tries, first, last, seen}
_JIT_LAST_RECONCILE: dict[str, Any] = {}
# 对账一轮要问 GitHub 好几个接口，串行问下来要十几秒，跟不上 30 秒的节拍。
# 并发上限放在这里，别一口气fork出几十个 gh 进程。
_JIT_GH_SLOTS = asyncio.Semaphore(6)


def _jit_os(machine: dict[str, Any]) -> str:
    return str(machine.get("os") or "").lower()


def _jit_home_dir(machine: dict[str, Any]) -> str:
    """机器上独立的 JIT 目录；SSH 探测得到的实际路径优先。

    兼容旧记录：Windows 从已有 runner 的安装目录推一层上去：
    C:\\Users\\Docker\\actions-runner → C:\\Users\\Docker\\jitlab
    """
    if machine.get("jit_dir"):
        return str(machine["jit_dir"])
    if _jit_os(machine) == "windows":
        base = str(machine.get("dir") or r"C:\actions-runner")
        parent = base.rsplit("\\", 1)[0] if "\\" in base else r"C:\Users"
        return parent.rstrip("\\") + "\\" + JIT_DIRNAME
    return f"~/{JIT_DIRNAME}"


def _jit_arch(machine: dict[str, Any]) -> str:
    return str(machine.get("arch") or "x64").lower()


def _jit_can_run(machine: dict[str, Any], labels: list[str]) -> bool:
    """这台机器能不能接这个 job。

    看三件事：job 要 self-hosted、平台对得上、架构也对得上（arm 的活不能发到 x64）。
    自定义 label（比如 jit-lab）不用管——票是我们自己发的，带上就是了。
    """
    want = {str(x).lower() for x in labels}
    if "self-hosted" not in want:
        return False
    plat = want & {"linux", "macos", "windows"}
    if plat and _jit_os(machine) not in plat:
        return False
    arch = want & {"x64", "arm64", "arm"}
    if arch and _jit_arch(machine) not in arch:
        return False
    return True


def _jit_auto_pool() -> None:
    """按 SSH 端点去重，同一台机器的旧名称不能重复占用并发名额。"""
    JIT_POOL["machines"] = [aliases[0][0] for aliases in _machine_groups().values()]
    JIT_POOL["max_parallel"] = max(1, len(JIT_POOL["machines"]))


# 按需配置存在 meta 表里。之前只活在内存，重启一次「仓库」页的开关就全变回关闭——
# 那不是「没开」，是「忘了」。开关是配置，得记住。
JIT_META_KEY = "jit-on-demand"


def _jit_save(store: Store) -> None:
    """记下「哪些仓库开着」，下次启动接着用。

    只存配置：仓库开关、机器池、并发数。running / history / stats 是此刻的运行时
    状态，存下来重启后反而是错的（会出现一批早就不存在的「正在跑」）。
    """
    try:
        body = {
            "repos": sorted(r for r, v in JIT_POOL["repos"].items() if v),
            "machines": list(JIT_POOL["machines"]),
            "max_parallel": int(JIT_POOL["max_parallel"]),
        }
        store.meta_set(JIT_META_KEY, json.dumps(body, ensure_ascii=False))
    except Exception as exc:                       # noqa: BLE001
        # 存不下只是「下次要重新勾」，不影响这一轮接活，别把接口打挂
        log.info("按需配置没存上（不影响接活）：%s", exc)


def _jit_restore(store: Store) -> None:
    """启动时把上次开着的仓库捡回来。

    机器池不照抄存下来的清单：那台机器可能已经删了、密码也过期了，照抄会恢复出
    一个点不亮的池子。改成拿「现在还能连的机器」重算一遍。

    恢复只恢复「盯着哪些仓库」，不会在机器上起任何进程：按需是等 job 派上来才开票，
    所以重启不会莫名冒出一堆 runner。
    """
    raw = store.meta_get(JIT_META_KEY, "")
    if not raw:
        return
    try:
        body = json.loads(raw)
    except ValueError:
        log.info("按需配置读不出来，当没开过：%s", raw[:60])
        return
    if not isinstance(body, dict):
        return
    # 名字不像 owner/name 的直接丢掉：宁可少盯一个，也别拿脏数据去拼 API 路径
    repos = [r for r in (body.get("repos") or [])
             if isinstance(r, str) and _REPO_RE.match(r)]
    if not repos:
        return
    JIT_POOL["repos"] = {r: True for r in repos}
    _jit_auto_pool()
    log.info("按需配置已恢复：接 %d 个仓库 %s，现在能连的机器 %d 台",
             len(repos), repos, len(JIT_POOL["machines"]))


def _jit_pick(labels: list[str]) -> Optional[dict[str, Any]]:
    """按物理端点排除忙碌/准备中的机器，使用 SSH 探测确认过的凭据和目录。"""
    busy = {_machine_endpoint(SSH_MACHINES.get(str(r.get("machine")), {}))
            for r in JIT_POOL["running"].values()}
    groups = _machine_groups()
    for name in JIT_POOL["machines"]:
        machine = SSH_MACHINES.get(name)
        if not machine or _machine_endpoint(machine) in busy:
            continue
        endpoint = _machine_endpoint(machine)
        hit = _MACHINE_PROBE_CACHE.get(endpoint)
        if not hit or time.time() - hit["at"] >= _MACHINE_PROBE_TTL:
            continue
        # 缓存可能是改凭据之前留下的：指纹对不上就不认，免得拿旧票去连
        if hit.get("signature") != _group_signature(groups.get(endpoint) or [(name, machine)]):
            continue
        probe = hit["result"]
        if not probe.get("connected") or not probe.get("ready") or probe.get("busy") or probe.get("error"):
            continue
        working = SSH_MACHINES.get(probe.get("credential_name")) or machine
        if _jit_can_run(working, labels):
            return {**working, "name": name, "jit_dir": probe.get("jit_dir")}
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
    """把票送过去、把进程拉起来。票只落到机器上的文件里，不进命令行也不进日志。"""
    box = SshBox(machine["host"], int(machine["port"]), machine["user"],
                 machine["password"], log.info)
    try:
        await asyncio.to_thread(box.connect)
    except Exception as exc:  # noqa: BLE001
        return False, f"连不上：{exc}"

    directory = _jit_home_dir(machine)
    try:
        if _jit_os(machine) == "windows":
            # 票先落到 jit.cfg。之前这里只有 Get-Content，从没 Write 过——
            # PowerShell 读不到文件只报非终止错误、退出码还是 0，于是“接活成功”
            # 了但机器上压根没起 runner。改成 SFTP 直接传，长度和引号都不操心。
            try:
                await asyncio.to_thread(box.put, f"{directory}\\jit.cfg", cfg.encode("utf-8"))
            except Exception as exc:  # noqa: BLE001
                return False, f"票没传上去：{exc}"
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
            text = (out or "").strip()
            # PowerShell 报非终止错误时退出码仍然是 0（读不到 jit.cfg 那次就是
            # 这样，日志里一堆 CategoryInfo 但 rc=0），所以还得看它到底吐没吐 pid。
            has_pid = any(tok.isdigit() for tok in text.replace("\r", " ").split())
            if rc != 0 or not has_pid:
                return False, (text[:200] or "PowerShell 没给出进程号")
            return True, ""

        # 票写进文件：base64 走 stdin 落地，省得处理引号和换行
        b64 = base64.b64encode(cfg.encode("utf-8")).decode("ascii")
        rc, out = await asyncio.to_thread(
            box.run,
            f'cd {shlex.quote(directory)} && printf %s {shlex.quote(b64)} | base64 -d > jit.cfg && '
            f'wc -c < jit.cfg',
            None, 60, True)
        if rc != 0:
            return False, (out or "").strip()[:200]
        # macOS 没有 setsid，nohup 就能活过 ssh 断开；顺手清掉上次被 kill 的残留
        cmd = (f'cd {shlex.quote(directory)} && rm -f .runner .credentials .credentials_rsaparams && '
               f'nohup ./run.sh --jitconfig "$(cat jit.cfg)" > run.log 2>&1 < /dev/null & '
               f'echo started')
        rc, out = await asyncio.to_thread(box.run, cmd, None, 60, True)
        return rc == 0, "" if rc == 0 else (out or "").strip()[:200]
    except Exception as exc:  # noqa: BLE001
        return False, f"拉起失败：{exc}"
    finally:
        box.close()


async def _jit_take_job(item: dict[str, Any], via: str = "webhook") -> bool:
    """一个 job 排队了：挑机器、发票、起进程。真起来了才算 True。"""
    payload = item.get("payload") or {}
    job = payload.get("workflow_job") or {}
    labels = [str(x) for x in (job.get("labels") or [])]
    repo = str(item.get("repo") or "")
    jid = str(job.get("id") or "")
    jname = str(job.get("name") or "")

    await _machines_live_view()
    machine = _jit_pick(labels)
    if not machine:
        if via == "webhook":
            # 对账那一轮挑不到机器不算「漏」，它下一轮还会再看一眼
            JIT_POOL["stats"]["skipped"] += 1
        return False
    runner_name = f"jit-{machine['name']}-{jid[-6:]}" if jid else f"jit-{machine['name']}"
    key = jid or runner_name
    if key in JIT_POOL["running"]:
        return True
    # 在第一个网络 await 之前占位，两个同时 queued 的 job 不能挑中同一台机器。
    JIT_POOL["running"][key] = {
        "job": jname, "machine": machine["name"], "name": runner_name,
        "labels": labels, "at": time.time(), "repo": repo,
        "run_id": job.get("run_id"), "workflow_name": job.get("workflow_name"),
    }
    started = False
    try:
        cfg, err = await _jit_ticket(repo, runner_name, labels)
        if cfg:
            started, err = await _jit_launch(machine, cfg)
        if not started:
            JIT_POOL["stats"]["failed"] += 1
            JIT_POOL["stats"]["last_error"] = err
            _jit_record("failed", job=jname, machine=machine["name"], job_id=jid, error=err,
                        via=via)
            return False
        JIT_POOL["stats"]["picked"] += 1
        _jit_record("picked", job=jname, machine=machine["name"], runner=runner_name,
                    job_id=jid, via=via)
        log.info("JIT 接活：%s → %s（%s）%s", jname, machine["name"], runner_name,
                 "（对账补发）" if via == "reconcile" else "")
        return True
    finally:
        if not started:
            JIT_POOL["running"].pop(key, None)


def _jit_bind_runner(job: dict[str, Any], jid: str) -> None:
    """runner 真正接住的 job 可能与它被发出去时对应的那个不同。

    JIT runner 注册后由 GitHub 按 labels 派活：同一套 labels 里可能同时有多个
    job 在排，谁先到先得。实测见过「给 windows-1 发的 runner 跑了 windows-2」。
    不把这条记录改绑到真实 job 上，windows-1 就会被当成「还在飞」而对账跳过它，
    白白等到 stale 清理才发现——那正是要修的那类卡死。
    """
    runner = str(job.get("runner_name") or "")
    if not runner or not jid or jid in JIT_POOL["running"]:
        return
    for key, entry in list(JIT_POOL["running"].items()):
        if str(entry.get("name") or "") != runner or key == jid:
            continue
        JIT_POOL["running"].pop(key, None)
        entry["job"] = str(job.get("name") or entry.get("job") or "")
        entry["job_id"] = jid
        JIT_POOL["running"][jid] = entry
        _jit_record("rebound", job=entry.get("job"), machine=entry.get("machine"),
                    runner=runner, job_id=jid, repo=entry.get("repo"),
                    error=f"同一个 runner 实际接的是这个 job（原先记的是 {key}）")
        log.info("runner %s 实际接的 job 是 %s（原先记的 %s）", runner, jid, key)
        return


async def _jit_on_event(item: dict[str, Any]) -> None:
    """按需模式的入口：只关心「有 job 排队」和「job 结束」。"""
    if not any(JIT_POOL["repos"].values()):
        return
    if item.get("event") != "workflow_job":
        return
    action = item.get("action")
    payload = item.get("payload") or {}
    job = payload.get("workflow_job") or {}
    jid = str(job.get("id") or "")

    if action in ("in_progress", "completed"):
        # 先按 runner 名核对一下：接到的可能不是我们为它挑的那个 job
        _jit_bind_runner(job, jid)

    if action == "completed":
        if JIT_POOL["running"].pop(jid, None) is not None:
            JIT_POOL["stats"]["done"] += 1
        return

    if action != "queued":
        return
    if not JIT_POOL["repos"].get(str(item.get("repo") or "")):
        return

    # 按 SSH 端点重建池子；接活前再探测连接和运行环境。
    _jit_auto_pool()
    if len(JIT_POOL["running"]) >= int(JIT_POOL["max_parallel"]):
        JIT_POOL["stats"]["skipped"] += 1
        return

    task = asyncio.create_task(_jit_take_job(item))
    _JIT_TASKS.add(task)
    task.add_done_callback(_JIT_TASKS.discard)


async def _repo_queued_jobs(repo: str) -> tuple[list[dict[str, Any]], str]:
    """GitHub 上「还排着队、一个 runner 都没分到」的 job。

    查两种 run：整个 run 还没起来的（status=queued），和 run 已经在跑、但其中
    某个 job 还在等的（status=in_progress）——后者正是「机器全忙那一刻被漏掉」
    的那种 job，只看 run 级别的 queued 是找不到它的。

    两条 run 查询和后面每个 run 的 job 查询都并发发出去，否则三个仓库串行问下来
    一轮要十几秒，对账就跟不上了。
    """
    scope = _gh_scope(repo)

    async def runs_with(status: str) -> tuple[list[dict[str, Any]], str]:
        async with _JIT_GH_SLOTS:
            rc, out, err = await gh_run(
                ["api", f"{scope}/actions/runs?status={status}&per_page=10",
                 "--jq", "[.workflow_runs[] | {id, name}]"], attempts=1)
        if rc != 0:
            return [], _gh_error_text(err or out, repo)
        try:
            data = json.loads(out or "[]")
        except ValueError:
            return [], "gh 返回的不是 JSON"
        return [r for r in data if isinstance(r, dict)], ""

    batches = await asyncio.gather(runs_with("queued"), runs_with("in_progress"))
    runs: list[dict[str, Any]] = []
    seen: set[Any] = set()
    error = ""
    for batch, err in batches:
        error = error or err
        for run in batch:
            if run.get("id") in seen:
                continue
            seen.add(run.get("id"))
            runs.append(run)

    async def jobs_of(run: dict[str, Any]) -> list[dict[str, Any]]:
        async with _JIT_GH_SLOTS:
            rc, out, _ = await gh_run(
                ["api", f"{scope}/actions/runs/{run['id']}/jobs", "--jq",
                 '[.jobs[] | select(.status == "queued") | {id, name, labels, created_at}]'],
                attempts=1)
        if rc != 0:
            return []
        try:
            jobs = json.loads(out or "[]")
        except ValueError:
            return []
        return [{
            "repo": repo,
            "job_id": str(job.get("id") or ""),
            "job": str(job.get("name") or ""),
            "labels": [str(x) for x in (job.get("labels") or [])],
            "created_at": job.get("created_at") or "",
            "run_id": run.get("id"),
            "run_name": str(run.get("name") or ""),
        } for job in jobs]

    found: list[dict[str, Any]] = []
    for group in await asyncio.gather(*[jobs_of(r) for r in runs[:8]]):
        found.extend(group)
    return found, error


async def _job_state(repo: str, job_id: str) -> tuple[str, str]:
    """GitHub 上这个 job 现在什么状态：active / finished / unknown。"""
    if not repo or not job_id.isdigit():
        return "unknown", "没有仓库或 job 号"
    rc, out, err = await gh_run(
        ["api", f"{_gh_scope(repo)}/actions/jobs/{job_id}", "--jq", "{status, conclusion}"],
        attempts=1)
    if rc != 0:
        low = (err or out).lower()
        # 404/410：job 已经不在了（仓库改名、run 被清），当结束处理
        if "not found" in low or "404" in low or "gone" in low or "410" in low:
            return "finished", "GitHub 上已经查不到这个 job"
        return "unknown", _gh_error_text(err or out, repo)
    try:
        data = json.loads(out or "{}")
    except ValueError:
        return "unknown", "gh 返回的不是 JSON"
    status = str((data or {}).get("status") or "")
    if status == "completed":
        return "finished", ""
    if status in ("queued", "in_progress", "waiting", "requested", "pending"):
        return "active", ""
    return "unknown", f"未知状态 {status}"


def _jit_item_for_job(job: dict[str, Any]) -> dict[str, Any]:
    """把对账查到的 job 拼成跟 webhook 一样的事件形状，复用同一条接活路径。"""
    return {
        "event": "workflow_job", "action": "queued", "repo": job.get("repo") or "",
        "payload": {"workflow_job": {
            "id": job.get("job_id"), "name": job.get("job"),
            "labels": list(job.get("labels") or []), "run_id": job.get("run_id"),
            "workflow_name": job.get("run_name"),
        }},
    }


async def _jit_release_stale(now: float) -> list[str]:
    """收回来被泄漏的并发名额：running 里挂了很久、GitHub 上其实早结束的。

    completed 事件丢了就会这样——名额一直被占着，慢慢把 max_parallel 吃满。
    """
    released: list[str] = []
    for key, entry in list(JIT_POOL["running"].items()):
        if len(released) >= JIT_RUNNING_CHECK_PER_PASS:
            break
        if now - float(entry.get("at") or 0) < JIT_RUNNING_CHECK_AFTER:
            continue
        state, why = await _job_state(str(entry.get("repo") or ""), str(key))
        if state != "finished":
            continue
        JIT_POOL["running"].pop(key, None)
        JIT_POOL["stats"]["released"] = int(JIT_POOL["stats"].get("released") or 0) + 1
        _jit_record("released", job=entry.get("job"), machine=entry.get("machine"),
                    job_id=str(key), repo=entry.get("repo"),
                    error=why or "GitHub 上已经结束了，名额收回来")
        log.info("收回泄漏的并发名额：%s（%s）", entry.get("job"), key)
        released.append(str(key))
    return released


async def _jit_reconcile_once() -> dict[str, Any]:
    """对一轮账：把漏掉的 job 补上 runner，顺手收回泄漏的名额。"""
    started = time.time()
    repos = [r for r, on in JIT_POOL["repos"].items() if on]
    summary: dict[str, Any] = {"at": started, "repos": repos, "queued": 0,
                               "launched": [], "waiting": 0, "released": [], "error": ""}
    if not repos:
        return summary
    # 挑机器要看探测结果，先把缓存热起来（TTL 内不会重复 SSH）
    await _machines_live_view()
    errors: list[str] = []
    for repo, (jobs, err) in zip(repos, await asyncio.gather(*[_repo_queued_jobs(r) for r in repos])):
        if err:
            errors.append(f"{repo}: {err}")
        for job in jobs:
            summary["queued"] += 1
            jid = str(job.get("job_id") or "")
            if not jid or jid in JIT_POOL["running"]:
                continue                      # 已经在飞，不重复派
            labels = [str(x) for x in (job.get("labels") or [])]
            if "self-hosted" not in {x.lower() for x in labels}:
                continue                      # GitHub 托管 runner 的活，不归我们管
            age = started - _iso_ms(job.get("created_at")) / 1000.0
            if age < JIT_RECONCILE_GRACE:
                continue                      # 刚排上，先让 webhook 那条路自己走
            record = _JIT_RETRY.get(jid)
            if record is None:
                record = _JIT_RETRY[jid] = {"tries": 0, "first": started, "last": 0.0}
            record["seen"] = started
            if int(record.get("tries") or 0) >= JIT_RECONCILE_MAX_TRIES:
                summary["waiting"] += 1        # 补够了还没接住，交给 GitHub 的 24 小时回收
                continue
            if started - float(record.get("last") or 0) < JIT_RECONCILE_GAP:
                summary["waiting"] += 1
                continue
            if len(JIT_POOL["running"]) >= int(JIT_POOL["max_parallel"]):
                summary["waiting"] += 1
                continue
            if _jit_pick(labels) is None:
                summary["waiting"] += 1        # 此刻没有能接的同平台机器，下一轮再看
                continue
            record["tries"] = int(record.get("tries") or 0) + 1
            record["last"] = time.time()
            if await _jit_take_job(_jit_item_for_job(job), via="reconcile"):
                JIT_POOL["stats"]["reconciled"] = int(JIT_POOL["stats"].get("reconciled") or 0) + 1
                summary["launched"].append({
                    "job": job.get("job"), "job_id": jid, "repo": job.get("repo"),
                    "machine": (JIT_POOL["running"].get(jid) or {}).get("machine"),
                })
            else:
                summary["waiting"] += 1
    summary["released"] = await _jit_release_stale(started)
    # 从队列里消失很久的记录可以忘了，不然这个表只涨不减
    for jid in list(_JIT_RETRY):
        gone_for = started - float(_JIT_RETRY[jid].get("seen") or started)
        if gone_for > JIT_RECONCILE_FORGET:
            _JIT_RETRY.pop(jid, None)
    summary["error"] = "；".join(errors)[:300]
    summary["took"] = round(time.time() - started, 2)
    return summary


async def jit_reconcile_loop() -> None:
    """定时对账。事件丢了、机器当时全忙，只能靠这一层把 job 接回来。"""
    while True:
        await asyncio.sleep(JIT_RECONCILE_INTERVAL)
        try:
            summary = await _jit_reconcile_once()
        except Exception as exc:  # noqa: BLE001
            log.warning("对账出错：%s", exc)
            JIT_POOL["stats"]["last_error"] = str(exc)[:200]
            continue
        _JIT_LAST_RECONCILE.clear()
        _JIT_LAST_RECONCILE.update(summary)
        if summary.get("launched") or summary.get("released") or summary.get("error"):
            log.info("对账：排队 %s，补发 %s，等机器 %s，收回 %s%s",
                     summary["queued"], len(summary["launched"]), summary["waiting"],
                     len(summary["released"]),
                     f"，出错：{summary['error']}" if summary.get("error") else "")


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
            "arch": _jit_arch(machine),
            "dir": _jit_home_dir(machine),
            "busy": busy,
        })
    return {
        # on 是推出来的：只要还开着一个仓库就算在干活
        "on": any(JIT_POOL["repos"].values()),
        "repos": dict(JIT_POOL["repos"]),
        "watching": sorted(r for r, v in JIT_POOL["repos"].items() if v),
        "max_parallel": JIT_POOL["max_parallel"],
        "machines": machines,
        "available": [aliases[0][0] for aliases in _machine_groups().values()],
        # 带平台和架构的版本：页面上要看得出哪台是 arm、哪台是 x64
        "available_info": [
            {"name": n, "os": _jit_os(m), "arch": _jit_arch(m)}
            for aliases in _machine_groups().values() for n, m in aliases[:1]
        ],
        "running": [
            {"job": r["job"], "machine": r["machine"], "name": r["name"],
             "seconds": round(now - float(r["at"]), 1)}
            for r in JIT_POOL["running"].values()
        ],
        "history": JIT_POOL["history"][:12],
        "stats": dict(JIT_POOL["stats"]),
        "reconcile": dict(_JIT_LAST_RECONCILE),
        "pool_size": len(_machine_groups()),
    }


async def handle_runner_jit_reconcile(request: web.Request) -> web.Response:
    """手动对一次账：排查「job 排着队没人接」时不用等下一个 30 秒。"""
    blocked = _local_only(request)
    if blocked:
        return blocked
    summary = await _jit_reconcile_once()
    _JIT_LAST_RECONCILE.clear()
    _JIT_LAST_RECONCILE.update(summary)
    return web.json_response({"reconcile": summary, "stats": dict(JIT_POOL["stats"])})


async def handle_runner_jit(request: web.Request) -> web.Response:
    blocked = _local_only(request)
    if blocked:
        return blocked
    return web.json_response(await _offload(_jit_view))


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

    raw = body.get("repos")
    if raw is None:
        one = str(body.get("repo") or "").strip()
        raw = [one] if one else []
    if not isinstance(raw, list):
        return web.json_response({"error": "repos 要是数组"}, status=400)
    repos: list[str] = []
    for item in raw:
        name = str(item or "").strip()
        if not name:
            continue
        if not _REPO_RE.match(name):
            return web.json_response(
                {"error": f"仓库名要写成 owner/name（只填组织名走组织级）：{name}"}, status=400)
        if name not in repos:
            repos.append(name)
    if not repos:
        return web.json_response(
            {"error": "至少要说一个仓库——去「仓库」页把要接的打开"}, status=400)

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
        # 不填就跟机器数一样：一台机器一次接一个 job，再大也没意义
        parallel = int(body.get("max_parallel") or max(1, len(names)))
    except (TypeError, ValueError):
        return web.json_response({"error": "max_parallel 要是数字"}, status=400)
    if parallel < 1 or parallel > 16:
        return web.json_response({"error": "max_parallel 要在 1~16 之间"}, status=400)

    if not names:
        return web.json_response(
            {"error": "池子是空的：先用「添加一台机器」把机器连一遍，密码只在 30 分钟内有效"},
            status=400)

    JIT_POOL.update(machines=names, max_parallel=parallel)
    JIT_POOL["repos"] = {r: True for r in repos}
    JIT_POOL["stats"]["last_error"] = ""
    log.info("JIT 开启：接 %d 个仓库 %s，机器 %s，并发 %d",
             len(repos), repos, names, parallel)
    _jit_save(request.app["store"])
    return web.json_response(_jit_view())


async def handle_runner_jit_stop(request: web.Request) -> web.Response:
    blocked = _local_only(request)
    if blocked:
        return blocked
    JIT_POOL["repos"] = {}
    log.info("JIT 关闭（所有仓库都停了）")
    _jit_save(request.app["store"])
    return web.json_response(_jit_view())


async def handle_runner_jit_repo(request: web.Request) -> web.Response:
    """仓库页那排开关：单独开/关一个仓库，不影响别的。"""
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
            {"error": "仓库名要写成 owner/name"}, status=400)
    on = body.get("on")
    if not isinstance(on, bool):
        return web.json_response({"error": "on 要是 true 或 false"}, status=400)

    if on:
        JIT_POOL["repos"][repo] = True
        # 池子不用人配：这一下就把当前能连的机器全算进来
        _jit_auto_pool()
        log.info("JIT 接上：%s", repo)
    else:
        if repo in JIT_POOL["repos"]:
            JIT_POOL["repos"][repo] = False
        log.info("JIT 放手：%s", repo)
    _jit_save(request.app["store"])
    return web.json_response(_jit_view())


async def handle_runner_jit_pool(request: web.Request) -> web.Response:
    """机器池和并发：跟仓库开关分开设，改这个不动仓库。"""
    blocked = _local_only(request)
    if blocked:
        return blocked
    try:
        body = await request.json()
    except Exception:  # noqa: BLE001
        body = {}
    if not isinstance(body, dict):
        body = {}

    live = _machines_live()
    wanted = body.get("machines")
    if wanted is None:
        names = list(JIT_POOL["machines"])
    else:
        if not isinstance(wanted, list):
            return web.json_response({"error": "machines 要是数组"}, status=400)
        names = [str(x) for x in wanted]
        missing = [n for n in names if n not in live]
        if missing:
            return web.json_response(
                {"error": f"这些机器没凭据或已过期：{', '.join(missing[:3])}"}, status=400)

    try:
        parallel = int(body.get("max_parallel") or JIT_POOL["max_parallel"] or 2)
    except (TypeError, ValueError):
        return web.json_response({"error": "max_parallel 要是数字"}, status=400)
    if parallel < 1 or parallel > 16:
        return web.json_response({"error": "max_parallel 要在 1~16 之间"}, status=400)

    JIT_POOL.update(machines=names, max_parallel=parallel)
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


# ── 随系统自动启动：macOS 写 launchd，Linux 写 systemd user unit ──────────
# 不依赖 sudo：两个平台都装在用户目录、以当前用户身份跑（macOS 登录后起，
# Linux 要开机就起得再开 linger）。服务文件里的绝对路径全部现场拼，换机器不用改。

SERVICE_LABEL = "local.github-local-action"
SERVICE_UNIT = "github-local-action.service"
SERVICE_TIMEOUT = 15.0
# launchd / systemd 拉起的进程没有登录 shell 的 PATH，gh 在 /opt/homebrew/bin 或
# /usr/local/bin 里就找不到了，所以显式给一条。
SERVICE_PATH = "/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"


def _service_platform() -> str:
    if sys.platform == "darwin":
        return "macos"
    if sys.platform.startswith("linux"):
        return "linux"
    return ""


def _service_paths(label: str = SERVICE_LABEL, unit: str = SERVICE_UNIT) -> dict[str, Path]:
    home = Path.home()
    if _service_platform() == "macos":
        return {"file": home / "Library/LaunchAgents" / f"{label}.plist",
                "log": home / "Library/Logs" / "github-local-action.log"}
    return {"file": home / ".config" / "systemd" / "user" / unit,
            "log": home / ".local" / "state" / "github-local-action.log"}


def _xml_escape(text: str) -> str:
    return (str(text).replace("&", "&amp;").replace("<", "&lt;")
            .replace(">", "&gt;").replace('"', "&quot;"))


def _service_plist(label: str, cmd: list[str], workdir: str, log: str) -> str:
    """launchd 的 LaunchAgent 配置。RunAtLoad：登录就起；KeepAlive：掉了再拉。"""
    args = "".join(f"    <string>{_xml_escape(x)}</string>\n" for x in cmd)
    return "\n".join([
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" '
        '"http://www.apple.com/DTDs/PropertyList-1.0.dtd">',
        '<plist version="1.0">', '<dict>',
        f'  <key>Label</key>\n  <string>{_xml_escape(label)}</string>',
        '  <key>ProgramArguments</key>', '  <array>',
        args.rstrip("\n"), '  </array>',
        f'  <key>WorkingDirectory</key>\n  <string>{_xml_escape(workdir)}</string>',
        '  <key>RunAtLoad</key>\n  <true/>',
        '  <key>KeepAlive</key>\n  <true/>',
        '  <key>ThrottleInterval</key>\n  <integer>10</integer>',
        f'  <key>StandardOutPath</key>\n  <string>{_xml_escape(log)}</string>',
        f'  <key>StandardErrorPath</key>\n  <string>{_xml_escape(log)}</string>',
        '  <key>EnvironmentVariables</key>', '  <dict>',
        f'    <key>PATH</key>\n    <string>{SERVICE_PATH}</string>',
        '  </dict>',
        '</dict>', '</plist>', "",
    ])


def _service_unit(cmd: list[str], workdir: str, log: str) -> str:
    """systemd user unit。Restart=on-failure 比 always 温和：正常退出不拉起来。"""
    return "\n".join([
        "[Unit]",
        "Description=GitHub Local Action 本地接收端",
        "After=network-online.target",
        "",
        "[Service]",
        "Type=simple",
        f"WorkingDirectory={workdir}",
        "ExecStart=" + " ".join(shlex.quote(x) for x in cmd),
        "Restart=on-failure",
        "RestartSec=5",
        f"Environment=PATH={SERVICE_PATH}",
        f"StandardOutput=append:{log}",
        f"StandardError=append:{log}",
        "",
        "[Install]",
        "WantedBy=default.target",
        "",
    ])


async def _svc_run(args: list[str]) -> tuple[int, str]:
    """跑一条 launchctl / systemctl，把 stdout+stderr 一起拿回来。"""
    try:
        proc = await asyncio.create_subprocess_exec(
            *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
    except FileNotFoundError:
        return 127, f"本机没有 {args[0]}"
    try:
        out, _ = await asyncio.wait_for(proc.communicate(), timeout=SERVICE_TIMEOUT)
    except asyncio.TimeoutError:
        proc.kill()
        return 124, f"{args[0]} 超过 {SERVICE_TIMEOUT:.0f}s 没返回"
    return proc.returncode or 0, out.decode("utf-8", "replace")


def _service_command(cfg: Config) -> list[str]:
    """服务里执行的命令：当前解释器 + 当前脚本 + 当前配置文件。"""
    args = [sys.executable, str(Path(__file__).resolve())]
    if cfg.path:
        args += ["-c", str(cfg.path)]
    return args


async def _service_domain() -> str:
    return f"gui/{os.getuid()}"


async def _service_launchd_state(label: str) -> tuple[bool, bool, int]:
    """(加载没有, 在跑没有, pid)。问不到就当没加载。"""
    rc, out = await _svc_run(["launchctl", "print", f"{await _service_domain()}/{label}"])
    if rc != 0:
        return False, False, 0
    pid = 0
    match = re.search(r"\bpid = (\d+)", out)
    if match:
        pid = int(match.group(1))
    running = pid > 0 or "state = running" in out
    return True, running, pid


async def _service_status() -> dict[str, Any]:
    plat = _service_platform()
    paths = _service_paths()
    info: dict[str, Any] = {
        "platform": plat, "supported": bool(plat),
        "installed": paths["file"].exists(),
        "file": str(paths["file"]), "log": str(paths["log"]),
        "label": SERVICE_LABEL, "enabled": False, "running": False,
        "managed": False, "linger": None, "hint": "", "error": "",
    }
    if not plat:
        info["hint"] = "只支持 macOS 和 Linux"
        return info
    if plat == "macos":
        loaded, running, pid = await _service_launchd_state(SERVICE_LABEL)
        info.update(loaded=loaded, running=running,
                    managed=bool(pid and pid == os.getpid()))
        info["enabled"] = info["installed"]        # LaunchAgent 放到目录里就算启用
        if info["managed"]:
            info["hint"] = "当前这个进程就是 launchd 拉起来的"
        elif info["installed"]:
            info["hint"] = "已装好，下次登录/开机自动启动"
        return info
    rc, out = await _svc_run(["systemctl", "--user", "is-enabled", SERVICE_UNIT])
    info["enabled"] = out.strip() == "enabled"
    rc2, out2 = await _svc_run(["systemctl", "--user", "is-active", SERVICE_UNIT])
    info["running"] = out2.strip() == "active"
    rc3, out3 = await _svc_run(["systemctl", "--user", "show", SERVICE_UNIT, "-p", "MainPID", "--value"])
    try:
        pid = int(out3.strip() or 0)
    except ValueError:
        pid = 0
    info["managed"] = bool(pid and pid == os.getpid())
    rc4, out4 = await _svc_run(["loginctl", "show-user", os.environ.get("USER", ""), "-p", "Linger"])
    if rc4 == 0 and "Linger=" in out4:
        info["linger"] = out4.strip().endswith("yes")
    if rc == 127:
        info["error"] = out or "本机没有 systemctl"
    elif rc != 0 and "Failed to connect to bus" in (out or ""):
        info["error"] = "当前用户没有可用的 systemd 会话（无图形登录/无 linger），服务无法在本会话里启停"
    if info["managed"]:
        info["hint"] = "当前这个进程就是 systemd 拉起来的"
    elif info["installed"]:
        info["hint"] = ("已装好，开机自动启动" if info["linger"] else
                         "已装好；登录后才会启动，开机就起还需 loginctl enable-linger")
    return info


async def _service_install(cfg: Config) -> dict[str, Any]:
    plat = _service_platform()
    if not plat:
        return {"ok": False, "error": "只支持 macOS 和 Linux"}
    paths = _service_paths()
    cmd = _service_command(cfg)
    workdir = str(Path(__file__).resolve().parent)
    try:
        paths["file"].parent.mkdir(parents=True, exist_ok=True)
        paths["log"].parent.mkdir(parents=True, exist_ok=True)
        text = (_service_plist(SERVICE_LABEL, cmd, workdir, str(paths["log"])) if plat == "macos"
                else _service_unit(cmd, workdir, str(paths["log"])))
        paths["file"].write_text(text, encoding="utf-8")
    except OSError as exc:
        return {"ok": False, "error": f"写服务文件失败：{exc}"}

    note = ""
    if plat == "linux":
        rc, out = await _svc_run(["systemctl", "--user", "daemon-reload"])
        if rc != 0:
            note = (out or "").strip()[:200] or f"daemon-reload 退出码 {rc}"
        else:
            rc, out = await _svc_run(["systemctl", "--user", "enable", SERVICE_UNIT])
            if rc != 0:
                note = (out or "").strip()[:200] or f"enable 退出码 {rc}"
    # 现在不去 bootstrap：当前这个进程还占着端口，拉起来只会撞端口然后被反复重启。
    return {"ok": True, "note": note, "file": str(paths["file"]), "log": str(paths["log"]),
            "needs_relaunch": True}


async def _service_uninstall() -> dict[str, Any]:
    plat = _service_platform()
    paths = _service_paths()
    note = ""
    if plat == "macos":
        rc, out = await _svc_run(["launchctl", "bootout",
                                  f"{await _service_domain()}/{SERVICE_LABEL}"])
        if rc != 0:
            note = (out or "").strip()[:200]
        await _svc_run(["launchctl", "disable", f"{await _service_domain()}/{SERVICE_LABEL}"])
    elif plat == "linux":
        rc, out = await _svc_run(["systemctl", "--user", "disable", "--now", SERVICE_UNIT])
        if rc != 0:
            note = (out or "").strip()[:200]
        await _svc_run(["systemctl", "--user", "daemon-reload"])
        removed = False
    try:
        if paths["file"].exists():
            paths["file"].unlink()
            removed = True
    except OSError as exc:
        return {"ok": False, "error": f"删服务文件失败：{exc}", "note": note}
    return {"ok": True, "removed": removed, "note": note}


async def handle_service(request: web.Request) -> web.Response:
    blocked = _local_only(request)
    if blocked:
        return blocked
    return web.json_response(await _service_status())


async def handle_service_install(request: web.Request) -> web.Response:
    blocked = _local_only(request)
    if blocked:
        return blocked
    result = await _service_install(request.app["cfg"])
    result["status"] = await _service_status()
    return web.json_response(result, status=200 if result.get("ok") else 500)


async def handle_service_uninstall(request: web.Request) -> web.Response:
    blocked = _local_only(request)
    if blocked:
        return blocked
    result = await _service_uninstall()
    result["status"] = await _service_status()
    return web.json_response(result, status=200 if result.get("ok") else 500)


# ─────────────────────────────────────────────────────────────
# 中继自检 / 一键部署（Cloudflare Worker + wrangler）
#
# 以前中继地址是手填的：先自己跑一遍 wrangler deploy，人眼从输出里抄一行 URL，
# 再回设置页粘上。抄错一个字母就是「连接失败」，日志里看不出是抄错了还是网断了。
# 这里让程序自己干：读 wrangler.toml 的 name → 跑 deploy → 从输出里抠出
# <name>.<子域>.workers.dev → 拼成 wss://…/ws 写回配置并重连。
#
# 页面上还要能看出「现在到底有没有权限」：没登录、OAuth 令牌过期、缺 workers
# write，这些错以前都要等到 deploy 那一刻才蹦出来。所以另给一个只读自检：
# whoami 看身份和 scope，deployments list 真读一次（读得到说明这个 worker 对
# 当前令牌可见、且令牌没过期），再顺手 GET 一下 /health 确认地址通不通。
# ─────────────────────────────────────────────────────────────

WORKER_DIRNAME = "remote-worker"
WORKER_DEPLOY_TIMEOUT = 300.0
WORKER_QUERY_TIMEOUT = 60.0
RELAY_CACHE_TTL = 20.0
_RELAY_CACHE: dict[str, Any] = {"at": 0.0, "data": None}

# wrangler 是 node 脚本。服务（launchd/systemd）拉起来的进程没有登录 shell 的
# PATH，/opt/homebrew/bin 里没有 node 时它就直接起不来，所以这些目录都得兜住。
_RELAY_BINS = ("/opt/homebrew/bin", "/usr/local/bin", "/usr/bin", "/bin",
               "/usr/sbin", "/sbin", "~/.volta/bin", "~/.local/bin", "~/.bun/bin")


def _relay_dir() -> Path:
    return Path(__file__).resolve().parent.parent / WORKER_DIRNAME


def _relay_env() -> dict[str, str]:
    """给 wrangler 用的环境：补 PATH，关掉花哨输出和遥测。"""
    dirs: list[str] = []
    for exe in ("wrangler", "node", "npx"):
        found = shutil.which(exe)
        if found:
            dirs.append(str(Path(found).parent))
    for d in _RELAY_BINS:
        dirs.append(str(Path(d).expanduser()))
    dirs += sorted(glob.glob(str(Path.home() / ".nvm/versions/node/*/bin")), reverse=True)
    seen: set[str] = set()
    path_list: list[str] = []
    for d in dirs:
        if d and d not in seen and Path(d).is_dir():
            seen.add(d)
            path_list.append(d)
    env = dict(os.environ)
    env["PATH"] = os.pathsep.join(path_list)
    env.update({"CI": "1", "NO_COLOR": "1", "FORCE_COLOR": "0",
                "WRANGLER_SEND_METRICS": "false"})
    return env


def _relay_strip_ansi(text: str) -> str:
    return re.sub(r"\x1b\[[0-9;]*[A-Za-z]", "", text or "")


def _worker_name() -> str:
    """从 wrangler.toml 抠 worker 名（部署出来的地址前缀就是它）。"""
    toml = _relay_dir() / "wrangler.toml"
    try:
        text = toml.read_text(encoding="utf-8")
    except OSError:
        return ""
    m = re.search(r'^\s*name\s*=\s*"([^"]+)"', text, re.M)
    return m.group(1) if m else ""


async def _relay_run(args: list[str], timeout: float) -> tuple[int, str]:
    """跑一条 wrangler 命令，stdout/stderr 合并返回。"""
    exe = shutil.which("wrangler")
    if not exe:
        return 127, ("没装 wrangler。装一个：npm i -g wrangler "
                     "（或 brew install wrangler / pnpm add -g wrangler）")
    try:
        proc = await asyncio.create_subprocess_exec(
            exe, *args, cwd=str(_relay_dir()), env=_relay_env(),
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
    except OSError as exc:
        return 126, f"启动 wrangler 失败：{exc}"
    try:
        out, _ = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except asyncio.TimeoutError:
        try:
            proc.kill()
        except ProcessLookupError:
            pass
        await proc.wait()
        return 124, f"wrangler {args[0] if args else ''} 超时（{int(timeout)}s）"
    return (proc.returncode or 0), _relay_strip_ansi((out or b"").decode("utf-8", "replace"))


def _relay_last_error(text: str) -> str:
    """从 wrangler 输出里挑一句像错误的（✘ [ERROR] 那行最好用）。

    行首那几个包装（✘、[ERROR]、ERROR:）都剥掉——页面上要显示的是「哪出错了」，
    不是 wrangler 自己的装饰。
    """
    for line in (text or "").splitlines():
        s = line.strip()
        if not s:
            continue
        if "ERROR" in s or "✘" in s or s.lower().startswith("error"):
            return _relay_clean_error(s)
    lines = [ln.strip() for ln in (text or "").splitlines() if ln.strip()]
    return _relay_clean_error(lines[-1]) if lines else ""


def _relay_clean_error(line: str) -> str:
    s = line.replace("✘", "").strip()
    s = re.sub(r"^\[?ERROR\]?[:：]?\s*", "", s, flags=re.I)
    return s.strip()[:300]


def _parse_whoami(text: str) -> dict[str, Any]:
    """解析 `wrangler whoami`：身份、账号、scope，以及能不能部署。"""
    info: dict[str, Any] = {
        "logged_in": False, "email": "", "account_id": "", "account_name": "",
        "scopes": [], "can_deploy": False, "missing_scopes": [],
    }
    low = (text or "").lower()
    if ("not authenticated" in low or "not logged in" in low
            or "wrangler login" in low and "you are logged in" not in low):
        return info

    m = re.search(r"email\s+(\S+@\S+?)[\.\s]*$", text, re.M)
    if m:
        info["email"] = m.group(1).rstrip(".")
    for line in text.splitlines():
        if "│" in line and re.search(r"\b[0-9a-f]{32}\b", line):
            cells = [c.strip() for c in line.strip("│").split("│")]
            if len(cells) >= 2 and re.fullmatch(r"[0-9a-f]{32}", cells[1]):
                info["account_name"], info["account_id"] = cells[0], cells[1]
                break

    for line in text.splitlines():
        m3 = re.match(r"^-\s+([A-Za-z0-9_.:-]+)(?:\s+\((read|write|admin)\))?\s*$",
                      line.strip())
        if m3:
            info["scopes"].append({"name": m3.group(1), "access": m3.group(2) or ""})
    # 老式令牌只有 workers (write)；新式是 workers_scripts (write)。两个都认。
    writable = {s["name"] for s in info["scopes"] if s["access"] in ("write", "admin")}
    info["can_deploy"] = bool(writable & {"workers", "workers_scripts"})

    # 「missing scopes are:」下面是另一批 - xxx，长得和上面的 scope 一模一样，
    # 所以要从那行切开分开解析，否则会被当成已有权限、然后被「不在 known 里」过滤掉。
    _, _, tail = text.partition("missing scopes are")
    for line in tail.splitlines():
        s = line.strip()
        if s.startswith("-"):
            name = s.lstrip("- ").split(" ")[0].strip()
            if name:
                info["missing_scopes"].append(name)
    info["logged_in"] = bool(info["email"] or info["account_id"])
    return info


def _parse_wrangler_version(text: str) -> str:
    m = re.search(r"wrangler\s+(\d+\.\d+\.\d+)", text or "")
    return m.group(1) if m else ""


_WORKER_URL_RE = re.compile(r"https://([A-Za-z0-9][A-Za-z0-9_.-]*\.workers\.dev)")


def _deploy_url_to_ws(url: str) -> str:
    """https://x.workers.dev → wss://x.workers.dev/ws（客户端连的是这个）。"""
    u = (url or "").strip().rstrip("/")
    if not u:
        return ""
    if u.endswith("/ws"):
        u = u[:-3]
    u = u.replace("https://", "wss://").replace("http://", "ws://")
    return u + "/ws"


def _relay_http_error(status: int, ctype: str, body: str) -> str:
    """把 HTTP 错误变成一句人话。

    worker 被删掉时 workers.dev 返回的是一整页 HTML（里面藏着 error code: 1042），
    原样贴进卡片就是一堆 <!DOCTYPE html>，谁也看不出是「worker 不在了」。
    """
    text = (body or "").strip()
    looks_html = "text/html" in (ctype or "").lower() or text.startswith("<")
    if looks_html:
        code = re.search(r"error code:\s*(\d+)", text)
        return f"HTTP {status}" + (f"（Cloudflare error code {code.group(1)}）" if code else "（返回了一个网页，不是接口）")
    return f"HTTP {status} " + text.replace("\n", " ")[:160]


async def _relay_health(cfg: Config) -> dict[str, Any]:
    """GET 一下中继的 /health：既验地址填得对不对，也验它是否活着。"""
    base = _worker_base(cfg)
    info: dict[str, Any] = {"ok": False, "base": base, "status": 0,
                           "events": None, "cursor": None, "error": ""}
    if not base:
        info["error"] = "配置里没有中继地址"
        return info
    try:
        timeout = aiohttp.ClientTimeout(total=8)
        async with aiohttp.ClientSession(timeout=timeout) as sess:
            async with sess.get(f"{base}/health") as resp:
                info["status"] = resp.status
                if resp.status == 200:
                    body = await resp.json(content_type=None)
                    info["ok"] = bool(body.get("ok"))
                    info["events"] = body.get("events")
                    info["cursor"] = body.get("cursor")
                else:
                    info["error"] = _relay_http_error(
                        resp.status, resp.headers.get("Content-Type", ""),
                        await resp.text())
    except Exception as exc:                       # noqa: BLE001
        info["error"] = f"{type(exc).__name__}: {exc}"[:200]
    return info


async def _relay_status(cfg: Config, force: bool = False) -> dict[str, Any]:
    """只读自检：wrangler 装没装、登录没登录、有没有权限、worker 在不在、地址通不通。

    whoami 和 deployments list 各要一两秒，页面每次进设置都等它不合适，缓存 20 秒；
    ?force=1（或点「重新检测」）绕过缓存。
    """
    now = time.time()
    cached = _RELAY_CACHE.get("data")
    if not force and cached and now - float(_RELAY_CACHE.get("at") or 0) < RELAY_CACHE_TTL:
        return cached

    exe = shutil.which("wrangler") or ""
    name = _worker_name()
    whoami_task = asyncio.ensure_future(_relay_run(["whoami"], WORKER_QUERY_TIMEOUT)) if exe else None
    deploy_task = (asyncio.ensure_future(
        _relay_run(["deployments", "list", "--name", name], WORKER_QUERY_TIMEOUT))
        if exe and name else None)

    data: dict[str, Any] = {
        "ws_url": cfg.ws_url,
        "worker_base": _worker_base(cfg),
        "worker_name": name,
        "worker_dir": str(_relay_dir()),
        "dir_exists": (_relay_dir() / "wrangler.toml").exists(),
        "wrangler": {"found": bool(exe), "path": exe, "version": ""},
        "auth": {"logged_in": False, "email": "", "account_id": "", "account_name": "",
                 "scopes": [], "can_deploy": False, "missing_scopes": [], "error": ""},
        "worker": {"exists": None, "deployments": 0, "latest": "", "error": ""},
        "health": await _relay_health(cfg),
        "checked_at": int(now),
    }
    if not exe:
        data["auth"]["error"] = "没装 wrangler，装一个才能部署"
    elif not name:
        data["worker"]["error"] = f"{_relay_dir()}/wrangler.toml 里找不到 name"
    else:
        rc, out = await whoami_task
        data["wrangler"]["version"] = _parse_wrangler_version(out)
        data["auth"].update(_parse_whoami(out))
        if rc != 0 and not data["auth"]["logged_in"]:
            data["auth"]["error"] = _relay_last_error(out) or f"wrangler whoami 退出码 {rc}"

        rc2, out2 = await deploy_task
        if rc2 == 0:
            data["worker"]["exists"] = True
            data["worker"]["deployments"] = len(re.findall(r"^Version\(s\):", out2, re.M))
            # 列表是旧→新，取最大值才是「最近一次」（ISO 时间字符串按字典序就是时间序）
            stamps = re.findall(r"^Created:\s+(\S+)", out2, re.M)
            data["worker"]["latest"] = max(stamps) if stamps else ""
        else:
            msg = _relay_last_error(out2) or f"wrangler deployments list 退出码 {rc2}"
            data["worker"]["exists"] = False
            data["worker"]["error"] = msg

    _RELAY_CACHE["at"] = now
    _RELAY_CACHE["data"] = data
    return data


async def _relay_deploy(cfg: Config, apply: bool = True) -> dict[str, Any]:
    """跑 wrangler deploy，把输出里的地址变成 ws_url。

    apply=True 时才写配置：先改内存、写盘失败就回滚，和 POST /config 一样。
    """
    name = _worker_name()
    started = time.time()
    rc, out = await _relay_run(["deploy"], WORKER_DEPLOY_TIMEOUT)
    lines = [ln for ln in out.splitlines() if ln.strip()]
    result: dict[str, Any] = {
        "ok": rc == 0, "rc": rc, "seconds": round(time.time() - started, 1),
        "worker_name": name, "url": "", "ws_url": "",
        "applied": False, "changed": False, "reconnect": False,
        "tail": "\n".join(lines[-14:]),
    }
    m = _WORKER_URL_RE.search(out)
    if m:
        result["url"] = "https://" + m.group(1)
        result["ws_url"] = _deploy_url_to_ws(result["url"])
    if rc != 0:
        result["error"] = _relay_last_error(out) or f"wrangler deploy 退出码 {rc}"
        return result
    if not result["ws_url"]:
        result["ok"] = False
        result["error"] = ("部署成功，但输出里没有 workers.dev 地址（可能只绑了自定义域名），"
                          "地址得手工填")
        return result
    if not apply:
        return result
    if cfg.ws_url == result["ws_url"]:
        result["applied"] = True
        return result
    previous = cfg.ws_url
    cfg.ws_url = result["ws_url"]
    try:
        save_config(cfg)
    except OSError as exc:
        cfg.ws_url = previous
        result["ok"] = False
        result["error"] = f"部署成功但写配置失败：{exc}"
        return result
    result["applied"] = True
    result["changed"] = True
    return result


async def handle_relay(request: web.Request) -> web.Response:
    blocked = _local_only(request)
    if blocked:
        return blocked
    force = request.query.get("force") in ("1", "true", "yes")
    return web.json_response(await _relay_status(request.app["cfg"], force))


async def handle_relay_deploy(request: web.Request) -> web.Response:
    blocked = _local_only(request)
    if blocked:
        return blocked
    try:
        body = await request.json()
    except Exception:                              # noqa: BLE001
        body = {}
    if not isinstance(body, dict):
        body = {}
    cfg: Config = request.app["cfg"]
    state: State = request.app["state"]
    result = await _relay_deploy(cfg, apply=body.get("apply", True) is not False)
    _RELAY_CACHE["data"] = None                    # 下一次查询重新采集
    if result.get("changed") and state.ws is not None:
        state.kick = True                          # 地址变了，让 ws_loop 用新地址重连
        try:
            await state.ws.close()
        except Exception:                          # noqa: BLE001
            pass
        result["reconnect"] = True
    result["status"] = await _relay_status(cfg, True)
    return web.json_response(result, status=200 if result.get("ok") else 500)


# ─────────────────────────────────────────────────────────────
# 自定义域名：把中继绑到账号里自己的域名
#
# workers.dev 那个地址不走国内 CDN、也记不住。Cloudflare 支持「Workers 自定义域名」：
# 账号里只要有这个 zone，绑上去就能用一个自己的子域名访问，证书自动签，不用改
# wrangler.toml、不用重新部署。
#
# 凭据用现成的：wrangler 登录后把 oauth_token 明文写在 ~/.wrangler/config/default.toml，
# 直接用它的 API 就行，不用另外叫人去建一个 API Token。令牌会过期，过期就跑一次
# wrangler（它自己会刷新并写回文件）再重试。
# ─────────────────────────────────────────────────────────────

CF_API = "https://api.cloudflare.com/client/v4"
DOMAINS_CACHE_TTL = 20.0
_DOMAINS_CACHE: dict[str, Any] = {"at": 0.0, "data": None}
_CF_ACCOUNT: dict[str, str] = {"id": ""}

_HOST_RE = re.compile(r"^(?=.{4,253}$)([a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,}$")


def _cf_credentials() -> tuple[str, float]:
    """读 wrangler 的 OAuth 令牌（明文在 ~/.wrangler/config/default.toml）。"""
    path = Path.home() / ".wrangler" / "config" / "default.toml"
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return "", 0.0
    tok = re.search(r'^oauth_token\s*=\s*"([^"]+)"', text, re.M)
    exp = re.search(r'^expiration_time\s*=\s*"([^"]+)"', text, re.M)
    ts = 0.0
    if exp:
        try:
            ts = datetime.fromisoformat(exp.group(1).replace("Z", "+00:00")).timestamp()
        except ValueError:
            ts = 0.0
    return (tok.group(1) if tok else ""), ts


async def _cf_refresh() -> bool:
    """令牌过期了就借 wrangler 刷一下：它自己会续期并写回同一个文件。"""
    rc, _ = await _relay_run(["whoami"], 60.0)
    return rc == 0


async def _cf_api(method: str, path: str, body: Any = None,
                  retry: bool = True) -> dict[str, Any]:
    """调 Cloudflare API。令牌过期时自动刷一次再重试。"""
    tok, exp = _cf_credentials()
    if not tok:
        return {"ok": False, "status": 0, "result": None,
                "error": "没有 wrangler 的登录凭据（先 wrangler login）"}
    if exp and exp < time.time() and retry:
        await _cf_refresh()
        tok, exp = _cf_credentials()
    try:
        timeout = aiohttp.ClientTimeout(total=25)
        async with aiohttp.ClientSession(timeout=timeout) as sess:
            async with sess.request(method, f"{CF_API}{path}", json=body,
                                    headers={"Authorization": f"Bearer {tok}"}) as resp:
                raw = await resp.text()
                try:
                    data = json.loads(raw) if raw.strip() else {}
                except ValueError:
                    data = {}
                # 204 是空的：删除类接口就靠它表示成功，别当成失败
                if resp.status in (200, 201, 204) and data.get("success", True):
                    return {"ok": True, "status": resp.status,
                            "result": data.get("result"), "error": ""}
                msg = "; ".join(str(e.get("message", ""))
                                for e in (data.get("errors") or []))[:300]
                low = msg.lower()
                if retry and resp.status in (401, 403) and (
                        "token" in low or "authenticat" in low or "expired" in low):
                    if await _cf_refresh():
                        return await _cf_api(method, path, body, retry=False)
                return {"ok": False, "status": resp.status,
                        "result": data.get("result"),
                        "error": msg or f"HTTP {resp.status}"}
    except Exception as exc:                       # noqa: BLE001
        return {"ok": False, "status": 0, "result": None,
                "error": f"{type(exc).__name__}: {exc}"[:200]}


def _valid_host(host: str) -> bool:
    return bool(_HOST_RE.match((host or "").strip().lower().rstrip(".")))


def _zone_for(host: str, zones: list[dict[str, Any]]) -> Optional[dict[str, Any]]:
    """挑最长匹配的 zone：api.x.example.com 属于 example.com，不是 x.example.com。"""
    name = (host or "").strip().lower().rstrip(".")
    best: Optional[dict[str, Any]] = None
    for z in zones:
        zn = str(z.get("name") or "").lower()
        if not zn:
            continue
        if name == zn or name.endswith("." + zn):
            if best is None or len(zn) > len(str(best.get("name") or "")):
                best = z
    return best


def _workers_dev_url(name: str, sub: str) -> str:
    if not name or not sub:
        return ""
    return f"wss://{name}.{sub}.workers.dev/ws"


async def _cf_account() -> str:
    """账号 id：先看缓存，没有再问 /accounts（wrangler 令牌只能看自己那个）。"""
    if _CF_ACCOUNT["id"]:
        return _CF_ACCOUNT["id"]
    res = await _cf_api("GET", "/accounts")
    rows = res.get("result") or []
    if res.get("ok") and rows:
        _CF_ACCOUNT["id"] = str(rows[0].get("id") or "")
    return _CF_ACCOUNT["id"]


async def _cf_subdomain(account: str) -> str:
    res = await _cf_api("GET", f"/accounts/{account}/workers/subdomain")
    if res.get("ok") and isinstance(res.get("result"), dict):
        return str((res["result"] or {}).get("subdomain") or "")
    return ""


async def _cf_domains(account: str, service: str) -> tuple[list[dict[str, Any]], str]:
    """列出这个 worker 已绑的自定义域名；附带错误文字。"""
    res = await _cf_api("GET", f"/accounts/{account}/workers/domains")
    if not res.get("ok"):
        return [], str(res.get("error") or "读不到已绑域名")
    rows = [r for r in (res.get("result") or []) if isinstance(r, dict)]
    mine = [r for r in rows if str(r.get("service") or "") == service]
    return mine, ""


async def _probe_host(host: str, seconds: float = 40.0) -> dict[str, Any]:
    """绑完等 HTTPS 真的通：证书签发要几秒到几十秒。"""
    started = time.time()
    last = ""
    while time.time() - started < seconds:
        try:
            timeout = aiohttp.ClientTimeout(total=6)
            async with aiohttp.ClientSession(timeout=timeout) as sess:
                async with sess.get(f"https://{host}/health") as resp:
                    if resp.status == 200:
                        body = await resp.json(content_type=None)
                        return {"ok": True, "status": 200, "seconds": round(time.time() - started, 1),
                                "events": body.get("events"), "cursor": body.get("cursor"), "error": ""}
                    last = f"HTTP {resp.status}"
        except Exception as exc:                   # noqa: BLE001
            last = f"{type(exc).__name__}: {exc}"[:160]
        await asyncio.sleep(2.0)
    return {"ok": False, "status": 0, "seconds": round(time.time() - started, 1),
            "events": None, "cursor": None, "error": last or "等超时了还没通"}


async def _relay_domains(cfg: Config, force: bool = False) -> dict[str, Any]:
    """域名面板：能用哪些 zone、现在绑了哪些、默认地址是什么。"""
    now = time.time()
    cached = _DOMAINS_CACHE.get("data")
    if not force and cached and now - float(_DOMAINS_CACHE.get("at") or 0) < DOMAINS_CACHE_TTL:
        return cached

    name = _worker_name()
    tok, exp = _cf_credentials()
    out: dict[str, Any] = {
        "worker_name": name, "has_token": bool(tok),
        "token_expired": bool(exp and exp < now),
        "account_id": "", "zones": [], "bound": [], "subdomain": "",
        "workers_dev_url": "", "error": "",
    }
    if not tok:
        out["error"] = "读不到 wrangler 的登录凭据，先 wrangler login"
        _DOMAINS_CACHE.update(at=now, data=out)
        return out

    account = await _cf_account()
    out["account_id"] = account
    if not account:
        out["error"] = "读不到 Cloudflare 账号 id"
        _DOMAINS_CACHE.update(at=now, data=out)
        return out

    zres = await _cf_api("GET", "/zones?per_page=50")
    if zres.get("ok"):
        out["zones"] = [{"name": z.get("name"), "id": z.get("id"),
                         "status": z.get("status")}
                        for z in (zres.get("result") or []) if isinstance(z, dict)]
    else:
        out["error"] = str(zres.get("error") or "读不到域名列表")

    sub = await _cf_subdomain(account)
    out["subdomain"] = sub
    out["workers_dev_url"] = _workers_dev_url(name, sub)

    bound, err = await _cf_domains(account, name)
    out["bound"] = [{"hostname": r.get("hostname"), "zone_name": r.get("zone_name"),
                     "environment": r.get("environment")} for r in bound]
    if err and not out["error"]:
        out["error"] = err

    _DOMAINS_CACHE.update(at=now, data=out)
    return out


async def _bind_domain(cfg: Config, hostname: str, apply: bool = True) -> dict[str, Any]:
    """把一个子域名绑到中继 worker，等它 HTTPS 通了才算成。"""
    host = (hostname or "").strip().lower().rstrip(".")
    if not _valid_host(host):
        return {"ok": False, "error": "域名格式不对，要像 relay.example.com 这样"}
    name = _worker_name()
    if not name:
        return {"ok": False, "error": "wrangler.toml 里没有 worker 名"}
    account = await _cf_account()
    if not account:
        return {"ok": False, "error": "读不到 Cloudflare 账号 id"}

    zres = await _cf_api("GET", "/zones?per_page=50")
    if not zres.get("ok"):
        return {"ok": False, "error": str(zres.get("error") or "读不到域名列表")}
    zones = [z for z in (zres.get("result") or []) if isinstance(z, dict)]
    zone = _zone_for(host, zones)
    if zone is None:
        names = "、".join(str(z.get("name")) for z in zones) or "（一个都没有）"
        return {"ok": False,
                "error": f"{host} 不在这账号的域名里（现有：{names}），绑不了"}

    res = await _cf_api("PUT", f"/accounts/{account}/workers/domains", {
        "zone_id": zone.get("id"), "hostname": host,
        "service": name, "environment": "production",
    })
    if not res.get("ok"):
        return {"ok": False, "error": str(res.get("error") or "绑定被 Cloudflare 拒了"),
                "hostname": host, "zone": zone.get("name")}

    probe = await _probe_host(host)
    out: dict[str, Any] = {"ok": probe["ok"], "hostname": host,
                           "zone": zone.get("name"), "probe": probe,
                           "ws_url": f"wss://{host}/ws", "changed": False,
                           "reconnect": False, "applied": False,
                           "error": "" if probe["ok"] else
                           f"绑上了，但 https://{host}/health 还没通（{probe['error']}）"}
    if not probe["ok"] or not apply:
        return out
    if cfg.ws_url == out["ws_url"]:
        out["applied"] = True
        return out
    previous = cfg.ws_url
    cfg.ws_url = out["ws_url"]
    try:
        save_config(cfg)
    except OSError as exc:
        cfg.ws_url = previous
        return {**out, "ok": False, "error": f"绑定成功了，但写配置失败：{exc}"}
    out["applied"] = True
    out["changed"] = True
    return out


async def _unbind_domain(cfg: Config, hostname: str) -> dict[str, Any]:
    """解绑；如果中继地址正好指着它，就退回 workers.dev 那个默认地址。"""
    host = (hostname or "").strip().lower().rstrip(".")
    if not _valid_host(host):
        return {"ok": False, "error": "域名格式不对"}
    account = await _cf_account()
    if not account:
        return {"ok": False, "error": "读不到 Cloudflare 账号 id"}
    # 解绑要按 attachment id 删：拿 hostname 当 DELETE 参数会被 Cloudflare 拒
    # （"Method not allowed for this authentication scheme"，看着像权限问题，其实不是）
    listed = await _cf_api("GET", f"/accounts/{account}/workers/domains")
    if not listed.get("ok"):
        return {"ok": False, "error": str(listed.get("error") or "读不到已绑域名")}
    rows = [r for r in (listed.get("result") or []) if isinstance(r, dict)]
    mine = [r for r in rows
            if str(r.get("hostname") or "").lower() == host
            and str(r.get("service") or "") == _worker_name()]
    if not mine:
        removed = True                 # 本来就没绑，幂等处理
    else:
        res = await _cf_api("DELETE",
                            f"/accounts/{account}/workers/domains/{mine[0].get('id')}")
        removed = bool(res.get("ok")) or res.get("status") == 404
        if not removed:
            return {"ok": False, "error": str(res.get("error") or "解绑失败")}

    out: dict[str, Any] = {"ok": True, "hostname": host, "removed": True,
                           "reverted": False, "ws_url": cfg.ws_url, "error": ""}
    if (urlparse(cfg.ws_url).hostname or "") == host:
        back = _workers_dev_url(_worker_name(), await _cf_subdomain(account))
        if back:
            cfg.ws_url = back
            try:
                save_config(cfg)
                out.update(reverted=True, ws_url=back)
            except OSError as exc:
                out["error"] = f"解绑成功了，但把地址改回默认值失败：{exc}"
    return out


async def handle_relay_domains(request: web.Request) -> web.Response:
    blocked = _local_only(request)
    if blocked:
        return blocked
    force = request.query.get("force") in ("1", "true", "yes")
    return web.json_response(await _relay_domains(request.app["cfg"], force))


async def _domain_body(request: web.Request) -> dict[str, Any]:
    try:
        body = await request.json()
    except Exception:                              # noqa: BLE001
        body = {}
    return body if isinstance(body, dict) else {}


async def handle_relay_bind(request: web.Request) -> web.Response:
    blocked = _local_only(request)
    if blocked:
        return blocked
    body = await _domain_body(request)
    cfg: Config = request.app["cfg"]
    state: State = request.app["state"]
    result = await _bind_domain(cfg, str(body.get("hostname") or ""),
                                apply=body.get("apply", True) is not False)
    _DOMAINS_CACHE["data"] = None
    _RELAY_CACHE["data"] = None
    if result.get("changed") and state.ws is not None:
        state.kick = True
        try:
            await state.ws.close()
        except Exception:                          # noqa: BLE001
            pass
        result["reconnect"] = True
    result["domains"] = await _relay_domains(cfg, True)
    return web.json_response(result, status=200 if result.get("ok") else 500)


async def handle_relay_unbind(request: web.Request) -> web.Response:
    blocked = _local_only(request)
    if blocked:
        return blocked
    body = await _domain_body(request)
    cfg: Config = request.app["cfg"]
    state: State = request.app["state"]
    result = await _unbind_domain(cfg, str(body.get("hostname") or ""))
    _DOMAINS_CACHE["data"] = None
    _RELAY_CACHE["data"] = None
    if result.get("reverted") and state.ws is not None:
        state.kick = True
        try:
            await state.ws.close()
        except Exception:                          # noqa: BLE001
            pass
        result["reconnect"] = True
    result["domains"] = await _relay_domains(cfg, True)
    return web.json_response(result, status=200 if result.get("ok") else 500)


def build_app(cfg: Config, store: Store, bus: Bus, state: State) -> web.Application:
    app = web.Application()
    app["store"] = store
    app["bus"] = bus
    app["state"] = state
    app["cfg"] = cfg
    app.router.add_get("/", handle_index)
    app.router.add_post("/mcp", handle_mcp_post)
    app.router.add_get("/mcp", handle_mcp_other)
    app.router.add_delete("/mcp", handle_mcp_other)
    app.router.add_route("OPTIONS", "/mcp", handle_mcp_other)
    app.router.add_get("/agent.md", handle_agent_doc)
    app.router.add_get("/agent.en.md", handle_agent_doc)
    app.router.add_get("/llms.txt", handle_agent_doc)
    app.router.add_get("/stream", handle_stream)
    app.router.add_get("/events", handle_events)
    app.router.add_get("/stats", handle_stats)
    app.router.add_get("/actions", handle_actions)
    app.router.add_get("/github/auth", handle_github_auth)
    app.router.add_get("/github/runs", handle_github_runs)
    app.router.add_get("/github/jobs", handle_github_jobs)
    app.router.add_get("/github/joblog", handle_github_joblog)
    app.router.add_post("/webhook/setup", handle_webhook_setup)
    app.router.add_get("/webhook/status", handle_webhook_status)
    app.router.add_get("/github/repos", handle_github_repos)
    app.router.add_get("/github/runners", handle_github_runners)
    app.router.add_post("/github/runners/remove", handle_github_runner_remove)
    app.router.add_get("/runners", handle_machines)
    app.router.add_get("/machines/live", handle_machines_live)
    app.router.add_post("/runners/delete", handle_machine_delete)
    app.router.add_post("/runners/ssh/add", handle_runner_ssh_add)
    app.router.add_get("/runners/proxy", handle_runner_proxy_get)
    app.router.add_post("/runners/proxy", handle_runner_proxy)
    app.router.add_post("/runners/ssh/remove", handle_runner_ssh_remove)
    app.router.add_get("/runners/ssh/job/{job}", handle_runner_ssh_job)
    app.router.add_get("/runners/jit", handle_runner_jit)
    app.router.add_post("/runners/jit/start", handle_runner_jit_start)
    app.router.add_post("/runners/jit/stop", handle_runner_jit_stop)
    app.router.add_post("/runners/jit/repo", handle_runner_jit_repo)
    app.router.add_post("/runners/jit/pool", handle_runner_jit_pool)
    app.router.add_post("/runners/jit/prepare", handle_runner_jit_prepare)
    app.router.add_post("/runners/jit/reconcile", handle_runner_jit_reconcile)
    app.router.add_get("/service", handle_service)
    app.router.add_post("/service/install", handle_service_install)
    app.router.add_post("/service/uninstall", handle_service_uninstall)
    app.router.add_get("/relay", handle_relay)
    app.router.add_post("/relay/deploy", handle_relay_deploy)
    app.router.add_get("/relay/domains", handle_relay_domains)
    app.router.add_post("/relay/domain", handle_relay_bind)
    app.router.add_post("/relay/domain/remove", handle_relay_unbind)
    app.router.add_get("/health", handle_health)
    app.router.add_get("/config", handle_config)
    app.router.add_post("/config", handle_config_save)
    return app


async def main_async(cfg: Config) -> None:
    global SSH_MACHINES
    store = Store(cfg.db_path)
    # 机器凭据跟事件放同一个库；密钥单独一个文件，别让人把库拷走就能读密码
    db = Path(cfg.db_path)
    SSH_MACHINES = MachineStore(str(db), str(db.with_name(db.name + ".machines.key")))
    log.info("机器清单 %s 台（来自 %s）", len(SSH_MACHINES), cfg.db_path)
    # 上一轮开着的「自动运行」仓库捡回来。必须在 MachineStore 建好之后：早一行
    # SSH_MACHINES 还是空的，机器池会恢复成 0 台。
    _jit_restore(store)
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
    # 对账：webhook 漏了或者当时机器全忙的 job，靠这条定时任务补回来
    reconcile_task = asyncio.create_task(jit_reconcile_loop())
    # 机器状态提前问一次，等真有人打开页面时是热的（冷启动要 1~4 秒）
    prime_task = asyncio.create_task(_prime_machine_live(app))
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
        reconcile_task.cancel()
        try:
            await reconcile_task
        except asyncio.CancelledError:
            pass
        prime_task.cancel()
        bus.close_all()
        await runner.cleanup()
        store.close()
        log.info("已退出")


def main() -> None:
    parser = argparse.ArgumentParser(description="GitHub Local Action 本地接收端")
    parser.add_argument("-c", "--config", default="config.toml", help="配置文件路径")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S",
    )

    cfg = Config.load(Path(args.config))
    global PROXY_URL
    PROXY_URL = cfg.proxy          # 装 runner 时把这一条写进各台机器的 .env
    asyncio.run(main_async(cfg))


if __name__ == "__main__":
    main()

/**
 * hub.ts —— Durable Object：唯一持有 WebSocket 连接的地方
 *
 * 为什么非得是 DO：Worker 无状态，A 请求打开的连接 B 请求摸不到。
 * DO 是单实例、有内存、地址唯一，才能当广播中心。
 *
 * 计费上的关键：用 state.acceptWebSocket() 而不是 pair[1].accept()。
 * 前者让对象在不处理消息时休眠，duration 只按唤醒的几十毫秒算；
 * 后者会让对象一直醒着，24 小时不停计费。
 *
 * 另一个纪律：这里绝对不能出现 setInterval —— 定时器存在就不能休眠。
 * 心跳一律由客户端发。
 */

import type { Env, EventPayload, EventRow, ClientMsg } from './protocol';

const REPLAY_LIMIT = 200;      // 每次补发条数
const REPLAY_MAX = 2000;       // 单次 hello 最多补发多少条，防止极端情况把对象拖死

function toPayload(row: EventRow): EventPayload {
  let parsed: unknown = null;
  try {
    parsed = JSON.parse(row.payload);
  } catch {
    parsed = row.payload;
  }
  return {
    type: 'event',
    id: row.id,
    event: row.type,
    repo: row.repo,
    action: row.action,
    at: row.created_at,
    payload: parsed,
  };
}

export class Hub implements DurableObject {
  constructor(
    private readonly state: DurableObjectState,
    private readonly env: Env,
  ) {}

  async fetch(req: Request): Promise<Response> {
    const url = new URL(req.url);

    if (url.pathname === '/ws') {
      return this.handleUpgrade();
    }
    if (url.pathname === '/broadcast') {
      return this.handleBroadcast(req);
    }
    if (url.pathname === '/stats') {
      return Response.json({ sockets: this.state.getWebSockets().length });
    }
    return new Response('not found', { status: 404 });
  }

  /** WebSocket 升级：接受连接后立刻返回，此后一切交给事件回调 */
  private handleUpgrade(): Response {
    const pair = new WebSocketPair();
    const [client, server] = [pair[0], pair[1]];
    // 休眠式接受：连接留着，对象在空闲 10 秒后可以休眠
    this.state.acceptWebSocket(server);
    // 新连接从游标 0 开始，等客户端 hello 送来真实游标
    server.serializeAttachment({ cursor: 0 });
    this.scheduleCleanup();
    return new Response(null, { status: 101, webSocket: client });
  }

  /** Worker 收到 webhook 后把事件推进来，这里扇出给所有连接 */
  private async handleBroadcast(req: Request): Promise<Response> {
    const text = await req.text();
    const sockets = this.state.getWebSockets();
    let sent = 0;
    for (const ws of sockets) {
      try {
        ws.send(text);
        sent += 1;
      } catch {
        // 连接已断，忽略；真正的清理靠 close 回调
      }
    }
    return new Response(JSON.stringify({ sent }), {
      headers: { 'content-type': 'application/json' },
    });
  }

  /** 客户端发来的消息：只有 hello 和 ping */
  async webSocketMessage(ws: WebSocket, raw: string | ArrayBuffer): Promise<void> {
    if (typeof raw !== 'string') return;

    let msg: ClientMsg;
    try {
      msg = JSON.parse(raw);
    } catch {
      return;
    }

    if (msg.type === 'ping') {
      ws.send(JSON.stringify({ type: 'pong', at: Date.now() }));
      return;
    }

    if (msg.type === 'hello') {
      const cursor = Number.isFinite(msg.cursor) ? Number(msg.cursor) : 0;
      // 先回 ready，让客户端知道续传位置已被接受
      ws.send(JSON.stringify({ type: 'ready', cursor }));
      await this.replay(ws, cursor);
      ws.serializeAttachment({ cursor });
    }
  }

  /** 按游标补发漏掉的事件（休眠醒来后 attachment 仍在） */
  private async replay(ws: WebSocket, cursor: number): Promise<void> {
    let last = cursor;
    let total = 0;

    while (total < REPLAY_MAX) {
      const { results } = await this.env.DB.prepare(
        `SELECT id, type, repo, action, payload, created_at
           FROM gh_events WHERE id > ? ORDER BY id LIMIT ?`,
      )
        .bind(last, REPLAY_LIMIT)
        .all<EventRow>();

      if (!results || results.length === 0) break;

      for (const row of results) {
        try {
          ws.send(JSON.stringify(toPayload(row)));
        } catch {
          return;
        }
        last = row.id;
        total += 1;
      }
      if (results.length < REPLAY_LIMIT) break;
    }

    if (total > 0) {
      ws.send(JSON.stringify({ type: 'ready', cursor: last, replayed: total }));
    }
  }

  async webSocketClose(ws: WebSocket, code: number, reason: string): Promise<void> {
    try {
      ws.close(code === 1005 || code === 1006 ? 1000 : code, reason || 'closed');
    } catch {
      /* 已经断了 */
    }
  }

  async webSocketError(ws: WebSocket): Promise<void> {
    try {
      ws.close(1011, 'socket error');
    } catch {
      /* ignore */
    }
  }

  /**
   * 每天跑一次，删掉超过保留期的事件。
   * 用 alarm 而不是 setInterval：alarm 不阻止休眠，setInterval 会。
   */
  private scheduleCleanup(): void {
    const next = Date.now() + 24 * 60 * 60 * 1000;
    void this.state.storage.setAlarm(next);
  }

  async alarm(): Promise<void> {
    const days = Number(this.env.RETENTION_DAYS ?? 90);
    const cutoff = Date.now() - days * 24 * 60 * 60 * 1000;
    await this.env.DB.prepare('DELETE FROM gh_events WHERE created_at < ?')
      .bind(cutoff)
      .run();
    this.scheduleCleanup();
  }
}

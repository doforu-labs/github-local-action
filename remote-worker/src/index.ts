/**
 * index.ts —— 公网入口（Worker）
 *
 * 只做三件事：
 *   POST /gh/webhook  GitHub 打进来：验签 → 写 D1 → 转给 DO 广播 → 秒回 202
 *   GET  /ws          本地客户端连上来：校验 token → 交给 DO 做 WebSocket 升级
 *   GET  /health      给自己看的
 *
 * 这里刻意不做任何业务逻辑：Worker 是无状态的，广播必须由 DO 做。
 */

import type { Env, EventPayload } from './protocol';

export { Hub } from './hub';

const enc = new TextEncoder();

/** HMAC-SHA256 十六进制。Workers 里没有 node:crypto，用 WebCrypto。 */
async function hmacHex(secret: string, body: ArrayBuffer): Promise<string> {
  const key = await crypto.subtle.importKey(
    'raw',
    enc.encode(secret),
    { name: 'HMAC', hash: 'SHA-256' },
    false,
    ['sign'],
  );
  const sig = await crypto.subtle.sign('HMAC', key, body);
  return [...new Uint8Array(sig)].map((b) => b.toString(16).padStart(2, '0')).join('');
}

/** Workers 没有 timingSafeEqual，手写定长异或比较，避免按字节短路泄漏签名。 */
function safeEqual(a: string, b: string): boolean {
  if (a.length !== b.length) return false;
  let diff = 0;
  for (let i = 0; i < a.length; i++) diff |= a.charCodeAt(i) ^ b.charCodeAt(i);
  return diff === 0;
}

async function ingest(req: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
  const delivery = req.headers.get('x-github-delivery');
  const event = req.headers.get('x-github-event');
  const sig = req.headers.get('x-hub-signature-256');

  if (!delivery || !event || !sig) {
    return new Response('missing github headers', { status: 400 });
  }

  // 必须用原始字节验签。先 req.json() 会把 body 消费掉，
  // 重新序列化后字段顺序/空格都变了，签名必然对不上。
  const raw = await req.arrayBuffer();
  const expected = 'sha256=' + (await hmacHex(env.GITHUB_WEBHOOK_SECRET, raw));
  if (!safeEqual(sig, expected)) {
    return new Response('bad signature', { status: 401 });
  }

  let body: Record<string, any>;
  try {
    body = JSON.parse(new TextDecoder().decode(raw));
  } catch {
    return new Response('bad json', { status: 400 });
  }

  const repo: string | null = body.repository?.full_name ?? null;
  const action: string | null = body.action ?? null;
  const now = Date.now();

  // delivery 唯一索引做幂等：GitHub 可能重复投递同一条
  const res = await env.DB.prepare(
    `INSERT OR IGNORE INTO gh_events (delivery, type, repo, action, payload, created_at)
     VALUES (?, ?, ?, ?, ?, ?)`,
  )
    .bind(delivery, event, repo, action, new TextDecoder().decode(raw), now)
    .run();

  // changes === 0 说明是重复投递，静默吞掉，不要重复推给客户端
  if (res.meta.changes > 0) {
    const id = Number(res.meta.last_row_id);
    const msg: EventPayload = {
      type: 'event',
      id,
      event,
      repo,
      action,
      at: now,
      payload: body,
    };
    const stub = env.HUB.get(env.HUB.idFromName('github'));
    // 放 waitUntil：DO 调用偶尔几十上百毫秒，不能拖慢给 GitHub 的响应（10 秒超时）
    ctx.waitUntil(
      stub
        .fetch('https://hub/broadcast', { method: 'POST', body: JSON.stringify(msg) })
        .catch(() => undefined),
    );
  }

  return new Response('accepted', { status: 202 });
}

async function upgrade(req: Request, url: URL, env: Env): Promise<Response> {
  if (req.headers.get('Upgrade')?.toLowerCase() !== 'websocket') {
    return new Response('expected websocket upgrade', { status: 426 });
  }
  // 这是公网地址，token 是唯一的门禁
  if (url.searchParams.get('token') !== env.CLIENT_TOKEN) {
    return new Response('unauthorized', { status: 401 });
  }
  const stub = env.HUB.get(env.HUB.idFromName('github'));
  // 原始 Request 直接转给 DO：Upgrade 头会一起带过去
  return stub.fetch(req);
}

export default {
  async fetch(req: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(req.url);

    if (req.method === 'POST' && url.pathname === '/gh/webhook') {
      return ingest(req, env, ctx);
    }
    if (url.pathname === '/ws') {
      return upgrade(req, url, env);
    }
    if (url.pathname === '/health') {
      const row = await env.DB.prepare(
        'SELECT COUNT(*) AS n, MAX(id) AS max_id FROM gh_events',
      ).first<{ n: number; max_id: number | null }>();
      return Response.json({ ok: true, events: row?.n ?? 0, cursor: row?.max_id ?? 0 });
    }

    return new Response('not found', { status: 404 });
  },
} satisfies ExportedHandler<Env>;

/**
 * protocol.ts —— Worker 与 DO、DO 与本地客户端之间的共同契约
 *
 * 一条 WebSocket 上的消息只有四种形态，协议小到能一眼看完：
 *
 *   客户端 → 服务端   {"type":"hello","cursor":1042}   连上后立刻发，告诉服务端从哪续
 *   客户端 → 服务端   {"type":"ping"}                  保活（必须客户端发，见 README）
 *   服务端 → 客户端   {"type":"ready","cursor":1042}   确认续传位置
 *   服务端 → 客户端   {"type":"event", ...Event}       一条事件
 */

export interface Env {
  DB: D1Database;
  HUB: DurableObjectNamespace;
  GITHUB_WEBHOOK_SECRET: string;
  CLIENT_TOKEN: string;
  /** 事件保留天数，默认 90。超过就由 DO 的 alarm 清掉。 */
  RETENTION_DAYS?: string;
}

/** D1 里的一行事件 */
export interface EventRow {
  id: number;
  type: string;
  repo: string | null;
  action: string | null;
  payload: string;
  created_at: number;
}

/** 推给客户端的形态：payload 由字符串还原成对象，省得客户端再解一次 */
export interface EventPayload {
  type: 'event';
  id: number;
  event: string;
  repo: string | null;
  action: string | null;
  at: number;
  payload: unknown;
}

export type ClientMsg =
  | { type: 'hello'; cursor?: number }
  | { type: 'ping' };

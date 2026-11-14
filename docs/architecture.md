# Architecture

[简体中文](architecture.zh-CN.md)

## Why two stages are unavoidable

GitHub only delivers webhooks to a **public address** — it cannot reach your home network. Your machine, on the other hand, can dial out freely (outbound NAT is unrestricted).

So the chain takes the shape of "one public foothold plus one long-lived connection you dial yourself":

```text
GitHub ──webhook──▶ remote-worker ──WebSocket──▶ local-client ──SSE──▶ LAN devices
        (public entry)   (Cloudflare)        (outbound from your box)   (localhost port)
```

- `remote-worker`: a Cloudflare Worker plus a Durable Object. Receives webhooks, verifies signatures, writes D1, broadcasts, and returns 202 immediately. It is stateless, so broadcasting has to go through the DO (only a DO can hold long-lived connections).
- `local-client`: `relay.py`. Dials out to the relay, persists to local SQLite, serves SSE. LAN devices only talk to it and never touch the public internet.

The benefit is not just NAT traversal: events are stored locally, so after a network outage, a restart or a remote redeploy, devices reconnect and catch up on what they missed.

## How data flows

```text
POST /gh/webhook
  ├─ verify signature (HMAC-SHA256, constant-time comparison)
  ├─ idempotent insert into D1 keyed on X-GitHub-Delivery (a redelivery is not stored twice)
  ├─ hand off to the DO, which broadcasts to every connected local client
  └─ 202, without waiting for client acknowledgement
```

On connect the local client sends `{"type":"hello","cursor":N}`; the server answers `ready` and then replays every event with `id > N`. So:

- **Events are not lost while offline**: anything in D1 is replayed on reconnect
- **Duplicates are not repeated**: the local side dedupes by `id`, and the SSE side advances by cursor
- **No polling**: only DO broadcasts and heartbeats

Heartbeats go **client → server, with the server replying**. That is a hard rule: the moment the server uses `setInterval` it can never go idle, and duration usage jumps from 0.12% to 85% of the free allowance.

## Protocol

| Direction | Message | Meaning |
|---|---|---|
| client → server | `{"type":"hello","cursor":N}` | I want everything after N |
| server → client | `{"type":"ready","cursor":N}` | replay finished, live push starts now |
| server → client | `{"type":"event","id":N,"event":"push",…}` | one event |
| client → server | `{"type":"ping"}` / server replies `{"type":"pong"}` | application-level heartbeat |

The two heartbeat layers are configured separately: `heartbeat` (protocol-level ping, keeps NAT from dropping an idle connection, 30 seconds is the usual value) and `app_ping` (application level, which doubles as a check that the peer is really still processing messages).

## Storage

**Remote D1** (`remote-worker/schema.sql`):

- `gh_events`: the auto-increment `id` *is* the resume cursor; a unique index on `delivery` (X-GitHub-Delivery) provides idempotency
- `webhook_secrets`: one webhook secret per repository, written by the panel's "configure webhook automatically". When absent, the global `GITHUB_WEBHOOK_SECRET` is used

**Local SQLite** (`events.db`): the `events` table holds events, `meta` holds cursors, and `machines` holds runner-machine connection details. Passwords are encrypted with Fernet, with the key stored separately in `events.db.machines.key` (mode 0600). Keeping them apart means copying `events.db` alone yields no passwords, and a leaked key yields nothing (decryption fails cleanly) rather than a crash or a silent connection to the wrong machine.

## Cost

Free plan, measured at 20 repositories and 300 events/day:

| Dimension | Peak usage |
|---|---|
| Workers requests | 0.44% |
| Durable Object requests | 0.58% |
| Duration | 0.12% |
| SQL storage | 3.07% (the only thing that accumulates) |

Everything that resets daily stays flat; only SQL storage grows slowly. If needed, delete old rows on D1 by `created_at` (`idx_gh_events_created` exists for exactly this).

## Why not something else

| Alternative | Why it was not chosen |
|---|---|
| Your own public VPS receiving webhooks | One more machine to maintain and pay for, plus certificates and hardening against scanners |
| A tunnel (frp / ngrok) | Needs a permanently occupied public port or a third-party domain, and reconnection behaviour is a black box |
| Polling the GitHub API on a timer | Rate limits, minute-scale latency, and webhook-only fields are unavailable |
| Connecting to GitHub from the browser | GitHub does not push webhooks to browsers, and REST has no realtime story |

# Manual for agents (GitHub Local Action)

This file is written for AI agents and scripts: read it and you can drive the service
running on this machine without guessing at endpoints.
The human-facing docs are [README.md](README.md) and
[docs/jit-on-demand-runner.md](../docs/jit-on-demand-runner.md).

> The service serves this file as-is: `curl -s http://127.0.0.1:18091/agent.en.md`
> (the Chinese original is at `/agent.md`).
> So pointing an agent at a URL is enough; you do not have to hand it a file first.

---

## What it is

It takes the events of every repository under the GitHub account and pushes them to
devices on the LAN in real time. It can also dispatch GitHub jobs onto your own
machines (permanent runners plus on-demand JIT temporary runners).

```text
GitHub ──webhook──▶ remote-worker ──WebSocket──▶ local-client ──SSE──▶ LAN devices
        (public entry)  (Cloudflare)      (long-running local)  (HTTP)
```

Two layers because GitHub can only post to a public address and a home/office network
does not accept inbound connections; the client dials out over one WebSocket (NAT lets
outbound through) and events come back down that same line. No polling, latency around
one second.

There is exactly one thing to run: the HTTP service in `local-client/relay.py`.

## Addresses

| Purpose | Address | Who can reach it |
|---|---|---|
| This machine (default) | `http://127.0.0.1:18091` | localhost only |
| LAN | `http://<machine LAN IP>:18091` | devices on the same subnet |

Port 18091 can be changed with `http_port` in `config.toml` (restart required). Probe first:

```bash
curl -s http://127.0.0.1:18091/health
# {"ok": true, "connected": true, "cursor": 797, "connects": 1,
#  "sse_clients": 2, "seconds_since_last_event": null, "last_error": ""}
```

`ok` / `connected` say whether the WebSocket to Cloudflare is up. If `ok` is false, read
`last_error` first instead of hammering the API.

## Permission boundary (important)

The service has no login state; it distinguishes callers by **source IP**:

- **Localhost only**: every `/github/*`, `/webhook/*`, `/runners*`, `/mcp`, and **writes** to `/config` (`POST /config`).
  These use the machine's `gh` credentials or machine SSH credentials, or can change config.
- **Readable from the LAN**: `/` (the web page), `/stream`, `/events`, `/stats`, `/actions`, `/config` (`GET`, token already masked), `/health`.

So calling `/github/jobs` from another machine on the LAN returns
`403 {"error": "..."}` — that is by design, not a fault.

## Endpoints

Everything returns JSON (`/stream` is SSE, `/agent.en.md` and `/agent.md` are markdown).
`repo` is always `owner/name`.

| Method path | What it does | Parameters |
|---|---|---|
| `GET /health` | Connection state, cursor, SSE client count | — |
| `GET /stats` | Today/total events, today/total runs, 24h buckets, account and repos | — |
| `GET /events` | Backfill historical events from the local database | `after` (id, default 0) `limit` (default 200, max 1000) |
| `GET /stream` | SSE live stream; `Last-Event-ID` resumes | `after` |
| `GET /actions` | Run list assembled from webhook events (no network calls) | `repo` `limit` (default 60) |
| `GET /github/auth` | Is `gh` installed and logged in | — |
| `GET /github/runs` | Ask GitHub for a run list directly | `repo` `limit` |
| `GET /github/jobs` | Jobs and per-step status for one run (finished runs hit the local cache) | `repo` `run` |
| `GET /github/joblog` | Plain-text log of one job (only once finished; 404 while running) | `repo` `job` |
| `GET /github/repos` | Repositories on the account, most recent commit first | — |
| `GET /github/runners` | Runner list on the GitHub side | `repo` (`owner/name` or an org name) |
| `GET /runners` | Machines managed from this install | — |
| `GET /runners/jit` | On-demand runner state, pool, what is running | — |
| `POST /runners/jit/repo` | Toggle on-demand takeover for one repo | `{"repo": "...", "on": true}` |
| `POST /runners/ssh/add` | Connect to a machine and install a runner (async task) | `{"host","port","user","password","repo"?,"name"?}` |
| `GET /runners/ssh/job/{id}` | Result of one of those tasks | — |
| `GET /webhook/status` | Whether the repo webhook points here | `repo` |
| `POST /webhook/setup` | Create the webhook on the repo for you | `{"repo": "..."}` |
| `POST /mcp` | MCP over HTTP endpoint (JSON-RPC 2.0, localhost only) | see the MCP section |

## Five recipes

```bash
B=http://127.0.0.1:18091

# 1. The five most recent runs (no network, no gh needed)
curl -s "$B/actions?limit=5" | jq -r '.runs[] | "\(.name) #\(.run_number) \(.status)/\(.conclusion) \(.sha)"'

# 2. Jobs and every step of one run (finished runs hit the local cache, so it is fast)
curl -s "$B/github/jobs?repo=example-org/relay-test&run=12345678901" \
  | jq '.jobs[] | {name, status, conclusion, steps: [.steps[] | "\(.number). \(.name) \(.conclusion // .status)"]}'

# 3. The log of one job (GitHub returns 404 while the job runs; this API returns pending:true)
curl -s "$B/github/joblog?repo=example-org/relay-test&job=42000000001" | jq -r .text | tail -40

# 4. Follow live events for 30 seconds (Ctrl-C to stop; reconnect with after=<last id>, nothing is lost)
curl -N "$B/stream"

# 5. Machine list (never returns passwords, only host/user/os/arch)
curl -s "$B/runners" | jq '.machines[] | "\(.name) \(.os)/\(.arch) \(.host):\(.port)"'
```

## Data shapes

**Events** (`/events`, `/stream`)

```json
{"id": 797, "event": "workflow_run", "repo": "owner/name",
 "action": "completed", "at": 1794417917000, "payload": { /* GitHub verbatim */ }}
```

- `id` is a remote auto-increment; the local side deduplicates by it, so reconnects and
  duplicate deliveries never insert twice.
- `at` is milliseconds. `payload` is the raw GitHub webhook body; field meanings are in
  GitHub's docs per event type.
- To resume: pass the last `id` of a batch as `after` next time.

**Runs** (`/actions`): `workflow_run` events deduplicated by `run.id`, one row per run.
Fields: `id repo name branch trigger path status conclusion run_number sha actor title started_at duration_s url`.

**Action count vs event count**: one run pushes several events (`requested` / `in_progress` /
`completed`), so `stats.total` (events) is much larger than `stats.runs_total` (runs).

## Rules to follow while working

1. **Read-only first**. All GETs above are safe to call freely. These POSTs change local
   config, touch long-running processes on other machines, or write to GitHub —
   ask the human first: `/config`, `/runners/delete`, `/runners/ssh/add`,
   `/runners/ssh/remove`, `/runners/jit/*`, `/webhook/setup`.
2. **Never put tokens or passwords into logs or replies**. `/config` only ever returns
   `token_hint` (`71a7••••6088`); SSH passwords travel in the request body — do not echo
   them, do not write them to disk.
3. **`/github/*` uses this machine's `gh` login** and therefore this account's API quota
   (60 requests/hour when unauthenticated). Use `/actions` (local database, zero cost) for
   listings, and touch `/github/*` only when you need jobs, steps or logs.
4. **Do not poll in a tight loop**. `/actions` and `/stats` ride the SSE stream well enough;
   once every 5 seconds per running job is the ceiling.
5. To decide whether something **finished**, read `status` (`queued` / `in_progress` /
   `completed`); to decide whether it **passed**, read `conclusion` (`success` / `failure` /
   `cancelled` / `skipped`).

## About MCP

**There are two transports; pick one**:

| Transport | How to attach | When |
|---|---|---|
| **HTTP** (Streamable HTTP) | endpoint `http://127.0.0.1:18091/mcp` | preferred when the client speaks remote/HTTP MCP; no extra process |
| stdio | `python3 local-client/mcp_server.py` | when the client can only spawn a subprocess (Claude Desktop and friends) |

Both share **one implementation** (the HTTP endpoint simply mounts the handlers from
`mcp_server.py` onto the aiohttp app); behaviour, arguments and responses are identical.

### HTTP

```json
{
  "mcpServers": {
    "github-local-action": {
      "type": "http",
      "url": "http://127.0.0.1:18091/mcp"
    }
  }
}
```

On the command line that is usually
`claude mcp add --transport http github-local-action http://127.0.0.1:18091/mcp`.
Protocol notes: `POST /mcp` carries JSON-RPC; the `initialize` response header returns an
`Mcp-Session-Id` which later requests must echo; pure notifications answer `202` with an
empty body; `GET /mcp` answers `405` (this service has nothing to push, so no SSE long-poll).

**Limit**: `/mcp` accepts localhost only. The tools reach `/github/*` (which spends the
machine's `gh` credentials), so exposing it to the LAN would walk around that wall.
To use it from another machine, open a tunnel:
`ssh -L 18091:localhost:18091 user@host`.

### stdio

```json
{
  "mcpServers": {
    "github-local-action": {
      "command": "python3",
      "args": ["/absolute/path/local-client/mcp_server.py"],
      "env": { "LOCAL_ACTION_URL": "http://127.0.0.1:18091" }
    }
  }
}
```

### Tools (identical over both transports)

| Tool | Endpoint | What it does |
|---|---|---|
| `health` | `/health` | Is the service up, is the link alive (call it first when unsure) |
| `get_stats` | `/stats` | Today/total events and runs (24h buckets are omitted by default) |
| `list_runs` | `/actions` | Recent runs (local database, zero quota) |
| `get_run_detail` | `/github/jobs` | Jobs and per-step status for one run; `include_logs` adds log tails |
| `get_job_log` | `/github/joblog` | Log of a single job, `max_chars` caps the length |
| `recent_events` | `/events` | Recent events (payload keys only by default) |
| `list_machines` | `/runners` | Managed machines (no passwords) |

Deliberate trade-offs:

- **One run's jobs, steps and logs are a single tool** (`get_run_detail`) rather than three,
  otherwise an agent has to look at status and then come back for logs — two extra rounds.
  Logs default to the last 60 lines; ask for everything with `log_tail_lines: 0` or switch
  to `get_job_log`.
- **Everything is read-only**. Changing config, installing or removing runners, toggling the
  on-demand pool — anything that touches someone else's machine or the local config — is not
  exposed as a tool. Let the human click it in the web UI, or call the HTTP endpoint directly
  (see the table above).
- A long-lived connection like `/stream` does not fit a tool call (MCP tool calls are
  request/response), so it becomes `recent_events(after=N)` and you use the value as a cursor.

You do not have to use MCP at all: it is a translation of the HTTP endpoints below, and
plain curl gets the same work done.

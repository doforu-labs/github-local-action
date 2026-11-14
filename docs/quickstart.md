# Quickstart

From nothing to the first event showing up in the panel: about 10 minutes.

[简体中文](quickstart.zh-CN.md)

## What you need

- A GitHub account (able to create a GitHub App and install it on your own account)
- A Cloudflare account (the free plan is enough, no card required)
- An always-on machine: macOS 10.15+ / Linux / Windows, with Python **3.11+** (TOML is read with the stdlib `tomllib`)
- **Node 22.12+** for the deploy half only (`wrangler` 4 refuses to start on older Node; the local receiver never touches Node)
- Optional: `gh` (GitHub CLI), already logged in. Without it you cannot see jobs and steps, fetch run history, or install runners from the panel

---

## 1. Deploy the remote relay (Cloudflare)

```bash
cd remote-worker
npm install
npx wrangler login          # opens a browser the first time

# Create the database. The free plan allows 10 D1 databases per account; this project needs one.
npx wrangler d1 create github_local_action
# The output contains database_id = "xxxx" — copy it into the same field in wrangler.toml

# Create the tables (two: the event log and per-repo webhook secrets)
npx wrangler d1 execute github_local_action --file schema.sql --remote

# Two secrets: one for GitHub signature verification, one for the local client's WebSocket
npx wrangler secret put GITHUB_WEBHOOK_SECRET   # make up a long random string; reuse it in the next step
npx wrangler secret put CLIENT_TOKEN            # another long random string; goes into the local config.toml

npx wrangler deploy
```

When it finishes it prints an address:

```
https://github-local-action.<your-subdomain>.workers.dev
```

Note it down. You can also attach your own domain — the panel's **Settings → Relay** card does it in one click, see [step 5](#5-bring-your-own-domain-optional).

Check it:

```bash
curl https://github-local-action.<your-subdomain>.workers.dev/health
# {"ok":true,"events":0,"cursor":0}
```

## 2. Create a GitHub App

Easier than configuring a webhook per repository: install it once and every repository under the account is covered.

1. Open <https://github.com/settings/apps> → **New GitHub App**
2. **Webhook URL**: `https://github-local-action.<your-subdomain>.workers.dev/gh/webhook`
3. **Webhook secret**: the same value you used for `GITHUB_WEBHOOK_SECRET`
4. **Subscribe to events**: at least these
   - `Push`, `Pull request`, `Issues`, `Issue comment`, `Release`, `Create`
   - **`Workflow run`** — without this the Actions view stays empty forever (this is the most common "configured but no data")
5. **Repository permissions**: nothing extra needed, keep the defaults (`Metadata: read-only`)
6. After creating it, click **Install App**, install it on your account, choose all repositories

> Do not want an App? You can add webhooks one repository at a time under Settings → Webhooks, with the same URL and the same secret.

## 3. Start the local receiver

```bash
cd local-client
python3 -m venv .venv && source .venv/bin/activate     # Windows: .venv\Scripts\activate
pip install -r requirements.txt                        # only aiohttp; add paramiko if you want SSH runner installs

cp config.example.toml config.toml
```

Two lines in `config.toml` are enough:

```toml
[relay]
ws_url = "wss://github-local-action.<your-subdomain>.workers.dev/ws"
token = "exactly the same as CLIENT_TOKEN"
```

Then:

```bash
python3 relay.py
```

These lines mean it is connected:

```
INFO 本地 SSE 监听 http://0.0.0.0:18091/stream
INFO 连接 wss://…/ws（游标 0）
INFO 已连上，从游标 0 续传
```

(The log lines are in Chinese: "SSE listening", "connecting", "connected, resuming from cursor 0".)

To install it as a command instead of running it from the directory every time:

```bash
pip install .          # or pipx install .
github-local-action -c ~/.config/github-local-action/config.toml
```

After installing the wheel, the status page and `/agent.md` are read from `<python prefix>/share/github-local-action/` — nothing to configure.

## 4. Connect LAN devices

Open `http://<this-machine's-LAN-IP>:18091/` in a browser for the panel. Machine-readable endpoints:

```bash
curl -N http://<LAN-IP>:18091/stream      # SSE event stream, supports Last-Event-ID and ?after=N resume
curl http://<LAN-IP>:18091/health         # connection state, cursor, number of SSE clients
curl http://<LAN-IP>:18091/stats          # today / total / 24h distribution / repo list
curl http://<LAN-IP>:18091/actions        # workflow runs (?repo= to filter)
curl http://<LAN-IP>:18091/config         # current config (token is masked); editing is only allowed on the machine running the receiver
```

In a web page, `EventSource` is enough — on reconnect the browser sends `Last-Event-ID` by itself and the local database fills the gap:

```js
const es = new EventSource('http://10.0.0.5:18091/stream');
es.onmessage = e => console.log(JSON.parse(e.data));
```

If you only want it on the local machine, set `http_host` to `127.0.0.1`.

## 5. Bring your own domain (optional)

`*.workers.dev` does not go through a CDN in mainland China and may be blocked outright. Cloudflare supports Workers custom domains: the panel's **Settings → Relay** card lists the zones in your account and the domains currently bound to this worker. Pick a subdomain, bind it, and once HTTPS works the panel switches `ws_url` over and reconnects by itself.

The same card also lets you:

- **Deploy with one click**: after changing worker code, deploy without going back to a terminal — the panel lifts the `*.workers.dev` address out of the output and writes it back into the config
- **Check deploy permissions**: runs `wrangler whoami` and lists the account, token scopes, and whether you can write workers

## Troubleshooting

| Symptom | Look here first |
|---|---|
| Logs keep saying "connection failed" | Is `ws_url` a `wss://…/ws` URL? (missing the `/ws` path gives a 404.) Does `token` match `CLIENT_TOKEN`? |
| Panel opens but no events | Did you tick `Workflow run` in the GitHub App? Only events subscribed there are delivered |
| Events arrive but Actions view is empty | Same cause; the Actions view only understands `workflow_run` |
| Other LAN devices cannot open it | Is `http_host` set to `0.0.0.0`? Does the firewall allow port 18091 on that machine? |
| `wrangler deploy` says the D1 database does not exist | `database_id` in `wrangler.toml` is still the placeholder |
| "Load jobs" on the Actions view errors | `gh` is missing or not logged in on that machine; `gh auth status` must show the `repo` scope |

The default port is 18091; changing `http_port` needs a process restart. Changing `ws_url` / `token` / heartbeat settings does not — saving in the panel reconnects.

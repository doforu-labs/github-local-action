# Docker

[简体中文](docker.zh-CN.md)

Only the daemon half goes into a container: `local-client` (the outbound long connection, the local SSE server and SQLite). The Worker is deliberately left out — deploying it is a single `wrangler deploy` command, so putting it in an image means carrying node and a few hundred MB of `node_modules` for nothing, and keeping a second copy of your wrangler login.

## Quick start

```bash
export GH_TOKEN=$(gh auth token)     # optional: reuse the gh login you already have
mkdir -p data
cp local-client/config.example.toml data/config.toml   # fill in ws_url and token
docker compose up -d --build
docker compose ps                    # wait for "healthy"
curl -s http://127.0.0.1:18091/health
```

Open <http://127.0.0.1:18091> and the status page is there. Deploy the Worker on the host first (see below), otherwise the relay has nothing to connect to.

If pulling gh from GitHub is slow where you are (it stalled for over ten minutes from a container network here), point the build at a mirror:

```bash
docker compose build --build-arg GH_BASE_URL=https://<your-mirror>/cli/cli/releases/download
```

`data/` ends up with `config.toml` and `events.db`. Back up that directory and you have backed up everything.

On Linux the bind mount needs to be writable by uid 10001, which is the user inside the image:

```bash
sudo chown -R 10001:10001 ./data
```

Docker Desktop on macOS or Windows handles that by itself.

## Why the config needs `trusted_peers = ["gateway"]`

Every write endpoint (change config, manage machines, call GitHub) only answers requests that come from the machine running the relay. That check compares the source address against `127.0.0.1`. Inside a container, the browser on the host arrives from the Docker bridge gateway (`172.17.0.1` or similar), not from loopback — so the page loads, everything reads fine, and every button that saves something returns 403.

`trusted_peers` lists addresses that also count as "this machine". The special value `gateway` makes the program read its own default route from `/proc/net/route` at startup, so you do not have to look the bridge address up by hand. The compose file sets it through `GLA_TRUSTED_PEERS=gateway`.

The list is config-file only — it cannot be changed through the HTTP API, because a gate whose allowlist is editable by a request is not a gate. Other devices on your LAN are still refused no matter what you put here.

## Reusing the gh you already have

The GitHub-backed endpoints (repository list, JIT runner creation) shell out to `gh`. Inside the container `gh` exists but is not logged in. The cleanest way to fix that is a token:

```bash
export GH_TOKEN=$(gh auth token)
docker compose up -d
```

`gh api` inside the container then works with no config directory at all — verified. Do not bake the token into the image and do not put it in the compose file; it is a live credential.

If you would rather mount the config directory, note that `~/.config/gh/hosts.yml` is `600` and owned by you, while the container runs as uid 10001. On macOS the bind mount usually reads through; on Linux you will hit permission errors. The token is less work.

## Reusing the wrangler you already have

You cannot reuse the host *binary*: node and a global `wrangler` install inside a container is a different OS and a different copy. What you can reuse is the login — by simply not putting wrangler in the container at all:

```bash
cd remote-worker
npx wrangler deploy
```

That runs on the host, with the wrangler login you already have, and it is the recommended path. Consequences:

- The status page's one-click deploy button will report that wrangler is missing. That is expected inside the container, not a bug.
- The image does not ship `remote-worker/`, so even with wrangler installed the one-click path has no `wrangler.toml` to read. If you really want one-click deploy inside the container, build a derived image with node plus wrangler and copy `remote-worker/` next to the package — at that point the image is several hundred MB bigger and you have two places holding Cloudflare credentials.

After deploying, put the resulting `wss://...workers.dev/ws` address and the token into `data/config.toml` and restart the container.

## Running as a service

Do not use the "install as a service" button inside the container — it writes launchd or systemd units, which is meaningless in there. `restart: unless-stopped` in the compose file is the container equivalent.

## Known edges

- **No live job logs.** True in containers too, and for the same reason: the design has no log streaming. See [known-limitations.md](known-limitations.md).
- **LAN read access.** Mapping `18091` lets other devices on your network read the stream and the status page. That is intended; writes stay local. If you want the container to be LAN-only for reads and nothing else, leave `trusted_peers` alone.
- **macOS `network_mode: host`.** Do not use it. It behaves differently from Linux and you would be relying on it to get loopback, which is exactly what `trusted_peers` avoids needing.
- **Machines managed by the container** are reached over SSH from inside the container, so their addresses must be reachable from the container's network, not just from the host's shell.

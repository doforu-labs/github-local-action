# remote-worker

Cloudflare Worker + Durable Object。公网上唯一的落脚点。

```text
POST /gh/webhook   GitHub 打进来 → 验签 → 写 D1 → 转 DO 广播 → 202
GET  /ws           本地客户端升级 WebSocket（token 校验后转给 DO）
GET  /health       事件总数与当前游标
```

## 部署

```bash
npm install

npx wrangler d1 create github_local_action
# 把输出的 database_id 填进 wrangler.toml

npx wrangler d1 execute github_local_action --file schema.sql --remote

npx wrangler secret put GITHUB_WEBHOOK_SECRET   # 和 GitHub App 里填的一致
npx wrangler secret put CLIENT_TOKEN            # 客户端连 WS 用的口令，随机长串

npx wrangler deploy
```

可选：`wrangler secret put RETENTION_DAYS`（默认 90），控制事件保留天数。

## GitHub 侧

建 GitHub App（Settings → Developer settings → GitHub Apps）：

- **Webhook URL**：`https://github-local-action.<你的子域>.workers.dev/gh/webhook`
- **Webhook secret**：就是上面的 `GITHUB_WEBHOOK_SECRET`
- **Repository permissions**：至少 `Metadata: read`；要看 PR / issue 就加对应只读权限
- **Subscribe to events**：push / pull_request / issues / issue_comment / release 按需勾
- 装到你的账户，选 All repositories

用 App 而不是逐仓库配 webhook 的理由：装一次覆盖账户下全部仓库，以后新建的仓库自动纳入。

## 本地验证（不需要 Cloudflare 账号）

```bash
npx wrangler d1 execute github_local_action --local --file schema.sql
npx wrangler dev --port 17903 \
  --var GITHUB_WEBHOOK_SECRET:testsecret \
  --var CLIENT_TOKEN:testtoken
```

另一个终端：

```bash
npm install --no-save typescript@5 @cloudflare/workers-types  # 类型检查用
npx tsc --noEmit

python3 tests/smoke.py --url http://127.0.0.1:17903 --token testtoken --secret testsecret
```

`smoke.py` 需要 `aiohttp`，直接借 `../local-client/.venv/bin/python` 跑即可。

## 三个不能改错的地方

**验签必须用原始字节。** `index.ts` 里是 `await req.arrayBuffer()` 而不是 `req.json()`。后者会消费掉 body，重新序列化后签名必然对不上。

**必须 `new_sqlite_classes`。** 免费计划只支持 SQLite 版 Durable Object，`wrangler.toml` 里写成 `new_classes` 部署会失败。

**DO 里不能出现 `setInterval`。** 定时器存在对象就无法休眠，duration 从 0.12% 跳到 85%。心跳一律由客户端发，清理用 `state.storage.setAlarm()`。

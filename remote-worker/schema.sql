-- 事件日志。id 自增，就是客户端续传的游标。
CREATE TABLE IF NOT EXISTS gh_events (
  id         INTEGER PRIMARY KEY AUTOINCREMENT,
  delivery   TEXT    NOT NULL UNIQUE,   -- X-GitHub-Delivery，幂等键
  type       TEXT    NOT NULL,          -- push / pull_request / issues ...
  repo       TEXT,                      -- owner/name
  action     TEXT,                      -- opened / closed / synchronize ...
  payload    TEXT    NOT NULL,          -- 原始 JSON
  created_at INTEGER NOT NULL           -- 毫秒时间戳
);

-- 按游标扫是最主要的查询（客户端 hello 时补发）
CREATE INDEX IF NOT EXISTS idx_gh_events_id ON gh_events(id);
-- 清理旧数据用
CREATE INDEX IF NOT EXISTS idx_gh_events_created ON gh_events(created_at);

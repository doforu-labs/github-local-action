#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""造一份 workflow_run 历史，用来预览「动作」页（不是测试，只给截图/演示用）。

用法： python3 tests/seed_actions.py /tmp/actions.db
生成的载荷字段按 GitHub 真实的 workflow_run 事件来写，
这样页面渲染出来的东西和真接上 webhook 时是一样的。
"""
import json
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

db = sys.argv[1] if len(sys.argv) > 1 else "/tmp/actions.db"
Path(db).unlink(missing_ok=True)
conn = sqlite3.connect(db)
conn.executescript(
    """
    CREATE TABLE events (id INTEGER PRIMARY KEY, event TEXT NOT NULL, repo TEXT,
                         action TEXT, at INTEGER, payload TEXT);
    CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);
    """
)

NOW = int(time.time() * 1000)
MIN, HOUR = 60 * 1000, 3600 * 1000


def iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def payload(repo, name, run_id, number, branch, status, conclusion, trigger,
            sha, title, actor, started, updated, attempt=1, pr=None,
            path=".github/workflows/ci.yml"):
    return {
        "action": "completed" if status == "completed" else status,
        "workflow_run": {
            "id": run_id,
            "name": name,
            "head_branch": branch,
            "head_sha": sha,
            "path": path,
            "display_title": title,
            "run_number": number,
            "run_attempt": attempt,
            "event": trigger,
            "status": status,
            "conclusion": conclusion,
            "html_url": f"https://github.com/{repo}/actions/runs/{run_id}",
            "created_at": iso(started),
            "updated_at": iso(updated),
            "run_started_at": iso(started),
            "actor": {"login": actor, "avatar_url": ""},
            "triggering_actor": {"login": actor},
            "head_commit": {"id": sha, "message": title, "author": {"name": "Alex"}},
            "pull_requests": [{"number": pr}] if pr else [],
            "workflow_id": number * 7,
        },
        "repository": {
            "full_name": repo,
            "owner": {"login": repo.split("/")[0]},
        },
        "sender": {"login": actor},
    }


A = "example-org/relay-test"
B = "example-org/docs"

# (repo, 名字, run_id, 号, 分支, 状态, 结论, 触发, sha, 标题, 发起人, 开始, 更新, 尝试, PR, 文件)
RUNS = [
    (A, "CI", 8801, 42, "main", "completed", "success", "push",
     "9c1f2ab3d4", "fix: 仓库列表按最近活跃排序", "dev", NOW - 2 * HOUR, NOW - 2 * HOUR + 192 * 1000,
     1, None, ".github/workflows/ci.yml"),
    (A, "CI", 8802, 43, "feat/actions-view", "in_progress", None, "pull_request",
     "41be77c9aa", "feat: 动作页第一版", "dev", NOW - 4 * MIN, NOW - 20 * 1000,
     1, 12, ".github/workflows/ci.yml"),
    (A, "Deploy Worker", 8803, 18, "main", "completed", "failure", "push",
     "7d0e51c2ff", "chore: 升 wrangler 到 3.114", "dev", NOW - 5 * HOUR, NOW - 5 * HOUR + 48 * 1000,
     1, None, ".github/workflows/deploy.yml"),
    (A, "CI", 8804, 41, "main", "completed", "cancelled", "schedule",
     "2fa8b0d417", " nightly 全量构建", "github-actions[bot]", NOW - 9 * HOUR, NOW - 9 * HOUR + 63 * 1000,
     1, None, ".github/workflows/ci.yml"),
    (A, "CI", 8805, 40, "main", "completed", "success", "push",
     "b73c11e0a8", "refactor: 拆出 build_actions", "dev", NOW - 26 * HOUR, NOW - 26 * HOUR + 214 * 1000,
     2, None, ".github/workflows/ci.yml"),
    (B, "Docs build", 7701, 7, "main", "completed", "success", "push",
     "cc90aa12be", "docs: 补 Actions 配置说明", "dev", NOW - 30 * HOUR, NOW - 30 * HOUR + 26 * 1000,
     1, None, ".github/workflows/docs.yml"),
    (B, "Docs build", 7702, 8, "main", "completed", "success", "push",
     "de33f7012c", "docs: 修正端口说明", "dev", NOW - 50 * HOUR, NOW - 50 * HOUR + 24 * 1000,
     1, None, ".github/workflows/docs.yml"),
]

rows = []
eid = 1
for (repo, name, run_id, number, branch, status, conclusion, trigger, sha,
     title, actor, started, updated, attempt, pr, path) in reversed(RUNS):
    # 同一次运行会有多次状态推送，这里补上前面的状态，验证按 run 去重
    if status == "completed":
        rows.append((eid, "workflow_run", repo, "requested", started - 3000, json.dumps(
            payload(repo, name, run_id, number, branch, "queued", None, trigger,
                    sha, title, actor, started, started, attempt, pr, path))))
        eid += 1
        rows.append((eid, "workflow_run", repo, "in_progress", started + 2000, json.dumps(
            payload(repo, name, run_id, number, branch, "in_progress", None, trigger,
                    sha, title, actor, started, started + 2000, attempt, pr, path))))
        eid += 1
    rows.append((eid, "workflow_run", repo, "completed" if status == "completed" else status,
                 updated, json.dumps(
                     payload(repo, name, run_id, number, branch, status, conclusion, trigger,
                             sha, title, actor, started, updated, attempt, pr, path))))
    eid += 1

# 掺几条别的事件，确认动作页只认 workflow_run
rows.append((eid, "push", A, "", NOW - 3 * HOUR, json.dumps({"ref": "refs/heads/main"})))
eid += 1
rows.append((eid, "issue_comment", A, "created", NOW - 4 * HOUR, json.dumps({"issue": {"number": 9}})))

conn.executemany("INSERT INTO events VALUES (?,?,?,?,?,?)", rows)
conn.execute("INSERT INTO meta VALUES ('cursor', ?)", (str(eid),))
conn.commit()
print(f"写入 {len(rows)} 条事件（{len(RUNS)} 次运行，含多次状态推送）→ {db}")

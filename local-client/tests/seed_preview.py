#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""造一份 24 小时的历史数据，用来预览状态页（不是测试，只是给截图/演示用）"""
import json
import random
import sqlite3
import sys
import time
from pathlib import Path

db = sys.argv[1] if len(sys.argv) > 1 else "/tmp/preview.db"
Path(db).unlink(missing_ok=True)
conn = sqlite3.connect(db)
conn.executescript(
    """
    CREATE TABLE events (id INTEGER PRIMARY KEY, event TEXT NOT NULL, repo TEXT,
                         action TEXT, at INTEGER, payload TEXT);
    CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);
    """
)

REPOS = ["example-org/core", "example-org/desktop", "acme/infra", "acme/web", "lab/notes"]
TYPES = ["push", "pull_request", "issues", "issue_comment", "release", "workflow_run", "create"]
ACTIONS = {"pull_request": ["opened", "closed", "synchronize"], "issues": ["opened", "closed"]}

now = int(time.time() * 1000)
random.seed(3)
rows, eid = [], 1
# 24 小时里越近越密，模拟一天的工作节奏
for hours_ago in range(24, 0, -1):
    weight = 1.0 if 9 <= (24 - hours_ago) <= 19 else 0.25
    n = max(0, int(random.gauss(11 * weight, 3)))
    for _ in range(n):
        t = now - hours_ago * 3600 * 1000 + random.randint(0, 3500) * 1000
        typ = random.choices(TYPES, weights=[52, 18, 9, 8, 3, 7, 3])[0]
        act = random.choice(ACTIONS.get(typ, [""]))
        rows.append((eid, typ, random.choice(REPOS), act, t, json.dumps({"seq": eid})))
        eid += 1

conn.executemany("INSERT INTO events VALUES (?,?,?,?,?,?)", rows)
conn.execute("INSERT INTO meta VALUES ('cursor', ?)", (str(eid - 1),))
conn.commit()
print(f"{db}: {len(rows)} 条，最大 id {eid - 1}")

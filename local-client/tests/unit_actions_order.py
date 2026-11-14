#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
unit_actions_order.py —— 动作页「这次运行现在什么状态」的验收（不连网、不起服务）

库里的 workflow_run 事件是 GitHub 一条条推过来的，而**到达顺序不等于发生顺序**：
投递会重试、会补发，实测过这么一对（run 43089073731）：

    #865  queued       updated_at=19:38:25
    #896  completed    updated_at=19:39:19
    #897  in_progress  updated_at=19:39:19   ← 后到的，时间戳一模一样，状态却退回去了

以前 build_actions 取「最后到达的那条」，于是这条运行在面板上永远停在「进行中」，
一晚上攒了 4 条这样的僵尸。这里把判定钉住：updated_at 说话，同一时刻按状态优先级。

跑法： python3 tests/unit_actions_order.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import relay  # noqa: E402


FAILS: list[str] = []
PASSES: list[str] = []


def ck(ok: bool, label: str, detail: str = "") -> None:
    if ok:
        PASSES.append(label)
    else:
        FAILS.append(f"{label}  {detail}".strip())


def ev(run_id: int, status: str, conclusion: str | None = None, updated: str | None = None,
       started: str = "2026-11-11T19:38:00Z", name: str = "Demo") -> dict:
    """造一条 workflow_run 事件（形状跟 GitHub 推过来的一致）。"""
    wr = {
        "id": run_id, "name": name, "run_number": 1, "status": status,
        "conclusion": conclusion, "updated_at": updated,
        "created_at": started, "run_started_at": started,
        "head_commit": {"message": "测试提交\n\n正文", "timestamp": "2026-11-11T19:37:00Z"}, "event": "push",
        "html_url": f"https://example.invalid/{run_id}", "head_sha": "abcdef1234567",
    }
    return {"repo": "example-org/demo", "action": status, "at": 1, "payload": json.dumps({"workflow_run": wr})}


class FakeStore:
    """build_actions 只用到 store.query(sql, params)，给个假的就够。

    SQL 里是 ORDER BY id DESC，所以这里把「到达顺序」倒过来喂。
    """

    def __init__(self, rows: list[dict]) -> None:
        self._rows = list(reversed(rows))

    def query(self, _sql: str, _params=()):
        return self._rows


def state_of(rows: list[dict], run_id: int = 1) -> str:
    out = relay.build_actions(FakeStore(rows))
    run = next((r for r in out["runs"] if r["id"] == run_id), None)
    assert run is not None, "这条运行没出现在结果里"
    return f'{run["status"]}/{run["conclusion"] or "-"}'


def main() -> int:
    # 1) 真实踩到的那个：completed 先到，同时间戳的 in_progress 后到
    rows = [
        ev(1, "queued", updated="2026-11-11T19:38:25Z"),
        ev(1, "completed", conclusion="cancelled", updated="2026-11-11T19:39:19Z"),
        ev(1, "in_progress", updated="2026-11-11T19:39:19Z"),
    ]
    ck(state_of(rows) == "completed/cancelled", "迟到的 in_progress 不能盖掉 completed", state_of(rows))

    # 2) 正常顺序：最后就是 completed
    rows = [
        ev(1, "queued", updated="2026-11-11T19:38:25Z"),
        ev(1, "in_progress", updated="2026-11-11T19:38:40Z"),
        ev(1, "completed", conclusion="success", updated="2026-11-11T19:39:00Z"),
    ]
    ck(state_of(rows) == "completed/success", "正常顺序取 completed", state_of(rows))

    # 3) 还没跑完：最后是 in_progress
    rows = [
        ev(1, "queued", updated="2026-11-11T19:38:25Z"),
        ev(1, "in_progress", updated="2026-11-11T19:38:40Z"),
    ]
    ck(state_of(rows) == "in_progress/-", "没跑完就照实说进行中", state_of(rows))

    # 4) 迟到的 in_progress 带更早的时间戳（补发的旧快照）
    rows = [
        ev(1, "queued", updated="2026-11-11T19:38:25Z"),
        ev(1, "completed", conclusion="failure", updated="2026-11-11T19:39:19Z"),
        ev(1, "in_progress", updated="2026-11-11T19:38:50Z"),
    ]
    ck(state_of(rows) == "completed/failure", "旧快照带更早时间戳 → 丢掉", state_of(rows))

    # 5) 真的更新了（时间戳更大）就得跟着走：completed 之后又来一条新的 in_progress
    #    这只会出现在重跑（新的 run attempt），状态该变
    rows = [
        ev(1, "completed", conclusion="failure", updated="2026-11-11T19:39:19Z"),
        ev(1, "in_progress", updated="2026-11-11T19:41:00Z"),
    ]
    ck(state_of(rows) == "in_progress/-", "时间戳更新的状态要采纳", state_of(rows))

    # 6) 全都没有 updated_at：靠状态优先级，completed 仍然赢
    rows = [
        ev(1, "completed", conclusion="success"),
        ev(1, "in_progress"),
        ev(1, "queued"),
    ]
    ck(state_of(rows) == "completed/success", "没有时间戳时按状态优先级", state_of(rows))

    # 7) 两条运行互不干扰
    rows = [
        ev(1, "completed", conclusion="success", updated="2026-11-11T19:39:00Z"),
        ev(2, "in_progress", updated="2026-11-11T19:40:00Z"),
        ev(1, "in_progress", updated="2026-11-11T19:39:00Z"),
    ]
    out = relay.build_actions(FakeStore(rows))
    got = {r["id"]: f'{r["status"]}/{r["conclusion"] or "-"}' for r in out["runs"]}
    ck(got.get(1) == "completed/success" and got.get(2) == "in_progress/-",
       "多条运行各算各的", repr(got))

    # 8) 排序：按开始时间倒序，跟状态无关
    rows = [
        ev(1, "completed", conclusion="success", started="2026-11-11T19:00:00Z"),
        ev(2, "in_progress", started="2026-11-11T20:00:00Z"),
    ]
    order = [r["id"] for r in relay.build_actions(FakeStore(rows))["runs"]]
    ck(order == [2, 1], "列表按开始时间倒序", repr(order))

    # 9) 提交时间：从 head_commit.timestamp 取，而且不能跟运行开始时间搞混
    #    （手动触发 / 重跑时，提交可能是几小时前写的）
    rows = [ev(1, "completed", conclusion="success",
               started="2026-11-11T19:38:00Z", updated="2026-11-11T19:39:00Z")]
    run = relay.build_actions(FakeStore(rows))["runs"][0]
    want = relay._iso_ms("2026-11-11T19:37:00Z")
    ck(run.get("commit_at") == want, "commit_at 取自 head_commit.timestamp",
       f'{run.get("commit_at")} vs {want}')
    ck(run.get("commit_at") != run.get("started_at"), "提交时间不等于运行开始时间",
       f'commit={run.get("commit_at")} started={run.get("started_at")}')

    print()
    for p in PASSES:
        print("  PASS  " + p)
    for f in FAILS:
        print("  FAIL  " + f)
    print()
    if FAILS:
        print(f"结果：{len(PASSES)} 通过 / {len(FAILS)} 失败")
        return 1
    print(f"结果：{len(PASSES)} 项全部通过")
    return 0


if __name__ == "__main__":
    sys.exit(main())

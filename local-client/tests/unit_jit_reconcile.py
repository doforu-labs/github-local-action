#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
unit_jit_reconcile.py —— 「漏掉的 job 会自己补回来」这条的验收（不连网、不起服务）

修的是这个毛病：只在 webhook 到达那一刻挑机器，那一刻机器全忙、或者事件压根没
送到，这个 job 就没人管了，一直排到 GitHub 的 24 小时上限才判失败。所以加了
定时对账：直接问 GitHub「还有谁在排队」，补发 runner；顺手收回泄漏的并发名额。

单测要盯住的几件事（顺序就是出错的顺序）：

  · 排够了 grace 还没人接 → 补一个，且走的是跟 webhook 同一条接活路径
  · 刚排上的不抢（先给 webhook 机会）、GitHub 托管 runner 的活不碰
  · 已经在飞的 job 不重复派；挑不到机器时不算「漏」，也不涨 skipped
  · 机器空出来之后下一轮能自己补上（这就是原来那个 bug 的现场）
  · 补过一次要隔 GAP 才补第二次，补够 MAX 次就停手（别把机器打爆）
  · running 里挂很久、GitHub 上其实已结束的 → 摘掉并计数（名额泄漏）
  · 还活着的长任务不能误摘
  · 没有开着的仓库就什么都不问；查询失败不抛，把错误带在结果里

跑法： python3 tests/unit_jit_reconcile.py
"""

from __future__ import annotations

import copy
import sys
import time
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import relay  # noqa: E402


def job(jid: str = "1001", name: str = "build", labels=("self-hosted", "linux", "X64"),
        age: float = 60.0, repo: str = "o/r") -> dict:
    created = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() - age))
    return {"repo": repo, "job_id": jid, "job": name, "labels": list(labels),
            "created_at": created, "run_id": 1, "run_name": "ci"}


def running_entry(at_offset: float = 0.0, repo: str = "o/r") -> dict:
    return {"job": "old", "machine": "m1", "name": "jit-m1-777", "labels": [],
            "at": time.time() - at_offset, "repo": repo}


class Reconcile(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.pool = copy.deepcopy(relay.JIT_POOL)
        self.pool.update(
            repos={"o/r": True}, machines=["m1"], max_parallel=1, running={}, history=[],
            stats={"picked": 0, "failed": 0, "done": 0, "skipped": 0,
                   "reconciled": 0, "released": 0, "last_error": ""},
        )
        self.pick = Mock(return_value={"name": "m1", "os": "linux", "arch": "x64"})
        self.take = AsyncMock(side_effect=self._fake_take)
        self.queued = AsyncMock(return_value=([], ""))
        self.state = AsyncMock(return_value=("active", ""))
        self.live = AsyncMock(return_value={})
        for patcher in (
            patch.object(relay, "JIT_POOL", self.pool),
            patch.object(relay, "_JIT_RETRY", {}),
            patch.object(relay, "_jit_pick", self.pick),
            patch.object(relay, "_jit_take_job", self.take),
            patch.object(relay, "_repo_queued_jobs", self.queued),
            patch.object(relay, "_job_state", self.state),
            patch.object(relay, "_machines_live_view", self.live),
            patch.object(relay, "JIT_RECONCILE_GRACE", 10.0),
        ):
            patcher.start()
            self.addCleanup(patcher.stop)

    async def _fake_take(self, item, via="webhook"):
        """模拟「真把 runner 拉起来了」：记一条在飞，返回 True。"""
        wj = (item.get("payload") or {}).get("workflow_job") or {}
        jid = str(wj.get("id") or "")
        self.pool["running"][jid] = {
            "job": wj.get("name"), "machine": "m1", "name": f"jit-m1-{jid}",
            "labels": wj.get("labels") or [], "at": time.time(), "repo": item.get("repo"),
        }
        return True

    # ── 补发 ────────────────────────────────────────────────

    async def test_launches_job_nobody_picked(self):
        self.queued.return_value = ([job()], "")
        out = await relay._jit_reconcile_once()
        self.assertEqual(out["queued"], 1)
        self.assertEqual(len(out["launched"]), 1)
        self.take.assert_awaited_once()
        item = self.take.await_args.args[0]
        wj = item["payload"]["workflow_job"]
        self.assertEqual((item["repo"], wj["id"], wj["labels"]),
                         ("o/r", "1001", ["self-hosted", "linux", "X64"]))
        self.assertEqual(self.take.await_args.kwargs.get("via"), "reconcile")
        self.assertEqual(self.pool["stats"]["reconciled"], 1)
        self.assertEqual(out["launched"][0]["machine"], "m1")

    async def test_skips_fresh_job_and_github_hosted(self):
        self.queued.return_value = ([job(jid="1", age=2.0),
                                     job(jid="2", labels=("ubuntu-latest",))], "")
        out = await relay._jit_reconcile_once()
        self.take.assert_not_awaited()
        self.assertEqual(out["queued"], 2)
        self.assertEqual(self.pool["stats"]["reconciled"], 0)

    async def test_skips_job_already_in_flight(self):
        self.pool["running"]["1001"] = running_entry()
        self.queued.return_value = ([job()], "")
        out = await relay._jit_reconcile_once()
        self.take.assert_not_awaited()
        self.assertEqual(out["launched"], [])

    async def test_no_machine_is_not_counted_as_missed(self):
        self.queued.return_value = ([job()], "")
        self.pick.return_value = None
        out = await relay._jit_reconcile_once()
        self.take.assert_not_awaited()
        self.assertEqual(out["waiting"], 1)
        self.assertEqual(self.pool["stats"]["skipped"], 0)

    async def test_picks_up_after_machine_frees(self):
        """原来那个 bug 的现场：机器全忙时排队的 job，空出来之后要自己补上。"""
        self.queued.return_value = ([job()], "")
        self.pick.return_value = None
        await relay._jit_reconcile_once()
        self.assertNotIn("1001", self.pool["running"])

        self.pick.return_value = {"name": "m1", "os": "linux", "arch": "x64"}
        out = await relay._jit_reconcile_once()
        self.assertEqual(len(out["launched"]), 1)
        self.assertIn("1001", self.pool["running"])

    async def test_backs_off_between_attempts(self):
        self.queued.return_value = ([job()], "")
        await relay._jit_reconcile_once()
        self.pool["running"].clear()          # runner 没接住就没了，job 还在排队
        out = await relay._jit_reconcile_once()
        self.assertEqual(out["launched"], [])  # 刚补过，GAP 内不再补

        relay._JIT_RETRY["1001"]["last"] -= relay.JIT_RECONCILE_GAP + 1
        out = await relay._jit_reconcile_once()
        self.assertEqual(len(out["launched"]), 1)

        relay._JIT_RETRY["1001"]["tries"] = relay.JIT_RECONCILE_MAX_TRIES
        relay._JIT_RETRY["1001"]["last"] = 0.0
        self.pool["running"].clear()
        out = await relay._jit_reconcile_once()
        self.assertEqual(out["launched"], [])
        self.assertEqual(out["waiting"], 1)

    async def test_forgets_retry_record_after_it_leaves_queue(self):
        self.queued.return_value = ([job()], "")
        await relay._jit_reconcile_once()
        self.assertIn("1001", relay._JIT_RETRY)
        relay._JIT_RETRY["1001"]["seen"] -= relay.JIT_RECONCILE_FORGET + 1
        self.queued.return_value = ([], "")
        await relay._jit_reconcile_once()
        self.assertNotIn("1001", relay._JIT_RETRY)

    async def test_capacity_gate_blocks_extra_launch(self):
        self.pool["running"]["999"] = running_entry()
        self.queued.return_value = ([job()], "")
        out = await relay._jit_reconcile_once()
        self.take.assert_not_awaited()
        self.assertEqual(out["waiting"], 1)

    # ── 回绑：runner 接到的未必是它的那个 job ──────────────

    async def test_in_progress_rebinds_to_real_job(self):
        """实测过的现场：给 windows-1 发的 runner 实际跑了 windows-2。"""
        self.pool["running"]["111"] = {"job": "windows-1", "machine": "m1",
                                       "name": "jit-m1-111", "labels": [],
                                       "at": time.time(), "repo": "o/r"}
        event = {"event": "workflow_job", "action": "in_progress", "repo": "o/r",
                 "payload": {"workflow_job": {"id": 222, "name": "windows-2",
                                              "runner_name": "jit-m1-111",
                                              "labels": ["self-hosted", "linux"]}}}
        await relay._jit_on_event(event)
        self.assertNotIn("111", self.pool["running"])
        self.assertIn("222", self.pool["running"])
        self.assertEqual(self.pool["running"]["222"]["job"], "windows-2")
        self.assertEqual(self.pool["history"][0]["kind"], "rebound")

        event["action"] = "completed"
        await relay._jit_on_event(event)
        self.assertNotIn("222", self.pool["running"])
        self.assertEqual(self.pool["stats"]["done"], 1)

    async def test_reconciler_retries_after_rebind(self):
        """回绑之后，那个被顶掉的 job 对账会重新给它发 runner。"""
        self.pool["running"]["111"] = {"job": "windows-1", "machine": "m1",
                                       "name": "jit-m1-111", "labels": [],
                                       "at": time.time(), "repo": "o/r"}
        event = {"event": "workflow_job", "action": "in_progress", "repo": "o/r",
                 "payload": {"workflow_job": {"id": 222, "name": "windows-2",
                                              "runner_name": "jit-m1-111"}}}
        await relay._jit_on_event(event)
        # windows-1 还在排队，而机器已空（222 那条在飞，先挪走模拟它已跑完）
        self.pool["running"].clear()
        self.queued.return_value = ([job(jid="111", name="windows-1")], "")
        out = await relay._jit_reconcile_once()
        self.assertEqual(len(out["launched"]), 1)
        self.assertIn("111", self.pool["running"])

    # ── 收回泄漏的名额 ─────────────────────────────────────

    async def test_releases_slot_whose_job_already_ended(self):
        self.pool["running"]["777"] = running_entry(at_offset=relay.JIT_RUNNING_CHECK_AFTER + 60)
        self.state.return_value = ("finished", "")
        out = await relay._jit_reconcile_once()
        self.assertEqual(out["released"], ["777"])
        self.assertNotIn("777", self.pool["running"])
        self.assertEqual(self.pool["stats"]["released"], 1)
        self.assertEqual(self.pool["history"][0]["kind"], "released")

    async def test_keeps_long_running_job(self):
        self.pool["running"]["777"] = running_entry(at_offset=relay.JIT_RUNNING_CHECK_AFTER + 60)
        self.state.return_value = ("active", "")
        out = await relay._jit_reconcile_once()
        self.assertEqual(out["released"], [])
        self.assertIn("777", self.pool["running"])

    async def test_does_not_check_recent_entries(self):
        self.pool["running"]["777"] = running_entry(at_offset=30)
        self.state.return_value = ("finished", "")
        out = await relay._jit_reconcile_once()
        self.state.assert_not_awaited()
        self.assertIn("777", self.pool["running"])

    # ── 边界 ────────────────────────────────────────────────

    async def test_no_watched_repo_asks_nothing(self):
        self.pool["repos"] = {"o/r": False}
        out = await relay._jit_reconcile_once()
        self.queued.assert_not_awaited()
        self.live.assert_not_awaited()
        self.assertEqual(out["repos"], [])

    async def test_query_error_is_reported_not_raised(self):
        self.queued.return_value = ([], "HTTP 502")
        out = await relay._jit_reconcile_once()
        self.assertIn("502", out["error"])


if __name__ == "__main__":
    unittest.main(verbosity=2)

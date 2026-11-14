#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
unit_job_stale.py —— 「job 说在跑、机器已经空了」这条交叉校验的验收（不连网、不起服务）

为什么要单测：这个判断只用来「多说一句话」，最容易出的错是反过来的那种——
说少了没人发现，说多了就是天天喊狼来了：

  · 刚跑完那一下 runner 先变空闲，job 状态还没跟上（正常现象）→ 不能标
  · queued 已经派到某台 runner 上（拿到活正要开干）→ 不能标
  · job 早就 completed 了，机器空着是应该的 → 不能标
  · runner 名字对不上（换了机器、JIT 名字带随机数）→ 不能标

真该标的是两种：机器 busy=false（worker 已经退了），和 runner 离线。

跑法： python3 tests/unit_job_stale.py
"""

from __future__ import annotations

import asyncio
import sys
import time
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


def _ago(seconds: float) -> str:
    """ISO 时间字符串，N 秒之前（GitHub 给的 started_at 就是这个形状）。"""
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() - seconds))


def _job(status: str = "in_progress", runner: str = "mac", started: float = 600) -> dict:
    return {"id": 1, "name": "build", "status": status, "conclusion": "",
            "runner_name": runner, "started_at": _ago(started) if started is not None else None}


class FakeRunners:
    """替掉 relay._repo_runners：记下被问了几次，按需要返回 runner 列表或抛错。"""

    def __init__(self, runners=None, boom: bool = False) -> None:
        self.runners = runners if runners is not None else []
        self.boom = boom
        self.calls: list[str] = []

    async def __call__(self, repo: str):
        self.calls.append(repo)
        if self.boom:
            raise RuntimeError("gh api 挂了")
        return self.runners, ""


def run_case(jobs: list[dict], runners: list[dict] | None = None, boom: bool = False):
    fake = FakeRunners(runners, boom)
    old = relay._repo_runners
    relay._repo_runners = fake
    try:
        asyncio.run(relay._mark_stale_jobs(jobs, "example-org/demo"))
    finally:
        relay._repo_runners = old
    return jobs, fake


def main() -> int:
    idle = [{"name": "mac", "status": "online", "busy": False}]
    busy = [{"name": "mac", "status": "online", "busy": True}]
    gone = [{"name": "mac", "status": "offline", "busy": True}]

    # 1) 该标的：跑了十分钟，机器空了
    jobs, fake = run_case([_job()], idle)
    ck(jobs[0].get("stale") == "runner_idle", "机器空闲 → 标 runner_idle", repr(jobs[0].get("stale")))
    ck(fake.calls == ["example-org/demo"], "确实去问了那个仓库的 runner", repr(fake.calls))

    # 2) 该标的：runner 掉线了，job 还挂着
    jobs, _ = run_case([_job()], gone)
    ck(jobs[0].get("stale") == "runner_offline", "runner 离线 → 标 runner_offline", repr(jobs[0].get("stale")))

    # 3) 不该标的：机器正忙着
    jobs, _ = run_case([_job()], busy)
    ck("stale" not in jobs[0], "机器正在忙 → 不标", repr(jobs[0].get("stale")))

    # 4) 不该标的：刚开跑 10 秒，还在缓冲期里（跑完那一下 runner 也会先空）
    jobs, _ = run_case([_job(started=10)], idle)
    ck("stale" not in jobs[0], "刚开始跑（缓冲期内）→ 不标", repr(jobs[0].get("stale")))

    # 5) 不该标的：job 已经 completed，机器空着是应该的
    jobs, _ = run_case([dict(_job(status="completed"), conclusion="success")], idle)
    ck("stale" not in jobs[0], "job 已完成 → 不标", repr(jobs[0].get("stale")))

    # 6) 不该标的：queued 但已经派到 runner 上（正要开干，很常见的一瞬）
    jobs, _ = run_case([_job(status="queued", started=None)], idle)
    ck("stale" not in jobs[0], "排队中且已派到机器 → 不标", repr(jobs[0].get("stale")))

    # 7) 不该标的：这条 runner 记录里没有它（JIT 名字带随机数、或换了机器）
    jobs, _ = run_case([_job(runner="jit-mac-hunter2")], idle)
    ck("stale" not in jobs[0], "runner 名字对不上 → 不标", repr(jobs[0].get("stale")))

    # 8) 没有 in_progress 的 job 时压根不该去问 GitHub（每次都是 1 秒起步的往返）
    _, fake = run_case([dict(_job(status="completed"), conclusion="success")], idle)
    ck(fake.calls == [], "没有在跑的 job → 一次都不问", repr(fake.calls))

    # 9) GitHub 那边挂了：不标、不崩，页面上还是原来的样子
    jobs, _ = run_case([_job()], boom=True)
    ck("stale" not in jobs[0], "查 runner 失败 → 不标也不崩", repr(jobs[0].get("stale")))

    # 10) 名字大小写不一样也要对上（GitHub 那边是 mac-Builder 这种）
    jobs, _ = run_case([_job(runner="MAC")], [{"name": "Mac", "status": "online", "busy": False}])
    ck(jobs[0].get("stale") == "runner_idle", "runner 名字大小写不敏感", repr(jobs[0].get("stale")))

    # 11) 一次运行里多个 job，只标该标的那个
    jobs, _ = run_case([
        _job(runner="mac"),
        dict(_job(runner="win"), id=2, name="build-win"),
    ], [{"name": "mac", "status": "online", "busy": False},
        {"name": "win", "status": "online", "busy": True}])
    ck(jobs[0].get("stale") == "runner_idle" and "stale" not in jobs[1],
       "多个 job 时只标空闲的那台", repr([j.get("stale") for j in jobs]))

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

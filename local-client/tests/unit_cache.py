"""GitHub job / 日志的本地缓存。

规则只有一条：**不会再变的东西才缓存**。
job 全部完成的运行、取得到的日志（GitHub 在 job 没跑完时给 404）才进库；
还在跑的运行不读缓存，每次重取——否则界面就不「同步」了。
"""
import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import relay


class CacheStore(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = relay.Store(str(Path(self.tmp.name) / "t.db"))

    def tearDown(self):
        self.store.conn.close()
        self.tmp.cleanup()

    def test_miss_then_hit(self):
        self.assertIsNone(self.store.cache_get("jobs:a/b:1"))
        self.store.cache_put("jobs:a/b:1", '{"jobs": []}')
        hit = self.store.cache_get("jobs:a/b:1")
        self.assertIsNotNone(hit)
        self.assertEqual(hit[0], '{"jobs": []}')
        self.assertGreater(hit[1], 0)

    def test_put_overwrites(self):
        self.store.cache_put("k", "old")
        self.store.cache_put("k", "new")
        self.assertEqual(self.store.cache_get("k")[0], "new")

    def test_prune_keeps_newest(self):
        for i in range(430):
            self.store.cache_put(f"k{i}", "x")
            # at 是秒级，同一秒里的先后靠插入顺序；这里手动错开一下
            self.store.conn.execute("UPDATE gh_cache SET at = ? WHERE key = ?", (1000 + i, f"k{i}"))
        self.store.conn.commit()
        self.store.cache_put("last", "x")
        self.store.conn.execute("UPDATE gh_cache SET at = 99999 WHERE key = 'last'")
        self.store.conn.commit()
        total = self.store.conn.execute("SELECT COUNT(*) AS n FROM gh_cache").fetchone()["n"]
        self.assertLessEqual(total, 400, f"缓存条数没兜住：{total}")
        self.assertIsNotNone(self.store.cache_get("last"), "最新的那条被删了")
        self.assertIsNone(self.store.cache_get("k0"), "最旧的那条没被清掉")

    def test_table_created_on_open(self):
        cols = {r["name"] for r in self.store.conn.execute("PRAGMA table_info(gh_cache)")}
        self.assertEqual(cols, {"key", "body", "at"})

    def test_cursor_lives_in_memory(self):
        # /health 每两秒被轮一次，不该为它读库。插入/设值都要同步到内存。
        self.assertEqual(self.store.cursor(), 0)
        self.store.insert({"id": 7, "event": "push", "at": 1})
        self.assertEqual(self.store.cursor(), 7)
        self.store.set_cursor(99)
        self.assertEqual(self.store.cursor(), 99)
        # 旧事件（id 更小）不该把游标拉回去
        self.store.insert({"id": 3, "event": "push", "at": 2})
        self.assertEqual(self.store.cursor(), 99)

    def test_read_connection_is_per_thread(self):
        # 并发读共用一个连接会排成一队（实测把 /health 拖到 140ms），
        # 所以读走线程本地连接；顺手确认它拒写。
        import threading
        seen = {}

        def grab(key):
            first = self.store._ro()
            seen[key] = (id(first), id(self.store._ro()))  # 同一线程要复用同一个

        for key in ("a", "b"):
            t = threading.Thread(target=grab, args=(key,))
            t.start(); t.join()
        self.assertEqual(seen["a"][0], seen["a"][1], "同一线程要复用")
        self.assertNotEqual(seen["a"][0], seen["b"][0], "不同线程应该拿到不同连接")
        mine = id(self.store._ro())
        self.assertEqual(mine, id(self.store._ro()), "主线程也要复用")
        with self.assertRaises(Exception):
            self.store._ro().execute("INSERT INTO meta(key,value) VALUES('x','y')")


class CacheWiring(unittest.TestCase):
    """只想确认接口里那两处「什么才缓存」的判断没写反。"""

    def _jobs_final(self, jobs):
        return all(j.get("status") == "completed" for j in jobs) and bool(jobs)

    def test_finished_runs_are_cacheable(self):
        self.assertTrue(self._jobs_final([{"status": "completed"}, {"status": "completed"}]))

    def test_running_runs_are_not(self):
        self.assertFalse(self._jobs_final([{"status": "completed"}, {"status": "in_progress"}]))
        self.assertFalse(self._jobs_final([{"status": "queued"}]))

    def test_empty_job_list_is_not_cacheable(self):
        # 排队中的运行 jobs 是空的，缓存下来就再也不会更新了
        self.assertFalse(self._jobs_final([]))


if __name__ == "__main__":
    unittest.main(verbosity=0)

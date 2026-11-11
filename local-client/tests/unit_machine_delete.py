"""机器删除：持久化、按端点整组、只动本地、忙碌保护。

改名功能已经去掉了（机器名就用主机名，够用），所以这里只剩删除。
"""
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import relay


class Request:
    def __init__(self, body, remote="127.0.0.1"):
        self.body, self.path, self.remote = body, "/runners/delete", remote

    async def json(self):
        return self.body


class MachineDeletes(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db, self.key = [str(Path(self.tmp.name) / n) for n in ("test.db", "key")]
        self.store = relay.MachineStore(self.db, self.key)
        self.machine = dict(host="host", port=22, user="user", password="secret",
                            pure=True, os="linux", arch="x64")
        self.store["stable-id"] = self.machine
        self.store["same-endpoint"] = {**self.machine, "user": "other"}
        self.store["different-port"] = {**self.machine, "port": 2222}
        self.patches = [
            patch.object(relay, "SSH_MACHINES", self.store),
            patch.object(relay, "JIT_POOL",
                         {"running": {}, "machines": [], "max_parallel": 3}),
            patch.object(relay, "_JIT_TASKS", set()),
            patch.object(relay, "SSH_JOBS", {}),
        ]
        for p in self.patches:
            p.start()

    def tearDown(self):
        for p in reversed(self.patches):
            p.stop()
        self.store.conn.close()
        self.tmp.cleanup()

    async def test_delete_removes_whole_endpoint_group_and_is_durable(self):
        relay._jit_auto_pool()
        response = await relay.handle_machine_delete(Request({"name": "stable-id"}))
        self.assertEqual(response.status, 200)
        # 同一端点下两条凭据一起清，别的端口那台留着
        self.assertEqual(self.store.keys(), ["different-port"])
        self.assertEqual(relay.JIT_POOL["machines"], ["different-port"])
        again = relay.MachineStore(self.db, self.key)
        self.assertEqual(again.keys(), ["different-port"], "删掉的不该在重启后回来")
        again.conn.close()

    async def test_busy_and_preparing_machines_cannot_be_deleted(self):
        relay.JIT_POOL["running"]["job"] = {"machine": "same-endpoint"}
        r = await relay.handle_machine_delete(Request({"name": "stable-id"}))
        self.assertEqual(r.status, 409)
        relay.JIT_POOL["running"].clear()

        import asyncio
        task = asyncio.create_task(asyncio.sleep(60))
        relay._JIT_TASKS.add(task)
        try:
            r = await relay.handle_machine_delete(Request({"name": "stable-id"}))
            self.assertEqual(r.status, 409)
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        self.assertIn("stable-id", self.store, "拒绝的时候一条都不能删")

    async def test_validation_and_remote_access(self):
        for bad in ([], {"name": 23}, {}, "\u4e0d\u662f\u5bf9\u8c61"):
            self.assertEqual(
                (await relay.handle_machine_delete(Request(bad))).status, 400, str(bad))
        r = await relay.handle_machine_delete(Request({"name": "missing"}))
        self.assertEqual(r.status, 404)
        # 局域网设备不能删本机凭据
        r = await relay.handle_machine_delete(
            Request({"name": "stable-id"}, remote="192.168.1.2"))
        self.assertEqual(r.status, 403)
        self.assertEqual(len(self.store), 3, "校验失败时不能动数据")

    async def test_machines_list_never_leaks_password(self):
        response = await relay.handle_machines(Request({}))
        data = json.loads(response.text)
        self.assertEqual(data["count"], 3)
        self.assertTrue(all("password" not in m for m in data["machines"]))
        self.assertTrue(all("secret" not in m for m in data["machines"]))


if __name__ == "__main__":
    unittest.main()

"""Machine edits: persistence, endpoint grouping, local access and busy protection."""
import asyncio
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import relay

class Request:
    def __init__(self, body, action='rename', remote='127.0.0.1'):
        self.body, self.path, self.remote = body, '/runners/' + action, remote
    async def json(self):
        return self.body

class MachineEdits(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db, self.key = [str(Path(self.tmp.name) / name) for name in ('test.db', 'key')]
        self.store = relay.MachineStore(self.db, self.key)
        self.machine = dict(host='host', port=22, user='user', password='secret', pure=True, os='linux', arch='x64')
        self.store['stable-id'] = self.machine
        self.store['same-endpoint'] = {**self.machine, 'user':'other'}
        self.store['different-port'] = {**self.machine, 'port':2222}
        self.patches = [patch.object(relay, 'SSH_MACHINES', self.store),
                        patch.object(relay, 'JIT_POOL', {'running':{}, 'machines':[], 'max_parallel':3}),
                        patch.object(relay, '_JIT_TASKS', set()), patch.object(relay, 'SSH_JOBS', {})]
        for p in self.patches: p.start()
    def tearDown(self):
        for p in reversed(self.patches): p.stop()
        self.store.conn.close()
        self.tmp.cleanup()
    async def test_rename_is_durable_metadata_only(self):
        secret = self.store.conn.execute("SELECT secret FROM machines WHERE name='stable-id'").fetchone()[0]
        result = await relay.handle_machine_edit(Request({'name':'stable-id','display_name':' 工作 Mac '}))
        self.assertEqual(result.status, 200)
        self.assertEqual(self.store['same-endpoint']['display_name'], '工作 Mac')
        self.assertEqual(self.store['different-port']['display_name'], '')
        self.assertEqual(self.store.conn.execute("SELECT secret FROM machines WHERE name='stable-id'").fetchone()[0], secret)
        again = relay.MachineStore(self.db, self.key)
        self.assertEqual(again['stable-id']['display_name'], '工作 Mac')
        self.assertEqual(again['stable-id']['password'], 'secret')
        again.conn.close()
        response = await relay.handle_machines(Request({}))
        data = json.loads(response.text)
        self.assertNotIn('password', data['machines'][0])
        self.assertEqual(next(m for m in data['machines'] if m['name']=='stable-id')['display_name'], '工作 Mac')
    async def test_delete_removes_all_endpoint_credentials_and_pool_entries(self):
        relay._jit_auto_pool()
        response = await relay.handle_machine_edit(Request({'name':'stable-id'}, 'delete'))
        self.assertEqual(response.status, 200)
        self.assertEqual(self.store.keys(), ['different-port'])
        self.assertEqual(relay.JIT_POOL['machines'], ['different-port'])
        again = relay.MachineStore(self.db, self.key)
        self.assertEqual(again.keys(), ['different-port'])
        again.conn.close()
    async def test_busy_and_preparing_machines_cannot_be_deleted(self):
        relay.JIT_POOL['running']['job'] = {'machine':'same-endpoint'}
        self.assertEqual((await relay.handle_machine_edit(Request({'name':'stable-id'}, 'delete'))).status,409)
        relay.JIT_POOL['running'].clear()
        task = asyncio.create_task(asyncio.sleep(60))
        relay._JIT_TASKS.add(task)
        try:
            self.assertEqual((await relay.handle_machine_edit(Request({'name':'stable-id'}, 'delete'))).status,409)
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        self.assertIn('stable-id', self.store)
    async def test_validation_and_remote_access(self):
        for label in ['', ' ', 'a'*65, 'a\nb', None, 23]:
            self.assertEqual((await relay.handle_machine_edit(Request({'name':'stable-id','display_name':label}))).status,400)
        self.assertEqual((await relay.handle_machine_edit(Request([]))).status,400)
        self.assertEqual((await relay.handle_machine_edit(Request({'name':'missing','display_name':'x'}))).status,404)
        self.assertEqual((await relay.handle_machine_edit(Request({'name':'stable-id'}, 'delete', '192.168.1.2'))).status,403)
        self.assertEqual(len(self.store),3)
    async def test_legacy_database_migrates_without_losing_credentials(self):
        self.store.conn.execute('ALTER TABLE machines DROP COLUMN display_name')
        self.store.conn.commit()
        again = relay.MachineStore(self.db, self.key)
        self.assertEqual(again['stable-id']['display_name'],'')
        self.assertEqual(again['stable-id']['password'],'secret')
        again.conn.close()

if __name__ == '__main__': unittest.main()

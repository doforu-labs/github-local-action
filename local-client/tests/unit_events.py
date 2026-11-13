"""Latest history and SSE catch-up regression tests; isolated temporary DB/server."""
import asyncio
import json
import sys
import tempfile
import unittest
from pathlib import Path
from aiohttp.test_utils import TestClient, TestServer
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import relay


class EventFlow(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = relay.Store(str(Path(self.tmp.name) / 'events.db'))
        self.bus = relay.Bus()
        relay.MEMO_EVENTS._data.clear()
        # Non-contiguous IDs ensure pagination uses the cursor, not row counts.
        for i in range(1, 1206):
            self.store.insert({'id': i * 2, 'event': 'workflow_job', 'repo': 'test/repo',
                               'action': 'completed', 'at': i, 'payload': {'workflow_job': {'id': i}}})
        self.client = TestClient(TestServer(relay.build_app(relay.Config(ws_url='ws://127.0.0.1/unused', token='test'), self.store, self.bus, relay.State())))
        await self.client.start_server()

    async def asyncTearDown(self):
        self.bus.close_all()
        await self.client.close()
        self.store.close()
        self.tmp.cleanup()

    async def ids(self, response, count):
        ids = []
        async def read():
            while len(ids) < count:
                line = await response.content.readline()
                if not line:
                    raise AssertionError(f'Stream ended after {len(ids)} events')
                if line.startswith(b'data: '):
                    ids.append(json.loads(line[6:])['id'])
        await asyncio.wait_for(read(), 3)
        return ids

    async def test_latest_and_cursor_history_are_distinct(self):
        for url, expected in [('/events?after=0&limit=60', list(range(2,122,2))),
                              ('/events?latest=1&limit=60', list(range(2292,2412,2))),
                              ('/events?after=0&limit=60', list(range(2,122,2)))]:
            response = await self.client.get(url)
            self.assertEqual(response.status, 200)
            self.assertEqual([e['id'] for e in (await response.json())['events']], expected)

    async def test_replay_more_than_two_pages_then_live(self):
        response = await self.client.get('/stream')
        try:
            self.assertEqual(await self.ids(response, 1205), list(range(2,2412,2)))
            self.bus.publish({'id': 2410, 'event': 'push'})  # overlap must be deduplicated
            event = {'id': 2412, 'event': 'push', 'at': 1}
            self.store.insert(event)
            self.bus.publish(event)
            self.assertEqual(await self.ids(response, 1), [2412])
        finally:
            response.close()

    async def test_snapshot_to_stream_gap_is_replayed(self):
        history = await (await self.client.get('/events?latest=1&limit=60')).json()
        cursor = history['events'][-1]['id']
        self.store.insert({'id': 2412, 'event': 'push', 'at': 1})
        response = await self.client.get(f'/stream?after={cursor}')
        try:
            self.assertEqual(await self.ids(response, 1), [2412])
        finally:
            response.close()

    async def test_reconnect_header_overrides_original_query(self):
        response = await self.client.get('/stream?after=2', headers={'Last-Event-ID': '2406'})
        try:
            self.assertEqual(await self.ids(response, 2), [2408, 2410])
        finally:
            response.close()

    async def test_empty_latest_history(self):
        self.store.conn.execute('DELETE FROM events')
        self.store.conn.commit()
        self.store.version += 1
        response = await self.client.get('/events?latest=1&limit=60')
        self.assertEqual((await response.json())['events'], [])


if __name__ == '__main__':
    unittest.main(verbosity=2)

"""Machine health must be independent of GitHub repositories and runner names."""
import asyncio
import copy
import json
import sys
import time
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import relay


class Inventory(dict):
    def touch(self, name, at=None, min_gap=20):
        self[name]['last_ok'] = int(at or time.time())


def machine(port=22, **extra):
    return dict(host='host', port=port, user='dev', password='private-test-password',
                os='linux', arch='x64', pure=True, last_ok=10, **extra)


READY = dict(connected=True, ready=True, busy=False, error='', jit_dir='/home/dev/jitlab')


class MachineHealth(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.inventory = Inventory({'host-name': machine(), 'old-runner-name': machine()})
        self.pool = copy.deepcopy(relay.JIT_POOL)
        self.pool.update(machines=[], running={}, repos={'owner/repo': True})
        self.probe = Mock(return_value=READY.copy())
        self.patches = [patch.object(relay, 'SSH_MACHINES', self.inventory),
                       patch.object(relay, 'JIT_POOL', self.pool),
                       patch.object(relay, '_MACHINE_PROBE_CACHE', {}),
                       patch.object(relay, '_MACHINE_PROBE_TASKS', {}),
                       patch.object(relay, '_MACHINE_PROBE_SLOTS', asyncio.Semaphore(4)),
                       patch.object(relay, '_probe_machine_ssh', self.probe),
                       patch.object(relay, 'github_auth', AsyncMock(side_effect=AssertionError('No GitHub dependency'))),
                       patch.object(relay, '_repo_runners', AsyncMock(side_effect=AssertionError('No repo scan')))]
        for p in self.patches: p.start()
        self.addCleanup(lambda: [p.stop() for p in reversed(self.patches)])

    async def test_online_without_repo_or_github_login_and_aliases_share_state(self):
        data=await relay._machines_live_view()
        self.assertEqual(data['repos'], [])
        self.assertEqual(data['source'], 'ssh')
        self.assertEqual(self.probe.call_count, 1)
        for item in data['machines'].values():
            self.assertEqual(item['state'], 'idle')
            self.assertGreater(item['last_online_at'], 10)
            self.assertEqual(item['repo'], '')
        self.assertNotIn('private-test-password', json.dumps(data))
        self.assertNotIn('credential_name', json.dumps(data))

    async def test_same_endpoint_single_flight_cache_and_credentials_invalidation(self):
        await asyncio.gather(*[relay._machines_live_view() for _ in range(6)])
        self.assertEqual(self.probe.call_count, 1)
        await relay._machines_live_view()
        self.assertEqual(self.probe.call_count, 1)
        self.inventory['host-name']['password']='changed'
        await relay._machines_live_view()
        self.assertEqual(self.probe.call_count, 2)

    async def test_offline_preserves_last_online_and_error(self):
        self.probe.return_value=dict(connected=False, ready=False, busy=False, error='SSH 认证失败')
        data=await relay._machines_live_view()
        for item in data['machines'].values():
            self.assertEqual(item['state'],'offline')
            self.assertEqual(item['last_online_at'],10)
            self.assertEqual(item['error'],'SSH 认证失败')

    async def test_failed_old_credentials_fall_back_to_other_alias(self):
        self.probe.side_effect=[dict(connected=False,error='SSH 认证失败'), READY.copy()]
        await relay._machines_live_view()
        relay._jit_auto_pool()
        picked=relay._jit_pick(['self-hosted','Linux','X64'])
        self.assertIsNotNone(picked)
        self.assertEqual(picked['jit_dir'],READY['jit_dir'])
        self.assertEqual(self.probe.call_count,2)

    async def test_connected_without_runtime_needs_setup(self):
        self.probe.return_value={**READY,'ready':False}
        data=await relay._machines_live_view()
        self.assertEqual(data['machines']['host-name']['state'],'unprepared')
        # SSH 连上了但环境没备好，不算「在线过」
        self.assertEqual(data['machines']['host-name']['last_online_at'],10)
        relay._jit_auto_pool()
        self.assertIsNone(relay._jit_pick(['self-hosted','Linux']))

    async def test_active_worker_is_busy_without_repo_lookup(self):
        self.probe.return_value={**READY,'busy':True}
        data=await relay._machines_live_view()
        self.assertTrue(all(m['state']=='busy' for m in data['machines'].values()))
        relay._jit_auto_pool()
        self.assertIsNone(relay._jit_pick(['self-hosted','Linux']))

    async def test_probe_failure_after_login_is_unknown_not_idle(self):
        self.probe.return_value={**READY,'error':'SSH 已连接，但运行环境检测失败'}
        data=await relay._machines_live_view()
        self.assertEqual(data['machines']['host-name']['state'],'unknown')
        # 探测出错同样不能把「最后在线」刷成现在
        self.assertEqual(data['machines']['host-name']['last_online_at'],10)

    async def test_pool_counts_endpoints_and_busy_alias_blocks_other_names(self):
        self.inventory['another-port']=machine(2222)
        relay._jit_auto_pool()
        self.assertEqual(len(self.pool['machines']),2)
        self.assertEqual(self.pool['max_parallel'],2)
        await relay._machines_live_view()
        self.pool['running']['42']={'machine':'old-runner-name','name':'jit-test','job':'build','run_id':7,'repo':'org/any'}
        data=await relay._machines_live_view()
        self.assertEqual(data['machines']['host-name']['state'],'busy')
        self.assertEqual(data['machines']['host-name']['job']['run_id'],7)
        self.assertEqual(relay._jit_pick(['self-hosted','Linux'])['port'],2222)

    async def test_concurrent_jobs_reserve_before_ticket_and_release_failures(self):
        relay._jit_auto_pool()
        entered=asyncio.Event();release=asyncio.Event()
        async def ticket(*args):
            entered.set();await release.wait();return '', 'ticket failure'
        def event(i):
            return {'repo':'org/arbitrary','payload':{'workflow_job':{'id':i,'name':'build','labels':['self-hosted','Linux']}}}
        with patch.object(relay,'_jit_ticket',AsyncMock(side_effect=ticket)) as tickets, patch.object(relay,'_jit_launch',AsyncMock()) as launch:
            first=asyncio.create_task(relay._jit_take_job(event(1)))
            await asyncio.wait_for(entered.wait(),1)
            await relay._jit_take_job(event(2))
            self.assertEqual(tickets.await_count,1)
            self.assertEqual(len(self.pool['running']),1)
            release.set();await first
            self.assertEqual(self.pool['running'],{})
            launch.assert_not_awaited()

    async def test_successful_job_uses_event_repo_and_probed_directory(self):
        relay._jit_auto_pool()
        event={'repo':'org/new-repo','payload':{'workflow_job':{'id':5,'run_id':8,'name':'build','labels':['self-hosted','Linux']}}}
        with patch.object(relay,'_jit_ticket',AsyncMock(return_value=('ticket',''))) as ticket, patch.object(relay,'_jit_launch',AsyncMock(return_value=(True,''))) as launch:
            await relay._jit_take_job(event)
            self.assertEqual(ticket.call_args.args[0],'org/new-repo')
            self.assertEqual(launch.call_args.args[0]['jit_dir'],READY['jit_dir'])
            self.assertEqual(self.pool['running']['5']['run_id'],8)


class ProbeCommands(unittest.TestCase):
    def test_windows_progress_output_does_not_hide_ready_runtime(self):
        box=Mock()
        box.run.return_value=(0, '#< CLIXML\n' + json.dumps({'ready':True,'busy':False,'jit_dir':r'C:\jitlab'}) + '\n<Objs>progress</Objs>')
        with patch.object(relay, 'SshBox', return_value=box):
            result=relay._probe_machine_ssh({**machine(), 'os':'windows'})
        self.assertTrue(result['connected'])
        self.assertTrue(result['ready'])
        self.assertEqual(result['jit_dir'],r'C:\jitlab')
        box.close.assert_called_once()


if __name__=='__main__': unittest.main(verbosity=2)

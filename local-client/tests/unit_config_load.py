"""配置加载：没有配置文件也得起得来，缺字段不该崩。

真实翻车记录（2026-11-14，加 Docker 支持时）：`Config.load()` 打出一行
「没有找到配置文件，使用默认值（不会连上任何地方）」，下一行却
`TypeError: missing 2 required positional arguments: 'ws_url' and 'token'` ——
因为 ws_url / token 在 dataclass 里是必填的。以前只有 macOS 上手动放好配置
才跑，所以一直没暴露；容器里第一次 `docker compose up`（data 目录是空的）
就是这条路径，起来就死。

顺带钉住两件相关的事：
- 空 ws_url 时 ws_loop 不许拿空串去连（原来会每轮报一次 InvalidURL）
- 写了不认识的键要报错，不能默默忽略（否则把字段拼错了查半天）
"""
import asyncio
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import relay  # noqa: E402


class ConfigLoad(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.dir = Path(self.tmp.name)

    def test_missing_file_still_starts(self):
        cfg = relay.Config.load(self.dir / "nope.toml")
        self.assertEqual(cfg.ws_url, "", "没有配置文件时地址应该是空串，不是崩溃")
        self.assertEqual(cfg.token, "")
        self.assertEqual(cfg.http_port, 18091)
        self.assertEqual(cfg.path, self.dir / "nope.toml")

    def test_partial_file_fills_the_rest(self):
        p = self.dir / "config.toml"
        p.write_text('[relay]\nws_url = "wss://x.test/ws"\n', encoding="utf-8")
        cfg = relay.Config.load(p)
        self.assertEqual(cfg.ws_url, "wss://x.test/ws")
        self.assertEqual(cfg.token, "", "没写的字段走默认值")
        self.assertEqual(cfg.trusted_peers, ())

    def test_unknown_key_is_an_error(self):
        p = self.dir / "config.toml"
        p.write_text('[relay]\nws_url = "wss://x.test/ws"\nws_urll = "typo"\n', encoding="utf-8")
        with self.assertRaises(TypeError):
            relay.Config.load(p)

    def test_empty_url_does_not_touch_the_network(self):
        """空地址时 ws_loop 应该等着，而不是拿空串去连。"""
        cfg = relay.Config(ws_url="", token="", db_path=str(self.dir / "e.db"))
        store = relay.Store(cfg.db_path)
        bus = relay.Bus()
        state = relay.State()
        stop = asyncio.Event()

        async def run():
            task = asyncio.create_task(relay.ws_loop(cfg, store, bus, state, stop))
            await asyncio.sleep(0.3)
            stop.set()
            await asyncio.wait_for(task, timeout=3)
            return state.connects

        connects = asyncio.run(run())
        self.assertEqual(connects, 0, "没配地址就不该发起连接")


if __name__ == "__main__":
    unittest.main()

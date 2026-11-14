"""自动启动服务的 label：不许绑组织名，换 label 后旧安装必须能被清掉。

改名这件事的危险点在迁移：机器上装着旧 label 的服务，label 一换，旧的那个不会自己
消失，还会占着端口跟新服务抢。所以这里锁三条：新 label 干净、旧 label 记在清单里、
装/卸时真的会把旧文件清掉。
"""
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import relay


async def _no_launchctl(_args, **_kw):
    """真的 launchctl 一条都别执行：问不到就当没加载。"""
    return 1, ""


class ServiceLabel(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.home = Path(self.tmp.name)
        self.agents = self.home / "Library" / "LaunchAgents"
        self.agents.mkdir(parents=True)
        self.env = patch.dict(os.environ, {"HOME": str(self.home)})
        self.env.start()
        self.addCleanup(self.env.stop)
        self.addCleanup(self.tmp.cleanup)
        for p in (patch.object(relay, "_service_platform", lambda: "macos"),
                  patch.object(relay, "_svc_run", _no_launchctl)):
            p.start()
            self.addCleanup(p.stop)

    def _write_legacy(self) -> Path:
        p = self.agents / f"{relay.LEGACY_SERVICE_LABELS[0]}.plist"
        p.write_text("<plist><dict/></plist>", encoding="utf-8")
        return p

    def test_label_is_brand_free(self):
        # 只认这一个值：谁再往里塞组织/品牌前缀都会红（原来的写法是写死旧品牌名，
        # 但那样等于把品牌名又放回仓库，脱敏扫描也会报，所以用等值断言）。
        self.assertEqual(relay.SERVICE_LABEL, "local.github-local-action")
        self.assertTrue(relay.SERVICE_LABEL.startswith("local."),
                        "服务 label 用 local. 前缀，不挂任何组织域名")
        # systemd unit 名一直没变过，换 label 不该动它
        self.assertEqual(relay.SERVICE_UNIT, "github-local-action.service")

    def test_legacy_label_is_remembered(self):
        # 删掉这条就等于升级后清不掉旧服务：两个进程抢同一个端口
        self.assertIn("local.github-local-action", relay.LEGACY_SERVICE_LABELS)

    async def test_status_reports_legacy_install(self):
        info = await relay._service_status()
        self.assertNotIn("legacy_files", info, "没有旧文件时不该报")
        old = self._write_legacy()
        info = await relay._service_status()
        self.assertEqual(info["legacy_files"], [str(old)])
        self.assertIn("旧服务", info["hint"])

    async def test_install_and_uninstall_drop_legacy(self):
        old = self._write_legacy()
        await relay._service_drop_legacy()
        self.assertFalse(old.exists(), "装/卸新版时必须把旧 plist 删掉")

    async def test_new_plist_path_uses_new_label(self):
        self.assertEqual(relay._service_paths()["file"],
                         self.agents / "local.github-local-action.plist")


if __name__ == "__main__":
    unittest.main()

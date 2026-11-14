"""自动启动服务的 label：不许绑组织名，plist 要落到当前用户目录。

这个文件曾经还负责「升级时清掉旧 label 的 plist」。后来把旧 label 常量整个删掉了：
仓库从未公开过，不存在别人的旧安装，为它长期在源码里留一个品牌字符串不划算。
装过改名前版本的人，用旧版的 `service uninstall` 卸一次即可（这里也把「卸载不留
残余、不动别人的文件」锁一下）。
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

    def test_label_is_brand_free(self):
        # 只认这一个值：谁再往里塞组织/品牌前缀都会红
        self.assertEqual(relay.SERVICE_LABEL, "local.github-local-action")
        self.assertTrue(relay.SERVICE_LABEL.startswith("local."),
                        "服务 label 用 local. 前缀，不挂任何组织域名")
        # systemd unit 名一直没变过，换 label 不该动它
        self.assertEqual(relay.SERVICE_UNIT, "github-local-action.service")

    def test_plist_path_follows_label(self):
        self.assertEqual(relay._service_paths()["file"],
                         self.agents / "local.github-local-action.plist")

    async def test_status_reports_clean_state(self):
        info = await relay._service_status()
        self.assertEqual(info["label"], relay.SERVICE_LABEL)
        self.assertFalse(info["installed"], "临时 HOME 里不该有服务文件")
        self.assertNotIn("legacy_files", info, "旧 label 兼容已经整段删掉了")

    async def test_uninstall_leaves_other_agents_alone(self):
        mine = self.agents / "local.github-local-action.plist"
        other = self.agents / "com.example.other.plist"
        mine.write_text("<plist><dict/></plist>", encoding="utf-8")
        other.write_text("<plist><dict/></plist>", encoding="utf-8")
        r = await relay._service_uninstall()
        self.assertTrue(r["ok"], r)
        self.assertFalse(mine.exists(), "自己的 plist 要删掉")
        self.assertTrue(other.exists(), "别人的 plist 一个都不能碰")


if __name__ == "__main__":
    unittest.main()

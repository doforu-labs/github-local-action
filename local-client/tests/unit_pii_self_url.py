"""脱敏门禁给「仓库自己的地址」开的那条洞，必须窄得刚刚好。

背景：README 想要 CI 徽章，徽章地址必须硬写 `github.com/<组织>/<仓库>/...`，
而门禁（`scripts/check_pii.py`）里「本机专有名词」这条规则要拦的正是组织名。
两边都是对的，所以开了个洞：从 `git remote get-url origin` 现读出仓库自己的
`组织/仓库`，扫之前把它抹成 `<this-repo>`。

洞必须窄——只放行完整的 `组织/仓库`：
- `https://github.com/<组织>/<仓库>/actions/.../badge.svg`  放行（徽章）
- `git clone https://github.com/<组织>/<仓库>.git`           放行（文档示例）
- `com.<组织>.whatever`                                      照拦（品牌进源码）
- `https://github.com/<组织>`                                照拦（只有组织名）

另外这里刻意把词拼起来写（`"do" + "foru"`），否则这条测试自己会被门禁拦住。
"""
import importlib.util
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parent.parent.parent


def _load_check_pii():
    path = ROOT / "scripts" / "check_pii.py"
    spec = importlib.util.spec_from_file_location("_gla_check_pii", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class PiiSelfUrl(unittest.TestCase):
    def setUp(self):
        self.cp = _load_check_pii()
        self.word = "do" + "foru"
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        # 门禁的本机词表是 gitignore 的，CI 上不存在；这里造一份，保证规则一定生效
        words = Path(self.tmp.name) / "words.local"
        words.write_text(self.word + "\n", encoding="utf-8")
        self._orig_words = self.cp.LOCAL_WORDS
        self.cp.LOCAL_WORDS = words
        self.cp._RULES = None
        self.addCleanup(self._restore)

    def _restore(self):
        self.cp.LOCAL_WORDS = self._orig_words
        self.cp._RULES = None

    def test_slug_comes_from_remote(self):
        slug = self.cp.self_slug()
        self.assertRegex(slug, r"^[\w.-]+/[\w.-]+$", "应该从 origin 读出「组织/仓库」")
        self.assertIn("github-local-action", slug)
        self.assertEqual(self.cp.redact_self(f"see {slug} here"), "see <this-repo> here",
                         "自家地址应该被抹成占位符")

    def test_hole_is_narrow(self):
        slug = self.cp.self_slug()
        owner = slug.split("/")[0]
        probe = Path(self.tmp.name) / "probe.md"
        probe.write_text(
            f"![CI](https://github.com/{slug}/actions/workflows/test.yml/badge.svg)\n"   # 1 放行
            f"git clone https://github.com/{slug}.git\n"                                 # 2 放行
            f'SERVICE_LABEL = "com.{self.word}.app"\n'                                   # 3 拦
            f"open https://github.com/{owner}\n",                                        # 4 拦
            encoding="utf-8",
        )
        hits = self.cp.scan(probe)
        self.assertEqual(sorted(h[0] for h in hits), [3, 4],
                         f"只该拦「品牌进源码」和「裸组织名」，实际命中：{hits}")
        self.assertTrue(all(h[1] == "本机专有名词" for h in hits), hits)

    def test_no_remote_means_no_hole(self):
        # 没有 origin（比如别人解压 tarball）：不开洞，宁可严
        with patch.object(self.cp.subprocess, "run",
                          side_effect=subprocess.CalledProcessError(1, "git")):
            self.assertEqual(self.cp.self_slug(), "")
        with patch.object(self.cp, "self_slug", return_value=""):
            text = "https://github.com/org/repo"
            self.assertEqual(self.cp.redact_self(text), text, "读不到 slug 就不该改文本")


if __name__ == "__main__":
    unittest.main()

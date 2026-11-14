"""测试汇总器本身：子进程的中文输出不许把整个 CI 带崩。

真实翻车记录（2026-11-14，仓库第一次在 GitHub Actions 上跑）：Windows runner 上
`node22 · windows-latest` 这个 job 红了，红的原因不在测试，在 `scripts/run_tests.py`。
两个叠加的错：

1. `subprocess.run(..., text=True)` 没写 encoding，Windows 默认用 cp1252 解码，
   测试输出是中文 → 读线程抛 UnicodeDecodeError 死掉；
2. 那个线程一死，`proc.stdout` 就变成 None，接着 `proc.stdout + proc.stderr`
   抛 TypeError，把真实结果整个盖掉，报错信息指向一行毫无关系的代码。

macOS / Linux 默认 UTF-8，所以本地永远复现不出来。这里把两件事都钉住。
"""
import importlib.util
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parent.parent.parent


def _load_runner():
    """导入仓库根的 scripts/run_tests.py（它不是包，只能按路径加载）。"""
    path = ROOT / "scripts" / "run_tests.py"
    spec = importlib.util.spec_from_file_location("_gla_run_tests", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class RunTestsDriver(unittest.TestCase):
    def setUp(self):
        self.rt = _load_runner()

    def test_chinese_output_from_child_is_decoded(self):
        # 子进程绕过 locale，直接往管道里写 UTF-8 字节：不管宿主机是什么编码都稳定
        script = "import sys; sys.stdout.buffer.write('中文崩不了\\n'.encode())"
        ok, last = self.rt._run([sys.executable, "-c", script])
        self.assertTrue(ok, f"子进程正常退出却被判失败：{last}")
        self.assertEqual(last, "中文崩不了")

    def test_child_failure_reports_stderr(self):
        script = "import sys; sys.stderr.write('断言炸了\\n'); sys.exit(3)"
        ok, last = self.rt._run([sys.executable, "-c", script])
        self.assertFalse(ok, "退出码 3 必须判失败")
        self.assertEqual(last, "断言炸了", "失败时要把子进程的 stderr 递给用户")

    def test_none_stdout_does_not_crash(self):
        # 复刻读线程崩掉之后的形态：stdout 是 None
        fake = SimpleNamespace(stdout=None, stderr="读线程死了", returncode=1)
        with patch.object(self.rt.subprocess, "run", return_value=fake):
            ok, last = self.rt._run(["node", "x.js"])
        self.assertFalse(ok)
        self.assertEqual(last, "读线程死了")

    def test_nothing_captured_falls_back(self):
        fake = SimpleNamespace(stdout=None, stderr=None, returncode=0)
        with patch.object(self.rt.subprocess, "run", return_value=fake):
            ok, last = self.rt._run(["node", "x.js"])
        self.assertTrue(ok)
        self.assertEqual(last, "（无输出）")

    def test_child_gets_utf8_io_env(self):
        seen = {}

        def fake_run(cmd, **kwargs):
            seen.update(kwargs)
            return SimpleNamespace(stdout="", stderr="", returncode=0)

        with patch.object(self.rt.subprocess, "run", side_effect=fake_run):
            self.rt._run([sys.executable, "-c", "pass"])
        self.assertEqual(seen.get("encoding"), "utf-8", "必须显式指定解码编码")
        self.assertIs(seen.get("errors"), "replace", "解不开的字节不该让整轮崩掉")
        self.assertEqual(seen.get("env", {}).get("PYTHONIOENCODING"), "utf-8",
                         "子进程也要 UTF-8，否则它在 Windows 上先崩")


if __name__ == "__main__":
    unittest.main()

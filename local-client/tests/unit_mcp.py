"""MCP 薄壳的协议层单测（不联网、不起服务）。

真接口的联调在验的时候手工跑；这里只钉协议细节，防止改坏了客户端连不上：
版本协商、通知不回、未知方法报 -32601、工具不存在/缺参数要 isError 而不是崩。
"""
import io
import json
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import mcp_server


class Protocol(unittest.TestCase):
    def test_initialize_echoes_known_version(self):
        out = mcp_server.handle({
            "jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {"protocolVersion": "2024-11-05", "capabilities": {},
                       "clientInfo": {"name": "t", "version": "0"}},
        })
        self.assertEqual(out["result"]["protocolVersion"], "2024-11-05")
        self.assertIn("tools", out["result"]["capabilities"])
        self.assertEqual(out["result"]["serverInfo"]["name"], "github-local-action")

    def test_initialize_unknown_version_falls_back(self):
        out = mcp_server.handle({
            "jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {"protocolVersion": "1999-01-01"},
        })
        self.assertEqual(out["result"]["protocolVersion"], mcp_server.DEFAULT_PROTOCOL)

    def test_notification_gets_no_reply(self):
        self.assertIsNone(mcp_server.handle(
            {"jsonrpc": "2.0", "method": "notifications/initialized"}))

    def test_unknown_method(self):
        out = mcp_server.handle({"jsonrpc": "2.0", "id": 7, "method": "no/such"})
        self.assertEqual(out["error"]["code"], -32601)

    def test_ping(self):
        out = mcp_server.handle({"jsonrpc": "2.0", "id": 2, "method": "ping"})
        self.assertEqual(out["result"], {})

    def test_tools_list_shape(self):
        out = mcp_server.handle({"jsonrpc": "2.0", "id": 3, "method": "tools/list"})
        tools = out["result"]["tools"]
        names = [t["name"] for t in tools]
        self.assertEqual(len(names), 7)
        self.assertEqual(len(set(names)), 7, "工具名不能重复")
        for tool in tools:
            self.assertTrue(tool["description"], f"{tool['name']} 缺描述")
            self.assertEqual(tool["inputSchema"]["type"], "object")
            for key, prop in tool["inputSchema"].get("properties", {}).items():
                self.assertIn("description", prop, f"{tool['name']}.{key} 没有说明")
            self.assertTrue(tool["annotations"]["readOnlyHint"], "工具都该标只读")

    def test_unknown_tool_is_error_not_crash(self):
        out = mcp_server.handle({"jsonrpc": "2.0", "id": 4, "method": "tools/call",
                                 "params": {"name": "nope", "arguments": {}}})
        self.assertTrue(out["result"]["isError"])
        self.assertIn("health", out["result"]["content"][0]["text"])

    def test_missing_required_arg(self):
        out = mcp_server.handle({"jsonrpc": "2.0", "id": 5, "method": "tools/call",
                                 "params": {"name": "get_run_detail",
                                            "arguments": {"repo": "a/b"}}})
        self.assertTrue(out["result"]["isError"])
        self.assertIn("run", out["result"]["content"][0]["text"])


class AgainstFakeService(unittest.TestCase):
    """把 http_json 换掉，只验工具自己的整形逻辑。"""

    def test_list_runs_trims_fields(self):
        fake = {"total": 1, "repos": [{"repo": "a/b", "runs": 1}], "runs": [
            {"id": 9, "repo": "a/b", "name": "CI", "run_number": 3, "status": "completed",
             "conclusion": "success", "branch": "main", "sha": "abc1234", "trigger": "push",
             "actor": "me", "started_at": 1, "duration_s": 12, "title": "t",
             "url": "https://x", "unused_field": "should be dropped"}]}
        with patch.object(mcp_server, "http_json", return_value=fake):
            data = mcp_server.tool_list_runs({"limit": 5})
        self.assertEqual(data["total"], 1)
        self.assertNotIn("unused_field", data["runs"][0])
        self.assertEqual(data["runs"][0]["sha"], "abc1234")

    def test_recent_events_shallow_by_default(self):
        fake = {"after": 0, "events": [
            {"id": 1, "event": "push", "action": None, "repo": "a/b", "at": 5,
             "payload": {"zen": "x", "hook": {"big": "y"}}}]}
        with patch.object(mcp_server, "http_json", return_value=fake):
            shallow = mcp_server.tool_recent_events({})
            full = mcp_server.tool_recent_events({"full": True})
        self.assertNotIn("payload", shallow["events"][0])
        self.assertEqual(shallow["events"][0]["payload_keys"], ["hook", "zen"])
        self.assertIn("payload", full["events"][0])

    def test_stats_drops_hourly_unless_raw(self):
        def fake(path, params=None):
            return {"today": 1, "total": 2, "runs_today": 1, "runs_total": 1,
                    "hourly": [{"label": "03:00", "count": 1}]}

        with patch.object(mcp_server, "http_json", side_effect=fake):
            self.assertNotIn("hourly", mcp_server.tool_stats({}))
            self.assertIn("hourly", mcp_server.tool_stats({"raw": True}))

    def test_run_detail_skips_log_for_unfinished_job(self):
        jobs = {"cached": False, "jobs": [
            {"id": 11, "name": "build", "status": "in_progress", "conclusion": None,
             "runner_name": "r", "html_url": "u", "steps": [
                 {"number": 1, "name": "Set up job", "status": "completed",
                  "conclusion": "success"}]}]}
        with patch.object(mcp_server, "http_json", return_value=jobs) as fake:
            data = mcp_server.tool_get_run_detail({"repo": "a/b", "run": "1",
                                                   "include_logs": True})
        self.assertIsNone(data["jobs"][0]["log"])
        self.assertIn("没跑完", data["jobs"][0]["log_note"])
        self.assertEqual(fake.call_count, 1, "没跑完的 job 不该去要日志")

    def test_run_detail_tail_lines(self):
        jobs = {"jobs": [{"id": 11, "name": "build", "status": "completed",
                          "conclusion": "success", "steps": []}]}
        log = {"text": "\n".join(f"line{i}" for i in range(100)), "truncated": False}

        def fake(path, params=None):
            return log if path == "/github/joblog" else jobs

        with patch.object(mcp_server, "http_json", side_effect=fake):
            data = mcp_server.tool_get_run_detail({"repo": "a/b", "run": "1",
                                                   "include_logs": True,
                                                   "log_tail_lines": 5})
        kept = data["jobs"][0]["log"].splitlines()
        self.assertEqual(len(kept), 5)
        self.assertEqual(kept[-1], "line99")

    def test_job_log_clips(self):
        with patch.object(mcp_server, "http_json",
                          return_value={"text": "x" * 500, "bytes": 500}):
            data = mcp_server.tool_get_job_log({"repo": "a/b", "job": "1",
                                                "max_chars": 100})
        self.assertEqual(len(data["text"]), 100)
        self.assertTrue(data["truncated"])

    def test_http_error_becomes_tool_error(self):
        import urllib.error

        def boom(*a, **k):
            raise urllib.error.HTTPError("http://x", 403, "Forbidden", {}, io.BytesIO(b'{"error":"nope"}'))

        with patch("urllib.request.urlopen", boom):
            with self.assertRaises(mcp_server.ToolError) as ctx:
                mcp_server.http_json("/runners")
        self.assertIn("403", str(ctx.exception))


class ArgCoercion(unittest.TestCase):
    """模型给的参数什么形状都有。验收时发现四个坑，这里逐个钉死。"""

    def test_repo_without_owner_is_error_not_empty(self):
        # 缺 owner 时上游返回空集，以前会被当成「真没跑过」
        out = mcp_server.call_tool("list_runs", {"repo": "demo-test"})
        self.assertTrue(out["isError"])
        self.assertIn("owner/name", out["content"][0]["text"])

    def test_repo_bad_shapes(self):
        for bad in ("a/b/c", "a b", "a/"):
            with self.assertRaises(mcp_server.ToolError, msg=bad):
                mcp_server._check_repo(bad)
        self.assertEqual(mcp_server._check_repo("example-org/demo-test"),
                         "example-org/demo-test")

    def test_string_false_is_false(self):
        for raw in ("false", "False", "0", "no", "off", "", "null"):
            self.assertFalse(mcp_server._as_bool(raw), raw)
        for raw in ("true", "1", "yes", True, 1):
            self.assertTrue(mcp_server._as_bool(raw), raw)

    def test_stats_raw_string_false_is_ignored(self):
        fake = {"today": 1, "total": 2, "runs_today": 1, "runs_total": 1,
                "hourly": [{"label": "03:00", "count": 1}]}
        with patch.object(mcp_server, "http_json", return_value=fake):
            self.assertNotIn("hourly", mcp_server.tool_stats({"raw": "false"}))

    def test_negative_max_chars_is_clamped(self):
        # 以前 max_chars=-5 会变成 text[:-5]，几乎全文
        with patch.object(mcp_server, "http_json",
                          return_value={"text": "x" * 5000, "bytes": 5000}):
            data = mcp_server.tool_get_job_log({"repo": "a/b", "job": "1",
                                                "max_chars": -5})
        self.assertEqual(len(data["text"]), 100)

    def test_limit_not_a_number_says_so(self):
        with self.assertRaises(mcp_server.ToolError) as ctx:
            mcp_server._as_int({"limit": "abc"}, "limit", 10, 1, 100)
        self.assertIn("整数", str(ctx.exception))
        self.assertNotIn("invalid literal", str(ctx.exception), "别把 ValueError 原文倒给模型")

    def test_limits_are_clamped(self):
        self.assertEqual(mcp_server._as_int({"limit": 999}, "limit", 10, 1, 100), 100)
        self.assertEqual(mcp_server._as_int({}, "limit", 10, 1, 100), 10)
        self.assertEqual(mcp_server._as_int({"log_tail_lines": 0}, "log_tail_lines", 60, 0), 0)

    def test_cursor_after_is_validated_too(self):
        # 复检时发现：after 传错值会被 relay 静默当 0，游标悄悄重置
        out = mcp_server.call_tool("recent_events", {"after": "abc"})
        self.assertTrue(out["isError"])
        self.assertIn("after", out["content"][0]["text"])

    def test_cursor_zero_still_passed(self):
        seen = {}

        def fake(path, params=None):
            seen.update(params or {})
            return {"after": 0, "events": []}

        with patch.object(mcp_server, "http_json", side_effect=fake):
            mcp_server.tool_recent_events({})
        self.assertIn("after", seen, "after=0 也要显式传，不能当成没填")
        self.assertEqual(seen["after"], 0)

    def test_url_credentials_are_masked(self):
        self.assertEqual(mcp_server._safe_url("http://u:p@host:18091/x"),
                         "http://***@host:18091/x")
        self.assertEqual(mcp_server._safe_url("http://127.0.0.1:18091/x"),
                         "http://127.0.0.1:18091/x")


class StdioLoop(unittest.TestCase):
    def test_main_survives_bad_input(self):
        lines = [
            "not json\n",
            json.dumps({"jsonrpc": "2.0", "id": 1, "method": "ping"}) + "\n",
            "\n",
            json.dumps([{"jsonrpc": "2.0", "id": 2, "method": "ping"}]) + "\n",
        ]
        out, err = io.StringIO(), io.StringIO()
        with patch.object(sys, "stdin", io.StringIO("".join(lines))), \
                patch.object(sys, "stdout", out), patch.object(sys, "stderr", err):
            self.assertEqual(mcp_server.main(), 0)
        replies = [json.loads(x) for x in out.getvalue().splitlines() if x.strip()]
        flat = [m for r in replies for m in (r if isinstance(r, list) else [r])]
        self.assertTrue(all(m["jsonrpc"] == "2.0" for m in flat), "stdout 只能是协议消息")
        self.assertEqual(replies[0]["error"]["code"], -32700)  # 坏 JSON
        self.assertEqual(replies[1]["result"], {})             # ping
        self.assertIsInstance(replies[2], list)                # 批量
        self.assertIn("启动", err.getvalue(), "日志要走去 stderr")


if __name__ == "__main__":
    unittest.main(verbosity=0)

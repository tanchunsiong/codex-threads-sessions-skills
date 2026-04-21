from __future__ import annotations

import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

from codex_thread_bridge_lib.core import CodexStore, OpenCodeMessage, render_opencode_message


class RenderOpenCodeMessageTests(unittest.TestCase):
    def test_renders_text_and_tool_summary(self) -> None:
        message = OpenCodeMessage(
            info={"id": "msg_1", "role": "assistant", "time": {"created": 1}},
            parts=[
                {"type": "text", "text": "alpha"},
                {
                    "type": "tool",
                    "tool": "bash",
                    "state": {
                        "input": {"description": "List files", "command": "ls -la"},
                        "output": "done",
                        "status": "completed",
                    },
                },
                {"type": "reasoning", "text": "hidden"},
            ],
        )

        rendered = render_opencode_message(message, include_tools=True, include_reasoning=False)

        self.assertIn("alpha", rendered)
        self.assertIn("[OpenCode tool: bash]", rendered)
        self.assertIn("ls -la", rendered)
        self.assertNotIn("hidden", rendered)

    def test_reasoning_is_optional(self) -> None:
        message = OpenCodeMessage(
            info={"id": "msg_2", "role": "assistant", "time": {"created": 1}},
            parts=[{"type": "reasoning", "text": "thoughts"}],
        )

        self.assertEqual(render_opencode_message(message, include_reasoning=False), "")
        self.assertIn("thoughts", render_opencode_message(message, include_reasoning=True))


class CodexJsonlRewriteTests(unittest.TestCase):
    def test_rewrite_jsonl_without_filters_matching_rows(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            codex_root = temp_path / ".codex"
            codex_root.mkdir()
            target = codex_root / "session_index.jsonl"
            target.write_text(
                "\n".join(
                    [
                        json.dumps({"id": "keep", "thread_name": "a"}),
                        json.dumps({"id": "drop", "thread_name": "b"}),
                        json.dumps({"id": "keep-2", "thread_name": "c"}),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            store = CodexStore(codex_root=codex_root)

            store._rewrite_jsonl_without(target, "id", "drop")

            remaining = target.read_text(encoding="utf-8").splitlines()
            self.assertEqual(len(remaining), 2)
            self.assertEqual([json.loads(line)["id"] for line in remaining], ["keep", "keep-2"])

    def test_search_threads_can_match_session_index_aliases(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            codex_root = temp_path / ".codex"
            codex_root.mkdir()

            state_db = codex_root / "state_5.sqlite"
            conn = sqlite3.connect(state_db)
            conn.execute(
                """
                CREATE TABLE threads (
                    id TEXT PRIMARY KEY,
                    rollout_path TEXT NOT NULL,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL,
                    source TEXT NOT NULL,
                    model_provider TEXT NOT NULL,
                    cwd TEXT NOT NULL,
                    title TEXT NOT NULL,
                    sandbox_policy TEXT NOT NULL,
                    approval_mode TEXT NOT NULL,
                    tokens_used INTEGER NOT NULL DEFAULT 0,
                    has_user_event INTEGER NOT NULL DEFAULT 0,
                    archived INTEGER NOT NULL DEFAULT 0,
                    archived_at INTEGER,
                    git_sha TEXT,
                    git_branch TEXT,
                    git_origin_url TEXT,
                    cli_version TEXT NOT NULL DEFAULT '',
                    first_user_message TEXT NOT NULL DEFAULT '',
                    agent_nickname TEXT,
                    agent_role TEXT,
                    memory_mode TEXT NOT NULL DEFAULT 'enabled',
                    model TEXT,
                    reasoning_effort TEXT,
                    agent_path TEXT,
                    created_at_ms INTEGER,
                    updated_at_ms INTEGER
                )
                """
            )
            conn.execute(
                """
                INSERT INTO threads (
                    id, rollout_path, created_at, updated_at, source, model_provider, cwd, title,
                    sandbox_policy, approval_mode, tokens_used, has_user_event, archived,
                    cli_version, first_user_message, memory_mode, created_at_ms, updated_at_ms
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "thread-1",
                    "/tmp/rollout.jsonl",
                    1,
                    2,
                    "cli",
                    "openai",
                    "/tmp",
                    "Current title",
                    '{"type":"danger-full-access"}',
                    "never",
                    0,
                    0,
                    0,
                    "0.122.0",
                    "",
                    "enabled",
                    1000,
                    2000,
                ),
            )
            conn.commit()
            conn.close()

            (codex_root / "logs_2.sqlite").write_bytes(b"")
            (codex_root / "session_index.jsonl").write_text(
                json.dumps({"id": "thread-1", "thread_name": "TBD: Old title", "updated_at": "2026-01-01T00:00:00Z"})
                + "\n",
                encoding="utf-8",
            )

            store = CodexStore(codex_root=codex_root)
            matches = store.search_threads(title_prefix="TBD", include_session_index=True)

            self.assertEqual(len(matches), 1)
            self.assertEqual(matches[0].thread_id, "thread-1")
            self.assertIn("TBD: Old title", matches[0].matched_titles)


if __name__ == "__main__":
    unittest.main()

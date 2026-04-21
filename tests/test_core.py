from __future__ import annotations

import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

from codex_thread_bridge_lib.core import (
    CodexStore,
    DEFAULT_IMPORT_TITLE_PREFIX,
    BridgeError,
    OpenCodeMessage,
    OpenCodeStore,
    OpenCodeSession,
    render_opencode_message,
)


def _create_codex_fixture(root: Path) -> tuple[Path, Path]:
    codex_root = root / ".codex"
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
    conn.execute("CREATE TABLE thread_dynamic_tools (thread_id TEXT NOT NULL, tool_name TEXT NOT NULL)")
    conn.execute(
        "CREATE TABLE thread_spawn_edges (parent_thread_id TEXT NOT NULL, child_thread_id TEXT NOT NULL)"
    )
    conn.commit()
    conn.close()

    logs_db = codex_root / "logs_2.sqlite"
    conn = sqlite3.connect(logs_db)
    conn.execute("CREATE TABLE logs (id INTEGER PRIMARY KEY AUTOINCREMENT, thread_id TEXT NOT NULL, payload TEXT)")
    conn.commit()
    conn.close()

    return codex_root, state_db


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
    def _create_codex_store(self, root: Path) -> CodexStore:
        codex_root = root / ".codex"
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
        conn.execute("CREATE TABLE thread_dynamic_tools (thread_id TEXT NOT NULL, tool_name TEXT NOT NULL)")
        conn.execute(
            """
            CREATE TABLE thread_spawn_edges (
                parent_thread_id TEXT NOT NULL,
                child_thread_id TEXT NOT NULL
            )
            """
        )
        conn.commit()
        conn.close()

        logs_db = codex_root / "logs_2.sqlite"
        conn = sqlite3.connect(logs_db)
        conn.execute(
            """
            CREATE TABLE logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                thread_id TEXT NOT NULL,
                ts INTEGER,
                level TEXT,
                message TEXT
            )
            """
        )
        conn.commit()
        conn.close()

        return CodexStore(codex_root=codex_root)

    def _rollout_payloads(self, rollout_path: Path) -> list[dict[str, object]]:
        return [json.loads(line) for line in rollout_path.read_text(encoding="utf-8").splitlines() if line.strip()]

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
            codex_root, state_db = _create_codex_fixture(temp_path)
            conn = sqlite3.connect(state_db)
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

    def test_import_uses_opencode_prefix_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            codex_root, _ = _create_codex_fixture(temp_path)
            session = OpenCodeSession(
                info={
                    "id": "ses_test",
                    "title": "Imported Session",
                    "directory": "/tmp",
                    "time": {"created": 1000, "updated": 2000},
                },
                messages=[],
            )

            store = CodexStore(codex_root=codex_root)
            result = store.import_opencode_session(session, dry_run=True)

            self.assertEqual(result.title, f"{DEFAULT_IMPORT_TITLE_PREFIX}Imported Session")

    def test_import_can_override_cwd_and_prefix_title_with_original_directory(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            store = self._create_codex_store(temp_path)
            session = OpenCodeSession(
                info={
                    "id": "ses_override",
                    "title": "Imported Session",
                    "directory": "/var/www/project",
                    "time": {"created": 1000, "updated": 2000},
                },
                messages=[
                    OpenCodeMessage(
                        info={"id": "msg_1", "role": "user", "time": {"created": 1100}},
                        parts=[{"type": "text", "text": "hello"}],
                    )
                ],
            )

            result = store.import_opencode_session(session, cwd_override="/home/dreamtcs", dry_run=False)
            thread = store.resolve_thread(result.thread_id)
            rollout_payloads = self._rollout_payloads(thread.rollout_path)
            session_meta = next(item for item in rollout_payloads if item["type"] == "session_meta")
            turn_context = next(item for item in rollout_payloads if item["type"] == "turn_context")

            self.assertEqual(thread.row["cwd"], "/home/dreamtcs")
            self.assertEqual(result.title, "opencode /var/www/project Imported Session")
            self.assertEqual(session_meta["payload"]["cwd"], "/home/dreamtcs")
            self.assertEqual(session_meta["payload"]["import_meta"]["opencode_directory"], "/var/www/project")
            self.assertEqual(session_meta["payload"]["import_meta"]["codex_cwd_override"], "/home/dreamtcs")
            self.assertEqual(turn_context["payload"]["cwd"], "/home/dreamtcs")

    def test_import_preserves_opencode_timestamps(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            codex_root, state_db = _create_codex_fixture(temp_path)
            session = OpenCodeSession(
                info={
                    "id": "ses_old",
                    "title": "Old Session",
                    "directory": "/tmp",
                    "time": {"created": 1000, "updated": 2000},
                },
                messages=[],
            )

            store = CodexStore(codex_root=codex_root)
            result = store.import_opencode_session(session, dry_run=False)

            conn = sqlite3.connect(state_db)
            row = conn.execute(
                "SELECT created_at_ms, updated_at_ms FROM threads WHERE id = ?",
                (result.thread_id,),
            ).fetchone()
            conn.close()

            self.assertIsNotNone(row)
            self.assertEqual(row[0], 1000)
            self.assertEqual(row[1], 2000)

    def test_import_writes_task_complete_for_assistant_turns(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            codex_root, _ = _create_codex_fixture(temp_path)

            session = OpenCodeSession(
                info={
                    "id": "ses_complete",
                    "title": "Completed Session",
                    "directory": "/tmp/project",
                    "time": {"created": 1000, "updated": 3000},
                },
                messages=[
                    OpenCodeMessage(
                        info={"id": "msg_user", "role": "user", "time": {"created": 1000}},
                        parts=[{"type": "text", "text": "hello"}],
                    ),
                    OpenCodeMessage(
                        info={"id": "msg_assistant", "role": "assistant", "time": {"created": 2000}},
                        parts=[{"type": "text", "text": "world"}],
                    ),
                ],
            )

            store = CodexStore(codex_root=codex_root)
            result = store.import_opencode_session(session, dry_run=False)
            rollout_lines = Path(result.rollout_path).read_text(encoding="utf-8").splitlines()
            task_complete = [
                json.loads(line)
                for line in rollout_lines
                if json.loads(line).get("type") == "event_msg"
                and (json.loads(line).get("payload") or {}).get("type") == "task_complete"
            ]

            self.assertEqual(len(task_complete), 1)
            self.assertEqual(task_complete[0]["payload"]["last_agent_message"], "world")

            assistant_messages = [
                json.loads(line)
                for line in rollout_lines
                if json.loads(line).get("type") == "response_item"
                and (json.loads(line).get("payload") or {}).get("type") == "message"
                and (json.loads(line).get("payload") or {}).get("role") == "assistant"
            ]
            self.assertEqual(len(assistant_messages), 1)
            self.assertEqual(assistant_messages[0]["payload"]["phase"], "final_answer")

    def test_repair_imported_thread_backfills_task_complete(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            codex_root, state_db = _create_codex_fixture(temp_path)
            rollout_path = codex_root / "sessions" / "2026" / "01" / "01" / "rollout-thread-1.jsonl"
            rollout_path.parent.mkdir(parents=True, exist_ok=True)
            rollout_path.write_text(
                "\n".join(
                    [
                        json.dumps(
                            {
                                "timestamp": "2026-01-01T00:00:00.000Z",
                                "type": "session_meta",
                                "payload": {
                                    "id": "thread-1",
                                    "timestamp": "2026-01-01T00:00:00.000Z",
                                    "cwd": "/tmp/project",
                                    "import_meta": {"bridge": "codex-thread-bridge", "source": "opencode"},
                                },
                            }
                        ),
                        json.dumps(
                            {
                                "timestamp": "2026-01-01T00:00:00.001Z",
                                "type": "event_msg",
                                "payload": {"type": "thread_name_updated", "thread_id": "thread-1", "thread_name": "opencode Demo"},
                            }
                        ),
                        json.dumps(
                            {
                                "timestamp": "2026-01-01T00:00:01.000Z",
                                "type": "event_msg",
                                "payload": {"type": "task_started", "turn_id": "turn-1", "started_at": 1},
                            }
                        ),
                        json.dumps(
                            {
                                "timestamp": "2026-01-01T00:00:01.001Z",
                                "type": "turn_context",
                                "payload": {"turn_id": "turn-1", "cwd": "/tmp/project"},
                            }
                        ),
                        json.dumps(
                            {
                                "timestamp": "2026-01-01T00:00:01.002Z",
                                "type": "response_item",
                                "payload": {
                                    "type": "message",
                                    "role": "user",
                                    "content": [{"type": "input_text", "text": "hello"}],
                                },
                            }
                        ),
                        json.dumps(
                            {
                                "timestamp": "2026-01-01T00:00:01.003Z",
                                "type": "event_msg",
                                "payload": {"type": "user_message", "message": "hello"},
                            }
                        ),
                        json.dumps(
                            {
                                "timestamp": "2026-01-01T00:00:02.000Z",
                                "type": "event_msg",
                                "payload": {"type": "agent_message", "message": "world", "phase": "final"},
                            }
                        ),
                        json.dumps(
                            {
                                "timestamp": "2026-01-01T00:00:02.001Z",
                                "type": "response_item",
                                "payload": {
                                    "type": "message",
                                    "role": "assistant",
                                    "content": [{"type": "output_text", "text": "world"}],
                                    "phase": "final",
                                },
                            }
                        ),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            conn = sqlite3.connect(state_db)
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
                    str(rollout_path),
                    1,
                    2,
                    "cli",
                    "openai",
                    "/tmp/project",
                    "opencode Demo",
                    '{"type":"danger-full-access"}',
                    "never",
                    0,
                    1,
                    0,
                    "0.122.0",
                    "hello",
                    "enabled",
                    1000,
                    2000,
                ),
            )
            conn.commit()
            conn.close()

            store = CodexStore(codex_root=codex_root)
            thread = store.resolve_thread("thread-1")
            result = store.repair_imported_thread(thread, backup_root=temp_path / "backups", dry_run=False)

            repaired = rollout_path.read_text(encoding="utf-8")
            self.assertEqual(result.inserted_task_complete_events, 1)
            self.assertEqual(result.normalized_final_answer_phases, 2)
            self.assertIsNotNone(result.backup_dir)
            self.assertIn('"type":"task_complete"', repaired)
            self.assertIn('"phase":"final_answer"', repaired)

            second_pass = store.repair_imported_thread(thread, dry_run=True)
            self.assertEqual(second_pass.inserted_task_complete_events, 0)
            self.assertEqual(second_pass.normalized_final_answer_phases, 0)

    def test_repair_imported_thread_writes_phase_only_fixes(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            codex_root, state_db = _create_codex_fixture(temp_path)
            rollout_path = codex_root / "sessions" / "2026" / "01" / "01" / "rollout-thread-2.jsonl"
            rollout_path.parent.mkdir(parents=True, exist_ok=True)
            rollout_path.write_text(
                "\n".join(
                    [
                        json.dumps(
                            {
                                "timestamp": "2026-01-01T00:00:00.000Z",
                                "type": "session_meta",
                                "payload": {
                                    "id": "thread-2",
                                    "timestamp": "2026-01-01T00:00:00.000Z",
                                    "cwd": "/tmp/project",
                                    "import_meta": {"bridge": "codex-thread-bridge", "source": "opencode"},
                                },
                            }
                        ),
                        json.dumps(
                            {
                                "timestamp": "2026-01-01T00:00:00.001Z",
                                "type": "event_msg",
                                "payload": {"type": "thread_name_updated", "thread_id": "thread-2", "thread_name": "opencode Demo"},
                            }
                        ),
                        json.dumps(
                            {
                                "timestamp": "2026-01-01T00:00:01.000Z",
                                "type": "event_msg",
                                "payload": {"type": "task_started", "turn_id": "turn-2", "started_at": 1},
                            }
                        ),
                        json.dumps(
                            {
                                "timestamp": "2026-01-01T00:00:01.001Z",
                                "type": "turn_context",
                                "payload": {"turn_id": "turn-2", "cwd": "/tmp/project"},
                            }
                        ),
                        json.dumps(
                            {
                                "timestamp": "2026-01-01T00:00:01.002Z",
                                "type": "response_item",
                                "payload": {
                                    "type": "message",
                                    "role": "user",
                                    "content": [{"type": "input_text", "text": "hello"}],
                                },
                            }
                        ),
                        json.dumps(
                            {
                                "timestamp": "2026-01-01T00:00:01.003Z",
                                "type": "event_msg",
                                "payload": {"type": "user_message", "message": "hello"},
                            }
                        ),
                        json.dumps(
                            {
                                "timestamp": "2026-01-01T00:00:02.000Z",
                                "type": "event_msg",
                                "payload": {"type": "agent_message", "message": "world", "phase": "final"},
                            }
                        ),
                        json.dumps(
                            {
                                "timestamp": "2026-01-01T00:00:02.001Z",
                                "type": "response_item",
                                "payload": {
                                    "type": "message",
                                    "role": "assistant",
                                    "content": [{"type": "output_text", "text": "world"}],
                                    "phase": "final",
                                },
                            }
                        ),
                        json.dumps(
                            {
                                "timestamp": "2026-01-01T00:00:02.002Z",
                                "type": "event_msg",
                                "payload": {
                                    "type": "task_complete",
                                    "turn_id": "turn-2",
                                    "last_agent_message": "world",
                                    "completed_at": 2,
                                    "duration_ms": 1002,
                                },
                            }
                        ),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            conn = sqlite3.connect(state_db)
            conn.execute(
                """
                INSERT INTO threads (
                    id, rollout_path, created_at, updated_at, source, model_provider, cwd, title,
                    sandbox_policy, approval_mode, tokens_used, has_user_event, archived,
                    cli_version, first_user_message, memory_mode, created_at_ms, updated_at_ms
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "thread-2",
                    str(rollout_path),
                    1,
                    2,
                    "cli",
                    "openai",
                    "/tmp/project",
                    "opencode Demo",
                    '{"type":"danger-full-access"}',
                    "never",
                    0,
                    1,
                    0,
                    "0.122.0",
                    "hello",
                    "enabled",
                    1000,
                    2000,
                ),
            )
            conn.commit()
            conn.close()

            store = CodexStore(codex_root=codex_root)
            thread = store.resolve_thread("thread-2")
            result = store.repair_imported_thread(thread, dry_run=False)

            repaired = rollout_path.read_text(encoding="utf-8")
            self.assertEqual(result.inserted_task_complete_events, 0)
            self.assertEqual(result.normalized_final_answer_phases, 2)
            self.assertIn('"phase":"final_answer"', repaired)

    def test_retarget_thread_cwd_rewrites_rollout_and_restores_from_backup(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            store = self._create_codex_store(temp_path)
            session = OpenCodeSession(
                info={
                    "id": "ses_retarget",
                    "title": "Imported Session",
                    "directory": "/var/www/project",
                    "time": {"created": 1000, "updated": 2000},
                },
                messages=[
                    OpenCodeMessage(
                        info={"id": "msg_1", "role": "user", "time": {"created": 1100}},
                        parts=[{"type": "text", "text": "hello"}],
                    )
                ],
            )
            imported = store.import_opencode_session(session, dry_run=False)
            thread = store.resolve_thread(imported.thread_id)

            result = store.retarget_thread_cwd(thread, new_cwd="/home/dreamtcs", dry_run=False)

            self.assertEqual(result.old_cwd, "/var/www/project")
            self.assertEqual(result.new_cwd, "/home/dreamtcs")
            self.assertEqual(result.old_title, "opencode Imported Session")
            self.assertEqual(result.new_title, "opencode /var/www/project Imported Session")
            self.assertIsNotNone(result.backup_dir)

            rewritten = store.resolve_thread(imported.thread_id)
            self.assertEqual(rewritten.row["cwd"], "/home/dreamtcs")
            self.assertEqual(rewritten.title, "opencode /var/www/project Imported Session")

            rollout_payloads = self._rollout_payloads(rewritten.rollout_path)
            session_meta = next(item for item in rollout_payloads if item["type"] == "session_meta")
            turn_context = next(item for item in rollout_payloads if item["type"] == "turn_context")
            thread_name_event = next(
                item
                for item in rollout_payloads
                if item["type"] == "event_msg" and item["payload"].get("type") == "thread_name_updated"
            )

            self.assertEqual(session_meta["payload"]["cwd"], "/home/dreamtcs")
            self.assertEqual(session_meta["payload"]["import_meta"]["opencode_directory"], "/var/www/project")
            self.assertEqual(session_meta["payload"]["import_meta"]["codex_cwd_override"], "/home/dreamtcs")
            self.assertEqual(turn_context["payload"]["cwd"], "/home/dreamtcs")
            self.assertEqual(thread_name_event["payload"]["thread_name"], "opencode /var/www/project Imported Session")

            session_index_lines = (temp_path / ".codex" / "session_index.jsonl").read_text(encoding="utf-8").splitlines()
            self.assertEqual(len(session_index_lines), 2)
            self.assertEqual(
                [json.loads(line)["thread_name"] for line in session_index_lines],
                ["opencode Imported Session", "opencode /var/www/project Imported Session"],
            )

            restored = store.restore_backup(result.backup_dir, force=True)
            self.assertEqual(restored.thread_id, imported.thread_id)

            reverted = store.resolve_thread(imported.thread_id)
            self.assertEqual(reverted.row["cwd"], "/var/www/project")
            self.assertEqual(reverted.title, "opencode Imported Session")


class OpenCodeStoreScopeTests(unittest.TestCase):
    def test_list_sessions_defaults_to_root_sessions_only(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            storage_root = Path(temp_dir)
            session_root = storage_root / "session"
            session_root.mkdir(parents=True)

            (session_root / "ses_root.json").write_text(
                json.dumps(
                    {
                        "id": "ses_root",
                        "title": "Root Session",
                        "parentID": None,
                        "time": {"created": 1000, "updated": 2000},
                    }
                ),
                encoding="utf-8",
            )
            (session_root / "ses_child.json").write_text(
                json.dumps(
                    {
                        "id": "ses_child",
                        "title": "Child Session",
                        "parentID": "ses_root",
                        "time": {"created": 1500, "updated": 2500},
                    }
                ),
                encoding="utf-8",
            )

            store = OpenCodeStore(storage_root=storage_root)

            self.assertEqual([session.id for session in store.list_sessions()], ["ses_root"])
            self.assertEqual(
                [session.id for session in store.list_sessions(include_child_sessions=True)],
                ["ses_child", "ses_root"],
            )

    def test_resolve_session_requires_all_sessions_for_child_matches(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            storage_root = Path(temp_dir)
            session_root = storage_root / "session"
            session_root.mkdir(parents=True)

            (session_root / "ses_root.json").write_text(
                json.dumps(
                    {
                        "id": "ses_root",
                        "title": "Root Session",
                        "parentID": None,
                        "time": {"created": 1000, "updated": 2000},
                    }
                ),
                encoding="utf-8",
            )
            (session_root / "ses_child.json").write_text(
                json.dumps(
                    {
                        "id": "ses_child",
                        "title": "Child Session",
                        "parentID": "ses_root",
                        "time": {"created": 1500, "updated": 2500},
                    }
                ),
                encoding="utf-8",
            )

            store = OpenCodeStore(storage_root=storage_root)

            with self.assertRaisesRegex(BridgeError, "--all-sessions"):
                store.resolve_session("ses_child")

            session = store.resolve_session("ses_child", include_child_sessions=True)
            self.assertEqual(session.id, "ses_child")


class OpenCodeDeleteRestoreTests(unittest.TestCase):
    def _create_open_code_fixture(self, root: Path) -> tuple[Path, Path]:
        storage_root = root / "storage"
        session_root = storage_root / "session" / "global"
        message_root = storage_root / "message"
        part_root = storage_root / "part"
        session_root.mkdir(parents=True)
        message_root.mkdir(parents=True)
        part_root.mkdir(parents=True)

        db_path = root / "opencode.db"
        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("CREATE TABLE project (id TEXT PRIMARY KEY)")
        conn.execute(
            """
            CREATE TABLE session (
                id TEXT PRIMARY KEY,
                project_id TEXT NOT NULL,
                parent_id TEXT,
                slug TEXT NOT NULL,
                directory TEXT NOT NULL,
                title TEXT NOT NULL,
                version TEXT NOT NULL,
                share_url TEXT,
                summary_additions INTEGER,
                summary_deletions INTEGER,
                summary_files INTEGER,
                summary_diffs TEXT,
                revert TEXT,
                permission TEXT,
                time_created INTEGER NOT NULL,
                time_updated INTEGER NOT NULL,
                time_compacting INTEGER,
                time_archived INTEGER,
                workspace_id TEXT,
                FOREIGN KEY(project_id) REFERENCES project(id) ON DELETE CASCADE
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE message (
                id TEXT PRIMARY KEY,
                session_id TEXT NOT NULL,
                time_created INTEGER NOT NULL,
                time_updated INTEGER NOT NULL,
                data TEXT NOT NULL,
                FOREIGN KEY(session_id) REFERENCES session(id) ON DELETE CASCADE
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE part (
                id TEXT PRIMARY KEY,
                message_id TEXT NOT NULL,
                session_id TEXT NOT NULL,
                time_created INTEGER NOT NULL,
                time_updated INTEGER NOT NULL,
                data TEXT NOT NULL,
                FOREIGN KEY(message_id) REFERENCES message(id) ON DELETE CASCADE
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE session_entry (
                id TEXT PRIMARY KEY,
                session_id TEXT NOT NULL,
                type TEXT NOT NULL,
                time_created INTEGER NOT NULL,
                time_updated INTEGER NOT NULL,
                data TEXT NOT NULL,
                FOREIGN KEY(session_id) REFERENCES session(id) ON DELETE CASCADE
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE session_share (
                session_id TEXT PRIMARY KEY,
                id TEXT NOT NULL,
                secret TEXT NOT NULL,
                url TEXT NOT NULL,
                time_created INTEGER NOT NULL,
                time_updated INTEGER NOT NULL,
                FOREIGN KEY(session_id) REFERENCES session(id) ON DELETE CASCADE
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE todo (
                session_id TEXT NOT NULL,
                content TEXT NOT NULL,
                status TEXT NOT NULL,
                priority TEXT NOT NULL,
                position INTEGER NOT NULL,
                time_created INTEGER NOT NULL,
                time_updated INTEGER NOT NULL,
                PRIMARY KEY(session_id, position),
                FOREIGN KEY(session_id) REFERENCES session(id) ON DELETE CASCADE
            )
            """
        )
        conn.execute("INSERT INTO project (id) VALUES ('proj')")

        sessions = [
            ("ses_root", None, "Root Session", 1000, 2000, "/tmp/root"),
            ("ses_child", "ses_root", "Child Session", 1100, 2100, "/tmp/child"),
            ("ses_grand", "ses_child", "Grandchild Session", 1200, 2200, "/tmp/grand"),
        ]
        for session_id, parent_id, title, created, updated, directory in sessions:
            conn.execute(
                """
                INSERT INTO session (
                    id, project_id, parent_id, slug, directory, title, version, share_url,
                    summary_additions, summary_deletions, summary_files, summary_diffs,
                    revert, permission, time_created, time_updated, time_compacting,
                    time_archived, workspace_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    session_id,
                    "proj",
                    parent_id,
                    session_id,
                    directory,
                    title,
                    "1.0.0",
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    created,
                    updated,
                    None,
                    None,
                    None,
                ),
            )
            (session_root / f"{session_id}.json").write_text(
                json.dumps(
                    {
                        "id": session_id,
                        "title": title,
                        "parentID": parent_id,
                        "directory": directory,
                        "time": {"created": created, "updated": updated},
                    }
                ),
                encoding="utf-8",
            )

        messages = [
            ("msg_root", "ses_root", "root text", 1300),
            ("msg_child", "ses_child", "child text", 1400),
            ("msg_grand", "ses_grand", "grand text", 1500),
        ]
        for message_id, session_id, text, created in messages:
            conn.execute(
                "INSERT INTO message (id, session_id, time_created, time_updated, data) VALUES (?, ?, ?, ?, ?)",
                (message_id, session_id, created, created, json.dumps({"id": message_id, "role": "user"})),
            )
            message_dir = message_root / session_id
            message_dir.mkdir(parents=True, exist_ok=True)
            (message_dir / f"{message_id}.json").write_text(
                json.dumps({"id": message_id, "sessionID": session_id, "role": "user", "time": {"created": created}}),
                encoding="utf-8",
            )

            part_id = f"prt_{message_id}"
            conn.execute(
                "INSERT INTO part (id, message_id, session_id, time_created, time_updated, data) VALUES (?, ?, ?, ?, ?, ?)",
                (part_id, message_id, session_id, created, created, json.dumps({"id": part_id, "type": "text"})),
            )
            part_dir = part_root / message_id
            part_dir.mkdir(parents=True, exist_ok=True)
            (part_dir / f"{part_id}.json").write_text(
                json.dumps({"id": part_id, "messageID": message_id, "sessionID": session_id, "type": "text"}),
                encoding="utf-8",
            )

        conn.execute(
            "INSERT INTO session_entry (id, session_id, type, time_created, time_updated, data) VALUES (?, ?, ?, ?, ?, ?)",
            ("entry_root", "ses_root", "note", 1600, 1600, json.dumps({"value": "root"})),
        )
        conn.execute(
            "INSERT INTO session_share (session_id, id, secret, url, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?)",
            ("ses_root", "share_root", "secret", "https://example.com/share", 1700, 1700),
        )
        conn.execute(
            "INSERT INTO todo (session_id, content, status, priority, position, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?, ?)",
            ("ses_child", "todo item", "open", "high", 0, 1800, 1800),
        )
        conn.commit()
        conn.close()
        return storage_root, db_path

    def test_delete_and_restore_session_tree(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            storage_root, db_path = self._create_open_code_fixture(temp_path)
            backup_root = temp_path / "backups"
            store = OpenCodeStore(storage_root=storage_root, db_path=db_path)

            root_session = store.resolve_session("ses_root")
            dry_run = store.delete_session(root_session, backup_root=backup_root, dry_run=True)

            self.assertEqual(dry_run.deleted_session_count, 3)
            self.assertEqual(dry_run.deleted_message_count, 3)
            self.assertEqual(dry_run.deleted_part_count, 3)

            conn = sqlite3.connect(db_path)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM session").fetchone()[0], 3)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM message").fetchone()[0], 3)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM part").fetchone()[0], 3)
            conn.close()

            result = store.delete_session(root_session, backup_root=backup_root)

            self.assertIsNotNone(result.backup_dir)
            conn = sqlite3.connect(db_path)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM session").fetchone()[0], 0)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM message").fetchone()[0], 0)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM part").fetchone()[0], 0)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM session_entry").fetchone()[0], 0)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM session_share").fetchone()[0], 0)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM todo").fetchone()[0], 0)
            conn.close()

            self.assertFalse((storage_root / "session" / "global" / "ses_root.json").exists())
            self.assertFalse((storage_root / "message" / "ses_root").exists())
            self.assertFalse((storage_root / "part" / "msg_root").exists())

            restored = store.restore_session_backup(result.backup_dir)

            self.assertEqual(restored.restored_session_count, 3)
            self.assertEqual(restored.restored_message_count, 3)
            self.assertEqual(restored.restored_part_count, 3)

            conn = sqlite3.connect(db_path)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM session").fetchone()[0], 3)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM message").fetchone()[0], 3)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM part").fetchone()[0], 3)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM session_entry").fetchone()[0], 1)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM session_share").fetchone()[0], 1)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM todo").fetchone()[0], 1)
            conn.close()

            self.assertTrue((storage_root / "session" / "global" / "ses_root.json").exists())
            self.assertTrue((storage_root / "message" / "ses_root" / "msg_root.json").exists())
            self.assertTrue((storage_root / "part" / "msg_root" / "prt_msg_root.json").exists())


if __name__ == "__main__":
    unittest.main()

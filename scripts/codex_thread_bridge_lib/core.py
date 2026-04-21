from __future__ import annotations

import copy
import json
import os
import shutil
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


class BridgeError(RuntimeError):
    """Raised when the bridge cannot safely complete a request."""


DEFAULT_IMPORT_TITLE_PREFIX = "opencode "


def _json_load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _json_dump(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")


def _utc_iso_from_ms(timestamp_ms: int) -> str:
    return (
        datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def _updated_at_iso(timestamp_ms: int) -> str:
    return (
        datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z")
    )


def _utc_ms_from_iso(value: str) -> int:
    return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1000)


def _local_rollout_path(codex_root: Path, created_ms: int, thread_id: str) -> Path:
    local_dt = datetime.fromtimestamp(created_ms / 1000).astimezone()
    day_dir = codex_root / "sessions" / local_dt.strftime("%Y") / local_dt.strftime("%m") / local_dt.strftime("%d")
    filename = f"rollout-{local_dt.strftime('%Y-%m-%dT%H-%M-%S')}-{thread_id}.jsonl"
    return day_dir / filename


def _compact_json(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=True, separators=(",", ":"))


def _default_import_title(title_prefix: str, session_title: str, *, original_cwd: str | None = None) -> str:
    if original_cwd:
        return f"{title_prefix}{original_cwd} {session_title}".strip()
    return f"{title_prefix}{session_title}".strip()


def _truncate(text: str, max_chars: int) -> str:
    if max_chars <= 0 or len(text) <= max_chars:
        return text
    return text[: max_chars - 15].rstrip() + "\n...[truncated]"


def _message_timestamp(payload: dict[str, Any]) -> int:
    time_info = payload.get("time") or {}
    return (
        time_info.get("created")
        or time_info.get("start")
        or time_info.get("completed")
        or time_info.get("updated")
        or 0
    )


def _part_sort_key(payload: dict[str, Any], fallback_name: str) -> tuple[int, str]:
    time_info = payload.get("time") or {}
    return (
        time_info.get("start")
        or time_info.get("created")
        or time_info.get("end")
        or 0,
        fallback_name,
    )


def _format_tool_part(part: dict[str, Any], tool_output_max_chars: int) -> str:
    tool_name = part.get("tool") or "tool"
    state = part.get("state") or {}
    lines = [f"[OpenCode tool: {tool_name}]"]

    title = state.get("title")
    if title:
        lines.append(f"Title: {title}")

    input_payload = state.get("input")
    if isinstance(input_payload, dict):
        if "description" in input_payload:
            lines.append(f"Description: {input_payload['description']}")
        if "command" in input_payload:
            lines.append("Command:")
            lines.append(str(input_payload["command"]))
        else:
            remaining = {k: v for k, v in input_payload.items() if k not in {"description"}}
            if remaining:
                lines.append("Input:")
                lines.append(_truncate(json.dumps(remaining, indent=2, ensure_ascii=True), tool_output_max_chars))
    elif input_payload is not None:
        lines.append("Input:")
        lines.append(_truncate(str(input_payload), tool_output_max_chars))

    output_text = state.get("output")
    if output_text is None:
        metadata = state.get("metadata") or {}
        output_text = metadata.get("output")
    if output_text:
        lines.append("Output:")
        lines.append(_truncate(str(output_text), tool_output_max_chars))

    status = state.get("status")
    if status:
        lines.append(f"Status: {status}")

    return "\n".join(lines).strip()


def render_opencode_message(
    message: "OpenCodeMessage",
    *,
    include_tools: bool = True,
    include_reasoning: bool = False,
    tool_output_max_chars: int = 1200,
) -> str:
    blocks: list[str] = []
    for part in message.parts:
        part_type = part.get("type")
        if part_type == "text":
            text = str(part.get("text") or "").strip()
            if text:
                blocks.append(text)
        elif part_type == "tool" and include_tools:
            blocks.append(_format_tool_part(part, tool_output_max_chars))
        elif part_type == "reasoning" and include_reasoning:
            text = str(part.get("text") or "").strip()
            if text:
                blocks.append("[OpenCode reasoning]\n" + text)
        elif part_type == "file":
            filename = part.get("filename") or "attachment"
            mime = part.get("mime") or "application/octet-stream"
            blocks.append(f"[OpenCode file] {filename} ({mime})")
        elif part_type == "agent":
            name = part.get("name") or "agent"
            blocks.append(f"[OpenCode agent] {name}")
    return "\n\n".join(block for block in blocks if block).strip()


@dataclass(slots=True)
class OpenCodeMessage:
    info: dict[str, Any]
    parts: list[dict[str, Any]]

    @property
    def id(self) -> str:
        return str(self.info["id"])

    @property
    def role(self) -> str:
        return str(self.info.get("role") or "assistant")

    @property
    def created_ms(self) -> int:
        return _message_timestamp(self.info)


@dataclass(slots=True)
class OpenCodeSession:
    info: dict[str, Any]
    messages: list[OpenCodeMessage]

    @property
    def id(self) -> str:
        return str(self.info["id"])

    @property
    def title(self) -> str:
        return str(self.info.get("title") or self.info.get("slug") or self.id)

    @property
    def parent_id(self) -> str | None:
        parent_id = self.info.get("parentID")
        if parent_id is None:
            parent_id = self.info.get("parent_id")
        if parent_id in {None, ""}:
            return None
        return str(parent_id)

    @property
    def is_root(self) -> bool:
        return self.parent_id is None

    @property
    def directory(self) -> str:
        return str(self.info.get("directory") or Path.home())

    @property
    def created_ms(self) -> int:
        return int((self.info.get("time") or {}).get("created") or 0)

    @property
    def updated_ms(self) -> int:
        time_info = self.info.get("time") or {}
        return int(time_info.get("updated") or time_info.get("created") or 0)


@dataclass(slots=True)
class CodexThread:
    row: dict[str, Any]

    @property
    def id(self) -> str:
        return str(self.row["id"])

    @property
    def title(self) -> str:
        return str(self.row.get("title") or self.id)

    @property
    def rollout_path(self) -> Path:
        return Path(str(self.row["rollout_path"]))

    @property
    def updated_ms(self) -> int:
        updated_ms = self.row.get("updated_at_ms")
        if updated_ms is not None:
            return int(updated_ms)
        return int(self.row.get("updated_at") or 0) * 1000


@dataclass(slots=True)
class ImportResult:
    thread_id: str
    title: str
    rollout_path: Path
    user_messages: int
    assistant_messages: int
    history_entries: int
    dry_run: bool


@dataclass(slots=True)
class DeleteResult:
    thread_id: str
    title: str
    backup_dir: Path | None
    rollout_deleted: bool
    shell_snapshots_deleted: int
    history_entries_removed: int
    session_index_entries_removed: int
    dry_run: bool


@dataclass(slots=True)
class RestoreResult:
    thread_id: str
    title: str
    restored_rollout: bool
    restored_shell_snapshots: int


@dataclass(slots=True)
class CodexSearchMatch:
    thread_id: str
    live_title: str
    matched_titles: list[str]
    cwd: str
    updated_ms: int


@dataclass(slots=True)
class CodexRetargetResult:
    thread_id: str
    old_title: str
    new_title: str
    old_cwd: str
    new_cwd: str
    backup_dir: Path | None
    dry_run: bool


@dataclass(slots=True)
class CodexRepairResult:
    thread_id: str
    title: str
    backup_dir: Path | None
    inserted_task_complete_events: int
    dry_run: bool


@dataclass(slots=True)
class OpenCodeSearchMatch:
    session_id: str
    title: str
    matched_titles: list[str]
    directory: str
    parent_id: str | None
    updated_ms: int


@dataclass(slots=True)
class OpenCodeDeleteResult:
    session_id: str
    title: str
    backup_dir: Path | None
    deleted_session_count: int
    deleted_message_count: int
    deleted_part_count: int
    dry_run: bool


@dataclass(slots=True)
class OpenCodeRestoreResult:
    session_id: str
    title: str
    restored_session_count: int
    restored_message_count: int
    restored_part_count: int


def _string_matches(
    value: str,
    *,
    title_prefix: str | None = None,
    title_contains: str | None = None,
    ignore_case: bool = True,
) -> bool:
    if ignore_case:
        candidate = value.lower()
        prefix = title_prefix.lower() if title_prefix is not None else None
        contains = title_contains.lower() if title_contains is not None else None
    else:
        candidate = value
        prefix = title_prefix
        contains = title_contains

    if prefix is not None and not candidate.startswith(prefix):
        return False
    if contains is not None and contains not in candidate:
        return False
    return prefix is not None or contains is not None


def _retargeted_opencode_title(current_title: str, original_cwd: str) -> str:
    if not original_cwd:
        return current_title
    base_title = current_title
    if base_title.startswith(DEFAULT_IMPORT_TITLE_PREFIX):
        base_title = base_title[len(DEFAULT_IMPORT_TITLE_PREFIX) :]
    cwd_prefix = f"{original_cwd} "
    if base_title.startswith(cwd_prefix):
        base_title = base_title[len(cwd_prefix) :]
    return f"{DEFAULT_IMPORT_TITLE_PREFIX}{original_cwd} {base_title}".strip()


class OpenCodeStore:
    def __init__(self, storage_root: Path | None = None, db_path: Path | None = None) -> None:
        default_data_root = Path.home() / ".local" / "share" / "opencode"
        self.storage_root = storage_root or default_data_root / "storage"
        self.data_root = self.storage_root.parent
        self.db_path = db_path or self.data_root / "opencode.db"
        self.session_root = self.storage_root / "session"
        self.message_root = self.storage_root / "message"
        self.part_root = self.storage_root / "part"

    def _db_conn(self) -> sqlite3.Connection:
        if not self.db_path.exists():
            raise BridgeError(f"OpenCode database was not found at '{self.db_path}'.")
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def _storage_session_files(self, session_id: str) -> list[Path]:
        return sorted(self.session_root.rglob(f"{session_id}.json"))

    def _session_stub(self, session_id: str, *, title: str, directory: str, parent_id: str | None) -> OpenCodeSession:
        return OpenCodeSession(
            info={
                "id": session_id,
                "title": title,
                "directory": directory,
                "parentID": parent_id,
                "time": {"created": 0, "updated": 0},
            },
            messages=[],
        )

    def search_sessions(
        self,
        *,
        title_prefix: str | None = None,
        title_contains: str | None = None,
        include_child_sessions: bool = False,
        ignore_case: bool = True,
    ) -> list[OpenCodeSearchMatch]:
        if title_prefix is None and title_contains is None:
            raise BridgeError("At least one search filter is required.")

        matches: list[OpenCodeSearchMatch] = []
        for session in self.list_sessions(include_child_sessions=include_child_sessions):
            if not _string_matches(
                session.title,
                title_prefix=title_prefix,
                title_contains=title_contains,
                ignore_case=ignore_case,
            ):
                continue
            matches.append(
                OpenCodeSearchMatch(
                    session_id=session.id,
                    title=session.title,
                    matched_titles=[session.title],
                    directory=session.directory,
                    parent_id=session.parent_id,
                    updated_ms=session.updated_ms,
                )
            )
        return matches

    def list_sessions(self, *, include_child_sessions: bool = False) -> list[OpenCodeSession]:
        sessions: dict[str, OpenCodeSession] = {}
        if not self.session_root.exists():
            return []
        for path in sorted(self.session_root.rglob("ses_*.json")):
            info = _json_load(path)
            session = OpenCodeSession(info=info, messages=[])
            if not include_child_sessions and not session.is_root:
                continue
            sessions[session.id] = session
        ordered = sorted(sessions.values(), key=lambda item: item.updated_ms, reverse=True)
        return ordered

    def resolve_session(
        self,
        ref: str,
        *,
        contains: bool = False,
        include_child_sessions: bool = False,
    ) -> OpenCodeSession:
        sessions = self.list_sessions(include_child_sessions=include_child_sessions)
        exact_id = [session for session in sessions if session.id == ref]
        if exact_id:
            return self.load_session(exact_id[0].id)

        exact_title = [session for session in sessions if session.title == ref]
        if len(exact_title) == 1:
            return self.load_session(exact_title[0].id)
        if len(exact_title) > 1:
            raise BridgeError(f"OpenCode reference '{ref}' matched multiple session titles.")

        if contains:
            matches = [session for session in sessions if ref.lower() in session.title.lower()]
            if len(matches) == 1:
                return self.load_session(matches[0].id)
            if len(matches) > 1:
                raise BridgeError(f"OpenCode reference '{ref}' matched multiple session titles.")

        if not include_child_sessions:
            child_sessions = self.list_sessions(include_child_sessions=True)
            child_by_id = [session for session in child_sessions if session.id == ref and not session.is_root]
            if child_by_id:
                raise BridgeError(
                    f"OpenCode session '{ref}' is a child/subagent session. Re-run with --all-sessions "
                    "to include child sessions."
                )

            child_by_title = [session for session in child_sessions if session.title == ref and not session.is_root]
            if len(child_by_title) == 1:
                raise BridgeError(
                    f"OpenCode session '{ref}' is a child/subagent session. Re-run with --all-sessions "
                    "to include child sessions."
                )
            if len(child_by_title) > 1:
                raise BridgeError(
                    f"OpenCode reference '{ref}' matched multiple child/subagent sessions. Re-run with "
                    "--all-sessions to include child sessions."
                )

            if contains:
                child_contains = [
                    session
                    for session in child_sessions
                    if not session.is_root and ref.lower() in session.title.lower()
                ]
                if len(child_contains) == 1:
                    raise BridgeError(
                        f"OpenCode session '{ref}' matched only a child/subagent session. Re-run with "
                        "--all-sessions to include child sessions."
                    )
                if len(child_contains) > 1:
                    raise BridgeError(
                        f"OpenCode reference '{ref}' matched multiple child/subagent sessions. Re-run with "
                        "--all-sessions to include child sessions."
                    )

        raise BridgeError(f"OpenCode session '{ref}' was not found.")

    def load_session(self, session_id: str) -> OpenCodeSession:
        session_path: Path | None = None
        for candidate in sorted(self.session_root.rglob(f"{session_id}.json")):
            session_path = candidate
            break
        if session_path is None:
            raise BridgeError(f"OpenCode session '{session_id}' was not found.")

        info = _json_load(session_path)
        messages: list[OpenCodeMessage] = []
        message_dir = self.message_root / session_id
        if message_dir.exists():
            for path in sorted(message_dir.glob("msg_*.json")):
                message_info = _json_load(path)
                part_dir = self.part_root / path.stem
                parts: list[dict[str, Any]] = []
                if part_dir.exists():
                    raw_parts = []
                    for part_path in sorted(part_dir.glob("*.json")):
                        payload = _json_load(part_path)
                        raw_parts.append((part_path.name, payload))
                    raw_parts.sort(key=lambda item: _part_sort_key(item[1], item[0]))
                    parts = [payload for _, payload in raw_parts]
                messages.append(OpenCodeMessage(info=message_info, parts=parts))

        messages.sort(key=lambda item: (item.created_ms, item.id))
        return OpenCodeSession(info=info, messages=messages)

    def _subtree_rows(self, session_id: str) -> list[dict[str, Any]]:
        with self._db_conn() as conn:
            rows = conn.execute(
                """
                WITH RECURSIVE tree(id, depth) AS (
                    SELECT id, 0 FROM session WHERE id = ?
                    UNION ALL
                    SELECT session.id, tree.depth + 1
                    FROM session
                    JOIN tree ON session.parent_id = tree.id
                )
                SELECT session.*, tree.depth
                FROM tree
                JOIN session ON session.id = tree.id
                ORDER BY tree.depth ASC, session.time_updated DESC, session.id ASC
                """,
                (session_id,),
            ).fetchall()
        if not rows:
            raise BridgeError(f"OpenCode session '{session_id}' was not found in the database.")
        return [dict(row) for row in rows]

    def _select_rows_for_session_ids(
        self,
        table: str,
        session_ids: list[str],
        *,
        order_by: str | None = None,
    ) -> list[dict[str, Any]]:
        if not session_ids:
            return []
        placeholders = ", ".join(["?"] * len(session_ids))
        query = f"SELECT * FROM {table} WHERE session_id IN ({placeholders})"
        if order_by:
            query += f" ORDER BY {order_by}"
        with self._db_conn() as conn:
            rows = conn.execute(query, session_ids).fetchall()
        return [dict(row) for row in rows]

    def delete_session(
        self,
        session: OpenCodeSession,
        *,
        backup_root: Path | None = None,
        create_backup: bool = True,
        dry_run: bool = False,
    ) -> OpenCodeDeleteResult:
        session_rows = self._subtree_rows(session.id)
        session_ids = [str(row["id"]) for row in session_rows]
        message_rows = self._select_rows_for_session_ids("message", session_ids, order_by="time_created ASC, id ASC")
        part_rows = self._select_rows_for_session_ids("part", session_ids, order_by="time_created ASC, id ASC")
        session_entry_rows = self._select_rows_for_session_ids(
            "session_entry",
            session_ids,
            order_by="time_created ASC, id ASC",
        )
        session_share_rows = self._select_rows_for_session_ids(
            "session_share",
            session_ids,
            order_by="time_created ASC, session_id ASC",
        )
        todo_rows = self._select_rows_for_session_ids(
            "todo",
            session_ids,
            order_by="position ASC, session_id ASC",
        )

        session_files: list[Path] = []
        for session_id in session_ids:
            session_files.extend(self._storage_session_files(session_id))

        message_dirs = [
            self.message_root / session_id
            for session_id in session_ids
            if (self.message_root / session_id).exists()
        ]
        part_dirs = [
            self.part_root / str(row["id"])
            for row in message_rows
            if (self.part_root / str(row["id"])).exists()
        ]

        backup_dir: Path | None = None
        if create_backup:
            backup_root = backup_root or (self.data_root / "thread-bridge-backups")
            timestamp = datetime.now().astimezone().strftime("%Y%m%dT%H%M%S")
            backup_dir = backup_root / f"{timestamp}-{session.id}"
            backup_dir.mkdir(parents=True, exist_ok=True)

            manifest = {
                "root_session_id": session.id,
                "title": session.title,
                "captured_at": datetime.now(tz=timezone.utc).isoformat(),
                "session_ids": session_ids,
                "session_files": [str(path.relative_to(self.storage_root)) for path in session_files],
                "message_dirs": [str(path.relative_to(self.storage_root)) for path in message_dirs],
                "part_dirs": [str(path.relative_to(self.storage_root)) for path in part_dirs],
            }
            _json_dump(backup_dir / "manifest.json", manifest)
            _json_dump(
                backup_dir / "state.json",
                {
                    "session": [{k: v for k, v in row.items() if k != "depth"} for row in session_rows],
                    "message": message_rows,
                    "part": part_rows,
                    "session_entry": session_entry_rows,
                    "session_share": session_share_rows,
                    "todo": todo_rows,
                },
            )

            storage_backup_root = backup_dir / "files" / "storage"
            for path in session_files:
                target = storage_backup_root / path.relative_to(self.storage_root)
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(path, target)
            for path in message_dirs + part_dirs:
                target = storage_backup_root / path.relative_to(self.storage_root)
                shutil.copytree(path, target, dirs_exist_ok=True)

        if not dry_run:
            with self._db_conn() as conn:
                for row in reversed(session_rows):
                    conn.execute("DELETE FROM session WHERE id = ?", (row["id"],))
                conn.commit()

            for path in part_dirs:
                if path.exists():
                    shutil.rmtree(path)
            for path in message_dirs:
                if path.exists():
                    shutil.rmtree(path)
            for path in session_files:
                if path.exists():
                    path.unlink()

        return OpenCodeDeleteResult(
            session_id=session.id,
            title=session.title,
            backup_dir=backup_dir,
            deleted_session_count=len(session_rows),
            deleted_message_count=len(message_rows),
            deleted_part_count=len(part_rows),
            dry_run=dry_run,
        )

    def _insert_rows(self, conn: sqlite3.Connection, table: str, rows: list[dict[str, Any]]) -> None:
        for row in rows:
            columns = ", ".join(row.keys())
            placeholders = ", ".join(["?"] * len(row))
            conn.execute(
                f"INSERT OR REPLACE INTO {table} ({columns}) VALUES ({placeholders})",
                list(row.values()),
            )

    def restore_session_backup(self, backup_dir: Path, *, force: bool = False) -> OpenCodeRestoreResult:
        manifest = _json_load(backup_dir / "manifest.json")
        state_payload = _json_load(backup_dir / "state.json")
        session_rows = list(state_payload.get("session", []))
        if not session_rows:
            raise BridgeError(f"Backup at '{backup_dir}' does not contain any OpenCode sessions.")

        session_ids = [str(row["id"]) for row in session_rows]
        root_session_id = str(manifest["root_session_id"])
        title = str(manifest.get("title") or root_session_id)

        with self._db_conn() as conn:
            existing_rows = conn.execute(
                f"SELECT id FROM session WHERE id IN ({', '.join(['?'] * len(session_ids))})",
                session_ids,
            ).fetchall()
        existing_ids = {str(row["id"]) for row in existing_rows}
        if existing_ids and not force:
            joined = ", ".join(sorted(existing_ids))
            raise BridgeError(
                f"OpenCode session(s) already exist: {joined}. Use --force to replace the saved subtree."
            )

        if force and root_session_id in existing_ids:
            root_row = next(row for row in session_rows if row["id"] == root_session_id)
            current = self._session_stub(
                root_session_id,
                title=str(root_row.get("title") or title),
                directory=str(root_row.get("directory") or Path.home()),
                parent_id=root_row.get("parent_id"),
            )
            self.delete_session(current, create_backup=False, dry_run=False)
            existing_ids.discard(root_session_id)

        if force and existing_ids:
            overlap_rows = [row for row in session_rows if row["id"] in existing_ids]
            overlap_message_rows = self._select_rows_for_session_ids(
                "message",
                list(existing_ids),
                order_by="time_created ASC, id ASC",
            )
            with self._db_conn() as conn:
                for row in reversed(overlap_rows):
                    conn.execute("DELETE FROM session WHERE id = ?", (row["id"],))
                conn.commit()

            for path in [
                self.part_root / str(row["id"])
                for row in overlap_message_rows
                if (self.part_root / str(row["id"])).exists()
            ]:
                shutil.rmtree(path)
            for session_id in existing_ids:
                for path in self._storage_session_files(session_id):
                    if path.exists():
                        path.unlink()
                message_dir = self.message_root / session_id
                if message_dir.exists():
                    shutil.rmtree(message_dir)

        with self._db_conn() as conn:
            self._insert_rows(conn, "session", session_rows)
            self._insert_rows(conn, "message", list(state_payload.get("message", [])))
            self._insert_rows(conn, "part", list(state_payload.get("part", [])))
            self._insert_rows(conn, "session_entry", list(state_payload.get("session_entry", [])))
            self._insert_rows(conn, "session_share", list(state_payload.get("session_share", [])))
            self._insert_rows(conn, "todo", list(state_payload.get("todo", [])))
            conn.commit()

        storage_backup_root = backup_dir / "files" / "storage"
        if storage_backup_root.exists():
            for path in sorted(storage_backup_root.rglob("*")):
                target = self.storage_root / path.relative_to(storage_backup_root)
                if path.is_dir():
                    target.mkdir(parents=True, exist_ok=True)
                    continue
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(path, target)

        return OpenCodeRestoreResult(
            session_id=root_session_id,
            title=title,
            restored_session_count=len(session_rows),
            restored_message_count=len(state_payload.get("message", [])),
            restored_part_count=len(state_payload.get("part", [])),
        )


class CodexStore:
    def __init__(self, codex_root: Path | None = None) -> None:
        self.codex_root = codex_root or Path.home() / ".codex"
        self.state_db = self.codex_root / "state_5.sqlite"
        self.logs_db = self.codex_root / "logs_2.sqlite"
        self.session_index_path = self.codex_root / "session_index.jsonl"
        self.history_path = self.codex_root / "history.jsonl"
        self.shell_snapshots_root = self.codex_root / "shell_snapshots"

    def _state_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.state_db)
        conn.row_factory = sqlite3.Row
        return conn

    def _logs_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.logs_db)
        conn.row_factory = sqlite3.Row
        return conn

    def list_threads(self) -> list[CodexThread]:
        with self._state_conn() as conn:
            rows = conn.execute(
                "SELECT * FROM threads ORDER BY COALESCE(updated_at_ms, updated_at * 1000) DESC"
            ).fetchall()
        return [CodexThread(dict(row)) for row in rows]

    def session_index_entries(self) -> list[dict[str, Any]]:
        if not self.session_index_path.exists():
            return []
        rows: list[dict[str, Any]] = []
        with self.session_index_path.open(encoding="utf-8") as handle:
            for raw_line in handle:
                line = raw_line.strip()
                if not line:
                    continue
                rows.append(json.loads(line))
        return rows

    def search_threads(
        self,
        *,
        title_prefix: str | None = None,
        title_contains: str | None = None,
        include_session_index: bool = False,
        ignore_case: bool = True,
    ) -> list[CodexSearchMatch]:
        if title_prefix is None and title_contains is None:
            raise BridgeError("At least one search filter is required.")

        threads = self.list_threads()
        thread_by_id = {thread.id: thread for thread in threads}
        matches_by_id: dict[str, list[str]] = {}

        for thread in threads:
            if _string_matches(
                thread.title,
                title_prefix=title_prefix,
                title_contains=title_contains,
                ignore_case=ignore_case,
            ):
                matches_by_id.setdefault(thread.id, []).append(thread.title)

        if include_session_index:
            for entry in self.session_index_entries():
                thread_id = str(entry.get("id") or "")
                if thread_id not in thread_by_id:
                    continue
                thread_name = str(entry.get("thread_name") or "")
                if _string_matches(
                    thread_name,
                    title_prefix=title_prefix,
                    title_contains=title_contains,
                    ignore_case=ignore_case,
                ):
                    matches_by_id.setdefault(thread_id, []).append(thread_name)

        results: list[CodexSearchMatch] = []
        for thread_id, matched_titles in matches_by_id.items():
            thread = thread_by_id[thread_id]
            unique_titles = list(dict.fromkeys(matched_titles))
            results.append(
                CodexSearchMatch(
                    thread_id=thread.id,
                    live_title=thread.title,
                    matched_titles=unique_titles,
                    cwd=str(thread.row.get("cwd") or ""),
                    updated_ms=thread.updated_ms,
                )
            )

        results.sort(key=lambda item: item.updated_ms, reverse=True)
        return results

    def resolve_thread(self, ref: str, *, contains: bool = False) -> CodexThread:
        threads = self.list_threads()
        exact_id = [thread for thread in threads if thread.id == ref]
        if exact_id:
            return exact_id[0]

        exact_title = [thread for thread in threads if thread.title == ref]
        if len(exact_title) == 1:
            return exact_title[0]
        if len(exact_title) > 1:
            raise BridgeError(f"Codex reference '{ref}' matched multiple thread titles.")

        if contains:
            matches = [thread for thread in threads if ref.lower() in thread.title.lower()]
            if len(matches) == 1:
                return matches[0]
            if len(matches) > 1:
                raise BridgeError(f"Codex reference '{ref}' matched multiple thread titles.")

        raise BridgeError(f"Codex thread '{ref}' was not found.")

    def imported_threads(self, *, source: str | None = None) -> list[CodexThread]:
        matches: list[CodexThread] = []
        for thread in self.list_threads():
            import_meta = self._thread_import_meta(thread)
            if not import_meta:
                continue
            if source is not None and str(import_meta.get("source") or "") != source:
                continue
            matches.append(thread)
        return matches

    def _latest_thread_row(self) -> dict[str, Any] | None:
        threads = self.list_threads()
        return threads[0].row if threads else None

    def _latest_rollout_templates(self) -> dict[str, dict[str, Any]]:
        latest = self._latest_thread_row()
        templates: dict[str, dict[str, Any]] = {}
        if latest:
            rollout_path = Path(str(latest["rollout_path"]))
            if rollout_path.exists():
                with rollout_path.open(encoding="utf-8") as handle:
                    for line in handle:
                        payload = json.loads(line)
                        line_type = payload.get("type")
                        if line_type in {"session_meta", "turn_context", "event_msg"}:
                            if line_type == "event_msg":
                                inner_type = (payload.get("payload") or {}).get("type")
                                if inner_type == "task_started" and "task_started" not in templates:
                                    templates["task_started"] = copy.deepcopy(payload["payload"])
                            elif line_type not in templates:
                                templates[line_type] = copy.deepcopy(payload["payload"])
                        if {"session_meta", "turn_context", "task_started"} <= set(templates):
                            break

        templates.setdefault(
            "session_meta",
            {
                "id": "",
                "timestamp": "",
                "cwd": str(Path.home()),
                "originator": "codex-tui",
                "cli_version": "",
                "source": "cli",
                "model_provider": "openai",
                "base_instructions": {"text": ""},
            },
        )
        templates.setdefault(
            "turn_context",
            {
                "turn_id": "",
                "cwd": str(Path.home()),
                "current_date": datetime.now().date().isoformat(),
                "timezone": datetime.now().astimezone().tzname() or "UTC",
                "approval_policy": "never",
                "sandbox_policy": {"type": "danger-full-access"},
                "model": "gpt-5.4",
                "personality": "pragmatic",
                "collaboration_mode": {
                    "mode": "default",
                    "settings": {"model": "gpt-5.4", "reasoning_effort": "medium", "developer_instructions": ""},
                },
                "realtime_active": False,
                "effort": "medium",
                "summary": "none",
                "truncation_policy": {"mode": "tokens", "limit": 10000},
            },
        )
        templates.setdefault(
            "task_started",
            {
                "type": "task_started",
                "turn_id": "",
                "started_at": 0,
                "model_context_window": 258400,
                "collaboration_mode_kind": "default",
            },
        )
        return templates

    def _thread_defaults(self) -> dict[str, Any]:
        latest = self._latest_thread_row()
        if latest is None:
            return {
                "source": "cli",
                "model_provider": "openai",
                "sandbox_policy": _compact_json({"type": "danger-full-access"}),
                "approval_mode": "never",
                "cli_version": "",
                "memory_mode": "enabled",
                "model": "gpt-5.4",
                "reasoning_effort": "medium",
            }
        return {
            "source": latest.get("source") or "cli",
            "model_provider": latest.get("model_provider") or "openai",
            "sandbox_policy": latest.get("sandbox_policy") or _compact_json({"type": "danger-full-access"}),
            "approval_mode": latest.get("approval_mode") or "never",
            "cli_version": latest.get("cli_version") or "",
            "memory_mode": latest.get("memory_mode") or "enabled",
            "model": latest.get("model"),
            "reasoning_effort": latest.get("reasoning_effort"),
        }

    def import_opencode_session(
        self,
        session: OpenCodeSession,
        *,
        title_prefix: str = DEFAULT_IMPORT_TITLE_PREFIX,
        title_override: str | None = None,
        cwd_override: str | None = None,
        include_tools: bool = True,
        include_reasoning: bool = False,
        tool_output_max_chars: int = 1200,
        dry_run: bool = False,
    ) -> ImportResult:
        thread_id = str(uuid.uuid4())
        target_cwd = cwd_override or session.directory
        if title_override is not None:
            title = title_override
        else:
            original_cwd = session.directory if target_cwd != session.directory else None
            title = _default_import_title(title_prefix, session.title, original_cwd=original_cwd)
        created_ms = session.created_ms or int(datetime.now(tz=timezone.utc).timestamp() * 1000)
        updated_ms = session.updated_ms or created_ms
        rollout_path = _local_rollout_path(self.codex_root, created_ms, thread_id)

        rendered_messages: list[tuple[OpenCodeMessage, str]] = []
        for message in session.messages:
            text = render_opencode_message(
                message,
                include_tools=include_tools and message.role == "assistant",
                include_reasoning=include_reasoning and message.role == "assistant",
                tool_output_max_chars=tool_output_max_chars,
            )
            if text:
                rendered_messages.append((message, text))

        user_messages = [item for item in rendered_messages if item[0].role == "user"]
        assistant_messages = [item for item in rendered_messages if item[0].role != "user"]
        first_user_message = user_messages[0][1] if user_messages else ""
        history_entries = len(user_messages)

        defaults = self._thread_defaults()
        thread_row = {
            "id": thread_id,
            "rollout_path": str(rollout_path),
            "created_at": created_ms // 1000,
            "updated_at": updated_ms // 1000,
            "source": defaults["source"],
            "model_provider": defaults["model_provider"],
            "cwd": target_cwd,
            "title": title,
            "sandbox_policy": defaults["sandbox_policy"],
            "approval_mode": defaults["approval_mode"],
            "tokens_used": int(
                sum(
                    int(((message.info.get("tokens") or {}).get("total") or 0))
                    for message in session.messages
                    if message.role == "assistant"
                )
            ),
            "has_user_event": 1 if user_messages else 0,
            "archived": 0,
            "archived_at": None,
            "git_sha": None,
            "git_branch": None,
            "git_origin_url": None,
            "cli_version": defaults["cli_version"],
            "first_user_message": first_user_message,
            "agent_nickname": None,
            "agent_role": None,
            "memory_mode": defaults["memory_mode"],
            "model": defaults["model"],
            "reasoning_effort": defaults["reasoning_effort"],
            "agent_path": None,
            "created_at_ms": created_ms,
            "updated_at_ms": updated_ms,
        }

        rollout_lines = self._build_rollout_lines(
            session=session,
            thread_id=thread_id,
            rendered_messages=rendered_messages,
            created_ms=created_ms,
            title=title,
            target_cwd=target_cwd,
        )

        session_index_line = _compact_json(
            {"id": thread_id, "thread_name": title, "updated_at": _updated_at_iso(updated_ms)}
        )
        history_lines = [
            _compact_json(
                {
                    "session_id": thread_id,
                    "ts": message.created_ms // 1000,
                    "text": text,
                }
            )
            for message, text in user_messages
        ]

        if not dry_run:
            rollout_path.parent.mkdir(parents=True, exist_ok=True)
            rollout_path.write_text("".join(line + "\n" for line in rollout_lines), encoding="utf-8")
            with self._state_conn() as conn:
                columns = ", ".join(thread_row.keys())
                placeholders = ", ".join(["?"] * len(thread_row))
                conn.execute(
                    f"INSERT INTO threads ({columns}) VALUES ({placeholders})",
                    list(thread_row.values()),
                )
                conn.commit()
            with self.session_index_path.open("a", encoding="utf-8") as handle:
                handle.write(session_index_line + "\n")
            if history_lines:
                with self.history_path.open("a", encoding="utf-8") as handle:
                    for line in history_lines:
                        handle.write(line + "\n")

        return ImportResult(
            thread_id=thread_id,
            title=title,
            rollout_path=rollout_path,
            user_messages=len(user_messages),
            assistant_messages=len(assistant_messages),
            history_entries=history_entries,
            dry_run=dry_run,
        )

    def _build_rollout_lines(
        self,
        *,
        session: OpenCodeSession,
        thread_id: str,
        rendered_messages: list[tuple[OpenCodeMessage, str]],
        created_ms: int,
        title: str,
        target_cwd: str,
    ) -> list[str]:
        templates = self._latest_rollout_templates()
        defaults = self._thread_defaults()
        session_meta = copy.deepcopy(templates["session_meta"])
        session_meta["id"] = thread_id
        session_meta["timestamp"] = _utc_iso_from_ms(created_ms)
        session_meta["cwd"] = target_cwd
        session_meta["source"] = defaults["source"]
        session_meta["model_provider"] = defaults["model_provider"]
        session_meta["import_meta"] = {
            "bridge": "codex-thread-bridge",
            "source": "opencode",
            "opencode_session_id": session.id,
            "opencode_title": session.title,
            "opencode_directory": session.directory,
        }
        if target_cwd != session.directory:
            session_meta["import_meta"]["codex_cwd_override"] = target_cwd

        lines: list[str] = [
            _compact_json(
                {
                    "timestamp": _utc_iso_from_ms(created_ms),
                    "type": "session_meta",
                    "payload": session_meta,
                }
            ),
            _compact_json(
                {
                    "timestamp": _utc_iso_from_ms(created_ms + 1),
                    "type": "event_msg",
                    "payload": {
                        "type": "thread_name_updated",
                        "thread_id": thread_id,
                        "thread_name": title,
                    },
                }
            ),
        ]

        grouped_turns: list[tuple[OpenCodeMessage, list[tuple[OpenCodeMessage, str]]]] = []
        current_user: OpenCodeMessage | None = None
        current_assistants: list[tuple[OpenCodeMessage, str]] = []
        for message, text in rendered_messages:
            if message.role == "user":
                if current_user is not None:
                    grouped_turns.append((current_user, current_assistants))
                current_user = message
                current_assistants = []
            else:
                if current_user is None:
                    synthetic_user = OpenCodeMessage(
                        info={
                            "id": f"synthetic-{message.id}",
                            "role": "user",
                            "time": {"created": message.created_ms},
                        },
                        parts=[{"type": "text", "text": "[Imported OpenCode session context]"}],
                    )
                    current_user = synthetic_user
                current_assistants.append((message, text))
        if current_user is not None:
            grouped_turns.append((current_user, current_assistants))

        for user_message, assistant_group in grouped_turns:
            turn_id = str(uuid.uuid4())
            started_ms = user_message.created_ms or created_ms
            task_started = copy.deepcopy(templates["task_started"])
            task_started["turn_id"] = turn_id
            task_started["started_at"] = started_ms // 1000
            turn_context = copy.deepcopy(templates["turn_context"])
            turn_context["turn_id"] = turn_id
            turn_context["cwd"] = target_cwd
            turn_context["current_date"] = datetime.fromtimestamp(started_ms / 1000).astimezone().date().isoformat()

            user_text = render_opencode_message(user_message, include_tools=False, include_reasoning=False).strip()
            lines.append(
                _compact_json(
                    {
                        "timestamp": _utc_iso_from_ms(started_ms),
                        "type": "event_msg",
                        "payload": task_started,
                    }
                )
            )
            lines.append(
                _compact_json(
                    {
                        "timestamp": _utc_iso_from_ms(started_ms + 1),
                        "type": "turn_context",
                        "payload": turn_context,
                    }
                )
            )
            lines.append(
                _compact_json(
                    {
                        "timestamp": _utc_iso_from_ms(started_ms + 2),
                        "type": "response_item",
                        "payload": {
                            "type": "message",
                            "role": "user",
                            "content": [{"type": "input_text", "text": user_text}],
                        },
                    }
                )
            )
            lines.append(
                _compact_json(
                    {
                        "timestamp": _utc_iso_from_ms(started_ms + 3),
                        "type": "event_msg",
                        "payload": {
                            "type": "user_message",
                            "message": user_text,
                            "images": [],
                            "local_images": [],
                            "text_elements": [],
                        },
                    }
                )
            )

            completed_ms: int | None = None
            last_assistant_text: str | None = None
            for index, (assistant_message, assistant_text) in enumerate(assistant_group, start=1):
                timestamp_ms = assistant_message.created_ms or (started_ms + 1000 * index)
                lines.append(
                    _compact_json(
                        {
                            "timestamp": _utc_iso_from_ms(timestamp_ms),
                            "type": "event_msg",
                            "payload": {
                                "type": "agent_message",
                                "message": assistant_text,
                                "phase": "final",
                                "memory_citation": None,
                            },
                        }
                    )
                )
                lines.append(
                    _compact_json(
                        {
                            "timestamp": _utc_iso_from_ms(timestamp_ms + 1),
                            "type": "response_item",
                            "payload": {
                                "type": "message",
                                "role": "assistant",
                                "content": [{"type": "output_text", "text": assistant_text}],
                                "phase": "final",
                            },
                        }
                    )
                )
                completed_ms = timestamp_ms + 1
                last_assistant_text = assistant_text

            if completed_ms is not None and last_assistant_text is not None:
                task_complete_ms = completed_ms + 1
                lines.append(
                    _compact_json(
                        {
                            "timestamp": _utc_iso_from_ms(task_complete_ms),
                            "type": "event_msg",
                            "payload": {
                                "type": "task_complete",
                                "turn_id": turn_id,
                                "last_agent_message": last_assistant_text,
                                "completed_at": task_complete_ms // 1000,
                                "duration_ms": max(0, task_complete_ms - started_ms),
                            },
                        }
                    )
                )

        return lines

    def retarget_thread_cwd(
        self,
        thread: CodexThread,
        *,
        new_cwd: str,
        backup_root: Path | None = None,
        create_backup: bool = True,
        dry_run: bool = False,
        allow_current_thread: bool = False,
    ) -> CodexRetargetResult:
        import_meta = self._thread_import_meta(thread)
        source = str(import_meta.get("source") or "")
        if source and source != "opencode":
            raise BridgeError(
                f"Codex thread '{thread.id}' is not an imported OpenCode thread and cannot be retargeted safely."
            )

        old_cwd = str(thread.row.get("cwd") or "")
        original_cwd = str(import_meta.get("opencode_directory") or old_cwd)
        new_title = _retargeted_opencode_title(thread.title, original_cwd)
        if not new_cwd:
            raise BridgeError("A non-empty target CWD is required.")

        if new_cwd == old_cwd and new_title == thread.title:
            return CodexRetargetResult(
                thread_id=thread.id,
                old_title=thread.title,
                new_title=new_title,
                old_cwd=old_cwd,
                new_cwd=new_cwd,
                backup_dir=None,
                dry_run=dry_run,
            )

        backup_dir: Path | None = None
        if not dry_run and create_backup:
            backup = self.delete_thread(
                thread,
                backup_root=backup_root,
                create_backup=True,
                dry_run=True,
                allow_current_thread=allow_current_thread,
            )
            backup_dir = backup.backup_dir

        if not dry_run:
            with self._state_conn() as conn:
                conn.execute("UPDATE threads SET cwd = ?, title = ? WHERE id = ?", (new_cwd, new_title, thread.id))
                conn.commit()

            renamed_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
            self._append_jsonl_line(
                self.session_index_path,
                _compact_json(
                    {
                        "id": thread.id,
                        "thread_name": new_title,
                        "updated_at": _updated_at_iso(renamed_ms),
                    }
                ),
            )
            self._rewrite_rollout_for_retarget(
                thread.rollout_path,
                new_title=new_title,
                new_cwd=new_cwd,
                original_cwd=original_cwd,
            )

        return CodexRetargetResult(
            thread_id=thread.id,
            old_title=thread.title,
            new_title=new_title,
            old_cwd=old_cwd,
            new_cwd=new_cwd,
            backup_dir=backup_dir,
            dry_run=dry_run,
        )

    def repair_imported_thread(
        self,
        thread: CodexThread,
        *,
        backup_root: Path | None = None,
        create_backup: bool = True,
        dry_run: bool = False,
        allow_current_thread: bool = False,
    ) -> CodexRepairResult:
        import_meta = self._thread_import_meta(thread)
        if str(import_meta.get("source") or "") != "opencode":
            raise BridgeError(f"Codex thread '{thread.id}' is not an imported OpenCode thread.")
        if not thread.rollout_path.exists():
            raise BridgeError(f"Codex rollout was not found at '{thread.rollout_path}'.")

        repaired_lines, inserted_task_complete_events = self._repair_rollout_lines(thread.rollout_path)

        backup_dir: Path | None = None
        if not dry_run and create_backup:
            backup = self.delete_thread(
                thread,
                backup_root=backup_root,
                create_backup=True,
                dry_run=True,
                allow_current_thread=allow_current_thread,
            )
            backup_dir = backup.backup_dir

        if not dry_run and inserted_task_complete_events:
            thread.rollout_path.write_text("".join(line + "\n" for line in repaired_lines), encoding="utf-8")

        return CodexRepairResult(
            thread_id=thread.id,
            title=thread.title,
            backup_dir=backup_dir,
            inserted_task_complete_events=inserted_task_complete_events,
            dry_run=dry_run,
        )

    def delete_thread(
        self,
        thread: CodexThread,
        *,
        backup_root: Path | None = None,
        create_backup: bool = True,
        dry_run: bool = False,
        allow_current_thread: bool = False,
    ) -> DeleteResult:
        current_thread_id = os.environ.get("CODEX_THREAD_ID")
        if current_thread_id and current_thread_id == thread.id and not allow_current_thread:
            raise BridgeError(
                "Refusing to delete the current live Codex thread. Re-run outside that thread or pass "
                "--allow-current-thread if you really mean it."
            )

        backup_dir: Path | None = None
        if create_backup:
            backup_root = backup_root or (self.codex_root / "thread-bridge-backups")
            timestamp = datetime.now().astimezone().strftime("%Y%m%dT%H%M%S")
            backup_dir = backup_root / f"{timestamp}-{thread.id}"
            backup_dir.mkdir(parents=True, exist_ok=True)

        with self._state_conn() as conn:
            dynamic_tools = [
                dict(row)
                for row in conn.execute(
                    "SELECT * FROM thread_dynamic_tools WHERE thread_id = ?", (thread.id,)
                ).fetchall()
            ]
            spawn_edges = [
                dict(row)
                for row in conn.execute(
                    "SELECT * FROM thread_spawn_edges WHERE parent_thread_id = ? OR child_thread_id = ?",
                    (thread.id, thread.id),
                ).fetchall()
            ]

        with self._logs_conn() as conn:
            logs_rows = [
                dict(row) for row in conn.execute("SELECT * FROM logs WHERE thread_id = ?", (thread.id,)).fetchall()
            ]

        session_index_lines = self._matching_jsonl_lines(self.session_index_path, "id", thread.id)
        history_lines = self._matching_jsonl_lines(self.history_path, "session_id", thread.id)
        shell_snapshots = sorted(self.shell_snapshots_root.glob(f"{thread.id}.*"))

        if create_backup and backup_dir is not None:
            manifest = {
                "thread_id": thread.id,
                "title": thread.title,
                "captured_at": datetime.now(tz=timezone.utc).isoformat(),
                "rollout_path": str(thread.rollout_path),
                "shell_snapshots": [path.name for path in shell_snapshots],
            }
            _json_dump(backup_dir / "manifest.json", manifest)
            _json_dump(
                backup_dir / "state.json",
                {
                    "thread": thread.row,
                    "thread_dynamic_tools": dynamic_tools,
                    "thread_spawn_edges": spawn_edges,
                },
            )
            _json_dump(backup_dir / "logs.json", {"rows": logs_rows})
            (backup_dir / "session_index.jsonl").write_text(
                "".join(line + "\n" for line in session_index_lines), encoding="utf-8"
            )
            (backup_dir / "history.jsonl").write_text("".join(line + "\n" for line in history_lines), encoding="utf-8")
            if thread.rollout_path.exists():
                rollout_backup = backup_dir / "files" / "rollout.jsonl"
                rollout_backup.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(thread.rollout_path, rollout_backup)
            for snapshot in shell_snapshots:
                target = backup_dir / "files" / "shell_snapshots" / snapshot.name
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(snapshot, target)

        if not dry_run:
            with self._state_conn() as conn:
                conn.execute("DELETE FROM thread_dynamic_tools WHERE thread_id = ?", (thread.id,))
                conn.execute(
                    "DELETE FROM thread_spawn_edges WHERE parent_thread_id = ? OR child_thread_id = ?",
                    (thread.id, thread.id),
                )
                conn.execute("DELETE FROM threads WHERE id = ?", (thread.id,))
                conn.commit()

            with self._logs_conn() as conn:
                conn.execute("DELETE FROM logs WHERE thread_id = ?", (thread.id,))
                conn.commit()

            self._rewrite_jsonl_without(self.session_index_path, "id", thread.id)
            self._rewrite_jsonl_without(self.history_path, "session_id", thread.id)

            if thread.rollout_path.exists():
                thread.rollout_path.unlink()
            for snapshot in shell_snapshots:
                snapshot.unlink()

        return DeleteResult(
            thread_id=thread.id,
            title=thread.title,
            backup_dir=backup_dir,
            rollout_deleted=thread.rollout_path.exists() if dry_run else not thread.rollout_path.exists(),
            shell_snapshots_deleted=len(shell_snapshots),
            history_entries_removed=len(history_lines),
            session_index_entries_removed=len(session_index_lines),
            dry_run=dry_run,
        )

    def restore_backup(self, backup_dir: Path, *, force: bool = False) -> RestoreResult:
        state_payload = _json_load(backup_dir / "state.json")
        manifest = _json_load(backup_dir / "manifest.json")
        thread_row = state_payload["thread"]
        thread_id = thread_row["id"]

        existing = {thread.id for thread in self.list_threads()}
        if thread_id in existing and not force:
            raise BridgeError(f"Codex thread '{thread_id}' already exists. Use --force to replace it.")

        with self._state_conn() as conn:
            if force and thread_id in existing:
                conn.execute("DELETE FROM thread_dynamic_tools WHERE thread_id = ?", (thread_id,))
                conn.execute(
                    "DELETE FROM thread_spawn_edges WHERE parent_thread_id = ? OR child_thread_id = ?",
                    (thread_id, thread_id),
                )
                conn.execute("DELETE FROM threads WHERE id = ?", (thread_id,))

            columns = ", ".join(thread_row.keys())
            placeholders = ", ".join(["?"] * len(thread_row))
            conn.execute(
                f"INSERT OR REPLACE INTO threads ({columns}) VALUES ({placeholders})",
                list(thread_row.values()),
            )

            for row in state_payload.get("thread_dynamic_tools", []):
                cols = ", ".join(row.keys())
                placeholders = ", ".join(["?"] * len(row))
                conn.execute(
                    f"INSERT OR REPLACE INTO thread_dynamic_tools ({cols}) VALUES ({placeholders})",
                    list(row.values()),
                )

            for row in state_payload.get("thread_spawn_edges", []):
                cols = ", ".join(row.keys())
                placeholders = ", ".join(["?"] * len(row))
                conn.execute(
                    f"INSERT OR REPLACE INTO thread_spawn_edges ({cols}) VALUES ({placeholders})",
                    list(row.values()),
                )

            conn.commit()

        logs_payload = _json_load(backup_dir / "logs.json")
        with self._logs_conn() as conn:
            if force:
                conn.execute("DELETE FROM logs WHERE thread_id = ?", (thread_id,))
            for row in logs_payload.get("rows", []):
                insert_row = {k: v for k, v in row.items() if k != "id"}
                cols = ", ".join(insert_row.keys())
                placeholders = ", ".join(["?"] * len(insert_row))
                conn.execute(f"INSERT INTO logs ({cols}) VALUES ({placeholders})", list(insert_row.values()))
            conn.commit()

        self._append_unique_lines(self.session_index_path, backup_dir / "session_index.jsonl")
        self._append_unique_lines(self.history_path, backup_dir / "history.jsonl")

        restored_rollout = False
        rollout_backup = backup_dir / "files" / "rollout.jsonl"
        rollout_target = Path(manifest["rollout_path"])
        if rollout_backup.exists():
            rollout_target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(rollout_backup, rollout_target)
            restored_rollout = True

        restored_shell_snapshots = 0
        shell_snapshot_dir = backup_dir / "files" / "shell_snapshots"
        if shell_snapshot_dir.exists():
            self.shell_snapshots_root.mkdir(parents=True, exist_ok=True)
            for snapshot in shell_snapshot_dir.glob("*"):
                shutil.copy2(snapshot, self.shell_snapshots_root / snapshot.name)
                restored_shell_snapshots += 1

        return RestoreResult(
            thread_id=thread_id,
            title=str(manifest.get("title") or thread_row.get("title") or thread_id),
            restored_rollout=restored_rollout,
            restored_shell_snapshots=restored_shell_snapshots,
        )

    def _matching_jsonl_lines(self, path: Path, key: str, value: str) -> list[str]:
        if not path.exists():
            return []
        matches: list[str] = []
        with path.open(encoding="utf-8") as handle:
            for raw_line in handle:
                line = raw_line.rstrip("\n")
                if not line.strip():
                    continue
                payload = json.loads(line)
                if payload.get(key) == value:
                    matches.append(line)
        return matches

    def _thread_import_meta(self, thread: CodexThread) -> dict[str, Any]:
        if not thread.rollout_path.exists():
            return {}
        with thread.rollout_path.open(encoding="utf-8") as handle:
            for raw_line in handle:
                line = raw_line.strip()
                if not line:
                    continue
                payload = json.loads(line)
                if payload.get("type") != "session_meta":
                    continue
                session_meta = payload.get("payload") or {}
                import_meta = session_meta.get("import_meta")
                if isinstance(import_meta, dict):
                    return dict(import_meta)
                return {}
        return {}

    def _rewrite_rollout_for_retarget(
        self,
        rollout_path: Path,
        *,
        new_title: str,
        new_cwd: str,
        original_cwd: str,
    ) -> None:
        if not rollout_path.exists():
            return

        rewritten: list[str] = []
        with rollout_path.open(encoding="utf-8") as handle:
            for raw_line in handle:
                line = raw_line.rstrip("\n")
                if not line.strip():
                    continue
                payload = json.loads(line)
                item_type = payload.get("type")
                inner = payload.get("payload")
                if isinstance(inner, dict):
                    if item_type == "session_meta":
                        inner["cwd"] = new_cwd
                        import_meta = inner.get("import_meta")
                        if not isinstance(import_meta, dict):
                            import_meta = {}
                        import_meta.setdefault("bridge", "codex-thread-bridge")
                        import_meta["source"] = "opencode"
                        if original_cwd:
                            import_meta["opencode_directory"] = original_cwd
                        if new_cwd != original_cwd:
                            import_meta["codex_cwd_override"] = new_cwd
                        else:
                            import_meta.pop("codex_cwd_override", None)
                        inner["import_meta"] = import_meta
                    elif item_type == "turn_context":
                        inner["cwd"] = new_cwd
                    elif item_type == "event_msg" and inner.get("type") == "thread_name_updated":
                        inner["thread_name"] = new_title
                rewritten.append(_compact_json(payload))

        rollout_path.write_text("".join(line + "\n" for line in rewritten), encoding="utf-8")

    def _repair_rollout_lines(self, rollout_path: Path) -> tuple[list[str], int]:
        repaired_entries: list[str] = []
        inserted = 0
        current_turn_id: str | None = None
        started_ms: int | None = None
        saw_completion = False
        last_agent_message: str | None = None
        last_turn_timestamp_ms: int | None = None

        def flush(next_timestamp_ms: int | None = None) -> None:
            nonlocal inserted, saw_completion
            if current_turn_id is None or saw_completion or last_agent_message is None or last_turn_timestamp_ms is None:
                return
            task_complete_ms = last_turn_timestamp_ms + 1
            if next_timestamp_ms is not None and next_timestamp_ms < task_complete_ms:
                task_complete_ms = next_timestamp_ms
            repaired_entries.append(
                _compact_json(
                    {
                        "timestamp": _utc_iso_from_ms(task_complete_ms),
                        "type": "event_msg",
                        "payload": {
                            "type": "task_complete",
                            "turn_id": current_turn_id,
                            "last_agent_message": last_agent_message,
                            "completed_at": task_complete_ms // 1000,
                            "duration_ms": max(0, task_complete_ms - (started_ms or task_complete_ms)),
                        },
                    }
                )
            )
            inserted += 1
            saw_completion = True

        with rollout_path.open(encoding="utf-8") as handle:
            for raw_line in handle:
                line = raw_line.rstrip("\n")
                if not line.strip():
                    continue
                payload = json.loads(line)
                item_type = payload.get("type")
                inner = payload.get("payload") if isinstance(payload.get("payload"), dict) else {}
                entry_timestamp_ms = _utc_ms_from_iso(str(payload["timestamp"]))

                if item_type == "event_msg" and inner.get("type") == "task_started":
                    flush(next_timestamp_ms=entry_timestamp_ms)
                    current_turn_id = str(inner.get("turn_id") or "")
                    started_ms = int(inner.get("started_at") or 0) * 1000 or entry_timestamp_ms
                    saw_completion = False
                    last_agent_message = None
                    last_turn_timestamp_ms = entry_timestamp_ms

                repaired_entries.append(_compact_json(payload))

                if current_turn_id is None:
                    continue

                if item_type == "event_msg":
                    inner_type = inner.get("type")
                    if inner_type in {"task_complete", "turn_aborted"} and str(inner.get("turn_id") or "") == current_turn_id:
                        saw_completion = True
                    elif inner_type == "agent_message":
                        message = inner.get("message")
                        if isinstance(message, str) and message:
                            last_agent_message = message
                        last_turn_timestamp_ms = entry_timestamp_ms
                elif item_type == "response_item":
                    if payload.get("payload", {}).get("type") == "message" and payload.get("payload", {}).get("role") == "assistant":
                        last_turn_timestamp_ms = entry_timestamp_ms
                        if last_agent_message is None:
                            for content_part in payload["payload"].get("content") or []:
                                if content_part.get("type") == "output_text":
                                    text = content_part.get("text")
                                    if isinstance(text, str) and text:
                                        last_agent_message = text
                                        break

        flush()
        return repaired_entries, inserted

    def _rewrite_jsonl_without(self, path: Path, key: str, value: str) -> None:
        if not path.exists():
            return
        kept: list[str] = []
        with path.open(encoding="utf-8") as handle:
            for raw_line in handle:
                line = raw_line.rstrip("\n")
                if not line.strip():
                    continue
                payload = json.loads(line)
                if payload.get(key) != value:
                    kept.append(line)
        path.write_text("".join(line + "\n" for line in kept), encoding="utf-8")

    def _append_jsonl_line(self, path: Path, line: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            existing = {existing_line for existing_line in path.read_text(encoding="utf-8").splitlines() if existing_line}
            if line in existing:
                return
        with path.open("a", encoding="utf-8") as handle:
            handle.write(line + "\n")

    def _append_unique_lines(self, target_path: Path, source_path: Path) -> None:
        existing = set()
        if target_path.exists():
            existing = {line.rstrip("\n") for line in target_path.read_text(encoding="utf-8").splitlines() if line}
        to_add = [line for line in source_path.read_text(encoding="utf-8").splitlines() if line and line not in existing]
        if to_add:
            with target_path.open("a", encoding="utf-8") as handle:
                for line in to_add:
                    handle.write(line + "\n")

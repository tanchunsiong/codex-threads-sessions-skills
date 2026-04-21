from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Sequence

from .core import BridgeError, CodexStore, DEFAULT_IMPORT_TITLE_PREFIX, OpenCodeStore


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="codex-thread-bridge",
        description="List, search, import, delete, and restore OpenCode sessions and local Codex threads.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    for name in ("list-opencode", "list-codex"):
        subparser = subparsers.add_parser(name)
        subparser.add_argument("--limit", type=int, default=20)
        subparser.add_argument("--json", action="store_true", dest="as_json")
        if name == "list-opencode":
            subparser.add_argument(
                "--all-sessions",
                action="store_true",
                help="Include child/subagent OpenCode sessions as well as top-level sessions.",
            )

    search_opencode_parser = subparsers.add_parser("search-opencode")
    search_opencode_parser.add_argument("--title-prefix")
    search_opencode_parser.add_argument("--title-contains")
    search_opencode_parser.add_argument("--all-sessions", action="store_true")
    search_opencode_parser.add_argument("--case-sensitive", action="store_true")
    search_opencode_parser.add_argument("--limit", type=int, default=20)
    search_opencode_parser.add_argument("--json", action="store_true", dest="as_json")

    search_parser = subparsers.add_parser("search-codex")
    search_parser.add_argument("--title-prefix")
    search_parser.add_argument("--title-contains")
    search_parser.add_argument("--include-session-index", action="store_true")
    search_parser.add_argument("--case-sensitive", action="store_true")
    search_parser.add_argument("--limit", type=int, default=20)
    search_parser.add_argument("--json", action="store_true", dest="as_json")

    import_parser = subparsers.add_parser("import-opencode")
    import_parser.add_argument("refs", nargs="+", help="OpenCode session IDs or exact titles.")
    import_parser.add_argument("--contains", action="store_true", help="Allow a unique title substring match.")
    import_parser.add_argument(
        "--all-sessions",
        action="store_true",
        help="Allow importing child/subagent OpenCode sessions too. By default only top-level sessions are matched.",
    )
    import_parser.add_argument("--dry-run", action="store_true")
    import_parser.add_argument("--title-prefix", default=DEFAULT_IMPORT_TITLE_PREFIX)
    import_parser.add_argument("--title")
    import_parser.add_argument("--skip-tools", action="store_true")
    import_parser.add_argument("--include-reasoning", action="store_true")
    import_parser.add_argument("--tool-output-max-chars", type=int, default=1200)

    delete_opencode_parser = subparsers.add_parser("delete-opencode")
    delete_opencode_parser.add_argument("refs", nargs="*", help="OpenCode session IDs or exact titles.")
    delete_opencode_parser.add_argument("--contains", action="store_true", help="Allow a unique title substring match.")
    delete_opencode_parser.add_argument("--title-prefix")
    delete_opencode_parser.add_argument("--title-contains")
    delete_opencode_parser.add_argument(
        "--all-sessions",
        action="store_true",
        help="Allow matching child/subagent OpenCode sessions too. Deletion always removes the matched subtree.",
    )
    delete_opencode_parser.add_argument("--case-sensitive", action="store_true")
    delete_opencode_parser.add_argument("--dry-run", action="store_true")
    delete_opencode_parser.add_argument("--yes", action="store_true", help="Required for a real deletion.")
    delete_opencode_parser.add_argument("--no-backup", action="store_true", help="Skip backup creation before deletion.")
    delete_opencode_parser.add_argument("--backup-root", type=Path)

    delete_parser = subparsers.add_parser("delete-codex")
    delete_parser.add_argument("refs", nargs="*", help="Codex thread IDs or exact titles.")
    delete_parser.add_argument("--contains", action="store_true", help="Allow a unique title substring match.")
    delete_parser.add_argument("--title-prefix")
    delete_parser.add_argument("--title-contains")
    delete_parser.add_argument("--include-session-index", action="store_true")
    delete_parser.add_argument("--case-sensitive", action="store_true")
    delete_parser.add_argument("--dry-run", action="store_true")
    delete_parser.add_argument("--yes", action="store_true", help="Required for a real deletion.")
    delete_parser.add_argument("--no-backup", action="store_true", help="Skip backup creation before deletion.")
    delete_parser.add_argument("--backup-root", type=Path)
    delete_parser.add_argument("--allow-current-thread", action="store_true")

    restore_parser = subparsers.add_parser("restore-codex")
    restore_parser.add_argument("backup_dir", type=Path)
    restore_parser.add_argument("--force", action="store_true")

    restore_opencode_parser = subparsers.add_parser("restore-opencode")
    restore_opencode_parser.add_argument("backup_dir", type=Path)
    restore_opencode_parser.add_argument("--force", action="store_true")

    return parser


def _print_rows(rows: list[dict[str, object]], *, limit: int) -> None:
    rows = rows[:limit]
    if not rows:
        print("No rows found.")
        return

    columns = list(rows[0].keys())
    widths = {
        column: max(len(column), *(len(str(row.get(column, ""))) for row in rows))
        for column in columns
    }
    header = "  ".join(column.ljust(widths[column]) for column in columns)
    print(header)
    print("  ".join("-" * widths[column] for column in columns))
    for row in rows:
        print("  ".join(str(row.get(column, "")).ljust(widths[column]) for column in columns))


def _handle_list_opencode(args: argparse.Namespace) -> int:
    store = OpenCodeStore()
    rows = []
    for session in store.list_sessions(include_child_sessions=args.all_sessions):
        row = {
            "id": session.id,
            "updated": session.updated_ms,
            "title": session.title,
            "directory": session.directory,
        }
        if args.all_sessions:
            row["parent_id"] = session.parent_id or ""
        rows.append(row)
    if args.as_json:
        print(json.dumps(rows[: args.limit], indent=2, ensure_ascii=True))
    else:
        _print_rows(rows, limit=args.limit)
    return 0


def _searched_opencode_sessions(args: argparse.Namespace) -> list[dict[str, object]]:
    store = OpenCodeStore()
    matches = store.search_sessions(
        title_prefix=args.title_prefix,
        title_contains=args.title_contains,
        include_child_sessions=args.all_sessions,
        ignore_case=not args.case_sensitive,
    )
    rows = []
    for match in matches:
        row = {
            "id": match.session_id,
            "updated": match.updated_ms,
            "title": match.title,
            "matched_titles": " | ".join(match.matched_titles),
            "directory": match.directory,
        }
        if args.all_sessions:
            row["parent_id"] = match.parent_id or ""
        rows.append(row)
    return rows


def _handle_search_opencode(args: argparse.Namespace) -> int:
    if not args.title_prefix and not args.title_contains:
        raise BridgeError("search-opencode requires --title-prefix or --title-contains.")
    rows = _searched_opencode_sessions(args)
    if args.as_json:
        print(json.dumps(rows[: args.limit], indent=2, ensure_ascii=True))
    else:
        _print_rows(rows, limit=args.limit)
    return 0


def _handle_list_codex(args: argparse.Namespace) -> int:
    store = CodexStore()
    rows = [
        {
            "id": thread.id,
            "updated": thread.updated_ms,
            "title": thread.title,
            "cwd": thread.row.get("cwd") or "",
        }
        for thread in store.list_threads()
    ]
    if args.as_json:
        print(json.dumps(rows[: args.limit], indent=2, ensure_ascii=True))
    else:
        _print_rows(rows, limit=args.limit)
    return 0


def _searched_threads(args: argparse.Namespace) -> list[dict[str, object]]:
    store = CodexStore()
    matches = store.search_threads(
        title_prefix=args.title_prefix,
        title_contains=args.title_contains,
        include_session_index=args.include_session_index,
        ignore_case=not args.case_sensitive,
    )
    return [
        {
            "id": match.thread_id,
            "updated": match.updated_ms,
            "live_title": match.live_title,
            "matched_titles": " | ".join(match.matched_titles),
            "cwd": match.cwd,
        }
        for match in matches
    ]


def _handle_search_codex(args: argparse.Namespace) -> int:
    if not args.title_prefix and not args.title_contains:
        raise BridgeError("search-codex requires --title-prefix or --title-contains.")
    rows = _searched_threads(args)
    if args.as_json:
        print(json.dumps(rows[: args.limit], indent=2, ensure_ascii=True))
    else:
        _print_rows(rows, limit=args.limit)
    return 0


def _unique_opencode_sessions_for_delete(
    store: OpenCodeStore,
    sessions: list,
) -> list:
    session_by_id = {session.id: session for session in store.list_sessions(include_child_sessions=True)}
    selected_ids = {session.id for session in sessions}
    unique_sessions = []
    seen_session_ids = set()
    for session in sessions:
        if session.id in seen_session_ids:
            continue
        parent_id = session.parent_id
        skip = False
        while parent_id is not None:
            if parent_id in selected_ids:
                skip = True
                break
            parent = session_by_id.get(parent_id)
            parent_id = parent.parent_id if parent is not None else None
        if skip:
            continue
        seen_session_ids.add(session.id)
        unique_sessions.append(session)
    return unique_sessions


def _handle_import(args: argparse.Namespace) -> int:
    opencode = OpenCodeStore()
    codex = CodexStore()
    for index, ref in enumerate(args.refs):
        session = opencode.resolve_session(
            ref,
            contains=args.contains,
            include_child_sessions=args.all_sessions,
        )
        result = codex.import_opencode_session(
            session,
            title_prefix=args.title_prefix,
            title_override=args.title if len(args.refs) == 1 else None,
            include_tools=not args.skip_tools,
            include_reasoning=args.include_reasoning,
            tool_output_max_chars=args.tool_output_max_chars,
            dry_run=args.dry_run,
        )
        prefix = "DRY RUN" if result.dry_run else "IMPORTED"
        print(
            f"{prefix}: {result.thread_id}  title={result.title!r}  "
            f"user_messages={result.user_messages}  assistant_messages={result.assistant_messages}  "
            f"rollout={result.rollout_path}"
        )
        if index + 1 < len(args.refs):
            print()
    return 0


def _handle_delete_opencode(args: argparse.Namespace) -> int:
    if not args.dry_run and not args.yes:
        raise BridgeError("delete-opencode is destructive. Re-run with --yes, or use --dry-run first.")
    if not args.refs and not args.title_prefix and not args.title_contains:
        raise BridgeError("delete-opencode requires explicit refs or a search filter.")

    opencode = OpenCodeStore()
    sessions = []
    if args.refs:
        sessions = [
            opencode.resolve_session(
                ref,
                contains=args.contains,
                include_child_sessions=args.all_sessions,
            )
            for ref in args.refs
        ]
    else:
        matches = opencode.search_sessions(
            title_prefix=args.title_prefix,
            title_contains=args.title_contains,
            include_child_sessions=args.all_sessions,
            ignore_case=not args.case_sensitive,
        )
        sessions = [
            opencode.resolve_session(match.session_id, include_child_sessions=True)
            for match in matches
        ]
        if not sessions:
            print("No rows found.")
            return 0

    unique_sessions = _unique_opencode_sessions_for_delete(opencode, sessions)
    for index, session in enumerate(unique_sessions):
        result = opencode.delete_session(
            session,
            backup_root=args.backup_root,
            create_backup=not args.no_backup,
            dry_run=args.dry_run,
        )
        prefix = "DRY RUN" if result.dry_run else "DELETED"
        backup_text = str(result.backup_dir) if result.backup_dir is not None else "disabled"
        print(
            f"{prefix}: {result.session_id}  title={result.title!r}  backup={backup_text}  "
            f"sessions={result.deleted_session_count}  messages={result.deleted_message_count}  "
            f"parts={result.deleted_part_count}"
        )
        if index + 1 < len(unique_sessions):
            print()
    return 0


def _handle_delete(args: argparse.Namespace) -> int:
    if not args.dry_run and not args.yes:
        raise BridgeError("delete-codex is destructive. Re-run with --yes, or use --dry-run first.")
    if not args.refs and not args.title_prefix and not args.title_contains:
        raise BridgeError("delete-codex requires explicit refs or a search filter.")

    codex = CodexStore()
    threads = []
    if args.refs:
        threads = [codex.resolve_thread(ref, contains=args.contains) for ref in args.refs]
    else:
        matches = codex.search_threads(
            title_prefix=args.title_prefix,
            title_contains=args.title_contains,
            include_session_index=args.include_session_index,
            ignore_case=not args.case_sensitive,
        )
        threads = [codex.resolve_thread(match.thread_id) for match in matches]
        if not threads:
            print("No rows found.")
            return 0

    unique_threads = []
    seen_thread_ids = set()
    for thread in threads:
        if thread.id in seen_thread_ids:
            continue
        seen_thread_ids.add(thread.id)
        unique_threads.append(thread)

    for index, thread in enumerate(unique_threads):
        result = codex.delete_thread(
            thread,
            backup_root=args.backup_root,
            create_backup=not args.no_backup,
            dry_run=args.dry_run,
            allow_current_thread=args.allow_current_thread,
        )
        prefix = "DRY RUN" if result.dry_run else "DELETED"
        backup_text = str(result.backup_dir) if result.backup_dir is not None else "disabled"
        print(
            f"{prefix}: {result.thread_id}  title={result.title!r}  backup={backup_text}  "
            f"history_removed={result.history_entries_removed}  "
            f"session_index_removed={result.session_index_entries_removed}  "
            f"shell_snapshots={result.shell_snapshots_deleted}"
        )
        if index + 1 < len(unique_threads):
            print()
    return 0


def _handle_restore(args: argparse.Namespace) -> int:
    codex = CodexStore()
    result = codex.restore_backup(args.backup_dir, force=args.force)
    print(
        f"RESTORED: {result.thread_id}  title={result.title!r}  "
        f"rollout_restored={result.restored_rollout}  shell_snapshots={result.restored_shell_snapshots}"
    )
    return 0


def _handle_restore_opencode(args: argparse.Namespace) -> int:
    opencode = OpenCodeStore()
    result = opencode.restore_session_backup(args.backup_dir, force=args.force)
    print(
        f"RESTORED: {result.session_id}  title={result.title!r}  "
        f"sessions={result.restored_session_count}  messages={result.restored_message_count}  "
        f"parts={result.restored_part_count}"
    )
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "list-opencode":
            return _handle_list_opencode(args)
        if args.command == "search-opencode":
            return _handle_search_opencode(args)
        if args.command == "list-codex":
            return _handle_list_codex(args)
        if args.command == "search-codex":
            return _handle_search_codex(args)
        if args.command == "import-opencode":
            return _handle_import(args)
        if args.command == "delete-opencode":
            return _handle_delete_opencode(args)
        if args.command == "delete-codex":
            return _handle_delete(args)
        if args.command == "restore-opencode":
            return _handle_restore_opencode(args)
        if args.command == "restore-codex":
            return _handle_restore(args)
        raise BridgeError(f"Unsupported command: {args.command}")
    except BridgeError as exc:
        parser.exit(2, f"error: {exc}\n")


if __name__ == "__main__":
    raise SystemExit(main())

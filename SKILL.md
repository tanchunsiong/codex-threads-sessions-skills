---
name: codex-threads-sessions-skills
description: Use when you need to inspect OpenCode sessions, import them into local Codex threads, or search, delete, and restore local Codex threads through the bundled thread bridge CLI.
---

# Codex Threads-Sessions Skills

Use the bundled CLI at `scripts/codex_thread_bridge.py`.

## Workflow

1. List before you mutate:
   - `python3 scripts/codex_thread_bridge.py list-opencode --limit 20`
   - `python3 scripts/codex_thread_bridge.py list-codex --limit 20`
   - `python3 scripts/codex_thread_bridge.py search-codex --title-prefix TBD --include-session-index`
2. Import OpenCode sessions with a dry run first:
   - `python3 scripts/codex_thread_bridge.py import-opencode <session-id> --dry-run`
   - `python3 scripts/codex_thread_bridge.py import-opencode <session-id>`
   - By default, imported thread titles are prefixed with `opencode `
3. Delete Codex threads only after a dry run:
   - `python3 scripts/codex_thread_bridge.py delete-codex <thread-id> --dry-run`
   - `python3 scripts/codex_thread_bridge.py delete-codex <thread-id> --yes`
   - Query delete is also supported: `python3 scripts/codex_thread_bridge.py delete-codex --title-prefix TBD --include-session-index --dry-run`
   - If you intentionally want irreversible cleanup, add `--no-backup`
4. Restore from the backup directory if needed:
   - `python3 scripts/codex_thread_bridge.py restore-codex <backup-dir>`

## Notes

- `delete-codex` refuses to remove the current live thread unless `--allow-current-thread` is passed.
- If a thread was renamed later, `search-codex --include-session-index` can still find it by an older recorded thread name from `~/.codex/session_index.jsonl`.
- Imported Codex thread titles default to `opencode <original title>`. Override that with `--title` or `--title-prefix` if needed.
- Deletion backs up the thread row, session index lines, history lines, rollout JSONL, shell snapshots, and SQLite log rows under `~/.codex/thread-bridge-backups/`.
- `delete-codex --no-backup` skips that safety net and removes the thread state directly.
- This cleans up Codex's structured thread state. It does not rewrite append-only plain text logs such as `~/.codex/log/codex-tui.log`.
- `import-opencode` reads local OpenCode storage directly. It preserves user and assistant text, and can include summarized tool output.

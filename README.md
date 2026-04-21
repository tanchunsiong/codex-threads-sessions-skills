# OpenCode Codex Threads Skill

This repository packages a Codex skill plus a bundled Python bridge for:

- listing local OpenCode sessions
- listing local Codex threads
- importing OpenCode sessions into Codex
- searching Codex threads by current or historical title
- deleting Codex threads
- restoring deleted Codex threads from backup

## Repository Layout

- `SKILL.md`: the Codex skill definition
- `scripts/codex_thread_bridge.py`: CLI entrypoint
- `scripts/codex_thread_bridge_lib/`: implementation
- `tests/`: basic regression tests

## Install As A Codex Skill

Clone this repository directly into your Codex skills directory:

```bash
git clone https://github.com/tanchunsiong/opencode-codex-threads-skill.git ~/.codex/skills/opencode-codex-threads
```

If you already cloned it elsewhere, copy the repository contents into a folder under `~/.codex/skills/` and keep `SKILL.md` at the folder root.

## CLI Usage

Run commands from the repository root:

```bash
python3 scripts/codex_thread_bridge.py list-opencode --limit 20
python3 scripts/codex_thread_bridge.py list-codex --limit 20
python3 scripts/codex_thread_bridge.py search-codex --title-prefix TBD --include-session-index
python3 scripts/codex_thread_bridge.py import-opencode <session-id> --dry-run
python3 scripts/codex_thread_bridge.py delete-codex <thread-id> --dry-run
python3 scripts/codex_thread_bridge.py restore-codex <backup-dir>
```

## Safety

- `delete-codex` requires `--yes` for real deletion.
- The default delete path creates a backup under `~/.codex/thread-bridge-backups/`.
- `--no-backup` disables that safety net and makes the cleanup irreversible.
- The current live Codex thread is protected unless `--allow-current-thread` is passed.

## Notes

- `search-codex --include-session-index` can find threads by older recorded names from `~/.codex/session_index.jsonl`.
- Deletion cleans structured Codex state. It does not rewrite append-only logs like `~/.codex/log/codex-tui.log`.

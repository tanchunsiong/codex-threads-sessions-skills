# Codex Threads-Sessions Skills

This repository packages a Codex skill plus a bundled Python bridge for:

- listing local OpenCode sessions
- searching local OpenCode sessions
- listing local Codex threads
- importing OpenCode sessions into Codex
- repairing older imported Codex threads that resume as interrupted turns
- retargeting imported Codex threads to a different working directory
- searching Codex threads by current or historical title
- deleting OpenCode sessions
- restoring deleted OpenCode sessions from backup
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
git clone https://github.com/tanchunsiong/codex-threads-sessions-skills.git ~/.codex/skills/codex-threads-sessions-skills
```

If you already cloned it elsewhere, copy the repository contents into a folder under `~/.codex/skills/` and keep `SKILL.md` at the folder root.

## CLI Usage

Run commands from the repository root:

```bash
python3 scripts/codex_thread_bridge.py list-opencode --limit 20
python3 scripts/codex_thread_bridge.py list-opencode --all-sessions --limit 20
python3 scripts/codex_thread_bridge.py search-opencode --title-prefix Greeting
python3 scripts/codex_thread_bridge.py list-codex --limit 20
python3 scripts/codex_thread_bridge.py search-codex --title-prefix TBD --include-session-index
python3 scripts/codex_thread_bridge.py import-opencode <session-id> --dry-run
python3 scripts/codex_thread_bridge.py import-opencode <session-id> --all-sessions --dry-run
python3 scripts/codex_thread_bridge.py import-opencode <session-id> --cwd-override /home/dreamtcs --dry-run
python3 scripts/codex_thread_bridge.py retarget-codex-cwd --title-prefix "opencode " --cwd /home/dreamtcs --dry-run
python3 scripts/codex_thread_bridge.py repair-codex-imports --dry-run
python3 scripts/codex_thread_bridge.py repair-codex-imports --yes
python3 scripts/codex_thread_bridge.py delete-opencode <session-id> --dry-run
python3 scripts/codex_thread_bridge.py delete-opencode --title-prefix Greeting --dry-run
python3 scripts/codex_thread_bridge.py restore-opencode <backup-dir>
python3 scripts/codex_thread_bridge.py delete-codex <thread-id> --dry-run
python3 scripts/codex_thread_bridge.py restore-codex <backup-dir>
```

Imported OpenCode sessions default to a Codex title of `opencode <original title>`. Use `--title-prefix` or `--title` if you need a different naming scheme.
If you import with `--cwd-override`, the title defaults to `opencode <original OpenCode cwd> <original title>` so the source directory stays visible even after the Codex `cwd` changes.
New imports now write completed Codex turns directly. If you imported threads before this fix and `codex resume` shows `Conversation interrupted`, run `repair-codex-imports`.
By default, OpenCode listing and import only consider top-level sessions, which matches the smaller set shown in OpenCode. Add `--all-sessions` if you want child/subagent sessions too.
`delete-opencode` deletes the matched session plus all descendant child sessions in the same tree.

## Safety

- `delete-opencode` requires `--yes` for real deletion.
- The default OpenCode delete path creates a backup under `~/.local/share/opencode/thread-bridge-backups/`.
- `delete-opencode --no-backup` disables that safety net and makes the cleanup irreversible.
- `repair-codex-imports` requires `--yes` for a real rewrite and creates a Codex backup by default.
- `retarget-codex-cwd` rewrites Codex thread metadata only and creates a Codex backup by default before the rewrite.
- `delete-codex` requires `--yes` for real deletion.
- The default delete path creates a backup under `~/.codex/thread-bridge-backups/`.
- `--no-backup` disables that safety net and makes the cleanup irreversible.
- The current live Codex thread is protected unless `--allow-current-thread` is passed.

## Notes

- `search-opencode` and `delete-opencode` default to top-level OpenCode sessions only. Add `--all-sessions` to target child/subagent sessions directly.
- OpenCode deletion removes the session subtree from the local OpenCode database and storage files, and `restore-opencode` rebuilds both from backup.
- `search-codex --include-session-index` can find threads by older recorded names from `~/.codex/session_index.jsonl`.
- `repair-codex-imports` backfills missing `task_complete` events on older OpenCode imports so assistant replies render normally in `codex resume`.
- `retarget-codex-cwd` keeps the original imported OpenCode directory in rollout metadata and renames the thread to `opencode <original cwd> <title>`.
- Deletion cleans structured Codex state. It does not rewrite append-only logs like `~/.codex/log/codex-tui.log`.

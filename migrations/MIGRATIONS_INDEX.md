# Migrations Index (Runtime SQL Only)

Execution order is lexical by filename.

- `001_inbox_queue.sql` - Inbox queue table for ingesting commands/updates.
- `010_tasks.sql` - Core `tasks` table.
- `011_subtasks.sql` - Core `subtasks` table linked to tasks.
- `012_tasks_completed_at.sql` - Adds `tasks.completed_at`.
- `013_subtasks_completed_at.sql` - Adds `subtasks.completed_at`.
- `014_tasks_source_msg_id.sql` - Adds `tasks.source_msg_id` for traceability.
- `015_subtasks_source_msg_id.sql` - Adds `subtasks.source_msg_id` for traceability.
- `016_tasks_state.sql` - Adds `tasks.state` field (runtime state machine).
- `017_tasks_planned_at.sql` - Adds `tasks.planned_at` for planning.
- `018_directions_projects_cycles.sql` - REQUIRED: directions/projects/cycles tables used by runtime goals/signals.
- `019_tasks_parent.sql` - Adds `tasks.parent_type/parent_id` for hierarchies.
- `020_user_settings.sql` - User settings base table.
- `021_user_nudges.sql` - REQUIRED: stores per-user nudge scheduling state.
- `022_user_settings_modules.sql` - Adds module toggles to user settings.
- `023_cycle_goals.sql` - REQUIRED: stores goals within cycles.
- `024_regulations.sql` - Regulations (recurring rules) table.
- `025_regulation_runs.sql` - Per-period execution state for regulations.
- `026_time_blocks.sql` - Time blocks linked to tasks.

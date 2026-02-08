P2 Stage 1 runtime module uses separate SQLite tables: `tasks` and `subtasks`.
Depth is fixed at 1: a subtask always references a root task.
Subtasks never touch calendar; only `tasks.calendar_event_id` exists.
Canonical statuses: NEW, IN_PROGRESS, DONE, FAILED (aliases: INBOX/TODO -> NEW).
Invariant: a task cannot be completed if any subtask is not DONE.
Invariant: cannot create a subtask for DONE or FAILED task.
Flag `P2_ENFORCE_STATUS=1` raises exceptions; `0` logs violations and continues.
`completed_at` is set when DONE (idempotent).
`source_msg_id` is optional (tasks/subtasks) and reserved for Stage 3 dedup.
Ready for Stage 2 without refactor: module is isolated from legacy `items`.

P2 Stage 2 endpoints/commands:
GET /health, GET /p2/tasks?status=, GET /p2/tasks/{id}, GET /p2/tasks/{id}/subtasks, GET /p2/subtasks/{id}
Worker commands: cmd_create_task, cmd_create_subtask, cmd_complete_subtask, cmd_complete_task
Runtime mounts: worker must mount DB RW; organizer-api mounts DB RO.
Curl: `curl http://127.0.0.1:8000/p2/tasks` and `curl http://127.0.0.1:8000/p2/tasks/1/subtasks`
Smoke: `python - <<'PY'\nfrom worker import cmd_create_task, cmd_create_subtask\nr = cmd_create_task('T','NEW')\nprint(r)\nprint(cmd_create_subtask(r['id'],'S','NEW'))\nPY`

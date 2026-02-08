P2 Stage 1 runtime module uses separate SQLite tables: `tasks` and `subtasks`.
Depth is fixed at 1: a subtask always references a root task.
Subtasks never touch calendar; only `tasks.calendar_event_id` exists.
Canonical statuses: NEW, IN_PROGRESS, DONE, FAILED (aliases: INBOX/TODO -> NEW).
Invariant: a task cannot be completed if any subtask is not DONE.
Invariant: cannot create a subtask for DONE or FAILED task.
Flag `P2_ENFORCE_STATUS=1` raises exceptions; `0` logs violations and continues.
`completed_at` is set when DONE (idempotent).
`source_msg_id` is optional (tasks/subtasks) and enables idempotent create.
`source_msg_id` should be namespaced (e.g., `tg:<chat_id>:<message_id>`) to reduce collisions.
Ready for Stage 2 without refactor: module is isolated from legacy `items`.

P2 Stage 2 endpoints/commands:
GET /health, GET /p2/tasks?status=, GET /p2/tasks/{id}, GET /p2/tasks/{id}/subtasks, GET /p2/subtasks/{id}
Worker commands: cmd_create_task, cmd_create_subtask, cmd_complete_subtask, cmd_complete_task
Telegram: bot calls worker cmd_* via HTTP with source_msg_id `tg:<chat_id>:<message_id>` (idempotent).
Runtime mounts: worker must mount DB RW; organizer-api mounts DB RO.
Bot commands: `/done <task_id>` or `/done#<task_id>` complete task; `/sdone <subtask_id>` or `/sdone#<subtask_id>` complete subtask (strict, no extra text). Stage 6 confirmation: `✅ Готово: task #<id>` and `✅ Готово: subtask #<id>`.
Stage 7 list: `/list`, `/list open`, `/list <task_id>` (read-only via organizer-api).
Stage 8 help: `/help` (compact command reference).
Stage 9 paging: `/open`, `/list pN`, `/list open pN`.
Host: organizer-api exposed on `http://127.0.0.1:8101` (container still listens on 8000).
Internal: services call `http://organizer-api:8000`.
Curl: `curl http://127.0.0.1:8101/p2/tasks` and `curl http://127.0.0.1:8101/p2/tasks/1/subtasks`
Smoke: `python - <<'PY'\nfrom worker import cmd_create_task, cmd_create_subtask\nr = cmd_create_task('T','NEW')\nprint(r)\nprint(cmd_create_subtask(r['id'],'S','NEW'))\nPY`

P4 Regulations: `Docs/04_REGULATIONS.md`.

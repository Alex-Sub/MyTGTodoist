# Runtime Context v1.0

## 0. Scope
Документ фиксирует **актуальный runtime‑контур** проекта «Мой ежедневник органайзер» (P2/P3) и правила его эксплуатации.

Принципы:
- runtime развивается **без правок legacy `src/*`**;
- P2/P3 использует отдельные таблицы `tasks` и `subtasks` (не `items`);
- Telegram‑бот пишет в P2/P3 только через **HTTP‑команды organizer-worker**;
- organizer-api runtime — **read-only**, organizer-worker — **единственный writer** в SQLite.

---

## 1. Services

### 1.1 organizer-worker
**Role:** writer + orchestrator.

Функции:
- P2 runtime ops: create/complete task + subtask, инварианты, идемпотентность.
- P3 calendar side-effects: create/patch событий Google Calendar из задач.

DB: RW (`/data/organizer.db`)

Internal HTTP command server:
- `http://organizer-worker:8002`
- GET `/health`
- POST:
  - `/p2/commands/create_task`
  - `/p2/commands/create_subtask`
  - `/p2/commands/complete_task`
  - `/p2/commands/complete_subtask`
  - `/p2/commands/plan_task`

### 1.2 organizer-api (runtime)
**Role:** read-only API над P2/P3 таблицами.

DB: RO

Internal URL: `http://organizer-api:8000`  
Host URL: `http://127.0.0.1:8101`

Endpoints (GET):
- `/health`
- `/p2/tasks?status=`
- `/p2/tasks?source_msg_id=`
- `/p2/tasks/{id}`
- `/p2/tasks/{id}/subtasks`
- `/p2/subtasks?source_msg_id=`

### 1.3 telegram-bot
**Role:** UX-контур (Telegram интерфейс), без импорта кода worker.

Writes:
- только через `WORKER_COMMAND_URL` → `organizer-worker:8002`

Reads:
- только через `ORGANIZER_API_URL` → `organizer-api:8000`

---

## 2. Ports & URLs
- organizer-worker command server: **8002 (internal)**
- organizer-api runtime: **8000 (internal)**, **8101 (host)**

---

## 3. Env (canonical)
- `DB_PATH=/data/organizer.db`
- `WORKER_COMMAND_URL=http://organizer-worker:8002`
- `ORGANIZER_API_URL=http://organizer-api:8000`
- `TIMEZONE_NAME` / `TIMEZONE` (например `Europe/Moscow`)
- `LOCAL_TZ_OFFSET_MIN` (fallback для TZ)
- `P2_ENFORCE_STATUS=1|0` (raise vs log invariant violations)
- `GOOGLE_SERVICE_ACCOUNT_FILE=/data/google_sa.json`
- `GOOGLE_CALENDAR_ID=<calendar id>`

---

## 4. Data model (runtime)

### 4.1 tasks
Columns (runtime):
- `id` INTEGER PK
- `title` TEXT
- `status` TEXT (canonical: NEW / IN_PROGRESS / DONE / FAILED)
- `state` TEXT (P3 source of truth for lifecycle; см. ниже)
- `planned_at` TEXT NULL (ISO, UTC)
- `calendar_event_id` TEXT NULL (Google event id или NULL)
- `source_msg_id` TEXT NULL (idempotency key)
- `created_at`, `updated_at` TEXT (ISO UTC)
- `completed_at` TEXT NULL

### 4.2 subtasks
Columns (runtime):
- `id` INTEGER PK
- `task_id` INTEGER FK(tasks.id)
- `title` TEXT
- `status` TEXT (canonical)
- `source_msg_id` TEXT NULL
- `created_at`, `updated_at` TEXT (ISO UTC)
- `completed_at` TEXT NULL

---

## 5. Migrations (runtime)
- `010_tasks.sql` / `011_subtasks.sql`
- `012_tasks_completed_at.sql` / `013_subtasks_completed_at.sql`
- `016_tasks_state.sql` (adds `tasks.state`)
- `017_tasks_planned_at.sql` (adds `tasks.planned_at`)

---

## 6. Idempotency: source_msg_id
Canonical format:
- `tg:<chat_id>:<message_id>`

Semantics:
- `create_task` / `create_subtask` with same `source_msg_id` возвращает уже созданную запись.
- Hardening: collision across tasks for subtasks (`source_msg_id` reused with другой `task_id`) → invariant violation (raise/log по `P2_ENFORCE_STATUS`).

---

## 7. Telegram UX

### 7.1 Create
- Plain text → create_task(title=text)
- `#<task_id> <text>` → create_subtask(task_id, title=text)
- Voice → create_task(title=`voice:<file_unique_id>`)

### 7.2 Complete
- `/done <task_id>` или `/done#<task_id>` (strict, без хвоста)
- `/sdone <subtask_id>` или `/sdone#<subtask_id>` (strict, без хвоста)
Ответы:
- `✅ Готово: task #<id>`
- `✅ Готово: subtask #<id>`
- `⚠️ Нельзя завершить: есть незавершённые подзадачи.`

### 7.3 List / navigation
- `/list` — page 1, 10 items
- `/open` = `/list open`
- `/list open` — NEW + IN_PROGRESS (merge+dedup, desc by id)
- `/list <task_id>` — task + subtasks
- paging: `/list pN`, `/list open pN` (N=1..50)
Если пусто → `Пусто.`

### 7.4 Help
- `/help` — строгая справка (1 сообщение)

Implementation notes:
- strict regex, no trailing text for commands
- HTTP client fallback: `requests` → `urllib`
- timeout normalization helper `_timeout_seconds()`

---

## 8. P3: States & Calendar

### 8.0 FSM: Task state machine (canonical)
States (canonical):
- NEW
- PLANNED
- SCHEDULED
- DONE
- FAILED
- CANCELLED

Allowed transitions:
From | To | Trigger
--- | --- | ---
NEW | PLANNED | plan_task
PLANNED | SCHEDULED | calendar create/patch success
SCHEDULED | PLANNED | re-plan (plan_task)
SCHEDULED | DONE | complete_task
SCHEDULED | FAILED | terminal failure (reserved)
SCHEDULED | CANCELLED | cancel (P4; explicit tick)
PLANNED | DONE | complete_task
PLANNED | CANCELLED | cancel (P4; explicit tick)

Forbidden transitions:
- DONE / FAILED / CANCELLED → NEW / PLANNED / SCHEDULED
- Any Calendar → Task transitions

Notes:
- `tasks.state` is the single source of truth for task lifecycle.
- `planned_at` represents planning intent only.
- Calendar is a side-effect, managed explicitly by worker ticks.

### 8.1 P3: Calendar sync responsibilities
Responsibilities are explicitly split:

Task domain:
- Owns `tasks.state` and `planned_at`.
- Enforces terminal states.
- Has no knowledge of Calendar.

Calendar adapter:
- Performs side-effects: create / patch.
- Returns structured results (`ok`, `http_status`, `err`).
- Never mutates task state directly.

Sync ticks:
- `create_tick` — creates calendar event for PLANNED tasks.
- `update_tick` — updates calendar event for re-planned tasks.
- `cancel_tick` — NOT active in P3 (scaffold only).

Control flag:
- `CALENDAR_SYNC_MODE = off | create | full`
  - `off` — calendar disabled
  - `create` — `create_tick` only
  - `full` — `create_tick` + `update_tick` (P3 default)

Unknown values default to `full`.

### 8.2 P4: Cancel flow (explicit tick)
Status: DONE
Purpose:
Provide a safe, idempotent cancellation of calendar events for tasks in CANCELLED state.

Trigger conditions:
- `tasks.state == CANCELLED`
- `calendar_event_id IS NOT NULL`

Flow:
1. `cancel_tick` selects tasks matching trigger conditions.
2. Calendar adapter attempts to cancel/delete the event.
3. Result handling:
   - Success (2xx) → clear `calendar_event_id`.
   - Not found (404) → treated as success, clear `calendar_event_id`.
   - Any other error → no data change; retry in next tick.

Idempotency:
- Repeated cancel attempts are safe.
- 404 is not an error.
- Clearing `calendar_event_id` terminates the cancel loop.
Clearing `calendar_event_id` is the terminal step of cancel flow.

Data rules:
- `tasks.state` is NOT changed.
- `planned_at` is NOT changed.
- No new fields or migrations.

Execution control:
- `cancel_tick` runs only when `CALENDAR_SYNC_MODE == full`.

Guarantees:
- No Calendar → Task autologic.
- No implicit state transitions.
- All actions are explicit and observable.

### 8.3 Logging & observability
P3 log prefixes:
- `P3_CALENDAR_CREATE`
- `P3_CALENDAR_UPDATE`

P4 log prefix:
- `P4_CALENDAR_CANCEL`

Log semantics:
- `action=*` — attempt without state change
- `transition=*` — explicit task state transition

Required log fields:
- `task_id`
- `calendar_event_id` (when applicable)
- `state_from` / `state_to` (for transitions)
- `http_status`
- `err`

Logs are informational and do not affect control flow.

---

## 9. Status summary

P3:
- FSM formalized and enforced by convention.
- Calendar sync responsibilities explicit.
- No side-effects introduced.
- Status: DONE.

P4:
- Cancel flow designed and documented.
- Implementation in progress.
- Status: IN PROGRESS.

---

## 10. Compose / secrets hardening
- secrets mount uses **relative** path:
  - `./secrets/alexey/google_sa.json:/data/google_sa.json:ro`
- `.gitignore` excludes secrets (`secrets/**/*.json`, `.env*`, token/key patterns), keeps `.gitkeep`
- worker startup check warns if SA path missing/is dir (warn-only)

---

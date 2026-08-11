# MYTGTODOIST_RUNBOOK

Primary runbook for the current production contour.

## 1. Current Production Contour

- Telegram adapter: `telegram-bot/app-integration/*`
- worker write surface: `/runtime/*`
- read API: `organizer-api/app.py`
- Google sync service: `google-sync`
- single DB writer: `organizer-worker`

Do not use as active paths:

- `src/main.py`
- `src/api/*`
- `src/telegram/*`
- `telegram-bot/bot.py`
- `compat_worker_bridge`

## 2. Canonical Deploy

Preferred scripted deploy:

```powershell
.\deploy_v2.ps1 -Services @("telegram-bot","organizer-worker","organizer-api","google-sync")
```

Canonical build contract:

- every production image that needs shared repo code uses `build.context: .`
- every such service uses explicit `build.dockerfile`
- Dockerfiles use only root-relative `COPY`
- every image must contain `/app/build-info.json`
- `deploy_v2.ps1` runs preflight before build and post-deploy verification after recreate

Equivalent compose deploy:

```bash
docker compose -p deploy --env-file .env.prod -f docker-compose.yml -f docker-compose.vps.override.yml up -d --build
docker compose -p deploy --env-file .env.prod -f docker-compose.yml -f docker-compose.vps.override.yml ps
```

Historical fallback only:

- `deploy.ps1`

Deploy/source-of-truth model:

- local git/workspace is the deployable code source of truth
- `/opt/mytgtodoist` on VPS is a deployment mirror, not a git source repo
- canonical deploy path is:
  - local source tree
  - sync to `/opt/mytgtodoist`
  - build selected images on VPS
  - recreate selected containers
  - verify markers and build proof inside the running containers

## 3. Required Runtime Expectations

- Telegram adapter mode resolves to `runtime_core_direct`
- `ALLOW_LEGACY_WORKER_QUEUE` is not enabled in production
- `WORKER_COMMAND_URL` targets `organizer-worker /runtime/command`
- `ML_GATEWAY_URL` is reachable from the Telegram runtime
- `common/date_resolver.py` is present in `telegram-bot`, `organizer-worker`, and `google-sync` images
- relative date phrases must be canonicalized before worker commit; unresolved phrases must remain in clarification flow
- runtime routing order is:
  - cancel
  - active scenario continuation
  - calendar/timeblock/task routing
  - ambiguous create prompt
  - explicit memory fallback only
- generic task text such as `купить хлеб завтра` or `позвонить врачу` must route to `task.create`, not to memory fallback

## 4. Post-Deploy Baseline Checks

Mandatory proof after deploy:

- `docker inspect` for each selected container
- `/app/build-info.json` inside each selected container
- for `telegram-bot`, route markers inside `/app/app-integration/src/app/handler.py`
- `/system` must show:
  - `route_rules_version`
  - short `git_sha`
  - short `handler_sha256`
  - `build_timestamp_utc`

Expected `/app/build-info.json` fields:

- `service`
- `git_sha`
- `build_timestamp_utc`
- `route_rules_version`
- `handler_sha256`
- `dockerfile_path`
- `build_context`
- `entrypoint_module`

```bash
docker compose -p deploy --env-file .env.prod -f docker-compose.yml -f docker-compose.vps.override.yml ps
docker compose -p deploy --env-file .env.prod -f docker-compose.yml -f docker-compose.vps.override.yml logs --tail=200 telegram-bot
docker compose -p deploy --env-file .env.prod -f docker-compose.yml -f docker-compose.vps.override.yml logs --tail=200 organizer-worker
docker compose -p deploy --env-file .env.prod -f docker-compose.yml -f docker-compose.vps.override.yml logs --tail=200 organizer-api
```

Check for:

- bot startup in `runtime_core_direct`
- startup `build_info` log with `git_sha`, `route_rules_version`, `handler_sha256`
- worker health
- no accidental legacy-queue startup
- no repeated commit-path failures

Telegram smoke after verified deploy:

- `/system`
- `купить хлеб завтра`
- `позвонить врачу завтра`
- `позвонить врачу`

Expected duplicate guard proof:

- `купить хлеб завтра` may warn about a same-day duplicate
- `позвонить врачу` must warn about an undated duplicate as `в InBox` when the existing active root task is undated
- undated subtask duplicate wording must say `без срока`, not a fake date

Log proof:

```bash
docker compose -p deploy --env-file deploy/tenants/.env.alexey -f docker-compose.yml -f docker-compose.vps.override.yml logs --tail=300 telegram-bot | grep -E "telegram_text_route_entry|runtime_route_selected|memory_fallback_before_return|generic_task_fallback_selected|stage67-hardguard-v2"
```

## 5. HTTP Health Checks

```bash
curl http://127.0.0.1:8101/health
curl http://127.0.0.1:8102/health
curl http://127.0.0.1:8101/google/health
curl http://127.0.0.1:8101/sync/conflicts
```

## 6. Runtime Sheets Sync

Policy:

- runtime SQLite DB is the source of truth
- Google Sheets is a read model only
- Google Sheets is the canonical operational UI for Google-side task visibility
- no production path writes Google Sheets edits back into runtime DB
- `GOOGLE_SHEETS_SYNC_ENABLED=1` is the canonical selector for runtime Sheets sync
- legacy `items` / `full_bidir` Sheets sync is non-canonical and must stay disabled unless explicitly needed for historical debugging
- for Alexey tenant on VPS, the active env file is `deploy/tenants/.env.alexey`

Google Tasks status:

- Google Tasks sidebar is currently outside the active runtime sync contour
- existing Google Tasks entries are external data and are not expected to appear in Google Sheets
- runtime DB -> Google Tasks projection is deferred
- if implemented later, it must start as one-way projection from runtime DB
- automatic Google Tasks -> runtime DB sync must not be introduced without a separate review/apply design

Required env:

- `GOOGLE_SHEETS_SYNC_ENABLED=1`
- `GOOGLE_SHEETS_SPREADSHEET_ID=<spreadsheet id>`
- optional: `GOOGLE_SHEETS_SYNC_INTERVAL_SEC=300`
- optional: `GOOGLE_SHEETS_SYNC_DRY_RUN=1`
- optional: `GOOGLE_SHEETS_SYNC_USER_ID=<user id>`

Legacy opt-in only:

- `GOOGLE_SYNC_ALLOW_LEGACY_ITEMS_MODE=1`
- use only with an explicit migration/debug task; it is not part of the active runtime contour

Current exported tabs:

- `Задачи`
  - main task screen keyed by internal runtime `ID`
  - visible projection is mutually exclusive with `InBox`
  - contains planned tasks and non-triage tasks
  - hierarchy columns:
    - `№`
    - `Уровень`
    - `Родитель ID`
    - `Родитель`
  - current visible order:
    - `№`, `ID`, `Уровень`, `Родитель ID`, `Родитель`, `Задача`, `Статус`, `Состояние`, `План`, `Создано`, `Обновлено`, `Комментарий`, `Кол-во блоков времени`
  - `№` is the generated user-facing tree number
  - parent task is exported before its child where hierarchy exists
- `InBox`
  - inbox-like task slice keyed by internal runtime `ID`
  - intended for triage-only work
  - visible projection is mutually exclusive with `Задачи`
  - hierarchy columns:
    - `№`
    - `Уровень`
    - `Родитель ID`
    - `Родитель`
  - current safe filter:
    - not completed/cancelled
    - no `planned_at`
    - no `parent_task_id`
    - no linked time blocks
    - no child subtasks
  - same runtime task `ID` must not appear in both `Задачи` and `InBox`
- `Календарь`
  - unified calendar screen keyed by typed IDs
  - exports:
    - `Блок времени` from runtime `time_blocks`
    - `Событие календаря` from live Google Calendar read in the bounded window
    - `Встреча` when a safe runtime-known meeting reference is available
  - readable operator layout:
    - `Тип`, `Название`, `Дата`, `День недели`, `Начало`, `Конец`, `Время`, `Длительность, мин`, `Связанная задача`, `Комментарий`
    - technical columns are kept at the end:
      - `ID`
      - `Calendar Event ID`
      - `Источник`
  - rows are sorted by `Начало` ascending
  - user-facing date/time format remains `dd.mm.yyyy HH:MM`
  - typed IDs:
    - `timeblock:<id>`
    - `meeting_event:<calendar_event_id>`
    - `external_event:<google_event_id>`
  - bounded window defaults:
    - `GOOGLE_SHEETS_CALENDAR_PAST_DAYS=30`
    - `GOOGLE_SHEETS_CALENDAR_FUTURE_DAYS=60`
- `Логи`
  - exporter-side bounded sync log for recent runs

Canonical Sheets formatting:

- applied only to:
  - `Задачи`
  - `InBox`
  - `Календарь`
  - `Логи`
- formatting is usability-only:
  - freeze header row
  - enable basic filter
  - auto-resize columns when Google Sheets accepts the request
- canonical sortable date/datetime columns are written as real Sheets typed values
  - `Задачи`: `План`, `Создано`, `Обновлено`
  - `InBox`: `Создано`
  - `Календарь`: `Дата`, `Начало`, `Конец`
  - `Логи`: `Время`
- canonical number formats:
  - date: `dd.mm.yyyy`
  - datetime: `dd.mm.yyyy hh:mm`
- formatting failure is non-fatal and must not block data export
- system columns remain visible; exporter does not hide `ID` or other technical columns

Deprecated tabs no longer updated by the canonical exporter:

- `RuntimeTasks`
- `RuntimeTimeBlocks`

Workbook tab cleanup:

- cleanup preview:
  - `python -m src.google.runtime_sheets_sync --cleanup-tabs`
- destructive cleanup:
  - `python -m src.google.runtime_sheets_sync --cleanup-tabs --delete-deprecated`
- cleanup keeps only canonical tabs:
  - `Задачи`
  - `InBox`
  - `Календарь`
  - `Логи`
- cleanup refuses to run if any canonical tab is missing
- cleanup deletes by resolved `sheetId`, not by title text alone

Planned manual reverse sync:

- no automatic Sheets -> DB apply
- no direct SQLite write from Google integration code
- accepted changes must go through `organizer-worker /runtime/command`
- CLI-first proposal:
  - `python -m src.google.runtime_sheets_reverse_sync --dry-run`
  - `python -m src.google.runtime_sheets_reverse_sync --review`
  - `python -m src.google.runtime_sheets_reverse_sync --review --output /data/sheets_review.json`
- current expected result is a review artifact, not a mutation
- after explicit approval, a later apply command should call worker runtime commands and then re-export DB state back to Sheets
- explicit absolute `dd.mm.yyyy` and `dd.mm.yyyy hh:mm` values stay authoritative during reverse sync; only supported relative phrases should be normalized through the shared Date Resolver
- current `--review` behavior:
  - prints a Russian human-readable summary grouped by sheet, entity, and status
  - prints machine-readable JSON in the same run
  - can save JSON with `--output`
  - returns exit code `0` for no changes, `1` for proposed/confirm-required changes, `2` for conflict/invalid, `3` for runtime/tool error
  - new task rows in `Задачи` / `InBox` are detected as `proposed_create`
    - valid shape: blank `ID`, non-empty `Задача`
    - optional valid `Родитель ID` turns the create proposal into a subtask create proposal
    - if the new row duplicates an active runtime task by normalized title + same `parent_task_id` scope:
      - same planned day for dated rows
      - `planned_at IS NULL` for undated rows
      review marks it as `confirm_required`
    - manual edits to `№` are ignored and overwritten by the next export
    - summary labels them as `Новая задача`
    - unknown non-empty `ID` remains invalid in MVP
    - invalid `Родитель ID` remains invalid in MVP

Manual Sheets Apply Flow:

- current safe commands:
  - `python -m src.google.runtime_sheets_reverse_sync --review --output /data/sheets_review.json`
  - `python -m src.google.runtime_sheets_reverse_sync --apply /data/sheets_review.json --change-id chg-00001`
  - `python -m src.google.runtime_sheets_reverse_sync --apply review.json --all-proposed`
- preferred operator wrapper on Windows:
  - `.\scripts\sheets_review_apply.ps1 -Mode review`
  - `.\scripts\sheets_review_apply.ps1 -Mode apply -ChangeId chg-00001`
  - default persistent artifact path: `/data/sheets_review.json`
  - optional: `.\scripts\sheets_review_apply.ps1 -Mode review -OutputPath /data/review.json`
- run the CLI through the `google-sync` service container, not through `organizer-api`
- `/data/...json` is a path inside the `google-sync` container backed by persistent volume storage
- do not type placeholder angle brackets literally in shell commands; replace `chg-00001` with a real `change_id` from the review output
- if review shows `total_changes=0`, do not run apply
- apply must read the review artifact, not re-interpret arbitrary sheet state ad hoc
- current safe scope is task-only:
  - `Задача` -> worker `task.update`
  - `Комментарий` -> worker `task.comment.update`
  - `Статус` -> worker `task.set_status`
  - new task row -> worker `task.create`
    - blank `ID` row is created only by explicit `-ChangeId`
    - if review payload contains valid `parent_task_id`, create runs as subtask create
    - duplicate create protection:
      - worker is the authoritative final guard
      - same active title + same planned day + same `parent_task_id` scope triggers duplicate confirmation
      - explicit override is allowed only after duplicate confirmation
    - after successful create, the consumed blank-`ID` proposal row is deleted from Sheets only if row metadata still matches the reviewed proposal
    - optional post-create follow-ups:
      - `Комментарий` -> worker `task.comment.update`
      - `Статус` -> worker `task.set_status`
- apply must reject `conflict` and `invalid`
- `confirm_required` remains blocked
- calendar / timeblock / meeting apply remains blocked
- unsupported task fields such as `План` / `Приоритет` remain blocked
- wrapper script does not expose bulk apply by default; `-Mode apply` requires explicit `-ChangeId`
- wrapper script keeps review artifacts under `/data` by default so review/apply can reuse the same file across `docker compose run --rm`
- wrapper script prints:
  - review artifact path
  - saved review change counters
  - apply artifact path used
- `--all-proposed` must not auto-apply `proposed_create`
- apply must call worker runtime endpoints, never SQLite directly
- after successful apply, runtime DB must be exported back to Sheets and the result must appear in `Логи`

Task create date persistence:

- if Telegram/runtime summary shows a due date for `task.create`, worker persistence must store canonical `planned_at`
- if parse/runtime produced only `due_date`, runtime must mirror it to `planned_at` before commit
- undated task create must reach worker with canonical `planned_at = null`
- duplicate protection for `task.create` is worker-authoritative and uses:
  - dated key: normalized title + persisted planned day + same `parent_task_id` scope
  - undated key: normalized title + `planned_at IS NULL` + same `parent_task_id` scope
- root undated duplicate warnings are framed as `в InBox`
- undated subtask duplicate warnings are framed as `без срока`
- task comments are part of task-create UX/persistence/export, but not part of the duplicate key
- `DONE` / `CANCELLED` / `CANCELED` / `ARCHIVED` tasks do not block
- review/apply reporting should show per-change `applied`, `skipped`, and `error`
- consumed blank-`ID` cleanup safety:
  - only for `Задачи` / `InBox`
  - only if reviewed `source_sheet_row`, `source_row_hash`, and row values still match
  - never delete rows with non-empty `ID`
  - cleanup failure does not roll back the created runtime task

Runtime subtask foundation:

- implemented in active runtime DB / worker / read API
- active DB shape:
  - nullable `parent_task_id` on active `tasks`
  - foreign-key style linkage to another task row
- active runtime rules:
  - root task: `parent_task_id = null`
  - subtask: `parent_task_id = <task id>`
  - parent must exist
  - task cannot be its own parent
  - no cycles
  - `parent_task_id` may reference any existing task, including another subtask
- active worker commands:
  - `task.create` may include `parent_task_id`
  - `task.parent.update` safely changes the parent task
- active read API may expose:
  - `parent_task_id`
  - `parent_title`
  - `level`
  - `depth`
- current Sheets scope:
  - `Задачи` / `InBox` already export `№`, `Уровень`, `Родитель ID`, `Родитель`
  - `№` is generated from recursive tree order for readability
  - `ID` stays the technical sync identity
  - `Родитель ID` stays the technical parent link
  - parent row first, subtasks directly below where possible
  - new blank-ID row with valid `Родитель ID` -> create proposal only
  - existing row `Родитель ID` change -> `confirm_required`
  - display field `Родитель` is non-editable
- parent reassignment from Sheets still stays blocked from apply unless a separate task approves it

Google Sheets Manual Review Button:

- safe purpose: trigger operator review flow only
- menu item label: `Проверить изменения`
- current MVP recommendation: Option B
  - Apps Script does not call VPS
  - Apps Script only shows instructions for operator review
  - no new server exposure
- required message:
  - `Проверка запущена. Для применения изменений используйте operator script with ChangeId.`
- hard limits:
  - no DB writes
  - no apply call
  - no bulk apply
  - no calendar / timeblock / meeting apply

Future integration options:

- Option A:
  - Apps Script calls a protected VPS endpoint that runs review
  - allowed later only with auth token, audit logs, and explicit review-only endpoint
- Option B:
  - Apps Script only shows instructions / local operator command
  - recommended MVP because it adds no server exposure
- Option C:
  - Telegram command triggers review and returns summary
  - now available as read-only `/sheets_review`
  - uses internal `google-sync` review summary endpoint at `http://google-sync:8010/review`
  - health endpoint: `http://google-sync:8010/health`
  - does not apply changes
  - operator still applies only through `.\scripts\sheets_review_apply.ps1 -Mode apply -ChangeId chg-00001`

Verified VPS review endpoint checks:

- `http://127.0.0.1:8010/health`
- `http://127.0.0.1:8010/review?limit=5`

Apps Script template:

```javascript
function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu('MyTGTodoist')
    .addItem('Проверить изменения', 'showManualReviewInstructions')
    .addToUi();
}

function showManualReviewInstructions() {
  var message = [
    'Проверка запущена. Для применения изменений используйте operator script with ChangeId.',
    '',
    'Безопасный MVP:',
    '1. Запустите review локально или на VPS:',
    '   .\\scripts\\sheets_review_apply.ps1 -Mode review',
    '2. Посмотрите change_id в review output.',
    '3. Применяйте только одну выбранную задачу:',
    '   .\\scripts\\sheets_review_apply.ps1 -Mode apply -ChangeId chg-00001',
    '',
    'Ограничения:',
    '- кнопка не применяет изменения',
    '- кнопка не пишет в БД',
    '- calendar/timeblock/meeting apply заблокирован'
  ].join('\\n');

  SpreadsheetApp.getUi().alert('MyTGTodoist Review', message, SpreadsheetApp.getUi().ButtonSet.OK);
}
```

Reverse-sync review should classify:

- unchanged -> ignored
- editable and valid -> proposed
- valid but risky -> confirm required
- stale or competing change -> conflict
- malformed or unsupported -> invalid

MVP editable fields:

- `Задачи` / `InBox`: `Задача`, `Статус`, `Приоритет`, `План`, `Комментарий`
- `Календарь`: `Начало`, `Конец` or `Длительность, мин`, `Название` / comment, `Связанная задача` when mapping is safe

MVP non-editable fields:

- `ID`
- `Версия`
- `Изменено в базе`
- `Calendar Event ID`
- technical source fields

Manual dry-run:

```bash
docker compose -p deploy --env-file deploy/tenants/.env.alexey -f docker-compose.yml -f docker-compose.vps.override.yml run --rm google-sync \
  python -m src.google.runtime_sheets_sync --dry-run
```

Manual apply:

```bash
docker compose -p deploy --env-file deploy/tenants/.env.alexey -f docker-compose.yml -f docker-compose.vps.override.yml run --rm google-sync \
  python -m src.google.runtime_sheets_sync
```

Single-user dry-run when runtime schema supports `time_blocks.user_id`:

```bash
docker compose -p deploy --env-file deploy/tenants/.env.alexey -f docker-compose.yml -f docker-compose.vps.override.yml run --rm google-sync \
  python -m src.google.runtime_sheets_sync --dry-run --user-id u-smoke
```

## 7. Worker Helper Checks

```bash
curl -X POST http://127.0.0.1:8102/runtime/task/search \
  -H "Content-Type: application/json" \
  -d '{"user_id":"u-smoke","target_hint":"test"}'
```

## 8. Telegram Functional Checks

Minimum smoke sequence:

1. `задача на завтра выдели время на нее`
2. `15`
3. `30`
4. `Комментарий`
5. `купить молоко`
6. confirm

Expected:

- flow remains `timeblock.create`
- comment update rerenders summary
- no fallback `Не нашел подходящую задачу.` after comment input
- command can complete without an existing matched task because optional linkage is allowed

Also verify explicit task-link/search flow separately:

- explicit unmatched task search should still return `Не нашел подходящую задачу.`

Duplicate-create smoke for undated tasks:

1. ensure there is an active undated root task `Позвонить врачу`
2. send `позвонить врачу`

Expected:

- Telegram shows duplicate warning before the normal create summary
- warning text says `Похоже, такая задача уже есть в InBox:`
- response offers `Создать ещё одну?`
- worker/runtime logs include:
  - `task_duplicate_precheck_start`
  - `planned_day=null`
  - `undated_scope=true`
  - `candidate_count`
  - `duplicate_found=true`

Live Telegram Sheets review smoke:

1. send `/sheets_review`

Expected response:

- `Проверка Google Sheets`
- `Всего изменений: ...`
- `Предложено: ...`
- `Нужно подтверждение: ...`
- `Конфликт: ...`
- `Некорректно: ...`

Also verify:

- if changes exist, the reply includes:
  - `.\scripts\sheets_review_apply.ps1 -Mode apply -ChangeId ...`
- no apply buttons are shown
- no mutation happens from Telegram

Verification note:

- in-container formatting for `/sheets_review` was verified on VPS
- one real inbound Telegram message still requires this manual operator check

## 9. Unified Edit-Flow Checks

Run one `meeting.update` and one `timeblock.update` scenario.

Verify:

- date, time, duration, and comment edits work
- duration quick picks `15/30/45/60` work
- summary rerenders before commit
- worker commit succeeds

## 10. Critical Logs

Telegram/runtime:

- `temporal_edit_*`
- callback stale/consumed logs

Worker:

- `runtime_command_in`
- `runtime_intent_dispatch_start`
- `runtime_intent_dispatch_result`
- `runtime_commit_path_result`

Runtime Sheets sync:

- `runtime_sheets_sync scheduler started`
- `runtime_sheets_sync tab=Задачи ...`
- `runtime_sheets_sync tab=InBox ...`
- `runtime_sheets_sync tab=Календарь ...`
- `runtime_sheets_sync done ... created= updated= skipped= stale=`

Planned reverse-sync review logs:

- reverse-sync start / source-sheet scan
- proposed change counts by status
- conflict / invalid summaries
- worker apply result for explicitly confirmed changes only
- no worker apply log should exist during pure `--review`

## 11. Incident Notes

Escalate if any of these repeat:

- `legacy_route_used`
- `runtime_commit_path_result ... ok=false`
- `google/health` consistently down
- unexpected task-search fallback during allocation/comment edit flow
- worker booting with legacy queue enabled

## 12. Related Documents

- `docs/architecture/CURRENT_PRODUCTION_ARCHITECTURE.md`
- `docs/operations/OPERATOR_SMOKE_CHECK.md`
- `docs/STARTUP_RUNBOOK_VERIFIED.md`

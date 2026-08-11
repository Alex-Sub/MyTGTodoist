# APP_RUNTIME_CONTRACT

Contract for the active MyTGTodoist app runtime.

## 1. Responsibility Split

ML layer:

- interprets user input
- returns structured parse output and retrieval results
- remains semantic source-of-truth for intent meaning

App runtime:

- owns clarification, confirmation, continuation, and edit-flow routing
- owns local session persistence and local command dedup lineage
- decides when a command is executable
- forwards executable commands to worker

Worker:

- owns final mutation
- owns persistent idempotency and trace dedup
- owns calendar side effects and sync-conflict writes

## 2. Active Runtime Mode

- active Telegram mode: `runtime_core_direct`
- compat path is retired from the active contour

Routing order:

1. cancel
2. active scenario continuation
3. calendar/timeblock/task routing
4. ambiguous create prompt
5. explicit memory fallback only

Routing implications:

- generic action phrases such as `купить хлеб завтра` and `позвонить врачу завтра` must route to `task.create`
- undated action phrases such as `позвонить врачу` must also route to `task.create`
- undated root tasks may later project into `InBox`
- memory fallback must not absorb generic task-like action text before task routing has run

## 3. Active Session Model

One active scenario per logical context:

- `(app_id, tenant_id, channel, chat_id, user_id)`

Continuation storage:

- local runtime DB in `telegram-bot/app-integration`
- `clarification_sessions`

## 4. Core Clarification Families

Create flow:

- `start_at_date`
- `start_at_time`
- `duration_minutes`
- `start_at_date_past_confirm`
- `temporal_commit_confirm`
- `task_create_confirm`
- `task_create_duplicate_confirm`
- `task_create_edit`

Update/edit flow:

- `temporal_edit_field`
- `meeting_update_*`
- `timeblock_update_*`
- `sync_conflict_resolution`

## 5. Temporal Contract

Temporal create family includes:

- `timeblock.create`
- `meeting.create`

Rules:

- clarification order is `date -> time -> duration`
- time-only input does not satisfy date
- parser-inferred date is not treated as explicit confirmed user date if the user did not actually provide it
- past-date requires explicit confirmation

Summary gate:

- temporal commands must stop at `temporal_commit_confirm` before commit

Deterministic date normalization:

- `common/date_resolver.py` is the single deterministic date normalization layer for the active contour
- ML extracts temporal entities and raw phrases, but does not finalize relative dates
- app runtime must normalize supported Russian date phrases before summary rendering, duplicate precheck, and worker handoff
- worker must apply the same resolver as a safety net before mutation
- runtime and worker must not persist raw relative date phrases such as `сегодня`, `завтра`, `послезавтра`, or weekday-only phrases when the resolver can canonicalize them
- unresolved phrases such as policy-dependent ranges must stay in clarification flow instead of reaching commit

## 6. Allocation-Phrase Contract

Allocation phrases such as:

- `выдели время на задачу`
- `задача на завтра выдели время на нее`

must resolve to `timeblock.create`.

Current required behavior:

- free-text comment/title is allowed
- existing task linkage is optional
- comment edit must rerender summary and stop
- task search fallback must not run unless the active flow is explicitly task search/link selection

Implementation marker:

- optional task hints are carried as `task_ref_optional`

## 7. Backing Task Contract

Because the active runtime schema still requires `time_blocks.task_id`, worker may create a backing task automatically when a timeblock is created without a resolved concrete task link.

Rules:

- this is allowed only for optional-linkage flows
- explicit task-link/search flows must still surface not-found or disambiguation behavior
- backing-task creation is a worker-side execution detail, not a Telegram UX requirement

## 8. Unified Edit-Flow Contract

Meeting and timeblock update flows must remain behaviorally aligned for:

- date edit
- time edit
- duration edit
- comment edit
- date+time edit
- stale callback guards
- summary rerender and final confirmation

Observability:

- runtime emits `temporal_edit_*` logs for field selection, value receipt, summary rerender, cancellation, and confirmation

## 9. Telegram Button Contract

Buttons are transport UI only.

Supported families:

- past-date yes/no
- temporal confirm yes/no
- task-create create/edit/cancel
- task-create duplicate yes/no
- duration quick choices `15/30/45/60`
- comment keep/clear

Requirement:

- callbacks and typed replies must run through the same logical continuation behavior

## 10. Execution Boundary

Worker `/runtime/command` may be called only after:

- required fields are complete
- relevant confirmation state is satisfied
- no blocking sync-conflict branch remains unresolved

## 11. Worker Runtime Side Effects

Worker helper endpoints used by app runtime:

- `/runtime/meeting/latest`
- `/runtime/meeting/search`
- `/runtime/task/search`
- `/runtime/timeblock/search`
- `/runtime/sync_conflict/action`

Worker commit-path logging must include:

- `runtime_commit_path_result`

## 12. Manual Sheets Reverse-Sync Contract

Reverse sync from Google Sheets is allowed only as a manual review-and-confirm flow.

Hard rules:

- runtime SQLite DB remains the source of truth
- no direct Google Sheets -> SQLite write path is allowed
- no accepted change may bypass `organizer-worker /runtime/command`
- reverse sync starts as an operator CLI workflow, not as an automatic scheduler path

Google Tasks boundary:

- Google Tasks is not part of the active runtime sync contract today
- Google Tasks sidebar state is external and may differ from runtime DB and Google Sheets
- Google Sheets remains the canonical operational UI on the Google side
- any future Google Tasks integration must start as one-way `runtime DB -> Google Tasks`
- no automatic `Google Tasks -> runtime DB` synchronization may be added without a separate review/apply contract

Google Sheets typed date boundary:

- canonical Sheets tabs may store date/datetime cells as real Sheets typed values
- display format must stay user-facing:
  - date: `dd.mm.yyyy`
  - datetime: `dd.mm.yyyy hh:mm`
- reverse sync continues to compare/read formatted user-facing values
- typed cell storage must not change the review/apply guardrails
- reverse sync may normalize supported date fields through the shared Date Resolver, but must not reinterpret already explicit absolute `dd.mm.yyyy` / `dd.mm.yyyy hh:mm` values incorrectly

Canonical MVP trigger:

- `python -m src.google.runtime_sheets_reverse_sync --dry-run`
- `python -m src.google.runtime_sheets_reverse_sync --review`

Required flow:

1. read current Sheets tabs by internal row identity
2. read current runtime DB snapshot
3. compare sheet rows to DB rows
4. build proposed field-level changes
5. validate and classify each change
6. create a review result
7. require explicit user/operator confirmation
8. apply only confirmed changes through worker runtime commands
9. export runtime DB state back to Sheets

Required classification:

- `ignored`: unchanged or irrelevant
- `proposed`: editable and valid
- `proposed_create`: valid new task row from `Задачи` / `InBox`
- `confirm_required`: valid but needs an explicit decision
- `conflict`: DB and sheet diverged or calendar/runtime conflict exists
- `invalid`: malformed or unsupported

MVP editable fields:

## 13. Task Create Date Canonicalization

Rules:

- `task.create` supports `title`, canonical `planned_at`, optional `parent_task_id`, and optional `comment`
- task comments are meaningful user data and must survive summary, persistence, and export
- `task.create` confirmation may show a user-facing due date, but worker persistence must use canonical `planned_at`
- if parser/runtime produces `due_date` without `planned_at`, runtime must mirror it into `planned_at` before worker commit
- if a task is undated, runtime bridge and app runtime must pass canonical `planned_at = null`
- worker `task.create` must treat `planned_at` as the persisted source-of-truth and may use `due_date/date/when` only as fallback normalization inputs
- duplicate detection for `task.create` is worker-authoritative and uses active-task-only matching
- dated duplicate key: same normalized title + same parent scope + same planned day
- undated duplicate key: same normalized title + same parent scope + `planned_at IS NULL`
- blank and `null` `parent_task_id` are the same root scope
- terminal tasks such as `DONE`, `CANCELLED` / `CANCELED`, and `ARCHIVED` do not block create
- task comment is not part of the duplicate key
- Telegram/runtime duplicate precheck must surface both dated and undated duplicate warnings before the normal task-create summary
- undated duplicate wording must not invent a date:
  - root scope -> `Похоже, такая задача уже есть в InBox:`
  - subtask scope -> `Похоже, такая подзадача уже есть без срока:`

Observability:

- `task_create_date_resolution`
  - `raw_text`
  - `extracted_date`
  - `planned_at`
  - `persisted_planned_at`
- `task_duplicate_precheck_start`
  - `planned_day`
  - `undated_scope`
- `task_duplicate_precheck_result`
  - `candidate_count`
  - `duplicate_found`
  - `planned_day`
  - `undated_scope`

## 14. Sheets Task Projection Rule

User-facing Google Sheets tabs are mutually exclusive visible projections:

- `Задачи`
  - planned tasks
  - non-triage tasks
  - hierarchy/main list view
- `InBox`
  - undated/unparsed triage tasks only

Hard rule:

- the same runtime task `ID` must not appear in both visible working tabs at the same time
- duplicate checks for root undated task creation must treat `InBox` as the user-facing projection for active root tasks with `planned_at IS NULL`

`Задачи` / `InBox`

- `Задача`
- `Статус`
- `Приоритет` when runtime schema supports it
- `План`
- `Комментарий`
- `Родитель ID`
  - blank `ID` + valid `Родитель ID` -> `proposed_create` for subtask creation
  - existing row `Родитель ID` change -> `confirm_required`
- new row creation in MVP:
  - blank `ID`
  - non-empty `Задача`
  - task-only, no calendar/timeblock creation
  - if a blank-`ID` task row duplicates an existing active runtime task by normalized title + same `parent_task_id` scope:
    - same planned day for dated rows
    - `planned_at IS NULL` for undated rows
    review must classify it as `confirm_required`, not `proposed_create`
- hierarchy display-only fields:
  - `№`
  - `Уровень`
  - `Родитель`
  - `№` is generated from tree order and is overwritten on export
  - these are not editable mutation fields

`Календарь`

- read-only unified calendar projection
- row types may include:
  - `Блок времени` from runtime `time_blocks`
  - `Встреча` when a safe runtime-known meeting reference is available
  - `Событие календаря` from live Google Calendar read in the bounded window
- calendar row IDs are typed:
  - `timeblock:<id>`
  - `meeting_event:<calendar_event_id>`
  - `external_event:<google_event_id>`
- `Начало`
- `Конец` or `Длительность, мин`
- `Название` / comment when mapped safely to a supported runtime command field
- `Связанная задача` only when entity mapping is safe and resolvable

Non-editable fields:

- `ID`
- `Версия`
- `Изменено в базе`
- `Calendar Event ID`
- technical source / lineage fields

Required system columns for reverse-sync-capable sheets:

- `entity_id`
- `entity_type`
- `db_updated_at`
- `exported_at`
- `row_hash`
- `sync_status`
- `sync_error`
- `last_review_id`

Review output shape:

- `change_id`
- `sheet`
- `entity_type`
- `entity_id`
- `field`
- `db_value`
- `sheet_value`
- `status`
- `question`

Representative review questions:

- `В таблице встреча перенесена на 18.05.2026 15:00, но это время занято. Что оставить?`
- `Задача изменилась и в базе, и в таблице. Какую версию оставить?`

Conflict and validation rules:

- DB changed after the last export -> `conflict`
- unknown `ID` -> `invalid` in MVP unless a future create-proposal mode is explicitly enabled
- required field missing -> `invalid`
- invalid date/time -> `invalid`
- calendar slot busy -> `conflict`
- target task or calendar entity not found -> `conflict`
- row cleared or deleted in Sheets -> `confirm_required`, never implicit delete
- unsupported column change -> `invalid`

Comparison-layer test plan:

- unchanged row ignored
- changed editable field becomes proposed change
- changed non-editable field is rejected
- stale DB version becomes conflict
- invalid date becomes invalid
- unknown ID becomes invalid in MVP

## 15. Manual Sheets Apply Flow

Manual apply is partially implemented for task-only review artifacts.

Proposed CLI shape:

- `python -m src.google.runtime_sheets_reverse_sync --review --output /data/sheets_review.json`
- `python -m src.google.runtime_sheets_reverse_sync --apply /data/sheets_review.json --change-id chg-00001`
- `python -m src.google.runtime_sheets_reverse_sync --apply review.json --all-proposed`
- run these commands through the `google-sync` service container, not through `organizer-api`
- `/data/...json` is a persistent container-side artifact path
- if review returns `total_changes=0`, do not run apply
- placeholder examples must be replaced with a real `change_id`; do not type angle brackets literally

Hard apply rules:

- no direct SQLite write is allowed
- every accepted change must become a runtime-safe worker call
- apply must use `organizer-worker /runtime/command` or an existing runtime-safe endpoint for the target entity
- `conflict` and `invalid` changes are never auto-applied
- `confirm_required` changes stay blocked unless a future explicit override mode is introduced

Current first safe apply scope:

- implemented for task-only changes from review artifacts
- allowed now:
  - `Задача` -> `task.update`
  - `Комментарий` -> `task.comment.update`
  - `Статус` -> `task.set_status`
  - `proposed_create` task row -> `task.create`
  - duplicate create protection:
    - worker is the authoritative final guard
    - duplicate match is active-task only
    - match key is normalized title + same `parent_task_id` scope plus:
      - same planned day for dated tasks
      - `planned_at IS NULL` for undated tasks
    - `DONE` / `CANCELLED` / `ARCHIVED` tasks do not block
    - task comment is not part of the duplicate key
    - no duplicate create is applied automatically
    - explicit override is allowed only after duplicate confirmation
    - valid review payload may include `parent_task_id`
    - optional follow-up `Комментарий` -> `task.comment.update`
    - optional follow-up `Статус` -> `task.set_status`
    - successful create may trigger safe cleanup of the consumed blank-`ID` proposal row in Google Sheets
- still blocked:
  - `План`
  - `Приоритет`
  - all `Календарь` / timeblock / meeting changes
- all `confirm_required`, `conflict`, and `invalid` review entries
  - `proposed_create` is explicit `change-id` only and is excluded from `--all-proposed`

Proposed apply flow:

1. load `/data/sheets_review.json`
2. validate that the review artifact format is complete and not empty
3. select either:
   - one `change_id`
   - multiple explicit `change_id`
   - `--all-proposed`
4. reject any selected change with status `conflict` or `invalid`
5. reject `confirm_required` unless a future explicit override contract is defined
6. translate each accepted review change into a runtime command payload
7. call worker runtime endpoints
8. collect per-change apply result: `applied`, `skipped`, `error`
9. rerun runtime DB -> Sheets export
10. append exporter/apply result into `Логи`
11. require the next review to reflect the post-apply DB state

Suggested runtime command mapping:

- task field changes -> worker `task.update` style command envelope through `/runtime/command`
- calendar timeblock field changes -> worker `timeblock.update` style command envelope through `/runtime/command`
- unsupported meeting changes remain blocked until a canonical meeting read model and command mapping exist

Current implemented CLI:

- `python -m src.google.runtime_sheets_reverse_sync --review --output /data/sheets_review.json`
- `python -m src.google.runtime_sheets_reverse_sync --apply /data/sheets_review.json --change-id chg-00001`
- `python -m src.google.runtime_sheets_reverse_sync --apply review.json --all-proposed`

Current apply result fields:

- `change_id`
- `entity_type`
- `entity_id`
- `created_task_id`
- `field`
- `status_before`
- `apply_status`
- `cleanup_status`
- `cleanup_error`
- `worker_result`
- `error`

Consumed blank-ID proposal row cleanup:

- applies only to successful `proposed_create` task rows
- cleanup uses review artifact metadata:
  - `source_sheet_row`
  - `source_row_hash`
  - `source_row_values`
- before deleting a row, apply must re-read it and verify:
  - sheet is `Задачи` or `InBox`
  - `ID` is still blank
  - `row_hash` still matches the reviewed proposal row
  - key fields such as title/comment/parent still match
- cleanup must never delete:
  - rows with non-empty `ID`
  - changed rows after review
  - `Календарь` rows
  - rows without cleanup metadata
- cleanup failure must not roll back a successfully created runtime task

Required apply result shape:

- `review_id`
- `change_id`
- `entity_type`
- `entity_id`
- `status`
- `runtime_command`
- `error`

Apply-stage test plan:

- selected proposed change builds the correct runtime command
- conflict change cannot be applied
- invalid change cannot be applied
- `--all-proposed` excludes `confirm_required`, `conflict`, and `invalid`
- no direct DB write exists in the apply path
- Sheets export is triggered after successful apply

## 16. Runtime Subtasks Contract

Subtasks are implemented in the active runtime foundation. Sheets parent reassignment apply remains blocked.

Runtime model:

- nullable `parent_task_id` on active `tasks`
- root task: `parent_task_id = null`
- subtask: `parent_task_id` references another task row

Required invariants:

- parent task must exist
- no cycles
- multi-level nesting is allowed
- parent completion/archive behavior must be explicit, never implicit

Current Sheets model:

- `Задачи` / `InBox` export:
  - `№`
  - `Уровень`
  - `Родитель ID`
  - `Родитель`
- `№` is user-facing and generated from recursive tree traversal
- `ID` stays the technical task identity
- `Родитель ID` stays the technical parent link
- sort parent first, then subtasks directly below parent, using `created_at ASC, id ASC` for roots and siblings

Current reverse-sync rules:

- new row with valid `Родитель ID` -> create proposal for subtask
- missing/unknown `Родитель ID` -> `invalid`
- changing `Родитель ID` for an existing task -> `confirm_required`
- editing `№` is ignored and overwritten by the next export
- editing `Родитель` display field -> `invalid`
- deleting or archiving parent with children -> blocked or `confirm_required`

Current apply boundary:

- explicit `ChangeId` create may call worker `task.create` with `parent_task_id`
- parent reassignment is not auto-applied
- no direct Sheets -> SQLite path exists

## 17. Telegram Sheets Review Command

Read-only operator command:

- Telegram command: `/sheets_review`
- purpose: show a compact summary of pending Google Sheets reverse-sync changes
- scope: review only
- no apply from Telegram
- no direct SQLite writes
- no worker runtime command calls
- no Sheets writes

Execution path:

1. Telegram adapter intercepts `/sheets_review` locally in `bot_main.py`
2. adapter calls the internal read-only `google-sync` review summary endpoint
3. `google-sync` runs the same review logic as `python -m src.google.runtime_sheets_reverse_sync --review`
4. adapter returns a compact Russian summary to the Telegram operator

Current internal endpoint shape:

- service: `google-sync`
- URL: `http://google-sync:8010/review?limit=N`
- health: `http://google-sync:8010/health`
- verified VPS local check:
  - `http://127.0.0.1:8010/health`
  - `http://127.0.0.1:8010/review?limit=5`
- response:
  - `summary.total_changes`
  - `summary.proposed`
  - `summary.confirm_required`
  - `summary.conflict`
  - `summary.invalid`
  - first `N` change records

Telegram response requirements:

- show:
  - total changes
  - proposed
  - confirm required
  - conflict
  - invalid
  - first few changes
- if changes exist, instruct operator to use:
  - `.\scripts\sheets_review_apply.ps1 -Mode apply -ChangeId <id>`

Blocked from Telegram:

- apply actions
- bulk apply
- calendar / timeblock / meeting apply
- any mutation path

Verification note:

- in-container Telegram formatting for `/sheets_review` was verified on VPS
- one live inbound Telegram message still requires a manual operator smoke check after deploy

## 18. Runtime Task Hierarchy Contract

Active runtime foundation now supports root tasks and multi-level subtasks inside the `tasks` table.

Schema boundary:

- runtime DB remains the source of truth
- active `tasks` has nullable `parent_task_id`
- `parent_task_id` references another runtime task row

Rules:

- root task: `parent_task_id = null`
- subtask: `parent_task_id = <task id>`
- parent task must exist
- task cannot be its own parent
- cycles are rejected
- root task = level 1
- subtask = level 2
- nested subtask = level 3+
- `parent_task_id` may reference any existing task row, including another subtask

Worker contract:

- `task.create` may include optional `parent_task_id`
- `task.create` may include optional `duplicate_check_override=true` only after an explicit duplicate-confirm step
- `task.parent.update` changes parent linkage through worker validation
- parent reassignment remains a runtime command concern; no direct DB write path is allowed outside worker

Read-model contract:

- task read endpoints may expose:
  - `parent_task_id`
  - `parent_title`
  - `level`
  - `depth`
  - generated Sheets tree number `№`

Guardrails:

- parent completion is not inferred from child completion
- parent/child delete or archive cascading is not automatic
- Sheets reverse-sync may use hierarchy only through review/apply later; no direct Sheets -> SQLite path is allowed

## 19. Historical / Removed

Not part of the active contract:

- `compat_worker_bridge`
- `src/main.py`
- `src/api/*`
- `src/telegram/*`
- `telegram-bot/bot.py`

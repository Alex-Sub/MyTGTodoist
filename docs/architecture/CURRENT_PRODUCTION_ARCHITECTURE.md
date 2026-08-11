# CURRENT PRODUCTION ARCHITECTURE

This document is the concise source-of-truth for the live MyTGTodoist production contour.

## Active Entry Points

- Telegram entry: `telegram-bot/app-integration/src/integrations/telegram/bot_main.py`
- Telegram runtime orchestration: `telegram-bot/app-integration/src/app/handler.py`
- Telegram runtime bridge: `telegram-bot/app-integration/src/integrations/telegram/runtime_bridge.py`
- Worker write entry: `organizer-worker /runtime/command`
- Read API entry: `organizer-api/app.py`
- Google sync service: `google-sync` via `src/google/scheduler.py`

## Active Production Flow

`User -> Telegram adapter -> ML gateway -> app runtime handler -> organizer-worker /runtime/* -> SQLite runtime DB -> organizer-api/google-sync`

Key rules:

- `organizer-worker` is the only production DB writer
- runtime SQLite DB is the production state source of truth
- local git/workspace is the source tree of truth for deployable code
- `/opt/mytgtodoist` on VPS is a deployment mirror, not a source repo

Canonical deploy path:

1. local source tree
2. sync to VPS mirror
3. build selected images on VPS
4. recreate selected containers
5. verify runtime markers inside the running containers

## Runtime Mode

- Active Telegram mode: `runtime_core_direct`
- `compat_worker_bridge`: retired from the active contour
- `telegram-bot/bot.py`: removed
- `src/main.py`: removed from production contour
- `src/api/*`: removed from production contour
- `src/telegram/*`: removed from production contour

## Deterministic Date Handling

- `common/date_resolver.py` is the shared deterministic Date Resolver for the active contour
- ML extracts intent and temporal entities, but does not finalize persisted relative dates
- app runtime must normalize supported Russian phrases such as `сегодня`, `завтра`, and weekday phrases before summary render, duplicate precheck, and worker handoff
- worker applies the same resolver as a safety net before mutation
- raw relative phrases must not be persisted as final runtime dates when canonicalization is available

## Runtime Routing Order

Current high-level routing order:

1. cancel
2. active scenario continuation
3. calendar/timeblock/task routing
4. ambiguous create prompt when the create family is still unclear
5. explicit memory fallback only

Required behavior:

- generic action phrases such as `купить хлеб завтра` and `позвонить врачу завтра` route to `task.create`
- undated action phrases such as `позвонить врачу` also route to `task.create`; if they stay undated they later project into `InBox`
- memory fallback is explicit-only and must not absorb generic task text before task routing rules run

## Worker Structure

`organizer-worker/worker.py` is bootstrap and compatibility wiring only.

Active runtime modules:

- `organizer-worker/runtime_server.py`
- `organizer-worker/runtime_handlers.py`
- `organizer-worker/runtime_calendar.py`
- `organizer-worker/runtime_sync_conflicts.py`
- `organizer-worker/runtime_search.py`
- `organizer-worker/shared_runtime.py`

Quarantined legacy path:

- `organizer-worker/legacy_queue.py`
- startup remains disabled unless `ALLOW_LEGACY_WORKER_QUEUE=1`

## Runtime Endpoints

Write path:

- `POST /runtime/command`

Read/helper paths used by Telegram runtime:

- `POST /runtime/meeting/latest`
- `POST /runtime/meeting/search`
- `POST /runtime/meeting/cleanup_stale`
- `POST /runtime/meeting/check_calendar_drift`
- `POST /runtime/task/search`
- `POST /runtime/timeblock/search`
- `POST /runtime/sync_conflict/action`
- `GET /health`

## Clarification And Edit Lifecycle

The app runtime owns:

- clarification sessions
- temporal summaries and confirmation gates
- task-create confirmation
- duplicate-create precheck UX
- meeting/timeblock unified edit flow
- callback normalization
- global cancel
- local idempotency lineage for continuation

The worker owns:

- runtime command dispatch
- persistent write-side idempotency
- trace dedup snapshotting
- calendar commit side effects
- sync-conflict persistence and action handling
- authoritative duplicate guard

## Task Create Behavior

`task.create` supports:

- `title`
- `planned_at`
- `parent_task_id`
- `comment`

Behavioral rules:

- normal task-create phrases create a root task unless `parent_task_id` is supplied explicitly
- task comments are part of the user-visible summary and must be persisted/exported
- confirmation screen must preserve comment and allow cancel/edit flow
- duplicate guard is worker-authoritative
- Telegram/runtime duplicate precheck must surface the duplicate before the normal create summary
- duplicate key for dated tasks:
  - normalized title
  - same planned day
  - same `parent_task_id` scope
- duplicate key for undated tasks:
  - normalized title
  - `planned_at IS NULL`
  - same `parent_task_id` scope
- blank and `null` `parent_task_id` are the same root scope
- comments do not affect duplicate matching
- root undated duplicate wording is surfaced as `в InBox`
- undated subtask duplicate wording is surfaced as `без срока`

## Temporal / Allocation Semantics

`timeblock.create` supports allocation phrases such as:

- `выдели время на задачу`
- `задача на завтра выдели время на нее`

Current behavior:

- existing task linkage is optional for allocation-style phrasing
- app runtime marks such task hints as optional
- worker does not fail the flow on unmatched optional task lookup
- because `time_blocks.task_id` remains required in the runtime schema, worker creates a backing task automatically when needed
- backing task title is derived from comment text, task hint, or generic fallback

## Task Hierarchy And Sheets Projection

- runtime tasks support multi-level subtasks
- `parent_task_id` may reference any existing task row
- parent linkage is explicit-only
- Google Sheets visible tree number `№` is generated for users only
- technical `ID` and `Родитель ID` remain the real sync/source identifiers
- export order is recursive parent -> child -> grandchild
- reverse sync ignores user edits to `№` and overwrites it on the next export

## Google Health And Sheets Topology

- `organizer-api /google/health` performs an actual Google Calendar write probe and cancellation
- `google-sync` remains the separate scheduler/sync service
- calendar commit side effects for temporal creates/meeting updates are executed on the worker side
- Google Sheets sync is one-way for the active runtime contour: runtime SQLite DB -> `google-sync` -> Google Sheets
- Google Sheets is a read model / operator screen, not a production write path
- `GOOGLE_SHEETS_SYNC_ENABLED=1` selects the canonical runtime Sheets path
- current runtime Sheets tabs:
  - `Задачи`
  - `InBox`
  - `Календарь`
  - `Логи`
- `Задачи` and `InBox` are mutually exclusive visible projections
- `Задачи` contains planned tasks and non-triage tasks
- `InBox` contains undated root triage tasks only
- deprecated technical tabs `RuntimeTasks` and `RuntimeTimeBlocks` are not canonical
- reverse Sheets -> runtime DB apply is not an automatic write path; the safe path is review -> explicit confirmation -> worker command apply
- legacy `items` / `full_bidir` Sheets branches in `src/google/scheduler.py` are non-canonical and require explicit opt-in via `GOOGLE_SYNC_ALLOW_LEGACY_ITEMS_MODE=1`

## Planned Manual Reverse Sync

Design direction for reverse sync:

`Google Sheets edits -> reverse-sync reader -> compare with runtime DB snapshot -> proposed changes -> validation/classification -> review artifact -> explicit confirmation -> organizer-worker /runtime/command -> runtime DB -> forward export back to Sheets`

Hard boundaries:

- no direct Google Sheets -> SQLite mutation
- no automatic scheduler apply mode
- no bypass around `organizer-worker /runtime/command`
- review/apply runs through `google-sync`, not through `organizer-api`

MVP design points:

- trigger by CLI first
- map rows by stable internal identity
- compare against DB snapshot and export metadata
- classify results into `proposed`, `confirm_required`, `conflict`, `invalid`
- use persistent review artifacts such as `/data/sheets_review.json`
- apply nothing until explicitly confirmed
- re-export DB state after accepted changes
- after successful blank-`ID` task create, cleanup of the consumed proposal row is allowed only if row hash/metadata still match the reviewed artifact

## Build Provenance

- `deploy_v2.ps1` is the canonical deploy script
- `deploy.ps1` is a compatibility wrapper
- services that import shared repo code must build with:
  - `build.context: .`
  - explicit `build.dockerfile`
  - root-relative `COPY`
- every production image must contain `/app/build-info.json` with:
  - `service`
  - `git_sha`
  - `build_timestamp_utc`
  - `route_rules_version`
  - `handler_sha256`
  - `dockerfile_path`
  - `build_context`
  - `entrypoint_module`
- `/system` must expose build proof for the running Telegram container
- post-deploy verification must inspect runtime markers inside the running container, not only compose/image metadata

## Observability Signals

Telegram/app runtime:

- `telegram_text_route_entry`
- `runtime_route_selected`
- `memory_fallback_before_return`
- `generic_task_fallback_selected`
- `task_duplicate_precheck_start`
- `task_duplicate_precheck_result`
- `task_create_date_resolution`
- `temporal_edit_*`
- summary render / callback consumption / stale callback guard logs

Worker/runtime:

- `runtime_command_in`
- `runtime_intent_dispatch_start`
- `runtime_intent_dispatch_result`
- `runtime_commit_path_result`
- `task_parent_resolution`

## Deprecated / Historical

Historical or quarantined only:

- `docs/Archive/*`
- `compat_worker_bridge`
- legacy queue/items loop
- removed monolith entrypoints under `src/*` and `telegram-bot/bot.py`

Do not treat those as active operator guidance.

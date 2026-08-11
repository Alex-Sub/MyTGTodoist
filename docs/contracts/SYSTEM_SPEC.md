# SYSTEM_SPEC

Short production-system specification for MyTGTodoist after the direct-runtime cutover and worker modularization.

## 1. Source Of Truth

Semantic source-of-truth:

- ML layer for intent meaning and interpretation

Production state source-of-truth:

- runtime SQLite DB written by `organizer-worker`

Deployable code source-of-truth:

- local git/workspace

VPS deploy model:

- `/opt/mytgtodoist` is a deployment mirror, not a git source repo

Operational source-of-truth documents:

- `docs/architecture/CURRENT_PRODUCTION_ARCHITECTURE.md`
- `docs/contracts/APP_RUNTIME_CONTRACT.md`
- `docs/operations/MYTGTODOIST_RUNBOOK.md`

## 2. Production Contour

Active flow:

`Telegram -> app runtime -> organizer-worker -> runtime DB -> organizer-api/google-sync`

Active entrypoints:

- `telegram-bot/app-integration/src/integrations/telegram/bot_main.py`
- `telegram-bot/app-integration/src/app/handler.py`
- `telegram-bot/app-integration/src/integrations/telegram/runtime_bridge.py`
- `organizer-worker /runtime/command`
- `organizer-api/app.py`

## 3. Runtime Mode And Routing

- active Telegram mode is `runtime_core_direct`
- routing order is:
  - cancel
  - active scenario continuation
  - calendar/timeblock/task routing
  - ambiguous create prompt
  - explicit memory fallback only
- generic action phrases such as `купить хлеб завтра` and `позвонить врачу завтра` route to `task.create`
- undated action phrases such as `позвонить врачу` also route to `task.create` and may later project into `InBox`
- `common/date_resolver.py` is the central deterministic Date Resolver
- raw relative date phrases must not be persisted as final runtime dates when canonicalization is available

## 4. Task Create Contract

`task.create` may include:

- `title`
- `planned_at`
- `parent_task_id`
- `comment`

Behavior:

- worker is the authoritative final duplicate guard
- Telegram/runtime precheck should surface duplicates before the normal create summary
- dated duplicate key:
  - normalized title
  - same planned day
  - same `parent_task_id` scope
- undated duplicate key:
  - normalized title
  - `planned_at IS NULL`
  - same `parent_task_id` scope
- blank and `null` `parent_task_id` are the same root scope
- comments do not affect duplicate matching
- duplicate warning remains a confirmation gate, not an implicit hard reject

## 5. Task Hierarchy And Sheets Projection

- multi-level subtasks are supported
- `parent_task_id` may reference any existing task row
- parent linkage is explicit-only
- visible Sheets tree number `№` is generated for users
- `ID` and `Родитель ID` remain technical source-of-truth identifiers for sync/apply
- export order is recursive parent -> child -> grandchild
- reverse sync ignores and overwrites manual edits to `№`

## 6. Google Sheets / Reverse Sync

- Google Sheets is a projection and safe review/apply surface, not the production state source of truth
- `Задачи` and `InBox` are mutually exclusive visible projections
- `Задачи` contains planned tasks and non-triage tasks
- `InBox` contains undated root triage tasks only
- reverse sync runs through `google-sync`, not `organizer-api`
- default review artifact path is `/data/sheets_review.json`
- current safe apply path is task-only
- calendar/timeblock/meeting apply remains blocked
- blank-`ID` proposal rows may be cleaned after successful create only if the reviewed row hash/metadata still match

## 7. Deprecated / Quarantined

Removed from the active contour:

- `src/main.py`
- `src/api/*`
- `src/telegram/*`
- `telegram-bot/bot.py`
- `compat_worker_bridge`

Quarantined only:

- `organizer-worker/legacy_queue.py`
- bootstrap gating in `organizer-worker/worker.py`

## 8. Worker Runtime Topology

`worker.py` is bootstrap/compat wiring.

Active runtime implementation:

- `runtime_server.py`
- `runtime_handlers.py`
- `runtime_calendar.py`
- `runtime_sync_conflicts.py`
- `runtime_search.py`
- `shared_runtime.py`

## 9. Runtime HTTP Surface

Write path:

- `POST /runtime/command`

Helper/read-side paths:

- `POST /runtime/meeting/latest`
- `POST /runtime/meeting/search`
- `POST /runtime/meeting/cleanup_stale`
- `POST /runtime/meeting/check_calendar_drift`
- `POST /runtime/task/search`
- `POST /runtime/timeblock/search`
- `POST /runtime/sync_conflict/action`
- `GET /health`

## 10. Observability

Important active signals:

- `telegram_text_route_entry`
- `runtime_route_selected`
- `memory_fallback_before_return`
- `generic_task_fallback_selected`
- `task_duplicate_precheck_start`
- `task_duplicate_precheck_result`
- `task_create_date_resolution`
- `temporal_edit_*`
- `runtime_command_in`
- `runtime_intent_dispatch_start`
- `runtime_intent_dispatch_result`
- `runtime_commit_path_result`
- `task_parent_resolution`

## 11. Deploy / Verification

Canonical deploy command family:

- `deploy_v2.ps1` for scripted VPS deploy
- `deploy.ps1` as compatibility wrapper

Build contract:

- services that need shared repo code use `build.context: .`
- `build.dockerfile` is explicit
- Dockerfiles use root-relative `COPY`
- every production image contains `/app/build-info.json`
- `/system` exposes running build proof
- post-deploy verification must check markers inside the running container

Canonical smoke references:

- `docs/operations/OPERATOR_SMOKE_CHECK.md`
- `docs/STARTUP_RUNBOOK_VERIFIED.md`

# SYSTEM_OVERVIEW

System overview for the active MyTGTodoist production contour.

## 1. Production Layers

`User -> Telegram adapter -> ML gateway -> app runtime orchestration -> worker execution -> SQLite runtime DB -> read API / Google sync`

Layer responsibilities:

- Telegram adapter:
  - transport, polling, Telegram reply rendering
- ML gateway:
  - ASR, parsing, retrieval support
- app runtime:
  - clarification, confirmation, edit-flow orchestration, runtime routing
- worker:
  - single writer, command execution, calendar commit side effects
- organizer-api:
  - read-only health and state inspection
- google-sync:
  - separate sync/scheduler service for Google integrations

## 2. Active Production Contour

Active entrypoints:

- `telegram-bot/app-integration/src/integrations/telegram/bot_main.py`
- `telegram-bot/app-integration/src/app/handler.py`
- `telegram-bot/app-integration/src/integrations/telegram/runtime_bridge.py`
- `organizer-worker /runtime/command`
- `organizer-api/app.py`

Current mode:

- Telegram adapter is direct-only: `runtime_core_direct`
- compat bridge is retired from active routing
- worker default startup is active-runtime-only
- local git/workspace is the deployable code source of truth
- `/opt/mytgtodoist` on VPS is a deployment mirror

## 3. Single-Writer Boundary

Production state authority:

- runtime SQLite DB is the production source of truth
- `organizer-worker` is the only component allowed to mutate it

Non-writers:

- Telegram adapter
- organizer-api
- ML gateway
- google-sync for runtime DB writes

Google Sheets policy:

- runtime DB remains source of truth
- Google Sheets is an external projection plus safe review/apply input
- no active path may write Sheets edits back into runtime DB except through the manual review/apply worker path

## 4. Worker Runtime Split

`organizer-worker/worker.py` is no longer the main implementation surface. It is bootstrap and compatibility wiring.

Active runtime modules:

- `runtime_server.py`
- `runtime_handlers.py`
- `runtime_calendar.py`
- `runtime_sync_conflicts.py`
- `runtime_search.py`
- `shared_runtime.py`

Quarantined legacy:

- `legacy_queue.py`
- queue/items startup remains disabled unless `ALLOW_LEGACY_WORKER_QUEUE=1`

## 5. Runtime Behavior Ownership

App runtime owns:

- clarification sessions
- confirmation gates
- temporal/task drafts
- unified edit flow for meeting/timeblock updates
- callback normalization and stale-callback guards
- comment edit continuation
- local continuation/idempotency context
- duplicate-create precheck UX

Worker owns:

- canonical command execution
- persistent idempotency write-side protection
- trace dedup snapshots
- calendar create/patch side effects
- sync-conflict persistence and action handling
- authoritative duplicate guard

## 6. Date Resolver And Routing

- `common/date_resolver.py` is the central deterministic Date Resolver
- ML may extract `завтра`, `сегодня`, or weekday phrases, but runtime must convert them to canonical dates before summaries, duplicate checks, and worker commit
- raw unresolved relative phrases must remain clarification-only and must not be persisted as final runtime dates

Current routing order:

1. cancel
2. active scenario continuation
3. calendar/timeblock/task routing
4. ambiguous create prompt
5. explicit memory fallback only

Current task routing expectations:

- generic action phrases such as `купить хлеб завтра` and `позвонить врачу завтра` route to `task.create`
- undated action phrases such as `позвонить врачу` also route to `task.create`
- explicit memory fallback must not steal generic task phrases before task routing completes

## 7. Task And Hierarchy Semantics

`task.create` behavior:

- supports `title`, `planned_at`, `parent_task_id`, and `comment`
- comments are meaningful and must be preserved in UX, persistence, and export
- normal task creation is root-level unless `parent_task_id` is provided explicitly
- duplicate guard is title/scope based:
  - dated: same normalized title + same planned day + same parent scope
  - undated: same normalized title + `planned_at IS NULL` + same parent scope
- comments do not affect duplicate matching

Hierarchy:

- multi-level subtasks are supported
- `parent_task_id` may point to any existing task row
- parent linkage is explicit-only
- generated Sheets tree number `№` is user-facing only
- `ID` and `Родитель ID` remain the technical identifiers for sync/apply
- export order is recursive parent -> child -> grandchild

## 8. Google Topology

- worker performs calendar commit side effects for temporal create/update flows
- organizer-api exposes `/google/health` as an active write-probe check
- `google-sync` remains a separate service for scheduler/sync responsibilities
- runtime Sheets sync, when enabled, exports runtime entities to Google Sheets by stable internal id
- current canonical tabs are:
  - `Задачи`
  - `InBox`
  - `Календарь`
  - `Логи`
- `Задачи` and `InBox` are mutually exclusive visible projections
- `Задачи` contains planned tasks and non-triage tasks
- `InBox` contains undated root triage tasks only
- reverse sync review/apply runs through `google-sync`, not `organizer-api`
- default persistent review artifact path is `/data/sheets_review.json`
- current safe apply scope is task-only
- blank-`ID` proposal rows may be cleaned after successful create only if the reviewed row hash/metadata still match

## 9. Deploy And Provenance

- canonical deploy script: `deploy_v2.ps1`
- compatibility wrapper: `deploy.ps1`
- services that need shared repo code must use repo-root build context and explicit Dockerfile
- production images must contain `/app/build-info.json`
- `/system` exposes running build proof
- post-deploy verification must inspect runtime markers inside the running container

## 10. Deprecated / Historical

Removed from the active contour:

- `src/main.py`
- `src/api/*`
- `src/telegram/*`
- `telegram-bot/bot.py`
- `compat_worker_bridge`

Quarantined, not active by default:

- legacy queue/items worker loop

Use `docs/architecture/CURRENT_PRODUCTION_ARCHITECTURE.md` for the shortest source-of-truth summary.

# PROJECT_QUICK_REFERENCE

Short operational reference for new ChatGPT/Codex sessions.

Source policy:

- synthesized from `/docs` only
- not a code reverse-engineering document

## 1. Project Overview

MyTGTodoist is a runtime-centered task and scheduling system with:

- Telegram as the main user interface
- external ML gateway for ASR, parsing, and retrieval support
- app runtime for routing, clarification, confirmation, and edit flow
- `organizer-worker` as the only production DB writer
- `organizer-api` as the read-only API
- `google-sync` as the separate Google integration and reverse-sync review/apply runner

Core production path:

`User -> Telegram adapter -> ML gateway -> app runtime -> organizer-worker -> SQLite runtime DB -> organizer-api / google-sync`

## 2. Source Of Truth And Deployment Model

State source of truth:

- runtime SQLite DB

Semantic source of truth:

- ML layer for intent meaning

Deployable code source of truth:

- local git/workspace

VPS model:

- `/opt/mytgtodoist` is a deployment mirror, not a source repo

Canonical deploy path:

1. local source tree
2. sync to `/opt/mytgtodoist`
3. build selected images on VPS
4. recreate selected containers
5. verify runtime markers and build proof inside the running containers

## 3. Current Production Architecture

Canonical mode:

- `runtime_core_direct`

Active entry points:

- Telegram entry: `telegram-bot/app-integration/src/integrations/telegram/bot_main.py`
- runtime orchestration: `telegram-bot/app-integration/src/app/handler.py`
- runtime bridge: `telegram-bot/app-integration/src/integrations/telegram/runtime_bridge.py`
- worker write entry: `organizer-worker /runtime/command`
- read API entry: `organizer-api/app.py`
- Google sync service: `google-sync`

Runtime ownership:

- app runtime owns routing, clarification, confirmation, local continuation state, and duplicate precheck UX
- worker owns final mutation, idempotency, trace dedup, calendar side effects, sync-conflict actions, and the authoritative duplicate guard

Worker topology:

- `organizer-worker/worker.py` is bootstrap and compatibility wiring only
- active runtime modules are `runtime_server.py`, `runtime_handlers.py`, `runtime_calendar.py`, `runtime_sync_conflicts.py`, `runtime_search.py`, and `shared_runtime.py`
- `organizer-worker/legacy_queue.py` is quarantined and stays disabled unless `ALLOW_LEGACY_WORKER_QUEUE=1`

## 4. Runtime Request Flow

High-level request path:

`Telegram update -> runtime bridge -> app handler -> worker /runtime/command -> runtime DB -> reply back to Telegram`

Clarification model:

- one active scenario per logical context
- continuation is stored in local runtime DB
- typed replies and callback replies follow the same logical continuation path

Execution boundary:

- worker commit is allowed only after required fields and confirmation gates are satisfied

## 5. Date Resolver

Central rule:

- `common/date_resolver.py` is the shared deterministic Date Resolver for the active contour

Behavior:

- ML may extract phrases such as `сегодня`, `завтра`, and weekday references
- runtime must canonicalize supported Russian date phrases before summary rendering, duplicate precheck, and worker handoff
- worker applies the same resolver as a safety net before mutation
- raw relative words must not be persisted as final runtime dates when canonicalization is available

## 6. Runtime Routing Rules

Current routing order:

1. cancel
2. active scenario continuation
3. calendar/timeblock/task routing
4. ambiguous create prompt
5. explicit memory fallback only

Practical rules:

- generic action phrases such as `купить хлеб завтра` and `позвонить врачу завтра` route to `task.create`
- undated action phrases such as `позвонить врачу` also route to `task.create`
- undated root tasks may later project into `InBox`
- memory fallback is explicit-only and must not absorb generic task text before task routing finishes
- allocation phrases such as `выдели время на задачу` route to `timeblock.create`

## 7. Task Behavior

`task.create` supports:

- `title`
- `planned_at`
- `parent_task_id`
- `comment`

Important behavior:

- normal task creation produces a root task unless `parent_task_id` is supplied explicitly
- comments are meaningful user data and must be preserved in confirmation UX, persistence, and export
- duplicate warning is a confirmation gate, not an implicit hard reject

Duplicate guard:

- worker is the authoritative final guard
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
- `DONE`, `CANCELLED` / `CANCELED`, and `ARCHIVED` do not block
- comments do not affect duplicate matching
- root undated duplicate wording is surfaced as `в InBox`
- undated subtask duplicate wording is surfaced as `без срока`

## 8. Task Hierarchy And Sheets Projection

Hierarchy:

- multi-level subtasks are supported
- `parent_task_id` may point to any existing task row
- parent linkage is explicit-only

Sheets projection:

- `Задачи` and `InBox` are mutually exclusive visible projections
- `Задачи` contains planned tasks and non-triage tasks
- `InBox` contains undated root triage tasks only
- visible tree number `№` is generated for users only
- `ID` and `Родитель ID` remain the technical source-of-truth identifiers for sync/apply
- export order is recursive parent -> child -> grandchild
- reverse sync ignores manual edits to `№` and overwrites it on the next export

## 9. Google Sheets Reverse Sync

Policy:

- runtime DB remains the source of truth
- Google Sheets is a projection plus safe review/apply input
- no direct Google Sheets -> SQLite write path is allowed

Current safe path:

1. run reverse-sync review through `google-sync`
2. save review artifact
3. inspect `proposed`, `confirm_required`, `conflict`, and `invalid`
4. apply explicit safe task changes only through worker commands
5. re-export DB state back to Sheets

Important details:

- review/apply runs through `google-sync`, not `organizer-api`
- default persistent artifact path is `/data/sheets_review.json`
- current safe apply scope is task-only
- calendar/timeblock/meeting apply remains blocked
- blank-`ID` proposal rows may be cleaned after successful create only if the reviewed row hash and metadata still match

Canonical apply example:

```bash
python -m src.google.runtime_sheets_reverse_sync --apply /data/sheets_review.json --change-id chg-00001
```

## 10. Deploy And Verification

Canonical scripts:

- `deploy_v2.ps1` is the canonical deploy script
- `deploy.ps1` is a compatibility wrapper

Build contract:

- services that need shared repo code use `build.context: .`
- `build.dockerfile` is explicit
- Dockerfiles use root-relative `COPY`

Runtime provenance:

- every production image must contain `/app/build-info.json`
- expected fields:
  - `service`
  - `git_sha`
  - `build_timestamp_utc`
  - `route_rules_version`
  - `handler_sha256`
  - `dockerfile_path`
  - `build_context`
  - `entrypoint_module`
- `/system` exposes running build proof for the Telegram container
- post-deploy verification must check route markers and build proof inside the running container

## 11. Observability Markers

Permanent runtime markers to know:

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

## 12. Legacy / Deprecated Areas

Not part of the active production contour:

- `compat_worker_bridge`
- `src/main.py`
- `src/api/*`
- `src/telegram/*`
- `telegram-bot/bot.py`
- legacy queue/items runtime path

Do not treat those as active deploy, runtime, or operator guidance.

## 13. Source Docs For Deeper Reading

Start here:

- [CURRENT_PRODUCTION_ARCHITECTURE.md](d:/My_AI_Prodgekt/MyTGTodoist/docs/architecture/CURRENT_PRODUCTION_ARCHITECTURE.md)
- [APP_RUNTIME_CONTRACT.md](d:/My_AI_Prodgekt/MyTGTodoist/docs/contracts/APP_RUNTIME_CONTRACT.md)
- [SYSTEM_SPEC.md](d:/My_AI_Prodgekt/MyTGTodoist/docs/contracts/SYSTEM_SPEC.md)
- [MYTGTODOIST_RUNBOOK.md](d:/My_AI_Prodgekt/MyTGTodoist/docs/operations/MYTGTODOIST_RUNBOOK.md)
- [OPERATOR_SMOKE_CHECK.md](d:/My_AI_Prodgekt/MyTGTodoist/docs/operations/OPERATOR_SMOKE_CHECK.md)
- [STARTUP_RUNBOOK_VERIFIED.md](d:/My_AI_Prodgekt/MyTGTodoist/docs/STARTUP_RUNBOOK_VERIFIED.md)

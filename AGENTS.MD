# AGENTS.md

## Entry points (authoritative)

- Telegram entry: `telegram-bot/app-integration/src/integrations/telegram/bot_main.py`
- Runtime command entry: `organizer-worker /runtime/command`
- Read API entry: `organizer-api/app.py`

These define the active production flow. Any change must respect these entry points.

## Active production contour

- `telegram-bot/app-integration/`: Telegram adapter, UX orchestration, clarification, confirmation, local dedup, ML gateway calls.
- `organizer-worker/`: runtime execution layer and single writer for production state.
- `organizer-api/`: read-only API over runtime SQLite state.
- `google-sync/`: separate sync/scheduler service for Google integrations.
- `migrations/*.sql`: runtime schema used by the active contour.

Worker runtime mode:
- `organizer-worker` starts in active-runtime-only mode by default.
- Active worker surface is `/runtime/*`.
- Legacy queue/items loop is quarantined and disabled unless `ALLOW_LEGACY_WORKER_QUEUE=1`.
- Do not enable `ALLOW_LEGACY_WORKER_QUEUE` in production compose/env.

Worker module layout:
- `organizer-worker/worker.py` is bootstrap / compatibility facade only.
- Active runtime modules:
  - `organizer-worker/runtime_server.py`
  - `organizer-worker/runtime_handlers.py`
  - `organizer-worker/runtime_calendar.py`
  - `organizer-worker/runtime_sync_conflicts.py`
  - `organizer-worker/runtime_search.py`
  - `organizer-worker/shared_runtime.py`
- Quarantined legacy queue/items module:
  - `organizer-worker/legacy_queue.py`

## Single-writer rule

- `organizer-worker` is the only component allowed to modify production state.
- No other service or module may write to runtime DB tables.

## ML layer authority

- ML/recognition layer defines intent and semantic interpretation.
- Runtime layer must not re-interpret intent beyond validation and execution rules.

## Legacy / transitional contour

- removed legacy monolith shell: `src/api/app.py`, `src/api/__init__.py`, `src/main.py`, `src/telegram/webhook.py`, `src/telegram/bot.py`, `src/telegram/parsers.py`, `src/telegram/__init__.py`, `telegram-bot/bot.py`
- remaining legacy trees: `src/google/*`
- legacy queue/items flow in `organizer-worker/legacy_queue.py` with bootstrap/guard wiring in `organizer-worker/worker.py`
- `organizer-worker/src/organizer_worker/time_legacy/*`

## Risky duplications

- `telegram-bot/app-integration/*` and removed historical `src/telegram/*`
- `organizer-api/app.py` and removed historical monolith entrypoints `src/api/app.py` / `src/main.py`
- `organizer-worker/src/organizer_worker/db.py` and `organizer-worker/src/p2_tasks_runtime.py`
- active `/runtime/*` modules and quarantined `organizer-worker/legacy_queue.py`
- temporal logic split between Telegram runtime decision code and worker execution validation

## No implicit refactoring

- Do not refactor or clean up legacy or duplicated code unless explicitly requested.
- Do not unify duplicated logic without identifying the active production path.

## Do not change without separate task

- Telegram -> worker contract: envelope, `intent/entities`, `trace_id`, `idempotency_key`, `/runtime/command`
- authoritative entry points and active production routing
- runtime DB schema and production tables
- idempotency, clarification-session, replay/dedup behavior
- temporal / meeting flows and calendar commit behavior
- `docker-compose.yml` active topology
- `canon/*.yml` and `schemas/*.json` contracts
- legacy `src/*` removal or rewiring

## Rules for future changes

- Treat runtime DB state as the production source of truth.
- Keep the active flow: Telegram -> bot adapter -> runtime decision/orchestration -> worker -> read API.
- Do not mix recognition/orchestration changes with execution/state-write changes unless the contract also changes.
- Prefer changes inside the active contour only.
- If a contract changes, update both sides in the same task.
- Do not edit legacy/transitional paths unless the task is explicitly about migration or removal.
- Before changing duplicated logic, identify which implementation is active in production.
- Preserve backward compatibility for active runtime commands unless the task explicitly allows a break.

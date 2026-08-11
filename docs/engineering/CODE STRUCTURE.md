# CODE STRUCTURE

Code-structure map for the active production contour.

## 1. Root

Important top-level directories:

- `telegram-bot/`
- `organizer-worker/`
- `organizer-api/`
- `google-sync/`
- `src/`
- `migrations/`
- `canon/`
- `schemas/`
- `docs/`

## 2. Telegram Runtime

Active runtime code lives in:

- `telegram-bot/app-integration/src/integrations/telegram/`
- `telegram-bot/app-integration/src/app/`
- `telegram-bot/app-integration/src/decision/`
- `telegram-bot/app-integration/src/reliability/`
- `telegram-bot/app-integration/src/core/`

Primary production files:

- `integrations/telegram/bot_main.py`
- `integrations/telegram/runtime_bridge.py`
- `app/handler.py`
- `integrations/telegram/reply_mapper.py`
- `integrations/telegram/update_mapper.py`

## 3. Worker Runtime

`organizer-worker/worker.py`:

- bootstrap and compatibility facade

Active runtime modules:

- `organizer-worker/runtime_server.py`
- `organizer-worker/runtime_handlers.py`
- `organizer-worker/runtime_calendar.py`
- `organizer-worker/runtime_sync_conflicts.py`
- `organizer-worker/runtime_search.py`
- `organizer-worker/shared_runtime.py`

Quarantined:

- `organizer-worker/legacy_queue.py`

Internal package:

- `organizer-worker/src/organizer_worker/`
  - canonical intent dispatch and DB helpers used by the active worker runtime

## 4. Read API

Active API entry:

- `organizer-api/app.py`

This is the canonical read-only API surface.

## 5. Google Integration

Active Google scheduler/sync path:

- `google-sync/`
- `src/google/`

Important note:

- `src/google/*` is active and must not be treated as dead monolith residue

## 6. Historical / Removed

Removed from the active contour:

- `src/main.py`
- `src/api/*`
- `src/telegram/*`
- `telegram-bot/bot.py`

These paths may still appear in historical docs and tests as removal history, but they are not live structure.

## 7. Runtime Surface Summary

Worker endpoints:

- `/runtime/command`
- `/runtime/meeting/latest`
- `/runtime/meeting/search`
- `/runtime/meeting/cleanup_stale`
- `/runtime/meeting/check_calendar_drift`
- `/runtime/task/search`
- `/runtime/timeblock/search`
- `/runtime/sync_conflict/action`

## 8. Documentation Pointers

- architecture truth: `docs/architecture/CURRENT_PRODUCTION_ARCHITECTURE.md`
- adapter truth: `docs/engineering/ADAPTERS.md`
- operator smoke: `docs/operations/OPERATOR_SMOKE_CHECK.md`

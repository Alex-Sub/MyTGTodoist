# LEGACY_INVENTORY_RUNTIME_CORE_DIRECT_2026-05-10

Legacy/quarantine inventory for the post-cutover production contour.

## Active Source-Of-Truth Paths

- `telegram-bot/app-integration/src/integrations/telegram/bot_main.py`
- `telegram-bot/app-integration/src/app/handler.py`
- `telegram-bot/app-integration/src/integrations/telegram/runtime_bridge.py`
- `organizer-worker /runtime/command`
- `organizer-api/app.py`
- `google-sync`

## Removed From Active Production Routing

- `src/main.py`
- `src/api/*`
- `src/telegram/*`
- `telegram-bot/bot.py`
- `compat_worker_bridge`

These should only remain in historical notes, archive docs, or removal-history tests.

## Quarantined, Not Active By Default

- `organizer-worker/legacy_queue.py`
- legacy queue/items bootstrap guard in `organizer-worker/worker.py`

Rule:

- do not enable `ALLOW_LEGACY_WORKER_QUEUE` in production

## Still Active Even Though They Look Legacy

- `src/google/*`
  - active through `google-sync`
- `organizer-worker/src/organizer_worker/*`
  - active internal runtime package code used by the worker contour

## Worker Split Status

`organizer-worker/worker.py` is bootstrap/compat only.

Active modules:

- `runtime_server.py`
- `runtime_handlers.py`
- `runtime_calendar.py`
- `runtime_sync_conflicts.py`
- `runtime_search.py`
- `shared_runtime.py`

## Current Documentation Rule

If a document mentions removed monolith paths or compat routing without explicitly marking them as historical, that document is stale and should be corrected.

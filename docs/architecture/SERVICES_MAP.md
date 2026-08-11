# SERVICES_MAP

Service responsibilities for the active production contour.

## 1. Telegram Adapter

Service:

- `telegram-bot`

Primary modules:

- `src/integrations/telegram/bot_main.py`
- `src/integrations/telegram/runtime_bridge.py`
- `src/app/handler.py`
- `src/integrations/telegram/reply_mapper.py`
- `src/integrations/telegram/update_mapper.py`

Responsible for:

- Telegram polling and transport mapping
- text/voice/callback intake
- ML client wiring through direct runtime mode
- clarification session continuation
- confirmation and edit-flow UX rendering
- worker execution handoff after runtime says the command is executable

Explicitly not responsible for:

- direct runtime DB writes
- bypassing worker execution
- reinterpretation of committed runtime state

## 2. ML Gateway

External service.

Responsible for:

- ASR
- chat/parse
- retrieval support

Explicitly not responsible for:

- production DB writes
- final execution authority

## 3. App Runtime Orchestration

Lives in `telegram-bot/app-integration`.

Responsible for:

- command normalization
- clarification routing
- draft building
- temporal/task confirmation gating
- unified edit flow
- session persistence in `clarification_sessions`
- transport-equivalent handling of typed replies and callback replies

## 4. Runtime Bridge

Module:

- `telegram-bot/app-integration/src/integrations/telegram/runtime_bridge.py`

Role:

- direct-only execution bridge between Telegram adapter and runtime handler/worker backend

Current behavior:

1. build the direct runtime handler from current settings
2. route update payload into the app runtime
3. preserve `WORKER_COMMAND_URL` because final execution still goes through `organizer-worker /runtime/command`
4. normalize worker/runtime output back into Telegram-facing result payload

Historical note:

- compat mode is retired
- `compat_worker_bridge` is not part of the active system

## 5. Organizer Worker

Service:

- `organizer-worker`

Active HTTP surface:

- `POST /runtime/command`
- `POST /runtime/meeting/latest`
- `POST /runtime/meeting/search`
- `POST /runtime/meeting/cleanup_stale`
- `POST /runtime/meeting/check_calendar_drift`
- `POST /runtime/task/search`
- `POST /runtime/timeblock/search`
- `POST /runtime/sync_conflict/action`
- `GET /health`

Responsible for:

- single-writer execution
- worker-side idempotency
- runtime trace dedup
- calendar commit path execution
- sync-conflict persistence and action resolution
- helper search endpoints used by Telegram clarification/update flows

Explicitly not responsible for:

- first-pass natural-language interpretation
- Telegram transport UX

## 6. Organizer API

Service:

- `organizer-api`

Responsible for:

- read-only API over runtime DB
- `/health`
- `/google/health`
- `/sync/conflicts`
- read-only operational inspection endpoints

Explicitly not responsible for:

- runtime DB mutation

## 7. Google Sync

Service:

- `google-sync`

Responsible for:

- separate scheduler/sync work
- active `src/google/*` integration path

Explicitly not responsible for:

- replacing worker as production command writer

## 8. Quarantined / Removed Components

Quarantined:

- `organizer-worker/legacy_queue.py`

Removed from active production contour:

- `src/main.py`
- `src/api/*`
- `src/telegram/*`
- `telegram-bot/bot.py`
- compat bridge execution path

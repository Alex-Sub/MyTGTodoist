# REPOSITORY_STRUCTURE

Repository structure for the active production contour.

## Top-Level Directories

- `telegram-bot/`
- `organizer-worker/`
- `organizer-api/`
- `google-sync/`
- `src/`
- `migrations/`
- `canon/`
- `schemas/`
- `docs/`
- `scripts/`

## Service Roots

Telegram runtime:

- `telegram-bot/app-integration/`

Worker runtime:

- `organizer-worker/`

Read API:

- `organizer-api/`

Google sync:

- `google-sync/`
- `src/google/`

## Important Notes

- `src/google/*` is active
- `organizer-worker/src/organizer_worker/*` is active internal runtime package code
- `organizer-worker/legacy_queue.py` is quarantined

## Removed From Active Production Routing

- `src/main.py`
- `src/api/*`
- `src/telegram/*`
- `telegram-bot/bot.py`

Use `docs/engineering/CODE STRUCTURE.md` for the more explicit code-map view.

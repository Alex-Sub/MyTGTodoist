# LEGACY_REMOVAL_PREP

Historical prep document. Compat removal is complete.

## Current State

Completed:

- `runtime_core_direct` is the only supported Telegram runtime mode
- compat bridge removal is complete
- `telegram-bot/bot.py` is removed from the active contour
- legacy monolith entrypoints under `src/main.py`, `src/api/*`, and `src/telegram/*` are removed from production routing

Still quarantined:

- `organizer-worker/legacy_queue.py`
- startup guard path in `organizer-worker/worker.py`

## Remaining Cleanup Boundary

Future narrow cleanup can target:

- reducing historical doc noise
- further isolating or eventually removing legacy queue/items code

Do not treat this file as an active runbook.

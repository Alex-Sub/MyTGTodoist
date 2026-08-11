# ML_STACK_CANON

Short ML/runtime integration canon for the active production contour.

## Current Canonical Facts

- Telegram entrypoint: `python -m src.integrations.telegram.bot_main`
- Telegram mode: `runtime_core_direct`
- canonical ML URL env: `ML_GATEWAY_URL`
- canonical VM ML stack path: `/mnt/hgfs/VMShare/infra/ml-stack`
- canonical tunnel path: VM `127.0.0.1:19000` -> VPS `127.0.0.1:19000`

## Current Runtime Boundaries

- ML remains interpretation authority
- MyTGTodoist runtime owns clarification/execution routing
- worker remains the only production DB writer

## Historical / Removed

- `telegram-bot/bot.py` is removed from the active contour
- `compat_worker_bridge` is retired
- old path examples that rely on legacy adapter or compat routing are stale

## Health/Smoke References

- API health: `http://127.0.0.1:8101/health`
- worker health: `http://127.0.0.1:8102/health`
- ML health via tunnel on VPS: `http://127.0.0.1:19000/health`

Use `docs/operations/OPERATOR_SMOKE_CHECK.md` for the operator sequence.

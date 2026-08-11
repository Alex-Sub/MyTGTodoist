# STARTUP_RUNBOOK_VERIFIED

Verified startup and deploy notes for the active production contour.

## 1. What Is Active

- Telegram adapter mode: `runtime_core_direct`
- worker default mode: active-runtime-only
- worker legacy queue startup: disabled unless `ALLOW_LEGACY_WORKER_QUEUE=1`
- production write path: `organizer-worker /runtime/command`

## 2. What Is Not Active

- `src/main.py`
- `src/api/*`
- `src/telegram/*`
- `telegram-bot/bot.py`
- `compat_worker_bridge`

These remain historical references only.

## 3. Canonical Deploy Commands

Preferred script:

```powershell
.\deploy_v2.ps1 -Services @("telegram-bot","organizer-worker","organizer-api","google-sync")
```

Equivalent compose form:

```bash
docker compose -p deploy --env-file .env.prod -f docker-compose.yml -f docker-compose.vps.override.yml up -d --build
docker compose -p deploy --env-file .env.prod -f docker-compose.yml -f docker-compose.vps.override.yml ps
```

Deploy/source model:

- local git/workspace is the deployable code source of truth
- `/opt/mytgtodoist` on VPS is a deployment mirror, not a git checkout
- `deploy_v2.ps1` is canonical
- `deploy.ps1` is a compatibility wrapper
- deploy must verify running-container markers after recreate

## 4. Immediate Verification

```bash
curl http://127.0.0.1:8101/health
curl http://127.0.0.1:8102/health
curl http://127.0.0.1:8101/google/health
```

Check logs:

```bash
docker compose -p deploy --env-file .env.prod -f docker-compose.yml -f docker-compose.vps.override.yml logs --tail=200 telegram-bot
docker compose -p deploy --env-file .env.prod -f docker-compose.yml -f docker-compose.vps.override.yml logs --tail=200 organizer-worker
```

Verify:

- bot logs show `runtime_core_direct`
- no legacy-queue startup
- worker is healthy
- `/system` shows build proof for the running Telegram container
- running images contain `/app/build-info.json`

## 5. Worker Active Surface

Worker endpoints verified as active:

- `GET /health`
- `POST /runtime/command`
- `POST /runtime/meeting/latest`
- `POST /runtime/meeting/search`
- `POST /runtime/meeting/cleanup_stale`
- `POST /runtime/meeting/check_calendar_drift`
- `POST /runtime/task/search`
- `POST /runtime/timeblock/search`
- `POST /runtime/sync_conflict/action`

## 6. Telegram Runtime Smoke

Required regression sequence:

1. `задача на завтра выдели время на нее`
2. `15`
3. `30`
4. `Комментарий`
5. `купить молоко`
6. confirm

Expected:

- summary rerenders after comment update
- no `Не нашел подходящую задачу.` after comment entry
- completion succeeds even without an existing matched task

Explicit task-search regression:

- explicit unmatched task lookup must still return `Не нашел подходящую задачу.`

## 7. Unified Edit-Flow Smoke

Verify on both `meeting.update` and `timeblock.update`:

- date edit
- time edit
- duration quick choices `15/30/45/60`
- comment edit
- final summary confirmation

## 8. Google And Sync-Conflict Checks

Google health:

- `/google/health` must perform a real calendar probe

Sync conflict:

- `/sync/conflicts` must be readable on `organizer-api`
- `/runtime/sync_conflict/action` remains the active worker action path

## 9. Key Observability Markers

Telegram/runtime:

- `temporal_edit_*`

Worker:

- `telegram_text_route_entry`
- `runtime_route_selected`
- `memory_fallback_before_return`
- `generic_task_fallback_selected`
- `task_duplicate_precheck_start`
- `task_duplicate_precheck_result`
- `task_create_date_resolution`
- `runtime_command_in`
- `runtime_intent_dispatch_start`
- `runtime_intent_dispatch_result`
- `runtime_commit_path_result`
- `task_parent_resolution`

## 10. Canonical Follow-Up Docs

- `docs/architecture/CURRENT_PRODUCTION_ARCHITECTURE.md`
- `docs/operations/OPERATOR_SMOKE_CHECK.md`
- `docs/operations/MYTGTODOIST_RUNBOOK.md`

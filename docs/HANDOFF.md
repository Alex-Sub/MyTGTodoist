# MyTGTodoist — Current Production Handoff

## Release Snapshot
- Tasks pull-loop enabled.
- Sheets apply-loop enabled (`apply=TRUE`).
- Conflicts are resolved via Telegram buttons.

## A) What Is Deployed
- Runtime-only architecture.
- `organizer-worker` is the single writer of state.
- `organizer-api` is read-only.
- `telegram-bot` handles UX/long polling.
- ML-core is external and called by URL (no ML services in this repo).
- Pull-loops enabled in scheduler: Calendar + Tasks + Sheets.
- Sheets changes are applied only for rows with `apply=TRUE`.

## B) Server
- OS: Ubuntu 24.04
- Project path: `/opt/mytgtodoist`
- Compose project name: `deploy`

## C) Services And Ports
- `organizer-api`: host `8101` -> container `8000`, health endpoint `/health`
- `organizer-worker`: command server `8002` (internal), health marker `/tmp/worker.ok`
- `telegram-bot`: Telegram long polling
- ML tunnel on VPS: `0.0.0.0:19000` (sshd), forwards to home/VM `127.0.0.1:9000`

## PROD (VPS) networking model
- VPS services:
- `organizer-api`
- `organizer-worker`
- `telegram-bot`
- `database`
- External ML stack:
- accessed via SSH reverse tunnel
- `127.0.0.1:19000` -> local `127.0.0.1:9000`
- Container access:
- `ML_CORE_URL=http://host.docker.internal:19000`
- `host-gateway` `extra_hosts` required

## D) Canon + Migrations Mounts
- `../canon:/canon:ro` (expects `/canon/intents_v2.yml`)
- `../migrations:/app/migrations:ro`
- `runtime_data:/data`

## E) How To Operate (Copy-Paste)
- From `/opt/mytgtodoist`

```bash
docker compose -p deploy -f deploy/docker-compose.prod.yml --env-file .env.prod ps
docker compose -p deploy -f deploy/docker-compose.prod.yml --env-file .env.prod logs --tail=200 organizer-worker
docker compose -p deploy -f deploy/docker-compose.prod.yml --env-file .env.prod logs --tail=200 telegram-bot
docker compose -p deploy -f deploy/docker-compose.prod.yml --env-file .env.prod logs --tail=200 organizer-api
curl -fsS http://127.0.0.1:8101/health
docker compose -p deploy -f deploy/docker-compose.prod.yml --env-file .env.prod exec -T organizer-worker python -c "import urllib.request;print(urllib.request.urlopen('http://127.0.0.1:8002/health', timeout=5).read().decode())"
ss -tulpn | grep 19000
curl -fsS http://127.0.0.1:19000/health
```

## F) Smoke Tests (Telegram)
- `текст`
- `закрой задачу`
- `создать цель <название> до <date>`

## G) Known Constraints
- Calendar may be `NOT_CONFIGURED` (accepted during current test stage).
- `curl` is not installed in worker image; use Python `urllib` health check from inside container.
- Open sync conflicts block normal command execution until resolved.
- Conflict resolution goes via Telegram inline buttons:
- `conflict:<conflict_id>:accept_remote`
- `conflict:<conflict_id>:keep_local`

## H) Next Steps
- Run 3–7 days of daily checks: `ps`, API health, worker/bot logs, Telegram smoke commands.
- During test period, monitor pull-loop stability for Calendar/Tasks/Sheets and conflict queue depth.
- Verify Sheets flow: only `apply=TRUE` rows are processed; `APPLIED` rows reset `apply` to `FALSE`; conflict rows are marked `CONFLICT`.
- Key env variables for pull-loops:
- `SYNC_IN_INTERVAL_SEC` (calendar pull)
- `SYNC_OUT_INTERVAL_SEC` (calendar push)
- `GOOGLE_TASKS_PULL_INTERVAL_SEC`
- `GOOGLE_SHEETS_PULL_INTERVAL_SEC`
- `GOOGLE_SHEETS_SPREADSHEET_ID`
- `GOOGLE_SHEETS_RANGE`
- Stable prod criteria:
- no repeated crashes/restarts of worker/bot/api
- no blocking errors in worker loop
- commands from Telegram produce deterministic expected results
- daily digest arrives on schedule
- ML tunnel health remains reachable on `127.0.0.1:19000/health`

## Documentation Scope
- Canonical docs for architecture/deploy: `docs/00_PHILOSOPHY.md` ... `docs/04_DEPLOYMENT.md`.
- `HANDOFF.md` is the required operational handoff file for a new chat/session.
- `OPS_JOURNAL.md` is optional tracking, not required for handoff.

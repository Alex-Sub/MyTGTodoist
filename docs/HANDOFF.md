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
- Compose file set: `docker-compose.yml` + `docker-compose.vps.override.yml`
- Required SA file on VPS: `/opt/mytgtodoist/secrets/alexey/google_sa.json` (regular file, not directory)

## Production Branch Policy
- Production always builds from branch: `runtime-stable`.
- Never develop directly on `main` on VPS.
- Any hotfix on VPS must go via a separate branch -> push -> merge (do not patch `main` directly on VPS).
- VPS must be clean (no modified/untracked files) before switching branch.
- Before any deploy, verify current branch on VPS and abort if branch is not `runtime-stable`.

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
- `./canon:/canon:ro` (expects `/canon/intents_v2.yml`)
- `./migrations:/app/migrations:ro`
- `db_data:/data` (shared named volume for `organizer-worker`, `organizer-api`, `telegram-bot`)
- Worker SA bind mount target: `/data/google_sa.json` from host path `${GOOGLE_SA_FILE_HOST:-./secrets/alexey/google_sa.json}`

## E) How To Operate (Copy-Paste)
- From `/opt/mytgtodoist`

```bash
branch="$(git rev-parse --abbrev-ref HEAD)"
if [ "$branch" != "runtime-stable" ]; then
  echo "[ABORT] deploy allowed only from runtime-stable; current branch=$branch" >&2
  exit 1
fi

docker compose -p deploy --env-file .env.prod -f docker-compose.yml -f docker-compose.vps.override.yml ps
docker compose -p deploy --env-file .env.prod -f docker-compose.yml -f docker-compose.vps.override.yml logs --tail=200 organizer-worker
docker compose -p deploy --env-file .env.prod -f docker-compose.yml -f docker-compose.vps.override.yml logs --tail=200 telegram-bot
docker compose -p deploy --env-file .env.prod -f docker-compose.yml -f docker-compose.vps.override.yml logs --tail=200 organizer-api
curl -fsS http://127.0.0.1:8101/health
docker compose -p deploy --env-file .env.prod -f docker-compose.yml -f docker-compose.vps.override.yml exec -T organizer-worker python -c "import urllib.request;print(urllib.request.urlopen('http://127.0.0.1:8002/health', timeout=5).read().decode())"
ss -tulpn | grep 19000
curl -fsS http://127.0.0.1:19000/health
```

## F) Smoke Tests (Telegram)
- `текст`
- `закрой задачу`
- `создать цель <название> до <date>`

## G) Known Constraints
- Calendar may be `NOT_CONFIGURED` (accepted during current test stage).
- For service account mode, do not use `GOOGLE_CALENDAR_ID=primary`; use the explicitly shared calendar ID (`...@group.calendar.google.com` or concrete calendar id visible in Calendar settings).
- В проде нельзя хардкодить `GOOGLE_CALENDAR_ID=primary` при service account.
- `curl` is not installed in worker image; use Python `urllib` health check from inside container.
- Open sync conflicts block normal command execution until resolved.
- Conflict resolution goes via Telegram inline buttons:
- `conflict:<conflict_id>:accept_remote`
- `conflict:<conflict_id>:keep_local`

## Calendar Env Smoke Check
```bash
dc="docker compose -p deploy --env-file .env.prod -f docker-compose.yml -f docker-compose.vps.override.yml"
$dc exec -T telegram-bot sh -lc 'echo $GOOGLE_CALENDAR_ID'
$dc exec -T organizer-worker sh -lc 'echo $GOOGLE_CALENDAR_ID'
```
- Значения должны совпадать.

## Known Production Constraints
- Runtime DB один: SQLite файл `/data/organizer.db` в volume `deploy_db_data` (compose key `db_data`).
- Нельзя запускать миграции без проверки целевого volume (`deploy_db_data`), иначе возможен split-brain.
- Google SA JSON обязан существовать на VPS как файл:
- `/opt/mytgtodoist/secrets/<user>/google_sa.json`
- В контейнере он должен быть смонтирован как файл по пути:
- `/data/google_sa.json`
- Если source-файл отсутствует, Docker может создать directory вместо файла; это ломает Google sync.
- Для service account нельзя использовать `GOOGLE_CALENDAR_ID=primary`; нужен ID расшаренного календаря.
- ML-функции зависят от активного reverse SSH tunnel `127.0.0.1:19000 -> 127.0.0.1:9000`.

## Google SA JSON Diagnostics (common failure)
- Частая ошибка: файл начинается с `"type": ...` без открывающей `{`.
- Вторая частая ошибка: лишний мусор после закрывающей `}`.
- В `organizer-worker` image нет утилиты `file`; проверку JSON делаем через Python:
```bash
python -c "import json; json.load(open('/data/google_sa.json','r',encoding='utf-8')); print('OK')"
```
- Диагностика на VPS:
```bash
nl -ba /opt/mytgtodoist/secrets/<user>/google_sa.json | sed -n '1,15p'
```
- Быстрый фикс:
- добавить `{` в самое начало файла, если её нет;
- удалить любой лишний текст после финальной `}`.
- Startup preflight (entrypoint) в `CALENDAR_SYNC_MODE=full/create` работает fail-fast:
- если `/data/google_sa.json` невалиден или `GOOGLE_CALENDAR_ID=primary`, контейнер завершится с понятной ошибкой.
- Симптом missing deps:
- `No module named 'google'` в логах worker => calendar принудительно переводится в `NOT_CONFIGURED`.
- Фикс:
- добавить Google deps в `pyproject.toml` и `poetry.lock`, затем пересобрать `organizer-worker` image и перезапустить контейнер.
- Важно:
- `pip install` внутри уже запущенного контейнера — временно и теряется при recreate.
- Зависимости должны быть зафиксированы в Dockerfile/образе и доставляться только через rebuild image.
- Если worker рестартится сразу:
- сначала смотреть `docker compose logs organizer-worker` на `ModuleNotFoundError`;
- фикс: добавить зависимость в `pyproject.toml` + `poetry.lock`, затем rebuild образа.

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

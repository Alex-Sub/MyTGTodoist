# 05_DOCS

Этот файл содержит операционную документацию проекта.
Ниже полностью перенесены источники из `docs/HANDOFF.md` и `docs/OPS_JOURNAL.md`.

---

## HANDOFF (full transfer)

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

## B.1) Compose Project Lock (mandatory)
- Запускать только с project name `deploy`.
- Команды без `-p deploy` запрещены (риск параллельных стеков).
- Каноничная команда:
```bash
docker compose -p deploy --env-file .env.prod -f docker-compose.yml -f docker-compose.vps.override.yml up -d --build
```
- Safe wrapper:
```bash
./run.sh up
```
- Health check:
```bash
./run.sh health
```
- Ожидаемый результат health check:
  - запущен ровно один `organizer-worker`
  - его `/data` смонтирован на volume `deploy_db_data`

## Production Branch Policy
- Production always builds from branch: `runtime-stable`.
- Never develop directly on `main` on VPS.
- Any hotfix on VPS must go via a separate branch -> push -> merge (do not patch `main` directly on VPS).
- VPS must be clean (no modified/untracked files) before switching branch.
- Before any deploy, verify current branch on VPS and abort if branch is not `runtime-stable`.
- Any change flow: local -> commit -> push -> VPS pull -> rebuild/recreate.
- Pre-deploy checks on VPS:
- `git status --porcelain` must be empty.
- `git rev-parse --abbrev-ref HEAD` must be `runtime-stable`.
- Branch sync commands:
- `git fetch --all --prune`
- `git checkout runtime-stable`
- `git pull --ff-only`

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
- `ML_CORE_URL` points to ML-Gateway (VPS example: `http://host.docker.internal:19000`)
- `host-gateway` `extra_hosts` required
- Voice flow in `telegram-bot`:
- `POST ${ML_CORE_URL}/voice-command?profile=organizer`
- multipart field: `file=@audio`
- header: `X-Timezone: Europe/Amsterdam`
- Relative date/time resolution is done only in ML-Gateway (`now_iso` context).

## D) Canon + Migrations Mounts
- `./canon:/canon:ro` (expects `/canon/intents_v2.yml`)
- `./migrations:/app/migrations:ro`
- `db_data:/data` (shared named volume for `organizer-worker`, `organizer-api`, `telegram-bot`)
- Worker SA bind mount target: `/data/google_sa.json` from host path `${GOOGLE_SA_FILE_HOST:-./secrets/alexey/google_sa.json}`
- `telegram-bot` must also mount canon: `./canon:/canon:ro`
- Canon diagnostics:
```bash
dc="docker compose -p deploy --env-file .env.prod -f docker-compose.yml -f docker-compose.vps.override.yml"
$dc exec -T organizer-worker ls -la /canon/intents_v2.yml
```

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
- No `asr-service` on VPS; voice uses ML-Gateway via `ML_CORE_URL` (`/voice-command`).
- Calendar may be `NOT_CONFIGURED` (accepted during current test stage).
- For service account mode, do not use `GOOGLE_CALENDAR_ID=primary`; use the explicitly shared calendar ID (`...@group.calendar.google.com` or concrete calendar id visible in Calendar settings).
- В проде нельзя хардкодить `GOOGLE_CALENDAR_ID=primary` при service account.
- `curl` is not installed in worker image; use Python `urllib` health check from inside container.
- Telegram long-poll guard: runtime enforces `TG_HTTP_READ_TIMEOUT >= TG_LONGPOLL_SEC + 10`.
- Telegram update dedup is enabled (in-memory LRU + persisted file near offset: `${STATE_PATH}.dedup.json`).
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

## Calendar Idempotency Smoke Check
```bash
dc="docker compose -p deploy --env-file .env.prod -f docker-compose.yml -f docker-compose.vps.override.yml"
$dc logs --tail=200 organizer-worker | grep -E "calendar_idempotent|iCalUID|google_event_id"
```
- Повторная обработка одного `item_id` должна логироваться как `action=reuse_*` или `action=update`, без второго insert.

## Google Calendar Stabilization
- `GOOGLE_CALENDAR_ID` must come from `.env.prod` (single source for worker + bot).
- Hardcoded `primary` is forbidden for service-account mode.
- `GOOGLE_SERVICE_ACCOUNT_FILE=/data/google_sa.json`.
- Required deps source-of-truth: `pyproject.toml` + `poetry.lock`.
- Required deps:
- `google-api-python-client`
- `google-auth`
- `google-auth-oauthlib`
- `PyYAML` (`import yaml`)
- Missing deps symptoms:
- `ModuleNotFoundError: No module named 'google'`
- `ModuleNotFoundError: No module named 'yaml'`
- Fix:
- add deps to `pyproject.toml`
- update `poetry.lock`
- rebuild `organizer-worker` image

## Telegram Stability
- Rule: `TG_HTTP_READ_TIMEOUT >= TG_LONGPOLL_SEC + 10`.
- Reason: lower read-timeout causes long-poll timeouts, retries, and repeated updates.
- Recommended prod values:
- `TG_LONGPOLL_SEC=25`
- `TG_HTTP_READ_TIMEOUT=60`

## Calendar Idempotency Standard
- Calendar create is performed only by `organizer-worker`.
- `iCalUID` format: `mytgtodoist-{item_id}@mytgtodoist`.
- Before `events.insert`, worker calls `events.list(iCalUID=...)`.
- If found, worker reuses existing event (no second create).
- `google_event_id` is stored in DB field `calendar_event_id`.
- If `calendar_event_id` exists, worker patches event (update path) instead of insert.

## ASR Preparation
- Voice path goes through `ML_CORE_URL` (ML-Gateway), not direct ASR endpoint.
- Required env key in `.env.prod`: `ML_CORE_URL`.
- Bot sends timezone context with header `X-Timezone: Europe/Amsterdam` (fixed value).
- `ASR_SERVICE_URL` remains legacy and non-critical in current prod contour.

## Troubleshooting Checklist
- If worker restarts:
- `docker compose -p deploy --env-file .env.prod -f docker-compose.yml -f docker-compose.vps.override.yml logs --tail=200 organizer-worker`
- search for `ModuleNotFoundError`
- verify canon mount `/canon/intents_v2.yml`
- verify google deps + yaml in image
- If calendar duplicates appear:
- verify Telegram timeout config (`TG_HTTP_READ_TIMEOUT`, `TG_LONGPOLL_SEC`)
- verify idempotency logs (`iCalUID`, `action=reuse_*`, `action=update`)
- If voice fails:
- verify `ML_CORE_URL` value in env
- verify ML-Gateway endpoints:
- `curl -fsS $ML_CORE_URL/health`
- `curl -fsS $ML_CORE_URL/diag/upstreams`
- if logs show `asr_unavailable status=415`, check input audio format; ensure bot sends WAV as required by `/voice-command` contract
- inspect `telegram-bot` logs
- Quick green tests (local):
- `pytest -q tests/test_telegram_voice_gateway.py tests/test_task_intents_v2.py`

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

---

## OPS_JOURNAL (full transfer)

# OPS Journal (prod-v0)

Цель: зафиксировать рабочий прод-контур и вести наблюдение 7–14 дней без функциональных изменений.

## Environment
- VPS/Host:
- OS:
- Project path: `/opt/mytgtodoist`
- Compose project: `deploy`
- Compose files: `docker-compose.yml` + `docker-compose.vps.override.yml`
- Env file: `.env.prod`
- DB path in container: `/data/organizer.db`
- Runtime volume: `deploy_db_data` (compose key: `db_data`)
- Calendar mode: `NOT_CONFIGURED` допустим на этапе теста

## Baseline snapshot
- Date (UTC):
- Git SHA:
- Compose SHA256:
- Services from `docker compose ps`:
- Health (`http://127.0.0.1:8101/health`):
- Env keys list only (no values):

## Daily checks (Day 1..14)
### Day N — YYYY-MM-DD
- `docker compose ps`: OK / FAIL
- API health: OK / FAIL
- Worker logs (last 120): OK / FAIL
- Bot logs (last 120): OK / FAIL
- Backup created: YES / NO
- Notes:

## Incidents and actions
### YYYY-MM-DD HH:MM UTC
- Symptom:
- Impact:
- Root cause (if known):
- Action taken:
- Result:

### 2026-02-22 00:00 UTC — Split SQLite volume incident
- Symptom: `P3_CALENDAR_*`/`P4_CALENDAR_CANCEL` ошибки вида `no such table: tasks`.
- Impact: calendar pipeline не могла корректно работать, runtime видел неполную схему.
- Root cause: миграции были применены в неверный volume (`deploy_runtime_data`), тогда как runtime использовал `/data/organizer.db` из `deploy_db_data`.
- Action taken: подтверждён фактический DB volume (`deploy_db_data`), миграции применены в него, таблица `tasks` подтверждена.
- Result: схема синхронизирована с runtime DB, инцидент закрыт.
- Prevention: перед любыми миграциями всегда проверять volume name (`deploy_db_data`) и `DB_PATH=/data/organizer.db`.

### 2026-02-23 00:00 UTC — Calendar/canon stabilization + ASR prep baseline
- Symptom: repeated `NOT_CONFIGURED` transitions, missing `/canon/intents_v2.yml`, and duplicate Calendar events under retries.
- Impact: unstable calendar sync, possible duplicate meetings, and worker restarts on missing dependencies/mounts.
- Root cause: non-deterministic deploy state (branch drift/local VPS edits), missing/incorrect mounts, weak long-poll timeout coupling, and non-idempotent event create path.
- Action taken:
- enforced prod branch policy (`runtime-stable`, clean tree before deploy);
- fixed critical mounts (`./canon:/canon:ro`, `./migrations:/app/migrations:ro`);
- documented Google requirements (`GOOGLE_CALENDAR_ID` from env, no `primary`, SA file at `/data/google_sa.json`);
- fixed Telegram stability rule (`TG_HTTP_READ_TIMEOUT >= TG_LONGPOLL_SEC + 10`) and dedup behavior;
- implemented calendar idempotency (`iCalUID`, pre-list/reuse, DB `calendar_event_id` reuse/update path);
- recorded ASR prep contract (`ASR_SERVICE_URL`, `/health`, `/asr`).
- Result: prod contour documented as deterministic with explicit troubleshooting checklist.
- Prevention: all prod updates go through local commit/push and VPS pull/rebuild from `runtime-stable` only.

### 2026-02-23 00:00 UTC — Voice ASR 415 incident
- Symptom: Telegram voice flow returns canonical message `Не получилось разобрать речь. Попробуй сказать ещё раз.`
- Evidence: `telegram-bot` logs show `gateway_call` to `/voice-command` and then `event=asr_unavailable status=415`.
- Interpretation: `415 Unsupported Media Type` on `/voice-command`; likely Telegram voice format (`ogg/opus`) is not accepted by current gateway contract (expects `audio.wav`).
- Action taken: documentation update only (no code changes).

## PROD-V0 STABLE

Дата фиксации: 2026-02-21

Контур:
- VPS: Ubuntu 24.04
- Docker compose (v2)
- 3 контейнера: worker / api / telegram-bot
- SQLite в volume
- Миграции авто-применяются
- Canon смонтирован ro
- Calendar: NOT_CONFIGURED (допустимо)

Система работает стабильно.
SSH-сессия может разрываться, но сервисы продолжают работу.

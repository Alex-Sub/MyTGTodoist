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

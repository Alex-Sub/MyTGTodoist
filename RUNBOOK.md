# Runbook (Stage 4)

Короткий практический чек-лист по основным инцидентам.

## 1) Queue degraded

Симптомы (по алерту):
- `[PROD] HEALTH ok -> degraded`
- `Reasons: queue=degraded (len=..., oldest=...sec)`

Диагностика:
- `docker compose logs --tail=200 organizer-worker`
- `docker compose exec -T organizer-api python -c "import sqlite3; c=sqlite3.connect('/data/organizer.db'); print(c.execute(\"select status,count(*) from inbox_queue group by status\").fetchall())"`

Восстановление:
- `docker compose restart organizer-worker`
- (если причина в ASR) `docker compose restart asr-service`

Критерий ok:
- `/health` показывает `queue=ok` и `oldest_age_sec` падает.

## 2) DB error

Симптомы (по алерту):
- `[PROD] HEALTH * -> error`
- `Reasons: db=error ...`

Диагностика:
- `docker compose logs --tail=200 organizer-api`
- `docker compose exec -T organizer-api python -c "import sqlite3; c=sqlite3.connect('/data/organizer.db'); print(c.execute('select 1').fetchone())"`

Восстановление:
- `docker compose restart organizer-api`
- проверить доступность volume `db_data`

Критерий ok:
- `/health` возвращает `db=ok` и `status=ok|degraded`.

## 3) API down (/health недоступен)

Симптомы:
- `curl https://<DOMAIN>/health` не отвечает / 502 / timeout
- нет новых алертов, но есть внешние ошибки

Диагностика:
- `docker compose ps`
- `docker compose logs --tail=200 organizer-api`

Восстановление:
- `docker compose restart organizer-api`
- проверить nginx proxy на `/health`

Критерий ok:
- `curl -I https://<DOMAIN>/health` возвращает 200.

## 4) Poller/scheduler dead (queue растёт, алертов нет)

Симптомы:
- очередь растёт, но нет алертов о деградации
- `/health` показывает `queue=degraded`, а Telegram молчит

Диагностика:
- `docker compose logs --tail=200 organizer-api | rg -n "health poller|health alert"`
- `docker compose exec -T organizer-api python -c "import sqlite3; c=sqlite3.connect('/data/organizer.db'); print(c.execute(\"select key,value,updated_at from ops_state where key in ('health_last','ingestion_pause_last_eval_at')\").fetchall())"`

Восстановление:
- `docker compose restart organizer-api`
- проверить `ALERT_CHAT_ID` и `TELEGRAM_BOT_TOKEN`

Критерий ok:
- обновляется `ops_state.health_last.updated_at`
- приходят алерты при смене состояния.

## 5) Delivery SLO degraded

Симптомы (по алерту):
- `[PROD] DELIVERY SLO degraded p90=..., overdue=...`

Диагностика:
- `docker compose exec -T organizer-api python -m digest --db /data/organizer.db`
- `docker compose exec -T organizer-api python -c "from delivery import compute_delivery_stats; print(compute_delivery_stats('/data/organizer.db'))"`

Восстановление:
- `docker compose restart organizer-worker`
- проверить очередь и Google Calendar credentials

Критерий ok:
- `compute_delivery_stats` показывает p90 ниже SLO и overdue <= max.

## 6) Google Calendar (service account)

Настройка:
1) В Google Calendar UI открой нужный календарь (не `primary`) → `Settings and sharing`.
2) `Share with specific people` → добавь `organizer-worker@mytgtodoist.iam.gserviceaccount.com`
   с правом **"Вносить изменения в события"**.
3) Узнай `calendarId` (обычно `...@group.calendar.google.com`) и поставь его в
   `deploy/tenants/.env.alexey` как `GOOGLE_CALENDAR_ID`.
4) Убедись, что service account json смонтирован в контейнер:
   `./secrets/alexey/google_sa.json -> /data/google_sa.json:ro`

Диагностика:
- Включи `CALENDAR_DEBUG=1` в env и перезапусти `organizer-worker`.
- В логах появятся строки `calendar_list id=... summary=...`.

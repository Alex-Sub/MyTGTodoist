# Operations

## Канон эксплуатации

- `organizer-worker` — **долгоживущий сервис** (в Docker).
- `telegram-bot` — **долгоживущий сервис** (polling).
- `asr-service` — сервис.
- База истины: SQLite `/data/organizer.db` (volume `db_data`).
- TODO: extract to dedicated ops doc — `telegram-bot` HTTP client: предпочитает `requests`, fallback на `urllib`, если `requests` нет.

Stage 0 (локально/тест):
- падения допустимы,
- перезапуск руками допустим.

Stage 1 (прод):
- `restart: unless-stopped`,
- без ручного вмешательства.

---

## Быстрая диагностика (Docker)

```bash
docker compose ps
docker compose logs --tail=200 organizer-worker
docker compose logs --tail=200 telegram-bot
docker compose logs --tail=200 organizer-api
docker compose logs --tail=200 asr-service
```

Health:
- API: `http://127.0.0.1:8000/health`
- ASR: `http://127.0.0.1:8001/health`

---

## Диагностика базы (SQLite внутри контейнера)

Важно: PowerShell **не поддерживает** heredoc `<<'PY'`.  
Используйте `python -c "..."` или `-T` для exec без TTY.

### Items (последние записи)
```bash
docker compose exec -T organizer-api python -c "import sqlite3; con=sqlite3.connect('/data/organizer.db'); print(con.execute('select id,type,status,start_at,calendar_event_id,attempts from items order by id desc limit 20').fetchall())"
```

### Calendar (PENDING/FAILED)
```bash
docker compose exec -T organizer-api python -c "import sqlite3; con=sqlite3.connect('/data/organizer.db'); print(con.execute(\"select id,title,status,start_at,calendar_event_id,attempts,substr(coalesce(last_error,''),1,120) from items where calendar_event_id in ('PENDING','FAILED') order by id desc limit 50\").fetchall())"
```

### Inbox queue (статусы)
```bash
docker compose exec -T organizer-api python -c "import sqlite3; con=sqlite3.connect('/data/organizer.db'); print(con.execute("select status,count(*) from inbox_queue group by status").fetchall())"
```

### Inbox queue (подозрительные CLAIMED)
```bash
docker compose exec -T organizer-api python -c "import sqlite3; con=sqlite3.connect('/data/organizer.db'); print(con.execute("select id,status,attempts,claimed_by,lease_until,substr(coalesce(last_error,''),1,120) from inbox_queue where status='CLAIMED' order by id desc limit 20").fetchall())"
```

---

## Интерпретация состояний

### Calendar
- `PENDING` — worker будет повторять
- `FAILED` — исчерпан лимит, смотреть `last_error`
- `<real_event_id>` — интеграция успешна

--- 

## E2E проверка календаря

Шаги:
1) Создать встречу (voice/text) → получить `item_id`.
2) Проверить в БД, что `start_at/end_at` заполнены:
   - `select id,start_at,end_at,calendar_event_id from items where id=<ID>;`
3) Проверить, что событие появилось в Google Calendar.
4) Перезапустить worker:
   - `docker compose restart organizer-worker`
5) Убедиться, что повторного создания нет:
   - `calendar_event_id` остаётся `<real_event_id>`,
   - worker логирует `calendar_state after=<real_event_id> (skip)`.

### Queue
- `NEW` растёт → worker не успевает / завис / ASR тормозит
- `CLAIMED` висит долго → lease/reaper не отрабатывает или worker завис
- `FAILED` растёт → смотреть `last_error` (часто ASR timeout)

---

## Runbook: типовые инциденты

### 1) ASR timeout
Симптом:
- в worker логах `Read timed out` на `asr-service`.

Действия:
- проверить `asr-service /health`,
- увеличить `ASR_HTTP_READ_TIMEOUT`,
- убедиться, что очередь не растёт бесконтрольно (backpressure).

### 2) Бот не отвечает / кнопки не появляются
Действия:
- `docker compose logs --tail=200 telegram-bot`,
- проверить, что `TELEGRAM_BOT_TOKEN` задан,
- убедиться, что sendMessage не возвращает ошибку (в логах).

### 3) «Создаёт новый item вместо уточнения»
Причина:
- нет состояния уточнения (clarify state), или оно истекло (TTL),
- или ветка обработки ответа пользователя не сработала.

Действия:
- проверить файл состояния (volume `bot_state`): `/data/bot.clarify.json`,
- проверить TTL: `CLARIFY_TTL_SEC`,
- проверить логи бота на `clarify_*`.

---

## Обновление

```bash
git pull
docker compose up -d --build
```

---

## Ссылки

- Цели периода (канон) — `Docs/04_USER_GUIDE.md`

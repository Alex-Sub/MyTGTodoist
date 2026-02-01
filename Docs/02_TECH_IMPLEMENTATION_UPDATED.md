# Техническая реализация проекта

## 1. Общая архитектура

Архитектура: **единое ядро (БД + worker) + несколько интерфейсов + подключаемые сервисы**.

### Компоненты
- **telegram-bot** — основной UI (polling), команды, inline-кнопки, персистентный offset.
- **organizer-api** — CRUD по `items`, `/health`, `/stats`, вспомогательные эндпоинты (например `/items/{id}/schedule`).
- **organizer-worker** — single consumer: сериализация обработки, распознавание дат/времени, интеграция с календарём, обработка очереди.
- **asr-service** — ASR (`/health`, `/transcribe`).
- **База данных (SQLite)** — источник истины: `items` + `inbox_queue` (+ служебные поля ретрая/интеграций).

Опционально (на будущее):
- **PostgreSQL (pgvector)** — для embedding / RAG / поиска. В текущей стадии ядро работает на SQLite и не зависит от Postgres.

---

## 2. Контейнерная модель (ключевое решение)

Проект разделён на сервисы в Docker Compose:

- `telegram-bot`
- `organizer-api`
- `organizer-worker`
- `asr-service`
- volume `db_data` (SQLite файл `/data/organizer.db`)
- volume `bot_state` (offset + clarify state)

---

## 3. Хранение и синхронизация

### База данных (источник истины)
**SQLite**, файл: `/data/organizer.db` (volume `db_data`).

Критично:
- любые состояния/идемпотентность фиксируются в БД,
- внешние интеграции (Telegram/Calendar) — производные.
- результаты чтения из SQLite приходят как `sqlite3.Row`:
  - доступ **только** через `row["field"]` или `dict(row)` один раз на входе,
  - `row.get(...)` **запрещён** (нет метода `.get` у `sqlite3.Row`).

### Google Calendar
- события создаются/обновляются **worker**-ом,
- `calendar_event_id` используется как состояние интеграции:
  - `PENDING` → будет ретрай
  - `FAILED` → исчерпан лимит попыток
  - `<real_event_id>` → событие создано
- state machine (идемпотентность):
  - `NULL` → попытка создания → `<id>` / `PENDING` / `FAILED`
  - `PENDING` → retry → `<id>` / `PENDING` / `FAILED`
  - `FAILED` → skip
  - `<id>` → skip
- лимит попыток: `CALENDAR_MAX_ATTEMPTS` (env, дефолт `5`).

---

## 4. Stage B2+ — очередь входящих сообщений (inbox_queue)

### Назначение
Telegram polling **не является очередью** (могут быть таймауты/повторы/догоняние backlog).  
Поэтому введён внутренний слой **B2: inbox_queue**.

### Поток данных (канон)
1) `telegram-bot` получает update → **enqueue** в `inbox_queue` (SQLite).
2) `organizer-worker` делает **claim/lease** одного элемента → обрабатывает → помечает `DONE/FAILED/DEAD`.
3) Результат отражается в `items`, и (для встреч) в Google Calendar.

### Схема inbox_queue (фактическая)
Таблица `inbox_queue` хранит:
- идентичность telegram update (идемпотентность),
- payload (JSON),
- состояние обработки (`NEW/CLAIMED/DONE/FAILED/DEAD`),
- lease/attempts/last_error.

Уникальность:
- `UNIQUE (source, tg_chat_id, tg_update_id)`.

Backpressure (порог перегруза):
- `B2_QUEUE_MAX_NEW` / `B2_QUEUE_MAX_TOTAL`,
- режим `B2_BACKPRESSURE_MODE=reject` — бот отвечает пользователю «очередь перегружена».

---

## 5. Stage B3–B4 — планирование встреч и уточнения

### Автораспознавание
Worker пытается детерминированно извлечь дату/время из текста (RU):
- относительные даты (сегодня/завтра/послезавтра),
- дни недели,
- «на следующей неделе/в следующем месяце/в этом году» (маркер),
- «третьего/четвертого …» (день месяца без указания месяца) — ближайший подходящий месяц.

### Маркеры неопределённости
Если времени нет или оно **двусмысленное** (например «в 9», без «утра/вечера»):
- встреча создаётся в `inbox` с маркерным временем (например 06:00) или без расписания,
- пользователю отправляется уточнение.

### Уточнение пользователем
- Команда: `/set #ID [дата] [в] HH[:MM] [мин]`
- Inline-кнопки: **Утро / Вечер / Отменить** (callback).

Важно:
- если у пользователя есть «ожидающее уточнение», его ответ **не должен создавать новый item** (обрабатываем как подтверждение/уточнение).

---

## 6. Среда разработки

- Python 3.11+
- Docker Compose
- FastAPI
- requests
- (опционально) SQLAlchemy/Alembic — только при переходе на миграции, сейчас база — SQLite файл.

---

## 7. Stage 0 — фактическая реализация (E2E)

**Сервисы (docker compose):**
- `organizer-api` (порт 8000) — CRUD + `/health` + `/stats`
- `organizer-worker` — single consumer (queue + calendar)
- `telegram-bot` — polling bot, voice/text → inbox_queue, команды `/status`, `/set`
- `asr-service` (порт 8001) — `/health` + `/transcribe`

**Источник истины:** SQLite файл `organizer.db` (volume `db_data`).

**Статистика:** `GET /stats` возвращает:
- counts по `status`
- `pending_calendar_count` (`calendar_event_id='PENDING'`)
- `failed_calendar_count` (`calendar_event_id='FAILED'`)
- `latest` (последние N записей)

---

## ASR контракт

**Endpoint:**
- `POST /transcribe`
- `multipart/form-data`, поле файла: `file`

**Типовые ошибки:**
- `422` — поле `file` отсутствует / запрос некорректный
- `500` — ffmpeg/конвертация/рантайм

Операционное правило:
- ASR может быть недоступен — система обязана:
  - не терять входящий запрос (через очередь),
  - честно сообщать пользователю о задержке/ошибке.

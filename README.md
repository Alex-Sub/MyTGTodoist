# MyTGTodoist

Runtime-only репозиторий: бизнес-логика (worker + Telegram UX + read-only API). ML core стек (ASR/embeddings/RAG) внешний.

## Runtime production path

Current active path:

- User -> Telegram -> ML -> Runtime decision -> organizer-worker -> DB -> google-sync

DB is source of truth.
`organizer-worker` is the only DB writer.
Legacy/compat paths are not default and must not be used for new features.

Worker mode:
- `organizer-worker` now starts in active-runtime-only mode by default.
- Legacy queue/items loop is quarantined and disabled unless `ALLOW_LEGACY_WORKER_QUEUE=1`.
- Do not enable `ALLOW_LEGACY_WORKER_QUEUE` in production compose/env.
- Active worker surface is `/runtime/*`.

Worker code layout:
- `organizer-worker/worker.py` is now a bootstrap / compatibility facade.
- Active runtime is split into:
  - `organizer-worker/runtime_server.py`
  - `organizer-worker/runtime_handlers.py`
  - `organizer-worker/runtime_calendar.py`
  - `organizer-worker/runtime_sync_conflicts.py`
  - `organizer-worker/runtime_search.py`
  - `organizer-worker/shared_runtime.py`
- Quarantined legacy queue/items lives in `organizer-worker/legacy_queue.py`.

Текущий production contour:
- Telegram runtime работает только в direct режиме (`runtime_core_direct`).
- Compat path выведен из активной системы.
- ML слой остаётся source-of-truth для интерпретации (`intent/entities/clarification`), runtime — для state changes и execution.

Legacy / rollback note:
- `telegram-bot/bot.py` больше не должен использоваться как обычный entrypoint.
- Если legacy path случайно выбран, startup должен завершаться с ошибкой.
- Инвентаризация и staged removal plan: `docs/development/LEGACY_INVENTORY_RUNTIME_CORE_DIRECT_2026-05-10.md`.

## Компоненты (каноничные)
- `organizer-worker/` : single writer, применяет команды и пишет в SQLite.
- `organizer-api/` : read-only API (порт `8101:8000` в локальном compose).
- `telegram-bot/` : UX только, не пишет в БД (ходит в worker по HTTP).
- `migrations/*.sql` : runtime SQL миграции, применяются worker'ом.

## Поддерживаемые intents
- `task.create`
- `task.complete`
- `task.set_status`
- `task.reschedule`
- `task.move_to`
- `task.move` (deprecated, backward compatible alias to `task.set_status`)
- `task.update`
- `subtask.create`
- `subtask.complete`
- `timeblock.create`
- `timeblock.move`
- `timeblock.delete`
- `reg.run`
- `reg.status`
- `state.get`

## Запуск (локально, docker compose)

Требуется Docker Desktop (или docker engine) запущенный.

```bash
docker compose -p deploy up --build -d
docker compose -p deploy ps
```

API health: `http://127.0.0.1:8101/health`

Worker smoke:

```bash
curl http://127.0.0.1:8102/health
curl -X POST http://127.0.0.1:8102/runtime/task/search \
  -H "Content-Type: application/json" \
  -d '{"user_id":"u-smoke","target_hint":"test"}'
```

Production note:
- In Docker, worker host port may not be published; check from inside compose network or via service logs.

## VPS deploy (single project lock)

Всегда запускайте только с `-p deploy`, чтобы не поднимать дублирующие compose-стеки.

```bash
docker compose -p deploy --env-file .env.prod -f docker-compose.yml -f docker-compose.vps.override.yml up -d --build
docker compose -p deploy --env-file .env.prod -f docker-compose.yml -f docker-compose.vps.override.yml ps
```

Health check (должен быть ровно один `organizer-worker`, и он должен использовать volume `deploy_db_data`):

```bash
./run.sh health
```

Safe runner (опционально):

```bash
./run.sh up
```

## Тесты

```bash
pytest -q
```

## Documentation

- Canonical docs root: `docs/`
- Entry point: `docs/DOCS_INDEX.md`
- Runtime contract: `docs/contracts/APP_RUNTIME_CONTRACT.md`
- Intent catalog: `docs/contracts/INTENT_CATALOG.md`
- System spec: `docs/contracts/SYSTEM_SPEC.md`
- Decision rules: `docs/architecture/DECISION_ENGINE_RULES.md`
- Runtime state machine: `docs/architecture/RUNTIME_STATE_MACHINE.md`
- Adapters: `docs/engineering/ADAPTERS.md`
- Operations runbook: `docs/operations/RUNBOOK.md`
- Startup verification runbook: `docs/STARTUP_RUNBOOK_VERIFIED.md`

## ASR (voice)

ASR/voice идет через ML gateway. Укажите URL:
- `ML_GATEWAY_URL` (например `http://host.docker.internal:19000`)

`ASR_SERVICE_URL` retired (legacy historical alias; no longer part of the current contour).

## Подтверждение действий (NLU/voice)

Команды с `/` выполняются сразу. Для NLU (обычный текст/голос) runtime использует draft/summary/confirmation перед commit для temporal и `task.create`.

Temporal flow:
- alias-aware вход (например: `забронируй время`, `выдели слот`, `запланируй мероприятие`);
- порядок уточнений: `date -> time -> duration`;
- past-date guard: если дата раньше текущей, запрашивается отдельное подтверждение;
- перед execution показывается summary: тип, дата, время, длительность.

Task create flow:
- поддержаны natural alias-фразы (`создай задачу`, `добавь задачу`, `надо сделать ...`, `нужно позвонить ...`, `напомни сделать ...`);
- перед commit показывается summary: текст, optional due date / priority / notes.

Telegram clarification UX:
- inline buttons: past-date confirm (`Да/Нет`), duration presets (`15/30/45/60`), `task_create_confirm` (`Да/Нет`), `temporal_commit_confirm` (`Да/Нет`);
- кнопки эквивалентны текстовому ответу (логика единая);
- под кнопками есть helper text, пользователь может ввести значение вручную.

Global cancel:
- поддержаны фразы: `отмена`, `стоп`, `не надо`, `отбой`, `всё, закончили` (и вариант `все закончили`);
- активный сценарий (clarification/draft/confirmation) прерывается без commit.

Dependency note:
- после перехода на YAML alias-layer в decision engine требуется runtime dependency `PyYAML` в `telegram-bot` image.

Минимальная проверка вручную:

- Отправьте temporal-команду без части полей (например, `запланируй встречу`).
- Завершите уточнения в порядке `дата -> время -> длительность`.
- Проверьте summary с кнопками `Да/Нет` перед commit.
- Нажмите `Да` и убедитесь, что после подтверждения выполняется execution.
- Повторите с `Нет` и убедитесь, что runtime возвращает в correction flow без commit.

### Smoke-тесты (ручные)

1) Temporal alias: `забронируй время` -> date/time/duration clarifications -> summary (`Да/Нет`) -> `Да`.
2) Time-only continuation: ответ `в 12` без даты не должен закрывать date field.
3) Past-date guard: ввести прошедшую дату -> получить вопрос подтверждения -> `Нет` -> re-ask date.
4) Task alias: `нужно позвонить клиенту` -> task summary (`Да/Нет`) -> `Да`.
5) Global cancel: на любом активном шаге написать `отмена` -> сценарий очищен без commit.

---

## Local read-only file service (v1)

Минимальный локальный HTTP-сервис только для чтения и поиска по файлам.

### Возможности

- `list_dir(path)`
- `find_files(query, root=None, max_results=100)`
- `read_text_file(path, max_chars=200000)`
- `project_tree(path, depth=3)`
- `grep_text(query, root=None, max_results=100)`

### Разрешённые корни

В `config.py`:

- `D:\My_AI_Prodgekt\MyTGTodoist`
- `D:\VMShare`

### Безопасность

- Любой путь нормализуется через `Path.resolve()`.
- Любая операция разрешена только внутри `ALLOWED_ROOTS`.
- Рекурсивные операции исключают: `.git`, `.venv`, `__pycache__`, `node_modules`, `dist`, `build`, `.mypy_cache`, `.pytest_cache`.
- Сервис read-only: записи/удаления/переименования/выполнения команд не реализованы.

### Установка и запуск

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
python server.py
```

Сервис поднимается на `http://127.0.0.1:8765`.

### Примеры вызовов

`list_dir`:

```bash
curl -X POST http://127.0.0.1:8765/list_dir \
  -H "Content-Type: application/json" \
  -d '{"path":"D:\\My_AI_Prodgekt\\MyTGTodoist"}'
```

`find_files`:

```bash
curl -X POST http://127.0.0.1:8765/find_files \
  -H "Content-Type: application/json" \
  -d '{"query":"readme","max_results":20}'
```

`read_text_file`:

```bash
curl -X POST http://127.0.0.1:8765/read_text_file \
  -H "Content-Type: application/json" \
  -d '{"path":"D:\\My_AI_Prodgekt\\MyTGTodoist\\README.md","max_chars":5000}'
```

`project_tree`:

```bash
curl -X POST http://127.0.0.1:8765/project_tree \
  -H "Content-Type: application/json" \
  -d '{"path":"D:\\My_AI_Prodgekt\\MyTGTodoist","depth":2}'
```

`grep_text`:

```bash
curl -X POST http://127.0.0.1:8765/grep_text \
  -H "Content-Type: application/json" \
  -d '{"query":"todoist","max_results":30}'
```

## MCP Filesystem Server (read-only)

Текущий файловый сервис доступен как MCP server через `stdio`.

### Точка входа

- `mcp_server.py`

### Запуск

Локальный `stdio` режим (например, для MCP Inspector/CLI):

```bash
python mcp_server.py --transport stdio
```

Remote режим для ChatGPT custom connector (streamable HTTP endpoint):

```bash
python mcp_server.py --transport http --host 0.0.0.0 --port 8787 --mcp-path /mcp
```

### Разрешённые корни

Задаются в `config.py`:

- `D:\My_AI_Prodgekt\MyTGTodoist`
- `D:\VMShare`

### Доступные MCP tools

- `list_dir(path)` -> `{ path, dirs, files }`
- `find_files(query, root=null, max_results=100)` -> `{ query, results }`
- `read_text_file(path, max_chars=200000)` -> `{ path, content, truncated }`
- `project_tree(path, depth=3)` -> `{ path, depth, tree }`
- `grep_text(query, root=null, max_results=100)` -> `{ query, matches }`
- `read_many_files(paths, max_chars_per_file=100000)` -> `{ items }`
- `file_info(path)` -> `{ path, exists, is_file, is_dir, suffix, size_bytes, modified_at }`

### Переменные окружения (remote)

- `MCP_TRANSPORT` (`stdio` | `http`, по умолчанию `stdio`)
- `MCP_HOST` (по умолчанию `0.0.0.0`)
- `MCP_PORT` (по умолчанию `8787`)
- `MCP_PATH` (по умолчанию `/mcp`)
- `MCP_AUTH_TOKEN` (опционально: если задан, обязателен `Authorization: Bearer <token>`)

### Поведение безопасности

- Сервер строго read-only.
- Любой путь проходит проверку через `Path.resolve()` и `ALLOWED_ROOTS`.
- Доступ вне разрешённых корней блокируется.
- Используются существующие модули безопасности и файловой логики: `config.py`, `safety.py`, `filesystem_tools.py`.

### Совместимость с ChatGPT connectors

- ChatGPT custom connectors ожидают публичный HTTPS endpoint вида `https://<host>/mcp`.
- Для remote MCP поддерживается `Streamable HTTP` (и также `HTTP/SSE` по документации OpenAI).
- Текущий сервер экспортирует MCP JSON-RPC по `POST /mcp`, плюс `OPTIONS /mcp` и `GET /` для health/info.
- Все tools отмечены как read-only через MCP `annotations`.

### Минимальный deployment guide (VPS)

1. Установить зависимости и запустить сервер:

```bash
cd /opt/mytgtodoist
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
MCP_TRANSPORT=http MCP_HOST=0.0.0.0 MCP_PORT=8787 MCP_PATH=/mcp python mcp_server.py
```

2. Открыть порт только для reverse proxy/firewall (рекомендуется не экспонировать `8787` наружу напрямую).

3. Поставить HTTPS перед сервером (например, Nginx/Caddy) и проксировать:

- `https://your-domain.example/mcp` -> `http://127.0.0.1:8787/mcp`

4. Ограничить доступ:

- вариант A: `MCP_AUTH_TOKEN` + проверка `Authorization: Bearer ...`
- вариант B: IP allowlist на уровне firewall/reverse proxy
- вариант C: OAuth 2.1 по MCP spec (рекомендуемый production-путь для ChatGPT Apps)

5. Проверка снаружи:

```bash
curl -X POST https://your-domain.example/mcp \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"smoke","version":"1"}}}'
```

### Пример конфигурации MCP-клиента

```json
{
  "mcpServers": {
    "filesystem-ro": {
      "command": "python",
      "args": ["D:\\My_AI_Prodgekt\\MyTGTodoist\\mcp_server.py"]
    }
  }
}
```

### Checklist: подключение в ChatGPT

1. Включить **Developer mode** в ChatGPT.
2. Открыть **Settings → Apps & Connectors**.
3. Нажать **Create / add custom connector**.
4. Указать публичный HTTPS URL сервера: `https://your-domain.example/mcp`.
5. Проверить, что ChatGPT видит tools из `tools/list`.
6. Запустить тестовый вызов (`file_info` или `list_dir`) и убедиться, что ответ корректный.
7. После изменений в tools нажать **Refresh** в карточке connector.

### Примеры MCP вызовов

`tools/call -> list_dir`:

```json
{
  "jsonrpc": "2.0",
  "id": 3,
  "method": "tools/call",
  "params": {
    "name": "list_dir",
    "arguments": {
      "path": "D:\\My_AI_Prodgekt\\MyTGTodoist"
    }
  }
}
```

`tools/call -> grep_text`:

```json
{
  "jsonrpc": "2.0",
  "id": 4,
  "method": "tools/call",
  "params": {
    "name": "grep_text",
    "arguments": {
      "query": "todoist",
      "root": "D:\\My_AI_Prodgekt\\MyTGTodoist",
      "max_results": 20
    }
  }
}
```

`tools/call -> read_many_files`:

```json
{
  "jsonrpc": "2.0",
  "id": 5,
  "method": "tools/call",
  "params": {
    "name": "read_many_files",
    "arguments": {
      "paths": [
        "D:\\My_AI_Prodgekt\\MyTGTodoist\\README.md",
        "D:\\My_AI_Prodgekt\\MyTGTodoist\\pyproject.toml",
        "D:\\My_AI_Prodgekt\\MyTGTodoist\\Фото 1.jpg"
      ],
      "max_chars_per_file": 5000
    }
  }
}
```

`tools/call -> file_info`:

```json
{
  "jsonrpc": "2.0",
  "id": 6,
  "method": "tools/call",
  "params": {
    "name": "file_info",
    "arguments": {
      "path": "D:\\My_AI_Prodgekt\\MyTGTodoist\\README.md"
    }
  }
}
```

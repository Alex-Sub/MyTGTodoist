# MyTGTodoist

Runtime-only репозиторий: бизнес-логика (worker + Telegram UX + read-only API). ML core стек (ASR/embeddings/RAG) внешний.

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
docker compose up --build -d
docker compose ps
```

API health: `http://127.0.0.1:8101/health`

## Тесты

```bash
pytest -q
```

## Documentation

- `docs/00_PHILOSOPHY.md`
- `docs/01_SYSTEM_OVERVIEW.md`
- `docs/02_RUNTIME_SPEC.md`
- `docs/03_ML_CONTRACT.md`

## ASR (voice)

ASR сервис внешний. Укажите URL:
- `ASR_SERVICE_URL` (например `http://localhost:8001`)

## Подтверждение действий (NLU/voice)

Команды с `/` выполняются сразу. Для NLU (обычный текст/голос) при высокой уверенности бот отправляет предпросмотр и кнопки подтверждения.

Минимальная проверка вручную:

- Отправьте обычный текст без `/` (или голос) с понятной командой.
- Убедитесь, что бот прислал предпросмотр и кнопки ✅/✏️/❌.
- Нажмите ✅ — действие выполняется.
- Нажмите ✏️ — бот попросит корректировку, следующее сообщение создаёт новый черновик.

### Smoke-тесты (ручные)

1) CREATE_MEETING без даты → запрос даты → "завтра" → preview → ✅ → создано.
2) CREATE_MEETING без времени → запрос времени.
3) Неверная дата → повторный запрос даты.
4) Pending expired → ✅ → "Черновик устарел. Повтори команду."
5) LLM выключен → поведение только rule-based, preview при conf>=0.6.

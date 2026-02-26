# 03_ADAPTERS

## Назначение
Документирует транспортный слой и UX-каналы, через которые пользователь взаимодействует с runtime.

Принцип границ:
- Адаптеры принимают вход пользователя.
- Адаптеры вызывают ML/Runtime.
- Адаптеры отображают результат.
- Адаптеры не принимают бизнес-решения и не меняют состояние напрямую.

## Каналы
- Telegram bot (`telegram-bot/bot.py`): text/voice UX, long polling, inline actions.
- API (`organizer-api/app.py`, `src/api/*`): read-only endpoints состояния/health.
- Worker command server (`organizer-worker/worker.py`): runtime execution endpoint (internal).

## Telegram Adapter Canon
- Long polling обязателен для прод-контура.
- Guard по таймаутам: `TG_HTTP_READ_TIMEOUT >= TG_LONGPOLL_SEC + 10`.
- Dedup updates обязателен (in-memory + persisted state file).
- Конфликты sync обрабатываются через inline callbacks:
  - `conflict:<conflict_id>:accept_remote`
  - `conflict:<conflict_id>:keep_local`

## Voice Adapter Canon
- Telegram voice flow вызывает только ML Gateway endpoint:
  - `POST ${ML_CORE_URL}/voice-command?profile=organizer`
- Формат отправки: `multipart/form-data`, поле `file`.
- Временной контекст передаётся заголовком `X-Timezone`.
- `X-Timezone` берётся из env `APP_TIMEZONE` (fallback: `TIMEZONE`, default: `Europe/Moscow`).
- Для production canonical значение: `Europe/Moscow`.
- Relative date/time resolution выполняется в ML Gateway (не в Telegram adapter).

## Reverse Tunnel Contract (VPS <-> VM)
- ML-Gateway запущен на VM (порт `9000`).
- Reverse SSH tunnel поднимается из VM в VPS:
  - `ssh -R 127.0.0.1:19000:127.0.0.1:9000 ...`
- На VPS контейнеры обращаются к ML через:
  - `ML_CORE_URL=http://host.docker.internal:19000`
- Проверка канала:
  - `GET http://127.0.0.1:19000/health` на VPS должен возвращать `ok=true`.

## API Adapter Canon
- `organizer-api` только read-only, state-mutation запрещён.
- Health endpoint обязателен: `/health`.
- API используется для наблюдения и витринных сценариев, но не для командной записи.

## Error/Clarification UX
- Если runtime вернул `ok=false`, adapter должен показать ровно один `clarifying_question`.
- Если runtime вернул `choices`, adapter обязан показать варианты выбора, без автовыбора.
- Safe-fail ответы runtime транслируются дословно без reinterpretation.

## Deploy/Infra Constraints for adapters
- В VPS режиме ML доступен через reverse tunnel (`host.docker.internal:19000`).
- `telegram-bot` обязан иметь mount `./canon:/canon:ro`.
- Adapter не должен обращаться напрямую к ASR/LLM в прод-контуре.

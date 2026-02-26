# ML_STACK_CANON

What: Краткий master canon для MyTGTodoist runtime + ML integration.
When: Быстрый вход в проект, handoff, deploy-check.
Not: Не заменяет полные документы `01_canon.MD`, `02_RUNTIME_SPEC.md`, `05_docs.md`.

## Architecture Snapshot
- Runtime-only core в этом репозитории.
- `organizer-worker` — единственный writer состояния.
- `organizer-api` — read-only.
- `telegram-bot` — transport/UX.
- ML-core внешний (через `ML_CORE_URL`).

## Runtime Canon
- Intent canon source: `canon/intents_v2.yml`.
- Envelope schema: `schemas/command_envelope.schema.json`.
- Runtime rules:
  - не интерпретирует текст;
  - не угадывает missing fields;
  - задаёт ровно один `clarifying_question`;
  - при ambiguity без `chosen_id` всегда clarification.

## Production Topology (VPS)
- Compose project: `deploy`.
- Services: `organizer-worker`, `organizer-api`, `telegram-bot`.
- Shared DB: SQLite `/data/organizer.db` в volume `deploy_db_data` (compose key `db_data`).
- ML доступ в проде через reverse tunnel:
  - VPS `127.0.0.1:19000` -> local `127.0.0.1:9000`.
  - containers use `ML_CORE_URL=http://host.docker.internal:19000`.

## Non-Negotiable Invariants
- Прод-деплой только из ветки `runtime-stable`.
- Перед деплоем рабочее дерево на VPS должно быть clean.
- Mounts обязательны:
  - `./canon:/canon:ro`
  - `./migrations:/app/migrations:ro`
- `GOOGLE_CALENDAR_ID` только из env, `primary` запрещён для service-account режима.
- `GOOGLE_SERVICE_ACCOUNT_FILE=/data/google_sa.json`.

## Health/Smoke
- API health: `http://127.0.0.1:8101/health`
- Worker health (internal): `http://127.0.0.1:8002/health`
- ML health via tunnel: `http://127.0.0.1:19000/health`
- Voice endpoint: `POST ${ML_CORE_URL}/voice-command?profile=organizer` + multipart `file`.

## Known Risks
- Tunnel down => voice/ML сценарии недоступны.
- Неверный DB volume при миграциях => split-brain риск.
- Невалидный SA JSON или missing google deps => calendar sync деградация.

# SYSTEM_MAP_CANVAS
Компактная карта текущего runtime/deploy контура MyTGTodoist.

## 1) Runtime canvas

User (Telegram)
↓
`telegram-bot`
↓
`ML_GATEWAY_URL=http://host.docker.internal:19000`
↓
Decision Engine
↓
`organizer-worker` (single writer)
↓
SQLite `/data/organizer.db`
↓
`organizer-api` (read/health)
↓
`google-sync` (calendar pull + sheets push)

## 2) ML connectivity canvas

- VM ML Gateway: `127.0.0.1:19000`
- Reverse tunnel (запуск только на VM):
  - `ssh -i ~/.ssh/id_ed25519 -NT -R 127.0.0.1:19000:127.0.0.1:19000 root@31.128.47.128`
- VPS host endpoint: `127.0.0.1:19000`
- VPS containers -> host:
  - `http://host.docker.internal:19000`
- Для `telegram-bot` и `organizer-worker` обязателен:
  - `extra_hosts: ["host.docker.internal:host-gateway"]`

## 3) Runtime guards canvas

- One active clarification per user.
- Clarification TTL: `5 минут`.
- Clarification reply продолжает исходную команду (variant A, без extra confirmation).
- Idempotent execution по source metadata (`message_id`/`trace_id`/`intent`).
- `timeblock.create` никогда не fallback в Inbox.
- Runtime не угадывает missing business data.

См. детально: `docs/architecture/RUNTIME_GUARDS.md`.

## 4) Known issues / planned fixes

- Queue replay после restart может auto-execute pending элементы без достаточной защиты.
- ML `/health` может давать ложный `ok`, если нет реальных проверок `asr/llm/embedding`.
- `/system` уже полезен для диагностики, но Google Sync health должен опираться на реальный calendar write/access test, а не только container status.

## 5) Operations signals

- `curl http://127.0.0.1:8101/health`
- `curl http://127.0.0.1:19000/health`
- `docker compose -p deploy --env-file .env.prod -f docker-compose.yml -f docker-compose.vps.override.yml ps`
- Live Telegram text + voice tests обязательны перед переходом с manual tunnel на autossh/systemd.

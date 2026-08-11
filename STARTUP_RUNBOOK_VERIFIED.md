# LEGACY DUPLICATE NOTICE
# This root-level file is a legacy duplicate.
# Canonical maintained copy: docs/STARTUP_RUNBOOK_VERIFIED.md
# Do not use this file for current operational decisions.
# Not maintained: content below is historical and may contain obsolete transition notes.

# STARTUP RUNBOOK (VERIFIED)

Дата верификации: 2026-04-05  
Проверено по коду и файлам в:
- `d:\My_AI_Prodgekt\MyTGTodoist`
- `D:\VMShare`

Ограничение: это документ по запуску Telegram/runtime + ML контура.  
Все выводы ниже основаны на фактических файлах (`compose`, `Dockerfile`, `scripts`, `entrypoints`, `env`, `docs`), а не на предположениях.

## 1. Краткая схема проекта

| Слой | Папка | Назначение | Как запускается | Зависит от |
|---|---|---|---|---|
| Telegram/runtime (prod) | `d:\My_AI_Prodgekt\MyTGTodoist` | `telegram-bot`, `organizer-worker`, `organizer-api`, `google-sync` | `docker compose -p deploy --env-file .env.prod -f docker-compose.yml -f docker-compose.vps.override.yml up -d --build` | Docker, `.env.prod`, tenant env, secrets, ML endpoint `host.docker.internal:19000` |
| ML gateway + REC stack (VM) | `D:\VMShare\infra\ml-stack` (и legacy-дубль `ml-core/ml-stack`) | `ml-gateway`, `rec-service`, `embedding-service`, `qdrant` | `docker compose up -d --build` (из папки стека) | Docker, `ml_gateway_env.docker.txt`, доступ к ASR/LLM, образы `embedding-server:latest`, `rec-server:latest` |
| ASR (опциональный docker-вариант) | `D:\VMShare\infra\asr-stack` | ASR service (`/health`, `/inference`) | `docker compose -f d:/VMShare/infra/asr-stack/docker-compose.yml up -d --build` | Docker, модель fast-whisper |
| Reverse tunnel VM->VPS | `D:\VMShare\infra\ml-stack\scripts` или `ml-core/ml-stack/scripts` | Проброс `127.0.0.1:19000` на VPS | `ssh -NT -R ...` или systemd install script | SSH key, доступ VM->VPS |
| Пользовательская точка входа | Telegram | Пользователь отправляет text/voice | `telegram-bot` polling | валидный `TELEGRAM_BOT_TOKEN`, живой runtime |

## 2. Какие папки и сервисы участвуют в запуске

### 2.1 Telegram/runtime (обязательный прод-контур)
- Корень: `d:\My_AI_Prodgekt\MyTGTodoist`
- Compose сервисы (`docker-compose.yml`):
  - `telegram-bot`
  - `organizer-worker`
  - `organizer-api`
  - `google-sync`
- Режимы адаптера:
  - historical note: compat mode/compat rollback path removed
  - canonical current mode: `runtime_core_direct` (see docs/STARTUP_RUNBOOK_VERIFIED.md)

### 2.2 ML слой
- Текущий рекомендуемый путь по структуре ML docs index: `D:\VMShare\infra\ml-stack`
- Legacy-дубль: `D:\VMShare\ml-core\ml-stack`
- Сервисы:
  - `qdrant`
  - `embedding-service`
  - `rec-service`
  - `ml-gateway` (порт `19000`)

### 2.3 Внешние зависимости ML
- ASR (часто Windows host `:8020`, либо `infra/asr-stack`)
- LLM (часто LM Studio `:1234`)

### 2.4 Дополнительный контур (не часть запуска TG+ML)
- `MCP/` в MyTGTodoist (`start_all.ps1`, `healthcheck.ps1`) — отдельный runtime MCP, не обязателен для Telegram-продукта.
- `server.py` (read-only file service) — отдельный локальный сервис, не нужен для поднятия Telegram/ML.

## 3. Обязательные зависимости

### 3.1 Общие
- Docker daemon запущен.
- Доступ к двум деревьям:
  - `d:\My_AI_Prodgekt\MyTGTodoist`
  - `D:\VMShare`

### 3.2 Для TG deploy скрипта (`deploy.ps1`)
- `ssh`, `scp`, `robocopy` (жестко проверяются скриптом).

### 3.3 Для VM ML
- Доступен `docker compose`.
- Для tunnel: SSH ключ и доступ на VPS.

## 4. Переменные окружения (реально используемые)

Ниже ключи, которые влияют на запуск и жизнеспособность контуров.  
Только фактические источники: `docker-compose*.yml`, `entrypoint.sh`, `bot_main.py`, `worker.py`, `app.py`, `ml_gateway_env*.txt`.

| Переменная | Где используется | Обязательность | Пример | Слой | Что ломается без нее |
|---|---|---|---|---|---|
| `TELEGRAM_BOT_TOKEN` | `bot_main.py`, `worker.py`, compose | Обязательна | `123456:...` | TG | Бот не стартует (ошибка required) |
| `APP_ID` | `bot_main.py`, app-integration config | Обязательна | `telegram-adapter` | TG | Бот не стартует |
| `APP_VERSION` | `bot_main.py`, app-integration config | Обязательна | `2026.03.09` | TG | Бот не стартует |
| `TELEGRAM_ADAPTER_MODE` | compose, `runtime_bridge.py` | Условно обязательна (есть default) | `runtime_core_direct` | TG | Неверный path (direct/compat) |
| `WORKER_COMMAND_URL` | compose, `runtime_bridge.py` | Нужна для compat/rollback | `http://organizer-worker:8002/runtime/command` | TG | compat path не работает |
| `ML_GATEWAY_URL` | compose, `bot_main.py`, `worker.py` | Практически обязательна | `http://host.docker.internal:19000` | TG↔ML | direct runtime/voice деградирует |
| `DB_PATH` | compose, `worker.py`, `organizer-api/app.py` | Обязательна для runtime | `/data/organizer.db` | TG | нет/не та БД |
| `TZ` | compose | Рекомендуется | `Europe/Moscow` | TG | timezone drift |
| `APP_TIMEZONE` | compose, bot settings | Рекомендуется | `Europe/Moscow` | TG | неверная интерпретация времени |
| `GOOGLE_CALENDAR_ID` | `entrypoint.sh`, scheduler | Обязательна при `CALENDAR_SYNC_MODE!=off` | `<id>@group.calendar.google.com` | TG/google-sync | worker preflight fail |
| `GOOGLE_SERVICE_ACCOUNT_FILE` | `entrypoint.sh`, scheduler/auth | Обязательна при sync on | `/data/google_sa.json` | TG/google-sync | worker preflight fail |
| `CALENDAR_SYNC_MODE` | `entrypoint.sh` | Опциональна (default `full`) | `off`/`full` | TG | при `full` без SA/ID worker не стартует |
| `GOOGLE_SYNC_MODE` | scheduler + compose | Опциональна | `calendar_pull_and_sheets_push` | TG/google-sync | не тот sync режим |
| `CALENDAR_PULL_ENABLED` | scheduler | Опциональна | `1` | TG/google-sync | pull не выполняется |
| `SHEETS_PUSH_ENABLED` | scheduler | Опциональна | `1` | TG/google-sync | push не выполняется |
| `SQLITE_PATH` | scheduler | Опциональна | `/data/organizer.db` | TG/google-sync | scheduler смотрит не туда |
| `HEALTH_PORT` | bot env | Опциональна | `8082` | TG | internal health server mismatch |
| `DIRECT_VOICE_ENABLED` | `bot_main.py`, override | Опциональна | `true`/`false` | TG | voice direct выключен/включен не по плану |
| `DIRECT_ASR_TIMEOUT_SEC` | `bot_main.py`, override | Опциональна | `10` | TG | таймауты voice path |
| `DIRECT_ASR_RETRIES` | `bot_main.py`, override | Опциональна | `1` | TG | retry behavior voice |
| `B2_REPLAY_MAX_AGE_SEC` | worker + compose | Опциональна | `900` | TG reliability | replay после restart может работать иначе |
| `B2_REPLAY_SCAN_LIMIT` | worker + compose | Опциональна | `200` | TG reliability | scan/replay behavior |
| `LLM_URL` | ML gateway config | Обязательна для LLM check | `http://172.29.96.1:1234` | ML | `/health` llm=down |
| `ASR_URL` | ML gateway config | Обязательна для ASR check | `http://host.docker.internal:8020` | ML | `/health` asr=down |
| `RAG_URL` | ML gateway config | Обязательна для RAG | `http://rec-service:8000` | ML | RAG path fails |
| `EMBEDDING_URL` | ML gateway config | Обязательна для embedding check | `http://embedding-service:8000` | ML | embedding down/degraded |
| `AUTH_ENABLED` | ML gateway middleware | Опциональна | `false` | ML | при `true` без ключа 401 |
| `API_KEY` | ML gateway middleware | Обязательна только при `AUTH_ENABLED=true` | `<secret>` | ML | unauthorized |
| `REMOTE_HOST/REMOTE_USER/REMOTE_PORT/LOCAL_PORT/SSH_IDENTITY` | tunnel systemd env example | Обязательны для systemd tunnel | `31.128.../root/19000/...` | Tunnel | tunnel unit не поднимется |

Примечание по env масштабу:  
`organizer-worker/worker.py` и `src/retrieval/main.py` содержат много дополнительных tuning-флагов.  
Они реально используются, но не обязательны для базового старта (имеют defaults).

## 5. Варианты запуска

### 5.1 Local dev (без Docker)
Статус: частично существует, но не канонический для текущего прод-контура.

Найдено:
- `src/main.py` (uvicorn + dev polling)
- `server.py` (read-only file service)

Оценка:
- Это не основной путь для split-архитектуры `telegram-bot + worker + api + google-sync` в compose.
- Использовать только для локальной разработки отдельных модулей.

### 5.2 Hybrid
Статус: это фактический боевой сценарий.

Схема:
- Windows host: ASR (`:8020`) + LLM (`:1234`)
- VM: ML compose (`ml-gateway` на `:19000`)
- VM -> VPS reverse tunnel `19000`
- VPS: TG runtime compose (`-p deploy`)

### 5.3 Полный Docker
Статус: подтвержден для TG слоя, частично подтвержден для ML.

- TG: полностью в Docker compose (подтверждено).
- ML: compose поднимает `ml-gateway` и инфраструктуру, но требует наличие образов `embedding-server:latest` и `rec-server:latest` (иначе старт не завершится).

### 5.4 VPS / deploy
Статус: основной production-like сценарий.

Команда:
`docker compose -p deploy --env-file .env.prod -f docker-compose.yml -f docker-compose.vps.override.yml up -d --build`

`deploy.ps1` является каноническим автоматизированным способом выкатки TG слоя на VPS.

## 6. Пошаговый порядок запуска (verified startup sequence)

### Шаг 1: Поднять ML слой на VM
- ОС: Linux VM
- Директория: `D:\VMShare\infra\ml-stack` (в VM это обычно `/mnt/hgfs/VMShare/infra/ml-stack`)
- Команды:
```bash
cd /mnt/hgfs/VMShare/infra/ml-stack
docker compose up -d --build
docker compose ps
curl -sS http://127.0.0.1:19000/health
curl -sS http://127.0.0.1:19000/diag/upstreams
```
- Ожидаемо:
  - `ml-gateway` слушает `19000`
  - `/health` отвечает JSON

### Шаг 2: Поднять reverse tunnel VM -> VPS
- ОС: Linux VM
- Ручной режим:
```bash
ssh -i ~/.ssh/id_ed25519 -o IdentitiesOnly=yes -NT -R 127.0.0.1:19000:127.0.0.1:19000 root@31.128.47.128
```
- Или systemd:
```bash
bash /mnt/hgfs/VMShare/infra/ml-stack/scripts/install_reverse_tunnel_service.sh
sudo systemctl status ml-gateway-reverse-tunnel --no-pager
```
- Проверка на VPS:
```bash
curl -sS http://127.0.0.1:19000/health
```

### Шаг 3: Поднять TG runtime на VPS
- ОС: Linux VPS (или Windows host с `deploy.ps1`)
- Ручной запуск в репозитории:
```bash
cd /opt/mytgtodoist
docker compose -p deploy --env-file .env.prod -f docker-compose.yml -f docker-compose.vps.override.yml up -d --build
docker compose -p deploy --env-file .env.prod -f docker-compose.yml -f docker-compose.vps.override.yml ps
```
- Автоматизированный запуск с Windows:
```powershell
cd d:\My_AI_Prodgekt\MyTGTodoist
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\deploy.ps1 -CopyEnv
```

### Шаг 4: Проверка связки TG->ML
- Проверка из контейнеров VPS:
```bash
docker exec -i deploy-telegram-bot-1 python - << 'PY'
import requests
print(requests.get("http://host.docker.internal:19000/health").text)
PY
```
- Проверка guard-скриптом (в корне MyTGTodoist):
```powershell
.\run.ps1 ps
.\run.ps1 health
.\run.ps1 logs
```

## 7. Проверка здоровья системы

### 7.1 ML слой
- `curl http://127.0.0.1:19000/health`
  - Норма: JSON с `status` и `services`
  - Поломка: timeout/connection refused/`status=down`
- `curl http://127.0.0.1:19000/diag/upstreams`
  - Норма: `asr`, `rag`, `ffmpeg` диагностируются
  - Поломка: upstream `down/refused/dns_error`

### 7.2 TG runtime слой
- `docker compose ... ps`
  - Норма: `telegram-bot`, `organizer-worker`, `organizer-api`, `google-sync` Up/healthy
- `curl http://127.0.0.1:8101/health`
  - Норма: `{"ok": true}`
- Worker health:
  - endpoint `http://organizer-worker:8002/health` возвращает `{"ok": true}`
- Логи:
  - `docker logs deploy-telegram-bot-1 --tail 200`
  - для direct режима ожидается `telegram_adapter bootstrap mode=runtime_core_direct`

### 7.3 End-to-end
- Отправить текст в Telegram, убедиться в ответе.
- Отправить голос, проверить поведение по текущему режиму (`DIRECT_VOICE_ENABLED`).

## 8. Типовые ошибки и исправления

| Симптом | Вероятная причина | Проверка | Исправление |
|---|---|---|---|
| `cd .../ml-core/ml-stack: No such file or directory` | В VM актуален `infra/ml-stack`, а docs указывают `ml-core/ml-stack` | `ls /mnt/hgfs/VMShare` | Использовать реальный путь (`infra/ml-stack`) |
| `docker compose ... no configuration file provided` | Запуск не из директории с `docker-compose.yml` | `pwd`, `ls` | Перейти в папку стека перед `docker compose` |
| `image embedding-server:latest not found` | Нет локально собранных образов REC/embedding | `docker images | grep -E 'embedding-server|rec-server'` | Собрать/загрузить эти образы заранее |
| `worker unhealthy` при старте | `GOOGLE_SERVICE_ACCOUNT_FILE`/`GOOGLE_CALENDAR_ID` невалидны при sync mode != off | логи `organizer-worker`, preflight ошибки | задать валидный SA json + календарь или `CALENDAR_SYNC_MODE=off` |
| `telegram-bot` не стартует | Нет `TELEGRAM_BOT_TOKEN`/`APP_ID`/`APP_VERSION` | логи бота | заполнить env |
| ML доступен на VPS host, но не из контейнера | нет `extra_hosts` host-gateway или неверный URL | exec curl из контейнера | использовать `host.docker.internal:19000` + `extra_hosts` |
| `ops_status.sh` падает с отсутствием compose файла | default путь `deploy/docker-compose.prod.yml` отсутствует | `test -f deploy/docker-compose.prod.yml` | запускать с `COMPOSE_FILE` override или использовать `run.ps1/run.sh` |
| 401 от ML gateway | включен `AUTH_ENABLED=true`, ключ не передан | проверить env ML | выключить auth или передавать `X-API-Key` |
| deploy на VPS завершился ошибкой после switch | отсутствует `.env.prod`/secrets на VPS | логи `deploy.ps1`/remote script | восстановить `.env.prod` и `secrets` |

## 9. Что в старой документации устарело/конфликтует

| Источник | Что написано | Что видно по коду/файлам | Вывод |
|---|---|---|---|
| `D:\VMShare\docs\RUNBOOK*.md` | Основной путь `ml-core/ml-stack` | В структуре также `infra/ml-stack`; у пользователя `ml-core` отсутствует | Документация неоднозначна, нужен единый canonical path |
| `D:\VMShare\docs\ARCHITECTURE_INDEX.md` | Infra section указывает `infra/ml-stack` | RUNBOOK указывает `ml-core/ml-stack` | Конфликт docs-docs |
| `D:\VMShare\docs\archive\legacy_flat\00_DOCS_MAP.md` | Ссылается на `ml-core/ml-stack/README.md` и `RUNBOOK.md` | Эти файлы отсутствуют (`Test-Path=False`) | Устаревшие ссылки |
| `MyTGTodoist/README.md` (раздел Documentation) | canonical root `docs-templates/*` | `docs-templates` пустой | Раздел устарел |
| `app-integration/docs/README.md` | Telegram entrypoint внешняя/deferred cutover | В `MyTGTodoist` compose уже запускает `python -m src.integrations.telegram.bot_main` | Документация отстает от реального deploy |
| `scripts/ops_status.sh`, `ops_snapshot.sh`, `backup_sqlite.sh` | default `deploy/docker-compose.prod.yml` | Файл отсутствует | Скрипты в default-конфиге нерабочие |
| `docker-compose.yml` default adapter mode | `compat_worker_bridge` | Prod override переключает в `runtime_core_direct` | Для prod всегда нужен override файл |

## 10. Самый короткий путь запуска (quick start)

1. VM: поднять ML стек в `infra/ml-stack`, проверить `:19000/health`.
2. VM: поднять reverse tunnel `-R 127.0.0.1:19000:127.0.0.1:19000`.
3. VPS/Windows deploy: поднять TG стек с `docker-compose.vps.override.yml`.
4. Проверить:
   - `docker compose ... ps`
   - `curl 127.0.0.1:8101/health`
   - `curl 127.0.0.1:19000/health`
   - лог бота с `runtime_core_direct`.

## 11. Команды one-shot для каждого режима

### 11.1 Windows (deploy TG)
```powershell
cd d:\My_AI_Prodgekt\MyTGTodoist
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\deploy.ps1 -CopyEnv
```

### 11.2 Linux/macOS (TG manual)
```bash
cd /opt/mytgtodoist
docker compose -p deploy --env-file .env.prod -f docker-compose.yml -f docker-compose.vps.override.yml up -d --build
docker compose -p deploy --env-file .env.prod -f docker-compose.yml -f docker-compose.vps.override.yml ps
```

### 11.3 Docker (ML VM stack)
```bash
cd /mnt/hgfs/VMShare/infra/ml-stack
docker compose up -d --build
docker compose ps
curl -sS http://127.0.0.1:19000/health
```

### 11.4 Deploy (prod-like full chain)
```bash
# VM
cd /mnt/hgfs/VMShare/infra/ml-stack
docker compose up -d --build
ssh -i ~/.ssh/id_ed25519 -o IdentitiesOnly=yes -NT -R 127.0.0.1:19000:127.0.0.1:19000 root@31.128.47.128

# VPS
cd /opt/mytgtodoist
docker compose -p deploy --env-file .env.prod -f docker-compose.yml -f docker-compose.vps.override.yml up -d --build
docker compose -p deploy --env-file .env.prod -f docker-compose.yml -f docker-compose.vps.override.yml ps
```

## Recommended canonical commands

### Windows
- TG deploy: `.\deploy.ps1 -CopyEnv`
- TG checks: `.\run.ps1 ps`, `.\run.ps1 health`, `.\run.ps1 logs`

### Linux/macOS
- TG up: `./run.sh up`
- TG checks: `./run.sh ps`, `./run.sh health`, `./run.sh logs`

### Docker
- TG canonical compose pair: `docker-compose.yml + docker-compose.vps.override.yml`
- ML canonical compose dir: `infra/ml-stack`

### Deploy
- Всегда фиксировать project name: `-p deploy`
- Для rollback режима адаптера добавлять `-f docker-compose.vps.rollback.compat.yml`

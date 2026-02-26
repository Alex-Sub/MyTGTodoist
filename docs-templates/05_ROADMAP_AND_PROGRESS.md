# 05_ROADMAP_AND_PROGRESS

## 1) Текущий статус
- Версия: runtime-stable / production-thin-runtime baseline
- Дата фиксации: 2026-02-23
- Статус контура: стабилизация завершена, идёт период наблюдения 3-14 дней.

## 2) Что уже реализовано
- Runtime-only архитектура с единым writer (`organizer-worker`).
- Read-only API и Telegram UX слой с dedup/long-poll guard.
- Pull loops: Calendar + Tasks + Sheets (`apply=TRUE`).
- Conflict resolution через Telegram inline действия.
- Calendar idempotency (`iCalUID`, reuse/update path).
- Canon/migrations mounts нормализованы для prod.
- Startup preflight и fail-fast проверки для Google SA JSON и calendar env.
- Milestone: **ASR End-to-End Stable via Reverse Tunnel** (VM ML-Gateway -> VPS thin-runtime).

## 3) Открытые риски и ограничения
- ML-функции зависят от активного reverse tunnel `127.0.0.1:19000 -> 127.0.0.1:9000`.
- `GOOGLE_CALENDAR_ID=primary` запрещён для service account.
- Ошибки формата голоса (например, `415 Unsupported Media Type`) возможны при несовпадении контракта gateway.
- Перед миграциями обязателен контроль целевого volume (`deploy_db_data`) для исключения split-brain.

## 4) Журнал ключевых событий
### 2026-02-22
- Инцидент split SQLite volume закрыт.
- Миграции применены в корректный runtime volume, таблицы подтверждены.

### 2026-02-23
- Проведена calendar/canon stabilization.
- Зафиксирован production deploy baseline на `runtime-stable`.
- Зафиксирован voice incident (`asr_unavailable status=415`) как документированный риск формата.

### 2026-02-26
- Decision: canonical timezone зафиксирована как `Europe/Moscow` (system-wide, включая `X-Timezone`).
- Decision: `create_event` удалён из канонического ML intent output; temporal meetings мапятся в `timeblock_create`.
- Decision: Inbox semantics закреплена: temporal intents не имеют fallback в Inbox, только clarification.

## 5) Ближайшие шаги
- 3-7 дней ежедневных health/log/smoke проверок прод-контура.
- Мониторинг стабильности pull-loops и глубины conflict queue.
- Подтверждение стабильной ежедневной отправки digest.
- Контроль недопущения branch drift и ручных VPS-правок вне standard flow.

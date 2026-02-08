# DEPRECATED (2026-02-08)
Replaced by: Docs/04_REGULATIONS.md
Reason: canonicalization; duplicate regulations doc.
---

# Regulations
Version: v1.0

## 1. Что это
Регламенты — регулярные обязательства по месяцам.

Регламенты ≠ задачи ≠ цели.
- Регламент — это обязательство по сроку.
- Задача — операционное действие.
- Цель — управленческое направление периода.

## 2. Сущности
**regulations**
- задаёт правило: название, день месяца, опциональное время
- статус: ACTIVE | DISABLED

**regulation_runs**
- конкретный запуск регламента в периоде
- уникальность: один run на период (regulation_id, period_key)
- статусы: OPEN | DONE | SKIPPED | MISSED

Примечание по статусам:
В runtime допустим статус OPEN как legacy‑синоним DUE.
Вся логика трактует OPEN ≡ DUE.
Канонический статус для документации и новых реализаций — DUE.

## 3. period_key
Формат: `YYYY-MM` (месяц).

period_key фиксирует, к какому месяцу относится run.

## 4. Monthly tick
Monthly tick создаёт runs для ACTIVE регламентов:
- создаёт только отсутствующие
- идемпотентен
- не меняет статусы существующих run

## 5. Команды (worker)
- POST `/p4/commands/create_regulation`
- POST `/p4/commands/update_regulation_schedule`
- POST `/p4/commands/archive_regulation`
- POST `/p4/commands/complete_reg_run`
- POST `/p4/commands/skip_reg_run`
- POST `/p4/commands/disable_reg`

## 6. API (read-only)
- GET `/p4/regulations`
- GET `/p4/regulations/{id}`
- GET `/p4/regulations/{id}/runs?period=YYYY-MM`
- GET `/p4/runs?period_key=YYYY-MM`
- GET `/p4/runs/{id}`

## 7. UX (Telegram)
Команда:
- `/regs` (или `/reg`) — экран «Регламенты месяца»

Навигация:
- «Пред. месяц» / «След. месяц»
- «Обновить»

Действия по run:
- ✅ Выполнить
- ⏭ Пропустить

Действия по regulation:
- ⛔ Отключить

## 8. Nudges (отдельный модуль)
Nudges — отдельный модуль, не связанный с целями и задачами.
- тумблер включения
- только уведомления
- без автологики и без изменения статусов

Режимы:
- `REG_NUDGES_MODE=off|daily|due_day` (default off)

Логи:
- `P4_REG_NUDGE action=emit reg_run_id=... period_key=... due_date=...`

## 9. LLM Integration Note
- Регламенты **не** обрабатываются LLM напрямую.
- LLM-слой отдаёт JSON-only ответы; любые side-effects выполняются детерминированным слоем.


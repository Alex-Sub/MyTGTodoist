# Release Notes

# Release v0.1

Дата: 2026-02-07

## P1 — Ingestion → Calendar
✅ Ingestion работает:
- text/voice → inbox_queue → item.
- Создание item подтверждается пользователю.
- Повторы не создают дублей.

✅ Calendar:
- create_tick создаёт события.
- patch_tick обновляет события.
- cancel_tick (P4) пока не включён.

## P2 — Tasks & Subtasks
✅ Runtime P2 работает отдельно от legacy `src/*`.
- таблицы tasks/subtasks.
- worker — writer, api — read-only.
- Telegram бот пишет только через worker.

✅ Инварианты:
- idempotency по source_msg_id.
- task/subtask статусные переходы.

## P3 — Calendar FSM
✅ FSM зафиксирован:
- NEW → PLANNED → SCHEDULED → DONE.
- cancel / failed без автологики.

## P4 — Calendar Cancel
⚠️ P4 в разработке:
- cancel_tick scaffold.

## Infra / Ops
✅ docker-compose и health endpoints.
✅ основной runtime в отдельном контуре.

## Notes
Release фиксирует контур P1–P3.
P4/P5 — зафиксированы как PRD, реализация частичная.

---

## P4.2–P4.5 (Regulations)
Добавлена подсистема регламентов:
- P4.2 data model + runs
- P4.3 Telegram UX (/regs)
- P4.4 nudges (log-only, toggle)
- P4.5 документация и канонизация

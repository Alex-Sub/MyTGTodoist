# CONFIRM_BEFORE_COMMIT

Цель: запретить commit до явного подтверждения пользователя для high-value сценариев.

## Covered flows

- Temporal scheduling family (`meeting/block/timeblock`)
- `task.create`

## Runtime model

1. collect required fields via clarification continuation
2. build draft summary
3. ask binary confirmation (`Да/Нет`)
4. commit only on `Да`
5. on `Нет` return to correction flow (no commit)

## Temporal summary fields

- `Тип`
- `Дата`
- `Время`
- `Длительность`

## Task summary fields

- `Текст`
- optional `Срок`
- optional `Приоритет`
- optional `Заметки`

## Transport UX

Telegram may render inline `Да/Нет` buttons, but button path maps to same logical replies as text input.

## Guard invariants

- No DB schema redesign is required for this model.
- No execution before confirmation.
- Global cancel may abort active draft/confirmation without commit.

## Related

- `docs/contracts/APP_RUNTIME_CONTRACT.md`
- `docs/architecture/RUNTIME_STATE_MACHINE.md`

# RUNTIME_GUARDS
Runtime guards (защитные инварианты runtime) для MyTGTodoist.

## Purpose
Документ фиксирует инварианты исполнения, которые должны соблюдаться всегда:
- при обычной обработке;
- при clarification flow;
- при retry/replay;
- при restart контейнеров.

## Core invariants

### 1) One active clarification per user
- Для каждого `user_id` (в контексте приложения/тенанта) разрешён только один активный clarification context.
- Новый context не должен молча перезаписывать существующий.

### 2) Clarification TTL
- Clarification context имеет TTL.
- Значение по умолчанию: `5 минут`.
- Просроченный context не исполняется.

### 3) Idempotent execution
- Один source command не должен исполняться дважды.
- Повторная обработка возвращает `already_executed` и ссылку на существующую сущность (если доступна).

### 4) Temporal intents never fallback to Inbox
- `timeblock.create` и другие temporal intents не переводятся в Inbox fallback.
- При нехватке required temporal полей runtime задаёт clarification.

### 5) Runtime does not guess
- Runtime не придумывает отсутствующие бизнес-данные.
- Исполнение только после заполнения required полей.

### 6) Single writer
- Единственный writer состояния: `organizer-worker`.
- Adapter/API не должны напрямую мутировать доменное состояние.

### 7) No fake healthy status
- Health/diagnostics не должны показывать ложный `ok` при деградации подсистем.
- `/system` должен отражать фактическое состояние проверяемых компонентов.

### 8) Queue replay guard
- Queue replay после restart не должен вслепую исполнять stale pending commands.
- Replay обязан учитывать TTL/dedup/idempotency.
- Протухшие команды не должны создавать дубли задач/блоков.

### 9) Update target-identification guard
- `meeting.update`/event-like update не продолжается без target anchor.
- Минимально допустимый anchor:
  - дата+время
  - дата
  - время
  - participant/name hint
  - title/text hint
  - shortlist selection / explicit candidate id
- Пока anchor не найден, runtime не должен спрашивать новые изменения (новую дату/время/комментарий).

### 10) Update shortlist and fallback guard
- После обнаружения anchor используется target search с shortlist до `3` кандидатов.
- Если кандидатов нет или пользователь ответил `ни один/не то`, runtime обязан перейти в state уточнения target.
- Следующий ввод в этом state идёт в repeat target search, а не в общий memory/search fallback.

### 11) Commit truthfulness guard
- Runtime не отправляет success-текст `создано/перенесено/обновлено`, если commit не вернул подтверждённый `calendar_event_id`.
- При `ok=false` success response блокируется.

## Clarification flow invariant
Ожидаемый сценарий:

1. Пользователь: `запланируй встречу завтра в 11`
2. Бот: `На сколько минут поставить блок?`
3. Пользователь: `30`

Результат:
- продолжается исходный `timeblock.create`;
- создаётся ровно один timeblock;
- Inbox item не создаётся.

## Update safety invariant
Ожидаемый сценарий:

1. Пользователь: `измени собрание`
2. Бот: `Какое именно событие нужно изменить? Укажите дату/время или другой ориентир.`
3. Пользователь: `собрание 16 числа в 13:00`
4. Бот: target search -> shortlist/confirm -> только после этого сбор изменений.

## Связанные документы
- `docs/architecture/APP_RUNTIME_CONTRACT.md`
- `docs/architecture/ML_API_USAGE.md`
- `docs/architecture/VOICE_PIPELINE.md`
- `docs/spec/SYSTEM_SPEC.md`
- External ML canon: `D:\VMShare\docs\ML_GATEWAY_CONTRACT.md`

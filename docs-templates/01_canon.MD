ML CONTRACT (ML -> Runtime)

Version: v2.2

## 1) Core Principle

ML interprets natural language.
Runtime validates and mutates state.
Runtime does not guess missing business data.

## 2) Inbox Semantics

- Inbox is ONLY for generic task-like items when BOTH date and time are missing.
- Inbox must not be used for explicit time-related intents.
- Explicit time-related intent is: `timeblock_create`.
- If a time-related intent has missing temporal fields, Runtime must return one `clarifying_question` and must not create an entity.

## 2.1) Timezone Canon

- Canonical system timezone: `Europe/Moscow`.
- Telegram adapter sends `X-Timezone: Europe/Moscow` (via `APP_TIMEZONE`).
- ML-Gateway resolves relative date/time using `now_iso` + `X-Timezone`.

## 3) Input Contract (CommandEnvelope)

```json
{
  "trace_id": "...",
  "source": {"channel": "telegram_voice"},
  "command": {
    "intent": "task_create|timeblock_create",
    "confidence": 0.0,
    "entities": {
      "title": "...",
      "planned_at": "2026-02-26T10:00:00+03:00",
      "start_at": "2026-02-26T10:00:00+03:00",
      "end_at": "2026-02-26T11:00:00+03:00",
      "duration_minutes": 60
    }
  }
}
```

## 4) Field Normalization Rules

- All entity fields use `snake_case`.
- Empty strings (`""`, `"   "`) are treated as `null`.
- Missing field and empty-string field are equivalent for validation.
- Canonical temporal fields in this contract:
  - `planned_at`
  - `start_at`
  - `end_at`
  - `duration_minutes`

## 5) One-Question Clarification Rule

- Runtime asks exactly one clarifying question per step.
- Clarification follows intent-specific precedence.
- Runtime must not execute an intent until required fields are present.

## 6) Intent: task_create

- Required:
  - `title`
- Optional:
  - `planned_at` (or `due_date` if adapter uses due-date model)
  - `duration_minutes`
  - `tags`
  - `priority`
- Clarifying question:
  - if `title` missing -> `"Как назвать задачу?"`
  - if user provided partial temporal data -> ask one concrete missing part
- Inbox behavior:
  - allowed only when item is generic task and both date/time are missing

### Examples (task_create)

- User phrase: `"Создай задачу купить молоко"`
  - expected_intent: `task_create`
  - entities: `{ "title": "купить молоко" }`
  - clarifying_question: `null`

- User phrase: `"Добавь задачу позвонить маме завтра"`
  - expected_intent: `task_create`
  - entities: `{ "title": "позвонить маме", "planned_at": "<resolved_datetime_or_date>" }`
  - clarifying_question: `null`

- User phrase: `"Создай задачу"`
  - expected_intent: `task_create`
  - entities: `{}`
  - clarifying_question: `"Как назвать задачу?"`

- User phrase: `"Задача: подготовить отчет, в пятницу"`
  - expected_intent: `task_create`
  - entities: `{ "title": "подготовить отчет", "planned_at": "<resolved_datetime_or_date>" }`
  - clarifying_question: `null`

## 7) Intent: timeblock_create

- Required:
  - `duration_minutes`
  - `start_at`
- Optional:
  - `title`
  - `end_at` (derived from `start_at + duration_minutes` if not provided)
  - `calendar_id`
- Clarifying question precedence (exactly one):
  - if `duration_minutes` missing -> `"На сколько минут поставить блок?"`
  - else if `start_at` missing -> `"На какое время поставить блок?"`
- Execution rule:
  - do not create anything until BOTH `duration_minutes` and `start_at` are present
- Inbox behavior:
  - never create in Inbox

### Examples (timeblock_create)

- User phrase: `"Поставь блок на завтра в 14:00 на 60 минут"`
  - expected_intent: `timeblock_create`
  - entities: `{ "start_at": "<resolved_datetime>", "duration_minutes": 60 }`
  - clarifying_question: `null`

- User phrase: `"Поставь блок"`
  - expected_intent: `timeblock_create`
  - entities: `{}`
  - clarifying_question: `"На сколько минут поставить блок?"`

- User phrase: `"Поставь блок на 60"`
  - expected_intent: `timeblock_create`
  - entities: `{ "duration_minutes": 60 }`
  - clarifying_question: `"На какое время поставить блок?"`

- User phrase: `"Поставь блок завтра в 9"`
  - expected_intent: `timeblock_create`
  - entities: `{ "start_at": "<resolved_datetime>" }`
  - clarifying_question: `"На сколько минут поставить блок?"`

- User phrase: `"Поставь блок на 60, время потом"`
  - expected_intent: `timeblock_create`
  - entities: `{ "duration_minutes": 60 }`
  - clarifying_question: `"На какое время поставить блок?"`

## 8) Meeting Phrase Mapping

- Meeting-related phrases (`встреча`, `собрание`, `созвон`, `appointment`) must map to `timeblock_create`.
- `create_event` is deprecated in ML output and must not be returned by ML-Gateway.
- If an upstream LLM returns `create_event`, gateway normalization converts it to `timeblock_create` before response.

## 9) Runtime Output Contract

- Success:

```json
{
  "ok": true,
  "user_message": "..."
}
```

- Clarification:

```json
{
  "ok": false,
  "clarifying_question": "...",
  "choices": []
}
```

- For `timeblock_create`, clarification is mandatory if required temporal fields are missing.
- For `timeblock_create`, fallback-to-Inbox behavior is forbidden.

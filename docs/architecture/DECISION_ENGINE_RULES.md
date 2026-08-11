# DECISION_ENGINE_RULES

Правила decision layer в app runtime (`telegram-bot/app-integration/src/decision`).

## 1) Source of Truth

- Canonical intent meaning and registry: ML/canon contracts (`canon/intents_v2.yml`).
- Alias layer config: `canon/intent_aliases_v1.yml`.
- Decision layer only maps/validates; execution logic belongs to runtime/worker.

## 2) Temporal Detection and Alias Rules

Temporal routing may be activated by:
- explicit temporal intents (`timeblock.create`, aliases);
- conservative temporal natural phrases via alias layer and obvious temporal markers.

Covered phrase families include:
- `запланируй мероприятие`
- `забронируй время`
- `забронируй слот`
- `выдели время`
- `выдели слот`

Rule:
- no broad fuzzy NLP redesign;
- require clear scheduling semantics (action + temporal/slot semantics).

## 3) Task Create Alias Rules

When parser returns `intent=unknown`, decision layer may map to `task.create` for conservative phrases:
- `создай задачу`
- `добавь задачу`
- `запиши задачу`
- `напомни сделать ...`
- `надо сделать ...`
- `нужно позвонить ...`
- `купить хлеб завтра`
- `позвонить врачу завтра`
- `позвонить врачу`

Guardrails:
- bare generic phrases (`надо`, `нужно`, `хочу`, `потом`) must not auto-map without actionable content.
- memory fallback is explicit-only and must not intercept generic task-like action phrases before task routing completes.

## 3.1) High-Level Routing Order

Current runtime routing order:
1. cancel
2. active scenario continuation
3. calendar/timeblock/task routing
4. ambiguous create prompt
5. explicit memory fallback only

Implications:
- undated task-like phrases may still route to `task.create`;
- if they remain undated after normalization/confirmation, they are later projected into `InBox`;
- a generic phrase without actionable task content still must not become `task.create`.

## 4) Temporal Required Fields and Strictness

Canonical clarification order:
1. `start_at_date`
2. `start_at_time`
3. `duration_minutes`

Strict behavior:
- time-only text (`в 12`) does not satisfy date;
- parser-inferred date-bearing values do not count as explicit user-confirmed date unless text confirms date.

Continuation accepts direct replies for:
- date
- time
- duration

## 5) Past-date Guard Integration

Past-date validation uses final normalized date.

If date is earlier than local current date:
- return clarification `start_at_date_past_confirm`;
- accepted replies:
  - yes/да -> continue;
  - no/нет -> clear date and re-ask date;
  - corrected date input -> continue with new date.

## 6) Confirmation-related Decision Fields

Decision/runtime interface includes binary confirmation fields:
- `start_at_date_past_confirm`
- `temporal_commit_confirm`
- `task_create_confirm`

All binary confirmations use shared `yes/no/unknown` normalization on runtime side.

## 7) Non-goals

Decision layer does not:
- change DB schema;
- execute business actions;
- maintain Telegram-specific callback logic.

## 8) Related Docs

- `docs/contracts/INTENT_CATALOG.md`
- `docs/contracts/APP_RUNTIME_CONTRACT.md`
- `docs/architecture/RUNTIME_STATE_MACHINE.md`
- `docs/contracts/INTENT_ALIAS_LAYER.md`

## 9) Date Parsing Rule (current temporary policy)

### Что работает сейчас
- Для форм без года:
  - `dd.mm`
  - `dd month` (например, `13 апреля`)
  используется текущий год.
- Для формы с явным годом (например, `21 апреля 2021`) сохраняется указанный год.
- Нельзя использовать fallback вида `dd -> yyyy` (число дня не трактуется как год).

### Что ещё не закрыто
- Live parsing дат без года остаётся чувствительным к route-specific legacy parser/fallback.
- Требуется единая стабилизация materialization в `entities.start_at_date`.

### Архитектурное намерение
- Будет выделен централизованный слой temporal parsing для human date/time.
- Рабочие runtime-слои отвечают за сценарий и safety.
- Централизованный temporal parsing layer отвечает только за понимание human date/time.
- До выделения слоя поддерживаем простые формы:
  - `сегодня`
  - `завтра`
  - явная дата (`13 апреля`, `13.04`)

Current deterministic rule:

- `common/date_resolver.py` is the central shared Date Resolver for the active contour;
- parser extraction is not enough to persist a final date;
- runtime and worker must persist canonical dates, not raw relative words, whenever resolver canonicalization is available.

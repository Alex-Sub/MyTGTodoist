# INTENT CATALOG

Каталог канонических runtime-intent'ов и alias-входов.

Source of truth:
- Canonical intent registry: `canon/intents_v2.yml`
- Alias layer: `canon/intent_aliases_v1.yml`

## 1) Intent Naming

Canonical runtime intents:
- `task.create`
- `task.complete`
- `task.set_status`
- `task.reschedule`
- `task.move_to`
- `task.update`
- `subtask.create`
- `subtask.complete`
- `timeblock.create`
- `timeblock.move`
- `timeblock.delete`
- `tasks.list_active`
- `tasks.list_today`
- `tasks.list_tomorrow`
- `goal.create`
- `goal.update`
- `goal.close`
- `goal.reschedule`
- `goal.link_task`
- `cycle.create`
- `cycle.close`
- `reg.run`
- `reg.status`
- `state.get`

Notes:
- Legacy naming (`task_create`, `timeblock_create`, `create_timeblock`, `create_event`) допускается как inbound alias, но execution идёт через canonical intent.
- Alias не создаёт новый intent, а маппится в существующий canonical intent.

## 2) Temporal Family (`timeblock.create`)

Natural phrase aliases (phase-1):
- `запланируй мероприятие`
- `забронируй время`
- `забронируй слот`
- `выдели время`
- `выдели слот`
- `поставь блок`
- `поставь время`
- `нужен слот`
- `нужно время`

Required fields:
- `start_at` (или эквивалентно: подтверждённые `date + time`, нормализованные в `start_at`)
- `duration_minutes`

Clarification order (fixed):
1. `start_at_date`
2. `start_at_time`
3. `duration_minutes`

Temporal strictness:
- Time-only reply (`в 12`) не закрывает date.
- Parser-inferred date без явного user signal не считается подтверждённой датой.

Past-date guard:
- Если `start_at_date < today(local)` -> `start_at_date_past_confirm`.
- Разрешены ответы: `yes/да`, `no/нет`, новая дата.

Commit model:
- До commit показывается temporal summary и confirmation (`temporal_commit_confirm`).
- Summary fields: `type`, `date`, `time`, `duration`.

## 3) Task Create (`task.create`)

Required:
- `title` (task text)

Optional:
- `planned_at` / `due_date`
- `priority`
- `notes`

Natural phrase aliases (phase-2 conservative set):
- `создай задачу`
- `добавь задачу`
- `запиши задачу`
- `напомни сделать`
- `надо сделать ...`
- `нужно позвонить ...`

Guardrails:
- Bare generic фразы (`надо`, `нужно`, `хочу`, `потом`) не должны автозапускать `task.create` без task-like content.

Commit model:
- До commit показывается task summary и confirmation (`task_create_confirm`).
- Summary fields: `text`, optional `due date`, optional `priority`, optional `notes`.

## 4) Telegram Clarification UX Contract

Inline buttons:
- `start_at_date_past_confirm`: `Да/Нет`
- `duration_minutes`: `15/30/45/60`
- `task_create_confirm`: `Да/Нет`
- `temporal_commit_confirm`: `Да/Нет`

Button callback contract:
- format: `clarify:v1:<family>:<value>`
- callbacks нормализуются в те же logical text replies (`yes/no/15/30/45/60`)
- button path не имеет отдельной бизнес-логики (reuse existing continuation flow)

Helper text:
- duration: `Если нужна другая длительность, введите её сообщением.`
- past-date: `Если нужна другая дата, введите её сообщением.`

## 5) Global Cancel Contract

Global cancel phrases:
- `отмена`
- `стоп`
- `не надо`
- `отбой`
- `всё, закончили`
- `все закончили`

Behavior:
- Если активен сценарий clarification/draft/confirmation: state очищается, commit не выполняется.
- Если активного сценария нет: runtime сообщает, что останавливать нечего.

## 6) Related Docs

- `docs/contracts/APP_RUNTIME_CONTRACT.md`
- `docs/contracts/SYSTEM_SPEC.md`
- `docs/architecture/DECISION_ENGINE_RULES.md`
- `docs/architecture/RUNTIME_STATE_MACHINE.md`
- `docs/architecture/CONFIRM_BEFORE_COMMIT.md`
- `docs/contracts/INTENT_ALIAS_LAYER.md`

1. Общий принцип

Runtime — детерминированный слой.

Он:

не интерпретирует смысл,

не делает предположений,

работает строго по canon.

2. Canon v2

Источник истины:
canon/intents_v2.yml

Для каждого intent зафиксировано:

обязательные поля,

правила уточнения,

правила disambiguation.

3. Один уточняющий вопрос

Если отсутствует обязательное поле —
возвращается:

ok = false

clarifying_question

Только один вопрос за шаг.

4. Disambiguation

Если ML передал:

список кандидатов,

chosen_id отсутствует,

Runtime:

не выбирает сам,

возвращает уточняющий вопрос,

возвращает top candidates.

5. Стратегические сущности

Runtime поддерживает:

cycles

goals

user_nudges

regulations

time_blocks

tasks

subtasks

6. Статусы

Поддерживаемые:

WAITING

PAUSED

IN_PROGRESS

DONE

WAITING и PAUSED не взаимозаменяемы.

7. Ответ Runtime
Успех
ok = true
user_message

Уточнение
ok = false
clarifying_question
choices (если есть)

---

# STRATEGIC LAYER CANON v1 (cycles / goals / nudges)

Этот слой нужен не для “ещё одного списка задач”, а для управленческой картины:
- есть обязательства со сроками,
- есть факт попадания/непопадания в срок,
- есть история переносов,
- есть сводка по периоду (циклу).

Важно: runtime не “давит” и не запрещает. Он делает видимым отклонение.

---

## 1) Cycle — рамка анализа периода

### Смысл (человечески)
Цикл — это период, по которому ты хочешь увидеть итог:
что удалось, что нет, где сорвался срок, где переносил.

Цикл не задаёт просрочку. Просрочку задаёт срок цели.

### Правила
- Цикл можно закрыть в любом состоянии (даже если есть активные просроченные цели).
- При закрытии цикл всегда формирует сводку.
- Закрытие цикла — управленческое действие, не автоматика.

### Intents
- `cycle.create`
  - required: `name`, `start_date`, `end_date`
  - one question (если не хватает): “Как назвать цикл?” / “До какого числа он длится?”
- `cycle.close`
  - required: `cycle_id`
  - результат: сводка по циклу (см. ниже)

> Примечание: `cycle.activate` опционален. В v1 можно считать “текущий активный цикл” выбранным пользователем/интерфейсом.

---

## 2) Goal — обязательство со сроком

### Смысл (человечески)
Цель — это результат, который ты берёшься сделать к определённой дате.
Ценность не только “сделал/не сделал”, но и “в срок/не в срок” и “сколько раз переносил”.

### Поля (минимум)
- `title`
- `success_criteria` (текст, как понять что достигнуто)
- `planned_end_date` (срок обязательства)
- `status`: `ACTIVE | DONE | DROPPED`
- `completed_at` (если DONE)
- `reschedule_events[]` (история переносов)

### Правила
- Просрочка не меняет статус. Цель остаётся `ACTIVE`.
- Просрочка считается по `planned_end_date` (не по циклу).
- Перенос срока фиксируется как событие (история не стирается).

### Intents
- `goal.create`
  - required: `title`, `success_criteria`, `planned_end_date`
  - optional: `cycle_id` (если нет — используем активный цикл)
  - one question:
    - если нет цикла: “В каком цикле создать цель?”
    - если нет критерия: “Как понять, что цель достигнута?”
    - если нет срока: “К какому числу должен быть результат?”
- `goal.update`
  - required: `goal_id`
  - requires at least one of: `title`, `success_criteria`, `status`
  - one question: “Что именно изменить в цели?”
- `goal.close`
  - required: `goal_id`
  - optional: `status` (`DONE` или `DROPPED`)
  - one question: “Закрыть как достигнутую или как снятую?”
- `goal.reschedule`
  - required: `goal_id`, `new_end_date`
  - side effects:
    - create reschedule event `{old_end_date, new_end_date, changed_at}`
  - one question: “К какому числу перенести срок?”

---

## 3) Nudges — сигналы отклонений

### Смысл (человечески)
Nudge — это “красный флажок”: система не ругает, а делает видимым то, что иначе легко спрятать в потоке.

### Принципы
- Nudge создаётся автоматически.
- Nudge не удаляется “по желанию”.
- Nudge можно только подтвердить (ack) — “я увидел”.

### Nudges v1 (минимум)
- `goal.overdue` (постоянный)
  - condition: `goal.status == ACTIVE` and `today > planned_end_date`
  - persists until: goal is DONE/DROPPED OR goal is rescheduled
- `goal.multiple_reschedules` (постоянный)
  - condition: reschedule_count >= N (по умолчанию N=3, позже — настройка)
  - persists until: цель закрыта или пользователь переносит/переоформляет цель осознанно
- `cycle.summary` (событие на закрытие цикла)
  - condition: cycle close
  - content:
    - total goals
    - done in time
    - done with reschedules
    - overdue active
    - average reschedules

### Intents
- `nudge.list`
  - required: none
- `nudge.ack`
  - required: `nudge_id`
  - one question: “Какой сигнал отметить как просмотренный?”

---

## 4) Закрытие цикла: что возвращает runtime

При `cycle.close` runtime возвращает сводку (человеческим текстом) и поля для интерфейса:

- `goals_total`
- `goals_done_in_time`
- `goals_done_with_reschedules`
- `goals_overdue_active`
- `avg_reschedules_per_goal`

Важно: закрытие цикла не блокируется из-за одной/нескольких просроченных целей.
После закрытия цикла просроченные цели остаются ACTIVE, и пользователь решает:
- перенести срок,
- перевести в новый цикл,
- снять (DROPPED),
- закрыть как DONE, если достигнута.

---

## 5) Daily Digest (обязательный сценарий)

Intent: `digest.daily`

Runtime считает и возвращает:

- `goals_active`
- `goals_overdue`
- `goals_due_soon`
- `goals_at_risk`
- `tasks_today`
- `tasks_tomorrow`
- `tasks_active_total`

Списочные intents для UX:

- `goals.list_overdue`
- `goals.list_due_soon`
- `goals.list_at_risk`
- `tasks.list_today`
- `tasks.list_tomorrow`
- `tasks.list_active`

Важно: runtime возвращает реальные списки из БД, UX только отображает.

---

## 6) Минимальная модель данных (Runtime)

### Стратегический слой
- `cycles` — периоды управления.
- `goals` — цели цикла (`ACTIVE | DONE | DROPPED`) с `planned_end_date`.
- `goal_reschedule_events` — неизменяемая история переносов.
- `nudge_ack` / `user_nudges` — сигналы и подтверждения просмотра.

### Операционный слой
- `tasks` — задачи (статус, плановая дата, связи).
- `subtasks` — подзадачи.
- `time_blocks` — блоки времени для задач.

### Связи
- `tasks.goal_id -> goals.id`
- `goals.cycle_id -> cycles.id`

Принцип: факты не перезаписываются исторически значимым образом; для переносов и сигналов ведётся явная история.


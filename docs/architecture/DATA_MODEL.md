# DATA MODEL
Модель данных системы (Data Model — структура сущностей и связей в базе).

Документ описывает:

- основные сущности системы
- связи между ними
- ключевые поля
- управленческий и операционный слой

---

# 1. Уровни модели данных

Система разделена на два слоя:

Operational Layer (операционный слой — текущие задачи)

Management Layer (управленческий слой — цели и контроль)

---

# 2. Operational Layer

## Task (задача)

Task — действие, которое нужно выполнить.

Основные поля:

id  
title  
status  
planned_at  
goal_id  
created_at  

Статусы:

WAITING  
PAUSED  
IN_PROGRESS  
DONE

---

## Subtask (подзадача)

Подзадача — часть задачи.

Поля:

id  
task_id  
title  
status

Связь:

subtasks.task_id → tasks.id

---

## TimeBlock (блок времени)

TimeBlock — запланированный интервал времени.

Поля:

id  
task_id  
start_at  
end_at  
duration_minutes  

Связь:

time_blocks.task_id → tasks.id

---

# 3. Management Layer

## Cycle (цикл)

Cycle — период управления.

Пример:

месяц  
квартал  
проект

Поля:

id  
name  
start_date  
end_date  

---

## Goal (цель)

Goal — результат, который должен быть достигнут.

Поля:

id  
title  
success_criteria  
planned_end_date  
status  
cycle_id  
completed_at  

Статусы:

ACTIVE  
DONE  
DROPPED

Связь:

goals.cycle_id → cycles.id

---

## Goal Reschedule Event (событие переноса срока)

История переносов цели.

Поля:

id  
goal_id  
old_end_date  
new_end_date  
changed_at  

Связь:

goal_reschedule_events.goal_id → goals.id

---

## Nudge (сигнал)

Nudge — системный сигнал об отклонении.

Примеры:

goal.overdue  
goal.multiple_reschedules

Поля:

id  
type  
goal_id  
created_at  

---

# 4. Связи сущностей

Главные связи системы:

Goal
↓
Task
↓
Subtask

Goal
↓
Cycle

Task
↓
TimeBlock

---

# 5. Логическая схема

Cycle
↓
Goals
↓
Tasks
↓
Subtasks

Tasks
↓
TimeBlocks

Goals
↓
Reschedule Events

Goals
↓
Nudges

---

# 6. Принцип хранения данных

Runtime использует SQLite.

Файл базы:

/data/organizer.db

Основные таблицы:

tasks  
subtasks  
time_blocks  
cycles  
goals  
goal_reschedule_events  
user_nudges

---

# 7. Основные инварианты

1. Runtime — единственный writer (единственный сервис записи).

2. История переносов не удаляется.

3. Просрочка цели не меняет статус автоматически.

4. Связи между сущностями сохраняются через ID.

---

# 8. Связанные документы

SYSTEM_OVERVIEW.md  
SYSTEM_MAP_CANVAS.md  
DECISION_ENGINE.md  
SYSTEM_SPEC.md
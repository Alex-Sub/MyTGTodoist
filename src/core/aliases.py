# Единый словарь русских отображаемых названий для статусов/интентов/кнопок/колонок.
# Правило: в логике используем алиасы (ключи словарей), в UI/таблицах показываем ТОЛЬКО значения на русском.

from __future__ import annotations

from typing import Final


# =========================
# Статусы задач
# =========================
TASK_STATUS_LABELS: Final[dict[str, str]] = {
    "inbox": "Неразобранная",
    "planned": "Запланирована",
    "in_progress": "В работе",
    "waiting": "Ожидает",
    "active": "Активна",
    "completed": "Завершена",
    "done": "Завершена",
    "canceled": "Отменена",
    "cancelled": "Отменена",
}

# (опционально) обратное преобразование: русский -> алиас
TASK_STATUS_ALIASES: Final[dict[str, str]] = {v: k for k, v in TASK_STATUS_LABELS.items()}


# =========================
# Типы действий (PendingAction / domain actions)
# =========================
ACTION_LABELS: Final[dict[str, str]] = {
    "CREATE_TASK": "Создание задачи",
    "PROCESS_TASK": "Разбор задачи",
    "UPDATE_TASK": "Изменение задачи",
    "DELETE_TASK": "Удаление задачи",
    "CREATE_MEETING": "Создание встречи",
    "MOVE_MEETING": "Перенос встречи",
    "EXPORT": "Экспорт данных",
    "SHOW_INBOX_TASKS": "Показать неразобранные задачи",
    "SHOW_TODAY": "Показать на сегодня",
    "SHOW_WEEK": "Показать на неделю",
}

ACTION_ALIASES: Final[dict[str, str]] = {v: k for k, v in ACTION_LABELS.items()}


# =========================
# Стадии диалога (PendingAction.stage)
# =========================
STAGE_LABELS: Final[dict[str, str]] = {
    "menu": "Что сделать дальше",
    "awaiting_date": "Ожидаю дату",
    "awaiting_time": "Ожидаю время",
    "awaiting_project": "Выбор проекта",
    "awaiting_title": "Изменение названия",
    "preview": "Предпросмотр изменений",
    "confirm": "Подтверждение",
}

STAGE_ALIASES: Final[dict[str, str]] = {v: k for k, v in STAGE_LABELS.items()}


# =========================
# Интенты NLU (если нужно выводить пользователю)
# =========================
INTENT_LABELS: Final[dict[str, str]] = {
    "CREATE_MEETING": "Создать встречу",
    "MOVE_MEETING": "Перенести встречу",
    "CREATE_TASK": "Создать задачу",
    "PLAN_TASK": "Запланировать задачу",
    "START_WORK": "Начать работу",
    "STOP_WORK": "Остановить работу",
    "EXPORT": "Экспорт",
    "NONE": "Не распознано",
}

INTENT_ALIASES: Final[dict[str, str]] = {v: k for k, v in INTENT_LABELS.items()}


# =========================
# Тексты кнопок (Inline/Reply)
# =========================
BUTTON_LABELS: Final[dict[str, str]] = {
    "BTN_CONFIRM": "✅ Верно",
    "BTN_CHANGE": "✏️ Изменить",
    "BTN_CANCEL": "❌ Отменить",
    "BTN_PROCESS": "🔍 Разобрать",
    "BTN_SET_DATE": "📅 Назначить дату",
    "BTN_SET_PROJECT": "🗂 Назначить проект",
    "BTN_RENAME": "✏️ Переименовать",
    "BTN_DONE": "✅ Готово",
    "BTN_DELETE": "❌ Удалить",
    "BTN_MOVE_NEW": "Перенести новую",
    "BTN_MOVE_EXISTING": "Перенести существующую",
    "BTN_ALLOW_OVERLAP": "Всё равно добавить",
    "BTN_ADD_NEW": "Добавить как новую",
    "BTN_EDIT_TITLE": "Изменить название",
    "BTN_RESCHEDULE": "Перепланировать",
    "BTN_SHOW_MORE": "Показать ещё",
    "BTN_BACK": "⬅️ Назад",
}

# Важно: тексты кнопок уникальны -> можно сделать обратный маппинг при необходимости
BUTTON_ALIASES: Final[dict[str, str]] = {v: k for k, v in BUTTON_LABELS.items()}


# =========================
# Подписи полей (для preview/форм/таблиц)
# =========================
FIELD_LABELS: Final[dict[str, str]] = {
    # общие
    "title": "Название",
    "status": "Статус",
    "source": "Источник",
    "confidence": "Уверенность",
    "missing": "Не хватает",
    "time": "Время",
    "minutes": "Минуты",
    "target": "Цель",
    "sync_state": "Состояние синхронизации",
    "etag": "ETag",
    "g_updated": "Обновлено в Google",

    # задачи
    "task_id": "ID задачи",
    "due_date": "Дата",
    "due_time": "Время",
    "project": "Проект",
    "created_at": "Создана",
    "updated_at": "Обновлена",

    # встречи
    "event_id": "ID встречи",
    "date": "Дата",
    "start_time": "Начало",
    "end_time": "Окончание",
    "duration": "Длительность",
    "calendar": "Календарь",
    "location": "Место",
}

FIELD_ALIASES: Final[dict[str, str]] = {v: k for k, v in FIELD_LABELS.items()}


# =========================
# Колонки таблиц Google Sheets / Excel (отображаемые названия)
# =========================
SHEET_COLUMNS_TASKS: Final[list[str]] = [
    "ID задачи",
    "Название",
    "Статус",
    "Дата",
    "Время",
    "Проект",
    "Источник",
    "Создана",
    "Обновлена",
]

SHEET_COLUMNS_MEETINGS: Final[list[str]] = [
    "ID встречи",
    "Название",
    "Дата",
    "Начало",
    "Окончание",
    "Длительность",
    "Статус",
    "Календарь",
]


# =========================
# Утилиты безопасного получения русских лейблов
# =========================
def label(mapping: dict[str, str], key: str, default: str | None = None) -> str:
    """Безопасно получить русский лейбл по алиасу."""
    if not key:
        return default or ""
    return mapping.get(key, default or key)


def task_status_ru(status_alias: str) -> str:
    return label(TASK_STATUS_LABELS, status_alias)


def action_ru(action_alias: str) -> str:
    return label(ACTION_LABELS, action_alias)


def intent_ru(intent_alias: str) -> str:
    return label(INTENT_LABELS, intent_alias)


def stage_ru(stage_alias: str) -> str:
    return label(STAGE_LABELS, stage_alias)


def button_ru(btn_alias: str) -> str:
    return label(BUTTON_LABELS, btn_alias)


def field_ru(field_alias: str) -> str:
    return label(FIELD_LABELS, field_alias)

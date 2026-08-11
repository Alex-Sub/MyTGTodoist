from __future__ import annotations

from typing import Any, Dict

from src.integrations.telegram.schemas import TelegramRuntimeResult

_DEFAULT_REPLY = "Запрос принят. Попробуйте еще раз чуть позже."
_DUPLICATE_REPLY = "Этот запрос уже обработан."
_PAST_DATE_CONFIRM_FIELD = "start_at_date_past_confirm"
_TEMPORAL_CONFIRM_FIELD = "temporal_commit_confirm"
_TEMPORAL_EDIT_FIELD = "temporal_edit_field"
_SYNC_CONFLICT_FIELD = "sync_conflict_resolution"
_MEETING_COMMENT_TEXT_FIELD = "meeting_comment_text"
_TASK_CREATE_CONFIRM_FIELD = "task_create_confirm"
_TASK_CREATE_DUPLICATE_CONFIRM_FIELD = "task_create_duplicate_confirm"
_TASK_CREATE_COMMENT_FIELD = "task_create_comment"
_AMBIGUOUS_CREATE_FIELD = "ambiguous_create_route"
_MEETING_UPDATE_TARGET_CONFIRM_FIELD = "awaiting_target_confirm"
_MEETING_UPDATE_EDIT_CHOICE_FIELD = "meeting_update_edit_choice"
_MEETING_UPDATE_EDIT_COMMENT_FIELD = "meeting_update_edit_comment"
_MEETING_UPDATE_FINAL_CONFIRM_FIELD = "awaiting_final_confirm"
_TIMEBLOCK_UPDATE_TARGET_CONFIRM_FIELD = "timeblock_update_target_confirm"
_TIMEBLOCK_UPDATE_EDIT_CHOICE_FIELD = "timeblock_update_edit_choice"
_TIMEBLOCK_UPDATE_EDIT_COMMENT_FIELD = "timeblock_update_edit_comment"
_TIMEBLOCK_UPDATE_FINAL_CONFIRM_FIELD = "timeblock_update_final_confirm"
_MEETING_COMMENT_TARGET_CONFIRM_FIELD = "meeting_comment_target_confirm"
_MEETING_COMMENT_FINAL_CONFIRM_FIELD = "meeting_comment_final_confirm"
_TASK_COMMENT_TARGET_CONFIRM_FIELD = "task_comment_target_confirm"
_TASK_COMMENT_FINAL_CONFIRM_FIELD = "task_comment_final_confirm"
_DURATION_FIELD = "duration_minutes"
_PAST_DATE_HELPER_TEXT = "Если нужна другая дата, введите её сообщением."
_DURATION_HELPER_TEXT = "Если нужна другая длительность, введите её сообщением."
_TEMPORAL_ACTIONS_HELPER_TEXT = "Можно выбрать поле для правки, подтвердить или отменить сценарий."
_TEMPORAL_CONFIRM_HELPER_TEXT = "Если нужно исправить параметры, выберите поле кнопкой или отправьте уточнение сообщением."
_TASK_CREATE_CONFIRM_HELPER_TEXT = "Если нужно исправить задачу, нажмите «Нет» и введите уточнение сообщением."
_BINARY_CONFIRM_RENDER_CONFIG = {
    _PAST_DATE_CONFIRM_FIELD: {"family": "past", "helper_text": _PAST_DATE_HELPER_TEXT},
    _TEMPORAL_CONFIRM_FIELD: {"family": "temporal", "helper_text": _TEMPORAL_CONFIRM_HELPER_TEXT},
    _TASK_CREATE_CONFIRM_FIELD: {"family": "task_create", "helper_text": _TASK_CREATE_CONFIRM_HELPER_TEXT},
    _TASK_CREATE_DUPLICATE_CONFIRM_FIELD: {"family": "task_create_duplicate", "helper_text": ""},
    _MEETING_UPDATE_TARGET_CONFIRM_FIELD: {"family": "meeting_target", "helper_text": ""},
    _MEETING_UPDATE_FINAL_CONFIRM_FIELD: {"family": "meeting_update_confirm", "helper_text": ""},
    _TIMEBLOCK_UPDATE_TARGET_CONFIRM_FIELD: {"family": "timeblock_target", "helper_text": ""},
    _TIMEBLOCK_UPDATE_FINAL_CONFIRM_FIELD: {"family": "timeblock_update_confirm", "helper_text": ""},
    _MEETING_COMMENT_TARGET_CONFIRM_FIELD: {"family": "meeting_comment_target", "helper_text": ""},
    _MEETING_COMMENT_FINAL_CONFIRM_FIELD: {"family": "meeting_comment_final", "helper_text": ""},
    _TASK_COMMENT_TARGET_CONFIRM_FIELD: {"family": "task_comment_target", "helper_text": ""},
    _TASK_COMMENT_FINAL_CONFIRM_FIELD: {"family": "task_comment_final", "helper_text": ""},
}


def _build_duration_inline_keyboard(callback_prefix: str, *, include_cancel: bool = True) -> Dict[str, Any]:
    keyboard = [
        [
            {"text": "15 минут", "callback_data": f"{callback_prefix}:15"},
            {"text": "30 минут", "callback_data": f"{callback_prefix}:30"},
        ],
        [
            {"text": "45 минут", "callback_data": f"{callback_prefix}:45"},
            {"text": "60 минут", "callback_data": f"{callback_prefix}:60"},
        ],
        [
            {"text": "Иное", "callback_data": f"{callback_prefix}:other"},
        ],
    ]
    if include_cancel:
        keyboard.append([{"text": "Отмена", "callback_data": f"{callback_prefix}:cancel"}])
    return {"inline_keyboard": keyboard}


def _extract_missing_field(result: TelegramRuntimeResult) -> str:
    raw = result.raw_runtime_payload if isinstance(result.raw_runtime_payload, dict) else {}
    rec = raw.get("rec") if isinstance(raw.get("rec"), dict) else {}
    for candidate in (
        rec.get("missing_field"),
        raw.get("missing_field"),
    ):
        if isinstance(candidate, str) and candidate.strip():
            return candidate.strip()
    return ""


def _extract_command(result: TelegramRuntimeResult) -> Dict[str, Any]:
    raw = result.raw_runtime_payload if isinstance(result.raw_runtime_payload, dict) else {}
    command = raw.get("command")
    return dict(command) if isinstance(command, dict) else {}


def _build_inline_keyboard_for_missing_field(missing_field: str) -> Dict[str, Any] | None:
    field = str(missing_field or "").strip()
    if field == _SYNC_CONFLICT_FIELD:
        return None
    binary_cfg = _BINARY_CONFIRM_RENDER_CONFIG.get(field)
    if isinstance(binary_cfg, dict):
        family = str(binary_cfg.get("family") or "").strip()
        if not family:
            return None
        if field == _MEETING_UPDATE_FINAL_CONFIRM_FIELD:
            return None
        if field == _TASK_CREATE_CONFIRM_FIELD:
            return {
                "inline_keyboard": [
                    [
                        {"text": "Создать", "callback_data": "clarify:v1:task_create:create"},
                        {"text": "Исправить", "callback_data": "clarify:v1:task_create:edit"},
                    ],
                    [
                        {"text": "Комментарий", "callback_data": "clarify:v1:task_create:comment"},
                    ],
                    [
                        {"text": "Не создавать", "callback_data": "clarify:v1:task_create:cancel"},
                    ],
                ]
            }
        if field == _TASK_CREATE_DUPLICATE_CONFIRM_FIELD:
            return {
                "inline_keyboard": [
                    [
                        {
                            "text": "Да, создать ещё одну",
                            "callback_data": "clarify:v1:task_create_duplicate:yes",
                        },
                    ],
                    [
                        {
                            "text": "Нет, не создавать",
                            "callback_data": "clarify:v1:task_create_duplicate:no",
                        },
                    ],
                ]
            }
        if field == _TEMPORAL_CONFIRM_FIELD:
            return {
                "inline_keyboard": [
                    [
                        {"text": "Дата", "callback_data": "clarify:v1:temporal_edit:date"},
                        {"text": "Время", "callback_data": "clarify:v1:temporal_edit:time"},
                    ],
                    [
                        {"text": "Длительность", "callback_data": "clarify:v1:temporal_edit:duration"},
                        {"text": "Комментарий", "callback_data": "clarify:v1:temporal_edit:comment"},
                    ],
                    [
                        {"text": "Подтвердить", "callback_data": f"clarify:v1:{family}:yes"},
                        {"text": "Отмена", "callback_data": "clarify:v1:temporal_edit:cancel"},
                    ],
                ]
            }
        return {
            "inline_keyboard": [
                [
                    {"text": "Да", "callback_data": f"clarify:v1:{family}:yes"},
                    {"text": "Нет", "callback_data": f"clarify:v1:{family}:no"},
                ]
            ]
        }
    if field == _DURATION_FIELD:
        return _build_duration_inline_keyboard("clarify:v1:duration")
    if field == _AMBIGUOUS_CREATE_FIELD:
        return {
            "inline_keyboard": [
                [
                    {"text": "Событие в календаре", "callback_data": "clarify:v1:ambiguous_create:meeting"},
                ],
                [
                    {"text": "Блок времени", "callback_data": "clarify:v1:ambiguous_create:timeblock"},
                ],
                [
                    {"text": "Задачу", "callback_data": "clarify:v1:ambiguous_create:task"},
                ],
            ]
        }
    if field == _TEMPORAL_EDIT_FIELD:
        return {
            "inline_keyboard": [
                [
                    {"text": "Дата", "callback_data": "clarify:v1:temporal_edit:date"},
                    {"text": "Время", "callback_data": "clarify:v1:temporal_edit:time"},
                ],
                [
                    {"text": "Длительность", "callback_data": "clarify:v1:temporal_edit:duration"},
                    {"text": "Комментарий", "callback_data": "clarify:v1:temporal_edit:comment"},
                ],
                [
                    {"text": "Подтвердить", "callback_data": "clarify:v1:temporal:yes"},
                    {"text": "Отмена", "callback_data": "clarify:v1:temporal_edit:cancel"},
                ],
            ]
        }
    if field == _MEETING_UPDATE_EDIT_CHOICE_FIELD:
        return {
            "inline_keyboard": [
                [
                    {"text": "Дату", "callback_data": "clarify:v1:meeting_update_edit:date"},
                    {"text": "Время", "callback_data": "clarify:v1:meeting_update_edit:time"},
                ],
                [
                    {"text": "Длительность", "callback_data": "clarify:v1:meeting_update_edit:duration"},
                    {"text": "Комментарий", "callback_data": "clarify:v1:meeting_update_edit:comment"},
                ],
                [
                    {"text": "Дату и время", "callback_data": "clarify:v1:meeting_update_edit:date_time"},
                    {"text": "Отмена", "callback_data": "clarify:v1:meeting_update_edit:cancel"},
                ],
            ]
        }
    if field == _TIMEBLOCK_UPDATE_EDIT_CHOICE_FIELD:
        return {
            "inline_keyboard": [
                [
                    {"text": "Дату", "callback_data": "clarify:v1:timeblock_update_edit:date"},
                    {"text": "Время", "callback_data": "clarify:v1:timeblock_update_edit:time"},
                ],
                [
                    {"text": "Длительность", "callback_data": "clarify:v1:timeblock_update_edit:duration"},
                    {"text": "Комментарий", "callback_data": "clarify:v1:timeblock_update_edit:comment"},
                ],
                [
                    {"text": "Дату и время", "callback_data": "clarify:v1:timeblock_update_edit:date_time"},
                    {"text": "Отмена", "callback_data": "clarify:v1:timeblock_update_edit:cancel"},
                ],
            ]
        }
    return None


def _build_inline_keyboard_for_command_state(result: TelegramRuntimeResult, missing_field: str) -> Dict[str, Any] | None:
    if str(missing_field or "").strip() == _SYNC_CONFLICT_FIELD:
        command = _extract_command(result)
        conflict = command.get("__sync_conflict") if isinstance(command.get("__sync_conflict"), dict) else {}
        conflict_id = str(conflict.get("id") or "").strip()
        if not conflict_id:
            return None
        return {
            "inline_keyboard": [
                [
                    {"text": "Удалить из базы", "callback_data": f"sync_conflict:delete_db:{conflict_id}"},
                ],
                [
                    {"text": "Восстановить в календаре", "callback_data": f"sync_conflict:restore_calendar:{conflict_id}"},
                ],
                [
                    {"text": "Изменить", "callback_data": f"sync_conflict:edit:{conflict_id}"},
                ],
                [
                    {"text": "Пропустить", "callback_data": f"sync_conflict:skip:{conflict_id}"},
                ],
            ]
        }
    if str(missing_field or "").strip() == _MEETING_UPDATE_FINAL_CONFIRM_FIELD:
        return {
            "inline_keyboard": [
                [
                    {"text": "Подтвердить", "callback_data": "clarify:v1:meeting_update_confirm:yes"},
                ],
                [
                    {"text": "Изменить дату", "callback_data": "clarify:v1:meeting_update_edit:date"},
                    {"text": "Изменить время", "callback_data": "clarify:v1:meeting_update_edit:time"},
                ],
                [
                    {"text": "Изменить длительность", "callback_data": "clarify:v1:meeting_update_edit:duration"},
                    {"text": "Изменить комментарий", "callback_data": "clarify:v1:meeting_update_edit:comment"},
                ],
                [
                    {"text": "Отмена", "callback_data": "clarify:v1:meeting_update_edit:cancel"},
                ],
            ]
        }
    if str(missing_field or "").strip() == "meeting_update_edit_duration":
        return _build_duration_inline_keyboard("clarify:v1:meeting_update_duration")
    if str(missing_field or "").strip() == _TIMEBLOCK_UPDATE_FINAL_CONFIRM_FIELD:
        return {
            "inline_keyboard": [
                [
                    {"text": "Подтвердить", "callback_data": "clarify:v1:timeblock_update_confirm:yes"},
                ],
                [
                    {"text": "Изменить дату", "callback_data": "clarify:v1:timeblock_update_edit:date"},
                    {"text": "Изменить время", "callback_data": "clarify:v1:timeblock_update_edit:time"},
                ],
                [
                    {"text": "Изменить длительность", "callback_data": "clarify:v1:timeblock_update_edit:duration"},
                    {"text": "Изменить комментарий", "callback_data": "clarify:v1:timeblock_update_edit:comment"},
                ],
                [
                    {"text": "Отмена", "callback_data": "clarify:v1:timeblock_update_edit:cancel"},
                ],
            ]
        }
    if str(missing_field or "").strip() == "timeblock_update_edit_duration":
        return _build_duration_inline_keyboard("clarify:v1:timeblock_update_duration")
    if str(missing_field or "").strip() != _TEMPORAL_EDIT_FIELD:
        return None
    command = _extract_command(result)
    active_edit_field = str(command.get("__temporal_edit_active_field") or "").strip()
    if active_edit_field == _DURATION_FIELD:
        return _build_duration_inline_keyboard("clarify:v1:temporal_duration")
    if (
        active_edit_field != _MEETING_COMMENT_TEXT_FIELD
        and str(missing_field or "").strip() not in {_MEETING_UPDATE_EDIT_COMMENT_FIELD, _TIMEBLOCK_UPDATE_EDIT_COMMENT_FIELD}
    ):
        return None
    return {
        "inline_keyboard": [
            [
                {"text": "Оставить без изменений", "callback_data": "clarify:v1:temporal_comment:keep"},
            ],
            [
                {"text": "Очистить комментарий", "callback_data": "clarify:v1:temporal_comment:clear"},
            ],
            [
                {"text": "Отмена", "callback_data": "clarify:v1:temporal_edit:cancel"},
            ],
        ]
    }


def _helper_text_for_missing_field(missing_field: str) -> str:
    field = str(missing_field or "").strip()
    binary_cfg = _BINARY_CONFIRM_RENDER_CONFIG.get(field)
    if isinstance(binary_cfg, dict):
        helper_text = binary_cfg.get("helper_text")
        if isinstance(helper_text, str):
            if field == _TEMPORAL_CONFIRM_FIELD:
                suffix = _TEMPORAL_ACTIONS_HELPER_TEXT
                return f"{helper_text}\n{suffix}".strip()
            return helper_text
    if field == _TEMPORAL_EDIT_FIELD:
        return _TEMPORAL_ACTIONS_HELPER_TEXT
    if field == _DURATION_FIELD:
        return _DURATION_HELPER_TEXT
    return ""


def select_reply_text(result: TelegramRuntimeResult) -> str:
    clarifying = (result.clarifying_question or "").strip()
    if clarifying:
        return clarifying

    reply = (result.reply_text or "").strip()
    if reply:
        return reply

    if result.duplicate_state:
        return _DUPLICATE_REPLY

    return _DEFAULT_REPLY


def map_result_to_send_payload(result: TelegramRuntimeResult) -> Dict[str, Any]:
    payload: Dict[str, Any] = {"text": select_reply_text(result)}
    if bool(result.needs_clarification):
        missing_field = _extract_missing_field(result)
        state_keyboard = _build_inline_keyboard_for_command_state(result, missing_field)
        keyboard = state_keyboard or _build_inline_keyboard_for_missing_field(missing_field)
        if keyboard:
            helper = "" if state_keyboard else _helper_text_for_missing_field(missing_field)
            if helper:
                payload["text"] = f"{str(payload.get('text') or '').strip()}\n\n{helper}"
            payload["reply_markup"] = keyboard
    return payload

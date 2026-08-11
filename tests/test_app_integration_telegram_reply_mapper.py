from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
APP_SRC = ROOT / "telegram-bot" / "app-integration" / "src"
MAPPER_PATH = APP_SRC / "integrations" / "telegram" / "reply_mapper.py"


def _load_reply_mapper_module():
    preserved: dict[str, Any] = {}
    for key in list(sys.modules.keys()):
        if key == "src" or key.startswith("src.") or key == "requests":
            preserved[key] = sys.modules.pop(key)

    requests_stub = types.ModuleType("requests")
    sys.modules["requests"] = requests_stub

    src_pkg = types.ModuleType("src")
    src_pkg.__path__ = [str(APP_SRC)]  # type: ignore[attr-defined]
    sys.modules["src"] = src_pkg

    spec = importlib.util.spec_from_file_location("app_integration_reply_mapper_module", MAPPER_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    for key in list(sys.modules.keys()):
        if key == "src" or key.startswith("src.") or key == "requests":
            sys.modules.pop(key)
    sys.modules.update(preserved)
    return module


class _FakeRuntimeResult:
    def __init__(self, *, clarifying_question: str, missing_field: str, command: dict[str, Any] | None = None) -> None:
        self.clarifying_question = clarifying_question
        self.reply_text = ""
        self.duplicate_state = None
        self.needs_clarification = True
        payload: dict[str, Any] = {"rec": {"missing_field": missing_field}}
        if command is not None:
            payload["command"] = dict(command)
        self.raw_runtime_payload = payload


def test_temporal_edit_field_renders_inline_buttons() -> None:
    mapper = _load_reply_mapper_module()
    result = _FakeRuntimeResult(clarifying_question="Что исправить?", missing_field="temporal_edit_field")

    payload = mapper.map_result_to_send_payload(result)
    assert payload["text"].startswith("Что исправить?")
    markup = payload.get("reply_markup")
    assert isinstance(markup, dict)
    keyboard = markup.get("inline_keyboard")
    assert isinstance(keyboard, list)
    callback_values = [
        str(btn.get("callback_data") or "")
        for row in keyboard
        if isinstance(row, list)
        for btn in row
        if isinstance(btn, dict)
    ]
    assert "clarify:v1:temporal_edit:date" in callback_values
    assert "clarify:v1:temporal_edit:time" in callback_values
    assert "clarify:v1:temporal_edit:duration" in callback_values
    assert "clarify:v1:temporal_edit:comment" in callback_values
    assert "clarify:v1:temporal:yes" in callback_values
    assert "clarify:v1:temporal_edit:cancel" in callback_values


def test_meeting_update_target_confirm_renders_yes_no_buttons() -> None:
    mapper = _load_reply_mapper_module()
    result = _FakeRuntimeResult(
        clarifying_question="Вы имеете в виду встречу 13.04.2026 14:00–15:00?\nПеренести её на 16:00?",
        missing_field="awaiting_target_confirm",
    )

    payload = mapper.map_result_to_send_payload(result)
    markup = payload.get("reply_markup")
    assert isinstance(markup, dict)
    keyboard = markup.get("inline_keyboard")
    assert isinstance(keyboard, list)
    callback_values = [
        str(btn.get("callback_data") or "")
        for row in keyboard
        if isinstance(row, list)
        for btn in row
        if isinstance(btn, dict)
    ]
    assert "clarify:v1:meeting_target:yes" in callback_values
    assert "clarify:v1:meeting_target:no" in callback_values


def test_temporal_confirm_renders_yes_no_and_duration_buttons() -> None:
    mapper = _load_reply_mapper_module()
    result = _FakeRuntimeResult(
        clarifying_question="Проверьте, всё ли верно.",
        missing_field="temporal_commit_confirm",
    )

    payload = mapper.map_result_to_send_payload(result)
    text = str(payload.get("text") or "")
    assert "Можно выбрать поле для правки, подтвердить или отменить сценарий." in text
    markup = payload.get("reply_markup")
    assert isinstance(markup, dict)
    keyboard = markup.get("inline_keyboard")
    assert isinstance(keyboard, list)
    callback_values = [
        str(btn.get("callback_data") or "")
        for row in keyboard
        if isinstance(row, list)
        for btn in row
        if isinstance(btn, dict)
    ]
    assert "clarify:v1:temporal_edit:date" in callback_values
    assert "clarify:v1:temporal_edit:time" in callback_values
    assert "clarify:v1:temporal_edit:duration" in callback_values
    assert "clarify:v1:temporal_edit:comment" in callback_values
    assert "clarify:v1:temporal:yes" in callback_values
    assert "clarify:v1:temporal_edit:cancel" in callback_values


def test_temporal_comment_waiting_state_renders_comment_action_buttons() -> None:
    mapper = _load_reply_mapper_module()
    result = _FakeRuntimeResult(
        clarifying_question="Введите новый комментарий.\n\nТекущий комментарий:\n-\n\nЕсли передумали — нажмите «Оставить без изменений» или «Отмена».",
        missing_field="temporal_edit_field",
        command={"__temporal_edit_active_field": "meeting_comment_text"},
    )

    payload = mapper.map_result_to_send_payload(result)
    markup = payload.get("reply_markup")
    assert isinstance(markup, dict)
    keyboard = markup.get("inline_keyboard")
    assert isinstance(keyboard, list)
    callback_values = [
        str(btn.get("callback_data") or "")
        for row in keyboard
        if isinstance(row, list)
        for btn in row
        if isinstance(btn, dict)
    ]
    assert "clarify:v1:temporal_comment:keep" in callback_values
    assert "clarify:v1:temporal_comment:clear" in callback_values
    assert "clarify:v1:temporal_edit:cancel" in callback_values


def test_temporal_duration_waiting_state_renders_quick_duration_buttons() -> None:
    mapper = _load_reply_mapper_module()
    result = _FakeRuntimeResult(
        clarifying_question="На сколько минут запланировать?",
        missing_field="temporal_edit_field",
        command={"__temporal_edit_active_field": "duration_minutes"},
    )

    payload = mapper.map_result_to_send_payload(result)
    markup = payload.get("reply_markup")
    assert isinstance(markup, dict)
    keyboard = markup.get("inline_keyboard")
    assert isinstance(keyboard, list)
    callback_values = [
        str(btn.get("callback_data") or "")
        for row in keyboard
        if isinstance(row, list)
        for btn in row
        if isinstance(btn, dict)
    ]
    assert "clarify:v1:temporal_duration:15" in callback_values
    assert "clarify:v1:temporal_duration:30" in callback_values
    assert "clarify:v1:temporal_duration:45" in callback_values
    assert "clarify:v1:temporal_duration:60" in callback_values
    assert "clarify:v1:temporal_duration:other" in callback_values
    assert "clarify:v1:temporal_duration:cancel" in callback_values


def test_meeting_update_duration_waiting_state_renders_scoped_quick_duration_buttons() -> None:
    mapper = _load_reply_mapper_module()
    result = _FakeRuntimeResult(
        clarifying_question="На сколько минут перенести/изменить встречу?",
        missing_field="meeting_update_edit_duration",
    )

    payload = mapper.map_result_to_send_payload(result)
    markup = payload.get("reply_markup")
    assert isinstance(markup, dict)
    keyboard = markup.get("inline_keyboard")
    assert isinstance(keyboard, list)
    callback_values = [
        str(btn.get("callback_data") or "")
        for row in keyboard
        if isinstance(row, list)
        for btn in row
        if isinstance(btn, dict)
    ]
    assert "clarify:v1:meeting_update_duration:15" in callback_values
    assert "clarify:v1:meeting_update_duration:30" in callback_values
    assert "clarify:v1:meeting_update_duration:45" in callback_values
    assert "clarify:v1:meeting_update_duration:60" in callback_values
    assert "clarify:v1:meeting_update_duration:other" in callback_values
    assert "clarify:v1:meeting_update_duration:cancel" in callback_values


def test_sync_conflict_resolution_renders_expected_action_buttons() -> None:
    mapper = _load_reply_mapper_module()
    result = _FakeRuntimeResult(
        clarifying_question="Найдено событие в базе, которого нет в Google Calendar.\nВстреча с Алексеем\n2026-05-10 11:00\nЧто сделать?",
        missing_field="sync_conflict_resolution",
        command={"__sync_conflict": {"id": "sc-123"}},
    )

    payload = mapper.map_result_to_send_payload(result)
    markup = payload.get("reply_markup")
    assert isinstance(markup, dict)
    keyboard = markup.get("inline_keyboard")
    assert isinstance(keyboard, list)
    callback_values = [
        str(btn.get("callback_data") or "")
        for row in keyboard
        if isinstance(row, list)
        for btn in row
        if isinstance(btn, dict)
    ]
    assert "sync_conflict:delete_db:sc-123" in callback_values
    assert "sync_conflict:restore_calendar:sc-123" in callback_values
    assert "sync_conflict:edit:sc-123" in callback_values
    assert "sync_conflict:skip:sc-123" in callback_values


def test_meeting_update_edit_choice_renders_unified_buttons() -> None:
    mapper = _load_reply_mapper_module()
    result = _FakeRuntimeResult(
        clarifying_question="Что изменить?",
        missing_field="meeting_update_edit_choice",
    )
    payload = mapper.map_result_to_send_payload(result)
    markup = payload.get("reply_markup")
    assert isinstance(markup, dict)
    keyboard = markup.get("inline_keyboard")
    assert isinstance(keyboard, list)
    callback_values = [
        str(btn.get("callback_data") or "")
        for row in keyboard if isinstance(row, list)
        for btn in row if isinstance(btn, dict)
    ]
    assert "clarify:v1:meeting_update_edit:date" in callback_values
    assert "clarify:v1:meeting_update_edit:time" in callback_values
    assert "clarify:v1:meeting_update_edit:duration" in callback_values
    assert "clarify:v1:meeting_update_edit:comment" in callback_values
    assert "clarify:v1:meeting_update_edit:date_time" in callback_values
    assert "clarify:v1:meeting_update_edit:cancel" in callback_values


def test_task_create_confirm_renders_create_edit_comment_cancel_buttons() -> None:
    mapper = _load_reply_mapper_module()
    result = _FakeRuntimeResult(
        clarifying_question="Проверь задачу перед созданием:\nТекст: Купить молоко\nСрок: 2026-05-18\n\nСоздать задачу?",
        missing_field="task_create_confirm",
    )

    payload = mapper.map_result_to_send_payload(result)
    markup = payload.get("reply_markup")
    assert isinstance(markup, dict)
    keyboard = markup.get("inline_keyboard")
    assert isinstance(keyboard, list)
    callback_values = [
        str(btn.get("callback_data") or "")
        for row in keyboard
        if isinstance(row, list)
        for btn in row
        if isinstance(btn, dict)
    ]
    assert "clarify:v1:task_create:create" in callback_values
    assert "clarify:v1:task_create:edit" in callback_values
    assert "clarify:v1:task_create:comment" in callback_values
    assert "clarify:v1:task_create:cancel" in callback_values


def test_task_create_duplicate_confirm_renders_yes_no_cancel_buttons() -> None:
    mapper = _load_reply_mapper_module()
    result = _FakeRuntimeResult(
        clarifying_question="Похоже, такая задача уже есть на 2026-05-18:\n№ 17 — Купить молоко\n\nСоздать ещё одну?",
        missing_field="task_create_duplicate_confirm",
    )

    payload = mapper.map_result_to_send_payload(result)
    markup = payload.get("reply_markup")
    assert isinstance(markup, dict)
    keyboard = markup.get("inline_keyboard")
    assert isinstance(keyboard, list)
    callback_values = [
        str(btn.get("callback_data") or "")
        for row in keyboard
        if isinstance(row, list)
        for btn in row
        if isinstance(btn, dict)
    ]
    assert "clarify:v1:task_create_duplicate:yes" in callback_values
    assert "clarify:v1:task_create_duplicate:no" in callback_values


def test_ambiguous_create_prompt_renders_entity_choice_buttons() -> None:
    mapper = _load_reply_mapper_module()
    result = _FakeRuntimeResult(
        clarifying_question="Что создать: событие в календаре, блок времени или задачу?",
        missing_field="ambiguous_create_route",
    )

    payload = mapper.map_result_to_send_payload(result)
    markup = payload.get("reply_markup")
    assert isinstance(markup, dict)
    keyboard = markup.get("inline_keyboard")
    assert isinstance(keyboard, list)
    callback_values = [
        str(btn.get("callback_data") or "")
        for row in keyboard
        if isinstance(row, list)
        for btn in row
        if isinstance(btn, dict)
    ]
    assert "clarify:v1:ambiguous_create:meeting" in callback_values
    assert "clarify:v1:ambiguous_create:timeblock" in callback_values
    assert "clarify:v1:ambiguous_create:task" in callback_values


def test_meeting_update_final_confirm_renders_edit_buttons() -> None:
    mapper = _load_reply_mapper_module()
    result = _FakeRuntimeResult(
        clarifying_question="Подтвердить изменение?",
        missing_field="awaiting_final_confirm",
    )
    payload = mapper.map_result_to_send_payload(result)
    markup = payload.get("reply_markup")
    assert isinstance(markup, dict)
    keyboard = markup.get("inline_keyboard")
    assert isinstance(keyboard, list)
    callback_values = [
        str(btn.get("callback_data") or "")
        for row in keyboard if isinstance(row, list)
        for btn in row if isinstance(btn, dict)
    ]
    assert "clarify:v1:meeting_update_confirm:yes" in callback_values
    assert "clarify:v1:meeting_update_edit:date" in callback_values
    assert "clarify:v1:meeting_update_edit:time" in callback_values
    assert "clarify:v1:meeting_update_edit:duration" in callback_values
    assert "clarify:v1:meeting_update_edit:comment" in callback_values

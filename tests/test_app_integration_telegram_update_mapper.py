from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
APP_SRC = ROOT / "telegram-bot" / "app-integration" / "src"
MAPPER_PATH = APP_SRC / "integrations" / "telegram" / "update_mapper.py"


def _load_mapper_module():
    preserved: dict[str, Any] = {}
    for key in list(sys.modules.keys()):
        if key == "src" or key.startswith("src.") or key == "requests":
            preserved[key] = sys.modules.pop(key)

    requests_stub = types.ModuleType("requests")
    sys.modules["requests"] = requests_stub

    src_pkg = types.ModuleType("src")
    src_pkg.__path__ = [str(APP_SRC)]  # type: ignore[attr-defined]
    sys.modules["src"] = src_pkg

    spec = importlib.util.spec_from_file_location("app_integration_update_mapper_module", MAPPER_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    for key in list(sys.modules.keys()):
        if key == "src" or key.startswith("src.") or key == "requests":
            sys.modules.pop(key)
    sys.modules.update(preserved)
    return module


def test_callback_temporal_yes_decodes_to_yes_text() -> None:
    mapper = _load_mapper_module()
    update = {
        "update_id": 101,
        "callback_query": {
            "id": "cb-1",
            "data": "clarify:v1:temporal:yes",
            "from": {"id": 1001},
            "message": {"message_id": 77, "chat": {"id": 2002}},
        },
    }
    req = mapper.map_update_to_request(update, app_id="app", tenant_id="tenant", timezone="Europe/Moscow")
    assert req.text == "yes"
    assert req.metadata["callback_query"]["data"] == "clarify:v1:temporal:yes"
    assert req.metadata.get("flow_id") == req.request_id


def test_callback_temporal_no_decodes_to_no_text() -> None:
    mapper = _load_mapper_module()
    update = {
        "update_id": 102,
        "callback_query": {
            "id": "cb-2",
            "data": "clarify:v1:temporal:no",
            "from": {"id": 1002},
            "message": {"message_id": 78, "chat": {"id": 2003}},
        },
    }
    req = mapper.map_update_to_request(update, app_id="app", tenant_id="tenant", timezone="Europe/Moscow")
    assert req.text == "no"
    assert req.metadata["callback_query"]["data"] == "clarify:v1:temporal:no"
    assert req.metadata.get("flow_id") == req.request_id


def test_callback_temporal_edit_buttons_decode_to_same_text_flow() -> None:
    mapper = _load_mapper_module()
    cases = {
        "clarify:v1:temporal_edit:date": "дата",
        "clarify:v1:temporal_edit:time": "время",
        "clarify:v1:temporal_edit:duration": "длительность",
        "clarify:v1:temporal_edit:comment": "Комментарий",
        "clarify:v1:temporal_edit:cancel": "отмена",
    }
    for callback_data, expected_text in cases.items():
        update = {
            "update_id": 200,
            "callback_query": {
                "id": "cb-edit",
                "data": callback_data,
                "from": {"id": 1010},
                "message": {"message_id": 88, "chat": {"id": 2020}},
            },
        }
        req = mapper.map_update_to_request(update, app_id="app", tenant_id="tenant", timezone="Europe/Moscow")
        assert req.text == expected_text


def test_callback_temporal_comment_buttons_decode_to_safe_text() -> None:
    mapper = _load_mapper_module()
    cases = {
        "clarify:v1:temporal_comment:keep": "оставить без изменений",
        "clarify:v1:temporal_comment:clear": "очистить комментарий",
    }
    for callback_data, expected_text in cases.items():
        update = {
            "update_id": 201,
            "callback_query": {
                "id": "cb-comment-action",
                "data": callback_data,
                "from": {"id": 1011},
                "message": {"message_id": 89, "chat": {"id": 2021}},
            },
        }
        req = mapper.map_update_to_request(update, app_id="app", tenant_id="tenant", timezone="Europe/Moscow")
        assert req.text == expected_text


def test_callback_sync_conflict_buttons_decode_to_safe_text() -> None:
    mapper = _load_mapper_module()
    cases = {
        "sync_conflict:delete_db:sc-1": "удалить из базы",
        "sync_conflict:restore_calendar:sc-1": "восстановить в календаре",
        "sync_conflict:edit:sc-1": "изменить",
        "sync_conflict:skip:sc-1": "пропустить",
    }
    for callback_data, expected_text in cases.items():
        update = {
            "update_id": 202,
            "callback_query": {
                "id": "cb-sync-conflict",
                "data": callback_data,
                "from": {"id": 1012},
                "message": {"message_id": 90, "chat": {"id": 2022}},
            },
        }
        req = mapper.map_update_to_request(update, app_id="app", tenant_id="tenant", timezone="Europe/Moscow")
        assert req.text == expected_text


def test_callback_meeting_update_buttons_decode_to_safe_text() -> None:
    mapper = _load_mapper_module()
    cases = {
        "clarify:v1:meeting_update_edit:date": "дата",
        "clarify:v1:meeting_update_edit:time": "время",
        "clarify:v1:meeting_update_edit:duration": "длительность",
        "clarify:v1:meeting_update_edit:comment": "Комментарий",
        "clarify:v1:meeting_update_edit:date_time": "дата и время",
        "clarify:v1:meeting_update_edit:cancel": "отмена",
        "clarify:v1:meeting_update_confirm:yes": "yes",
    }
    for callback_data, expected_text in cases.items():
        update = {
            "update_id": 203,
            "callback_query": {
                "id": "cb-meeting-update",
                "data": callback_data,
                "from": {"id": 1013},
                "message": {"message_id": 91, "chat": {"id": 2023}},
            },
        }
        req = mapper.map_update_to_request(update, app_id="app", tenant_id="tenant", timezone="Europe/Moscow")
        assert req.text == expected_text


def test_callback_meeting_target_yes_decodes_to_yes_text() -> None:
    mapper = _load_mapper_module()
    update = {
        "update_id": 301,
        "callback_query": {
            "id": "cb-mt-1",
            "data": "clarify:v1:meeting_target:yes",
            "from": {"id": 2001},
            "message": {"message_id": 99, "chat": {"id": 3001}},
        },
    }
    req = mapper.map_update_to_request(update, app_id="app", tenant_id="tenant", timezone="Europe/Moscow")
    assert req.text == "yes"


def test_callback_meeting_target_no_decodes_to_no_text() -> None:
    mapper = _load_mapper_module()
    update = {
        "update_id": 302,
        "callback_query": {
            "id": "cb-mt-2",
            "data": "clarify:v1:meeting_target:no",
            "from": {"id": 2002},
            "message": {"message_id": 100, "chat": {"id": 3002}},
        },
    }
    req = mapper.map_update_to_request(update, app_id="app", tenant_id="tenant", timezone="Europe/Moscow")
    assert req.text == "no"


def test_callback_comment_confirm_yes_decodes_to_yes_text() -> None:
    mapper = _load_mapper_module()
    update = {
        "update_id": 401,
        "callback_query": {
            "id": "cb-comment-1",
            "data": "clarify:v1:meeting_comment_final:yes",
            "from": {"id": 2003},
            "message": {"message_id": 101, "chat": {"id": 3003}},
        },
    }
    req = mapper.map_update_to_request(update, app_id="app", tenant_id="tenant", timezone="Europe/Moscow")
    assert req.text == "yes"


def test_callback_task_create_buttons_decode_to_safe_text() -> None:
    mapper = _load_mapper_module()
    cases = {
        "clarify:v1:task_create:create": "создать",
        "clarify:v1:task_create:edit": "исправить",
        "clarify:v1:task_create:comment": "Комментарий",
        "clarify:v1:task_create:cancel": "не создавать",
        "clarify:v1:task_create_duplicate:yes": "создать ещё одну",
        "clarify:v1:task_create_duplicate:no": "не создавать",
    }
    for callback_data, expected_text in cases.items():
        update = {
            "update_id": 402,
            "callback_query": {
                "id": "cb-task-create",
                "data": callback_data,
                "from": {"id": 2004},
                "message": {"message_id": 102, "chat": {"id": 3004}},
            },
        }
        req = mapper.map_update_to_request(update, app_id="app", tenant_id="tenant", timezone="Europe/Moscow")
        assert req.text == expected_text


def test_callback_ambiguous_create_buttons_decode_to_safe_text() -> None:
    mapper = _load_mapper_module()
    cases = {
        "clarify:v1:ambiguous_create:meeting": "Событие в календаре",
        "clarify:v1:ambiguous_create:timeblock": "Блок времени",
        "clarify:v1:ambiguous_create:task": "Задачу",
    }
    for callback_data, expected_text in cases.items():
        update = {
            "update_id": 404,
            "callback_query": {
                "id": "cb-ambiguous-create",
                "data": callback_data,
                "from": {"id": 2004},
                "message": {"message_id": 102, "chat": {"id": 3004}},
            },
        }
        req = mapper.map_update_to_request(update, app_id="app", tenant_id="tenant", timezone="Europe/Moscow")
        assert req.text == expected_text

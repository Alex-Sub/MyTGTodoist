from __future__ import annotations

import importlib.util
import sys
import types
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict


ROOT = Path(__file__).resolve().parents[1]
APP_SRC = ROOT / "telegram-bot" / "app-integration" / "src"
RUNTIME_BRIDGE_PATH = APP_SRC / "integrations" / "telegram" / "runtime_bridge.py"


def _load_module(module_path: Path, module_name: str):
    preserved: Dict[str, Any] = {}
    for key in list(sys.modules.keys()):
        if key == "src" or key.startswith("src."):
            preserved[key] = sys.modules.pop(key)

    src_pkg = types.ModuleType("src")
    src_pkg.__path__ = [str(APP_SRC)]  # type: ignore[attr-defined]
    sys.modules["src"] = src_pkg

    spec = importlib.util.spec_from_file_location(module_name, module_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)

    for key in list(sys.modules.keys()):
        if key == "src" or key.startswith("src."):
            sys.modules.pop(key)
    sys.modules.update(preserved)
    return module


class _FakeResponse:
    def __init__(self, body: Dict[str, Any], status_code: int = 200) -> None:
        self._body = body
        self.status_code = status_code

    def json(self) -> Dict[str, Any]:
        return dict(self._body)


def _date_iso(days_from_today: int) -> str:
    return (date.today() + timedelta(days=days_from_today)).isoformat()


def test_task_create_duplicate_worker_response_becomes_persisted_clarification() -> None:
    runtime_bridge = _load_module(RUNTIME_BRIDGE_PATH, "app_integration_runtime_bridge_module_task_duplicate")
    upserts: list[dict[str, Any]] = []

    class _FakeLocalDb:
        def upsert_clarification_session(self, **kwargs):  # noqa: ANN003
            upserts.append(dict(kwargs))

    def _fake_direct_handler(_req):
        return {
            "outcome": "success",
            "needs_clarification": False,
            "rec": {"outcome": "skipped", "reason": "clarification_executable_guard"},
            "command": {
                "intent": "task.create",
                "title": "Купить молоко",
                "planned_at": "2026-05-18",
                "entities": {"title": "Купить молоко", "planned_at": "2026-05-18"},
            },
        }

    setattr(_fake_direct_handler, "_local_db", _FakeLocalDb())

    def _fake_post(url, json, timeout):  # noqa: ANN001
        _ = (url, json, timeout)
        return _FakeResponse(
            {
                "ok": False,
                "outcome": "needs_clarification",
                "needs_clarification": True,
                "missing_field": "task_create_duplicate_confirm",
                "clarifying_question": "Похоже, такая задача уже есть на 18.05.2026:\n№ 17 — Купить молоко\n\nСоздать ещё одну?",
                "debug": {
                    "duplicate_found": True,
                    "existing_task": {
                        "id": 17,
                        "title": "Купить молоко",
                        "planned_at": "2026-05-18T09:00:00Z",
                        "parent_task_id": None,
                    },
                },
            }
        )

    bridge = runtime_bridge.RuntimeBridge(
        worker_command_url="http://worker/runtime/command",
        direct_handler=_fake_direct_handler,
        http_post=_fake_post,
    )
    req = runtime_bridge.TelegramRuntimeRequest(
        request_id="req-task-dup-1",
        app_id="app",
        tenant_id="tenant",
        channel="telegram",
        user_id="u1",
        chat_id="c1",
        source_message_id="m1",
        text="создай задачу купить молоко завтра",
        timezone="Europe/Moscow",
        metadata={},
    )

    result = bridge.process_request(req)

    assert result.needs_clarification is True
    assert "Создать ещё одну?" in str(result.clarifying_question or "")
    raw = result.raw_runtime_payload if isinstance(result.raw_runtime_payload, dict) else {}
    assert raw.get("rec", {}).get("missing_field") == "task_create_duplicate_confirm"
    assert upserts
    stored_payload = upserts[-1]["payload"]
    assert stored_payload["duplicate_check_override"] is True
    assert stored_payload["entities"]["duplicate_check_override"] is True


def test_task_create_confirm_is_replaced_by_duplicate_warning_before_user_sees_normal_summary() -> None:
    runtime_bridge = _load_module(RUNTIME_BRIDGE_PATH, "app_integration_runtime_bridge_module_task_duplicate_precheck")
    upserts: list[dict[str, Any]] = []

    class _FakeLocalDb:
        def upsert_clarification_session(self, **kwargs):  # noqa: ANN003
            upserts.append(dict(kwargs))

    def _fake_direct_handler(_req):
        return {
            "outcome": "rec_needs_clarification",
            "needs_clarification": True,
            "clarifying_question": "Проверь задачу перед созданием:\nТекст: Купить молоко\nСрок: 2026-05-18\n\nСоздать задачу?",
            "rec": {"outcome": "needs_clarification", "needs_clarification": True, "missing_field": "task_create_confirm"},
            "command": {
                "intent": "task.create",
                "title": "Купить молоко",
                "planned_at": "2026-05-18",
                "entities": {"title": "Купить молоко", "planned_at": "2026-05-18"},
            },
        }

    setattr(_fake_direct_handler, "_local_db", _FakeLocalDb())
    calls: list[dict[str, Any]] = []

    def _fake_post(url, json, timeout):  # noqa: ANN001
        calls.append({"url": url, "json": dict(json), "timeout": timeout})
        return _FakeResponse(
            {
                "ok": True,
                "duplicate_found": True,
                "existing_task": {
                    "id": 17,
                    "title": "Купить молоко",
                    "planned_at": "2026-05-18T09:00:00Z",
                    "parent_task_id": None,
                },
            }
        )

    bridge = runtime_bridge.RuntimeBridge(
        worker_command_url="http://worker/runtime/command",
        direct_handler=_fake_direct_handler,
        http_post=_fake_post,
    )
    req = runtime_bridge.TelegramRuntimeRequest(
        request_id="req-task-dup-precheck-1",
        app_id="app",
        tenant_id="tenant",
        channel="telegram",
        user_id="u1",
        chat_id="c1",
        source_message_id="m1",
        text="создай задачу купить молоко завтра",
        timezone="Europe/Moscow",
        metadata={},
    )

    result = bridge.process_request(req)

    assert len(calls) == 1
    assert calls[0]["url"] == "http://worker/runtime/task/check_duplicate_create"
    assert calls[0]["json"]["title"] == "Купить молоко"
    assert calls[0]["json"]["planned_at"] == "2026-05-18"
    assert result.needs_clarification is True
    question = str(result.clarifying_question or "")
    assert "Похоже, такая задача уже есть" in question
    assert "Создать ещё одну?" in question
    assert "Создать задачу?" not in question
    raw = result.raw_runtime_payload if isinstance(result.raw_runtime_payload, dict) else {}
    assert raw.get("rec", {}).get("missing_field") == "task_create_duplicate_confirm"
    assert upserts


def test_task_create_due_date_is_canonicalized_to_planned_at_in_worker_payload() -> None:
    runtime_bridge = _load_module(RUNTIME_BRIDGE_PATH, "app_integration_runtime_bridge_module_task_due_date")
    posts: list[dict[str, Any]] = []

    def _fake_direct_handler(_req):
        return {
            "outcome": "success",
            "needs_clarification": False,
            "rec": {"outcome": "skipped", "reason": "clarification_executable_guard"},
            "command": {
                "intent": "task.create",
                "title": "Купить молоко",
                "due_date": "2026-05-18",
                "entities": {
                    "text": "создай задачу купить молоко завтра",
                    "title": "Купить молоко",
                    "due_date": "2026-05-18",
                },
            },
        }

    def _fake_post(url, json, timeout):  # noqa: ANN001
        posts.append({"url": url, "json": dict(json), "timeout": timeout})
        return _FakeResponse({"ok": True, "user_message": "created", "task_id": 48})

    bridge = runtime_bridge.RuntimeBridge(
        worker_command_url="http://worker/runtime/command",
        direct_handler=_fake_direct_handler,
        http_post=_fake_post,
    )
    req = runtime_bridge.TelegramRuntimeRequest(
        request_id="req-task-due-date-1",
        app_id="app",
        tenant_id="tenant",
        channel="telegram",
        user_id="u1",
        chat_id="c1",
        source_message_id="m1",
        text="создай задачу купить молоко завтра",
        timezone="Europe/Moscow",
        metadata={},
    )

    result = bridge.process_request(req)

    assert result.needs_clarification is False
    assert len(posts) == 1
    assert posts[0]["url"] == "http://worker/runtime/command"
    entities = posts[0]["json"]["command"]["entities"]
    assert entities["due_date"] == "2026-05-18"
    assert entities["planned_at"] == "2026-05-18"


def test_task_create_undated_duplicate_precheck_replaces_normal_summary() -> None:
    runtime_bridge = _load_module(RUNTIME_BRIDGE_PATH, "app_integration_runtime_bridge_module_task_duplicate_undated")
    upserts: list[dict[str, Any]] = []

    class _FakeLocalDb:
        def upsert_clarification_session(self, **kwargs):  # noqa: ANN003
            upserts.append(dict(kwargs))

    def _fake_direct_handler(_req):
        return {
            "outcome": "rec_needs_clarification",
            "needs_clarification": True,
            "clarifying_question": "Проверь задачу перед созданием:\nТекст: Позвонить врачу\nСрок: -\n\nСоздать задачу?",
            "rec": {"outcome": "needs_clarification", "needs_clarification": True, "missing_field": "task_create_confirm"},
            "command": {
                "intent": "task.create",
                "title": "Позвонить врачу",
                "planned_at": None,
                "entities": {"title": "Позвонить врачу", "planned_at": None},
            },
        }

    setattr(_fake_direct_handler, "_local_db", _FakeLocalDb())
    calls: list[dict[str, Any]] = []

    def _fake_post(url, json, timeout):  # noqa: ANN001
        calls.append({"url": url, "json": dict(json), "timeout": timeout})
        return _FakeResponse(
            {
                "ok": True,
                "duplicate_found": True,
                "planned_day": None,
                "undated_scope": True,
                "existing_task": {
                    "id": 17,
                    "title": "Позвонить врачу",
                    "planned_at": None,
                    "parent_task_id": None,
                },
            }
        )

    bridge = runtime_bridge.RuntimeBridge(
        worker_command_url="http://worker/runtime/command",
        direct_handler=_fake_direct_handler,
        http_post=_fake_post,
    )
    req = runtime_bridge.TelegramRuntimeRequest(
        request_id="req-task-dup-undated-1",
        app_id="app",
        tenant_id="tenant",
        channel="telegram",
        user_id="u1",
        chat_id="c1",
        source_message_id="m1",
        text="позвонить врачу",
        timezone="Europe/Moscow",
        metadata={},
    )

    result = bridge.process_request(req)

    assert len(calls) == 1
    assert calls[0]["json"]["planned_at"] is None
    assert result.needs_clarification is True
    question = str(result.clarifying_question or "")
    assert "Такая задача уже есть в InBox" in question
    assert "Создать задачу?" not in question
    raw = result.raw_runtime_payload if isinstance(result.raw_runtime_payload, dict) else {}
    assert raw.get("rec", {}).get("missing_field") == "task_create_duplicate_confirm"
    assert upserts


def test_task_create_relative_due_date_is_canonicalized_to_planned_at_in_worker_payload() -> None:
    runtime_bridge = _load_module(RUNTIME_BRIDGE_PATH, "app_integration_runtime_bridge_module_task_due_date_relative")
    posts: list[dict[str, Any]] = []

    def _fake_direct_handler(_req):
        return {
            "outcome": "success",
            "needs_clarification": False,
            "rec": {"outcome": "skipped", "reason": "clarification_executable_guard"},
            "command": {
                "intent": "task.create",
                "title": "Купить молоко",
                "due_date": "завтра",
                "entities": {
                    "text": "создай задачу купить молоко завтра",
                    "title": "Купить молоко",
                    "due_date": "завтра",
                },
            },
        }

    def _fake_post(url, json, timeout):  # noqa: ANN001
        posts.append({"url": url, "json": dict(json), "timeout": timeout})
        return _FakeResponse({"ok": True, "user_message": "created", "task_id": 48})

    bridge = runtime_bridge.RuntimeBridge(
        worker_command_url="http://worker/runtime/command",
        direct_handler=_fake_direct_handler,
        http_post=_fake_post,
    )
    req = runtime_bridge.TelegramRuntimeRequest(
        request_id="req-task-due-date-relative-1",
        app_id="app",
        tenant_id="tenant",
        channel="telegram",
        user_id="u1",
        chat_id="c1",
        source_message_id="m1",
        text="создай задачу купить молоко завтра",
        timezone="Europe/Moscow",
        metadata={},
    )

    result = bridge.process_request(req)

    assert result.needs_clarification is False
    assert len(posts) == 1
    entities = posts[0]["json"]["command"]["entities"]
    assert entities["due_date"] == _date_iso(1)
    assert entities["planned_at"] == _date_iso(1)

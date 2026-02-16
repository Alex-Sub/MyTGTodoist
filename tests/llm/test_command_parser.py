import json
from collections import deque
from typing import Any, Deque

import pytest
from jsonschema import ValidationError

from organizer_worker.llm import router


def _base_payload(intent: str) -> dict[str, Any]:
    return {
        "intent": intent,
        "confidence": 0.9,
        "text_original": "orig",
        "text_normalized": "norm",
        "datetime_context": {
            "now_iso": "2026-02-08T12:00:00+03:00",
            "timezone": "Europe/Moscow",
        },
        "entities": {},
        "needs_clarification": False,
        "clarifying_question": None,
        "notes": None,
    }


def _mock_chat(monkeypatch, responses: list[str], captured: list | None = None) -> None:
    queue: Deque[str] = deque(responses)

    def fake_chat(self, messages, model):
        if captured is not None:
            captured.append({"messages": messages, "model": model})
        if not queue:
            raise AssertionError("No more mock responses")
        return queue.popleft()

    monkeypatch.setattr(router.OpenRouterProvider, "chat", fake_chat)


def _mock_fewshot(monkeypatch) -> list[dict[str, str]]:
    fewshot = [
        {"role": "user", "content": "EXAMPLE_USER"},
        {"role": "assistant", "content": "{\"intent\":\"list_tasks\"}"},
    ]
    monkeypatch.setattr(router, "_load_command_examples", lambda: fewshot)
    return fewshot


def test_command_parser_valid_json(monkeypatch):
    payload = _base_payload("create_task")
    payload["entities"] = {
        "title": "Buy milk",
        "project": None,
        "due_iso": None,
        "priority": None,
        "labels": [],
        "reminder_iso": None,
    }
    _mock_fewshot(monkeypatch)
    captured: list[dict[str, Any]] = []
    _mock_chat(monkeypatch, [json.dumps(payload)], captured)

    result = router.route_llm(
        router.LLMRequest(kind="voice_command", text="buy milk", now_iso=payload["datetime_context"]["now_iso"])
    )

    assert result.validation_ok is True
    assert result.payload is not None
    assert result.payload["intent"] == "create_task"
    assert captured
    assert any(msg["content"] == "EXAMPLE_USER" for msg in captured[0]["messages"])
    assert len(captured[0]["messages"]) > 2


def test_command_parser_retry_then_valid(monkeypatch):
    payload = _base_payload("list_tasks")
    payload["entities"] = {"filter": "today", "value": None}

    _mock_fewshot(monkeypatch)
    captured: list[dict[str, Any]] = []
    _mock_chat(monkeypatch, ["{not json", json.dumps(payload)], captured)

    result = router.route_llm(
        router.LLMRequest(kind="text_command", text="list today", now_iso=payload["datetime_context"]["now_iso"])
    )

    assert result.validation_ok is True
    assert result.payload is not None
    assert result.payload["intent"] == "list_tasks"
    assert captured
    assert captured[0]["messages"][0]["role"] == "system"
    assert captured[1]["messages"][0]["content"] == router.STRICT_JSON_PROMPT


def test_command_parser_retry_then_invalid(monkeypatch):
    _mock_fewshot(monkeypatch)
    _mock_chat(monkeypatch, ["{not json", "also not json"])

    result = router.route_llm(router.LLMRequest(kind="voice_command", text="bad", now_iso=None))

    assert result.validation_ok is False
    assert result.payload is None
    assert result.error


def test_needs_clarification_requires_question(monkeypatch):
    payload = _base_payload("create_task")
    payload["needs_clarification"] = True
    payload["clarifying_question"] = None
    payload["entities"] = {"title": "Call mom"}

    _mock_chat(monkeypatch, [json.dumps(payload), json.dumps(payload)])

    result = router.route_llm(router.LLMRequest(kind="voice_command", text="call mom", now_iso=None))

    assert result.validation_ok is False
    assert result.error


# Schema contract tests

def _load_schema() -> dict[str, Any]:
    return router._load_schema()


def test_schema_create_task_minimal_valid():
    payload = _base_payload("create_task")
    payload["entities"] = {
        "title": "Buy milk",
        "project": None,
        "due_iso": None,
        "priority": None,
        "labels": [],
        "reminder_iso": None,
    }

    schema = _load_schema()
    import jsonschema

    jsonschema.validate(instance=payload, schema=schema)


def test_schema_create_task_missing_title_fails():
    payload = _base_payload("create_task")
    payload["entities"] = {"project": None}

    schema = _load_schema()
    import jsonschema

    with pytest.raises(ValidationError):
        jsonschema.validate(instance=payload, schema=schema)


def test_schema_list_tasks_today_valid():
    payload = _base_payload("list_tasks")
    payload["entities"] = {"filter": "today", "value": None}

    schema = _load_schema()
    import jsonschema

    jsonschema.validate(instance=payload, schema=schema)


def test_schema_unknown_intent_requires_clarification():
    payload = _base_payload("unknown")
    payload["needs_clarification"] = False
    payload["clarifying_question"] = None

    schema = _load_schema()
    import jsonschema

    with pytest.raises(ValidationError):
        jsonschema.validate(instance=payload, schema=schema)


def test_schema_unknown_intent_valid():
    payload = _base_payload("unknown")
    payload["needs_clarification"] = True
    payload["clarifying_question"] = "Уточните запрос."

    schema = _load_schema()
    import jsonschema

    jsonschema.validate(instance=payload, schema=schema)

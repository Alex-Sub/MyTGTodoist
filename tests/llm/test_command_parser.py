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


def test_voice_meeting_intent_create_event_normalized_to_timeblock_create(monkeypatch):
    payload = _base_payload("create_event")
    payload["text_original"] = "Запланируй завтра на 9 утра собрание"
    payload["text_normalized"] = "запланировать собрание завтра на 09:00"
    payload["entities"] = {
        "title": "Собрание",
        "start_at": "2026-02-09T09:00:00+03:00",
        "duration_minutes": None,
    }
    payload["needs_clarification"] = True
    payload["clarifying_question"] = "На сколько минут поставить блок?"

    _mock_fewshot(monkeypatch)
    _mock_chat(monkeypatch, [json.dumps(payload)])

    result = router.route_llm(
        router.LLMRequest(
            kind="voice_command",
            text="Запланируй завтра на 9 утра собрание",
            now_iso=payload["datetime_context"]["now_iso"],
        )
    )

    assert result.validation_ok is True
    assert result.payload is not None
    assert result.payload["intent"] == "timeblock_create"
    assert result.payload["needs_clarification"] is True
    assert result.payload["clarifying_question"] == "На сколько минут поставить блок?"


def test_needs_clarification_requires_question(monkeypatch):
    payload = _base_payload("create_task")
    payload["needs_clarification"] = True
    payload["clarifying_question"] = None
    payload["entities"] = {"title": "Call mom"}

    _mock_chat(monkeypatch, [json.dumps(payload), json.dumps(payload)])

    result = router.route_llm(router.LLMRequest(kind="voice_command", text="call mom", now_iso=None))

    assert result.validation_ok is False
    assert result.error


def test_interpret_ambiguous_call_tomorrow_returns_clarify_choices(monkeypatch):
    payload = {
        "type": "clarify",
        "clarify": {
            "clarifying_question": "Что выбрать: создать блок времени или создать задачу?",
            "choices": [
                {"id": "timeblock_create", "title": "создать блок времени"},
                {"id": "task_create", "title": "создать задачу"},
            ],
            "draft_envelope": {
                "trace_id": "tr-amb",
                "source": {"channel": "llm_gateway"},
                "command": {
                    "intent": "timeblock_create",
                    "confidence": 0.6,
                    "entities": {"title": "созвон завтра", "start_at": None, "duration_minutes": None},
                },
            },
        },
    }
    _mock_chat(monkeypatch, [json.dumps(payload)])

    res = router.interpret("созвонись завтра", now_iso="2026-02-08T12:00:00+03:00")
    assert res.type == "clarify"
    assert res.clarifying_question == "Что выбрать: создать блок времени или создать задачу?"
    assert res.expected_answer == "choice_id"
    assert isinstance(res.choices, list)
    ids = {c.id for c in res.choices}
    assert ids == {"task_create", "timeblock_create"}


def test_interpret_meeting_with_time_and_duration_returns_timeblock_command(monkeypatch):
    payload = {
        "type": "command",
        "command": {
            "trace_id": "tr-1",
            "source": {"channel": "llm_gateway"},
            "command": {
                "intent": "timeblock_create",
                "confidence": 0.92,
                "entities": {
                    "title": "Встреча",
                    "start_at": "2026-02-09T10:00:00+03:00",
                    "duration_minutes": 30,
                    "end_at": "",
                },
            },
        },
    }

    _mock_chat(monkeypatch, [json.dumps(payload)])

    res = router.interpret(
        "поставь встречу завтра в 10 на 30 минут",
        now_iso="2026-02-08T12:00:00+03:00",
    )
    assert res.type == "command"
    assert res.envelope is not None
    env = res.envelope.to_dict()
    assert env["command"]["intent"] == "timeblock_create"
    assert env["command"]["entities"]["start_at"] == "2026-02-09T10:00:00+03:00"
    assert env["command"]["entities"]["duration_minutes"] == 30
    assert env["command"]["entities"]["end_at"] is None


def test_interpret_buy_milk_returns_task_create_command(monkeypatch):
    payload = {
        "type": "command",
        "command": {
            "trace_id": "tr-2",
            "source": {"channel": "llm_gateway"},
            "command": {
                "intent": "task_create",
                "confidence": 0.95,
                "entities": {
                    "title": "Купить молоко",
                    "planned_at": None,
                    "start_at": None,
                    "duration_minutes": None,
                },
            },
        },
    }

    _mock_chat(monkeypatch, [json.dumps(payload)])

    res = router.interpret("купи молоко", now_iso="2026-02-08T12:00:00+03:00")
    assert res.type == "command"
    assert res.envelope is not None
    env = res.envelope.to_dict()
    assert env["command"]["intent"] == "task_create"
    assert env["command"]["entities"]["title"] == "Купить молоко"


def test_interpret_unknown_intent_forced_to_clarify(monkeypatch):
    payload = {
        "type": "command",
        "command": {
            "trace_id": "tr-3",
            "source": {"channel": "llm_gateway"},
            "command": {"intent": "calendar_delete", "confidence": 0.8, "entities": {"title": "x"}},
        },
    }
    _mock_chat(monkeypatch, [json.dumps(payload)])
    res = router.interpret("удали встречу", now_iso="2026-02-08T12:00:00+03:00")
    assert res.type == "clarify"
    assert res.clarifying_question == "Не понял команду. Это задача или блок времени?"
    ids = {c.id for c in res.choices}
    assert ids == {"task_create", "timeblock_create"}


def test_interpret_invalid_json_fallback_to_clarify(monkeypatch):
    _mock_chat(monkeypatch, ["{not json", "{still bad"])
    res = router.interpret("созвонись завтра", now_iso="2026-02-08T12:00:00+03:00")
    assert res.type == "clarify"
    assert res.clarifying_question == "Не понял команду. Это задача или блок времени?"


def test_build_prompt_contains_timezone_and_allowed_intents():
    system_prompt, user_prompt = router.build_prompt(
        "купи молоко",
        "2026-02-08T12:00:00+03:00",
        "Europe/Moscow",
        ["task_create", "timeblock_create"],
    )
    assert "интерпретатор команд" in system_prompt
    payload = json.loads(user_prompt)
    assert payload["timezone"] == "Europe/Moscow"
    assert payload["allowed_intents"] == ["task_create", "timeblock_create"]


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

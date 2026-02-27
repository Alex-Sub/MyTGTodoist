from __future__ import annotations

from src.llm import router as ml_router


def _llm_ok(payload: dict):
    return ml_router.LLMResult(
        agent="command_parser",
        provider="test",
        model="test",
        payload=payload,
        raw_text="{}",
        validation_ok=True,
        error=None,
    )


def test_organizer_profile_unsupported_intent_returns_task_or_meeting_clarify(monkeypatch) -> None:
    monkeypatch.setattr(
        ml_router,
        "route_llm",
        lambda _req: _llm_ok({"intent": "goal.create", "entities": {}}),
    )

    res = ml_router.interpret("создай цель на неделю")
    assert res.type == "clarify"
    assert res.question == "Не понял команду. Это задача или встреча?"
    assert isinstance(res.choices, list)
    ids = {c["id"] for c in (res.choices or [])}
    assert ids == {"task.create", "timeblock.create"}


def test_organizer_profile_meeting_with_time_maps_to_timeblock_create(monkeypatch) -> None:
    monkeypatch.setattr(
        ml_router,
        "route_llm",
        lambda _req: _llm_ok(
            {
                "intent": "create_event",
                "entities": {
                    "title": "Собрание",
                    "start_at": "2026-02-28T08:00:00+03:00",
                    "duration_minutes": 60,
                },
            }
        ),
    )

    res = ml_router.interpret("запланируй собрание завтра в 8 утра")
    assert res.type == "command"
    assert isinstance(res.command, dict)
    assert res.command["intent"] == "timeblock.create"


def test_action_text_without_list_prefix_maps_to_task_create_with_planned_at(monkeypatch) -> None:
    def _should_not_call_llm(_req):
        raise AssertionError("LLM must not be called for rule-based action text")

    monkeypatch.setattr(ml_router, "route_llm", _should_not_call_llm)

    res = ml_router.interpret("купить молоко завтра", now_iso="2026-02-27T12:00:00+03:00")
    assert res.type == "command"
    assert isinstance(res.command, dict)
    assert res.command["intent"] == "task.create"
    assert res.command["args"]["title"] == "молоко завтра"
    assert str(res.command["args"].get("planned_at") or "") != ""


def test_list_prefix_maps_to_tasks_list_active_and_not_task_create(monkeypatch) -> None:
    def _should_not_call_llm(_req):
        raise AssertionError("LLM must not be called for list prefix")

    monkeypatch.setattr(ml_router, "route_llm", _should_not_call_llm)

    res = ml_router.interpret("список активных задач", now_iso="2026-02-27T12:00:00+03:00")
    assert res.type == "command"
    assert isinstance(res.command, dict)
    assert res.command["intent"] == "tasks.list_active"
    assert res.command["intent"] != "task.create"

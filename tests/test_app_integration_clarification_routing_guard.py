from __future__ import annotations

import importlib.util
import sys
import types
from datetime import date, timedelta
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
APP_SRC = ROOT / "telegram-bot" / "app-integration" / "src"
HANDLER_PATH = APP_SRC / "app" / "handler.py"
UPDATE_MAPPER_PATH = APP_SRC / "integrations" / "telegram" / "update_mapper.py"


def _date_iso(days_from_today: int) -> str:
    return (date.today() + timedelta(days=days_from_today)).isoformat()


def _dt_iso(days_from_today: int, hour: int) -> str:
    return f"{_date_iso(days_from_today)}T{hour:02d}:00:00+03:00"


def _load_handler_module():
    preserved: dict[str, Any] = {}
    for key in list(sys.modules.keys()):
        if key == "src" or key.startswith("src."):
            preserved[key] = sys.modules.pop(key)

    src_pkg = types.ModuleType("src")
    src_pkg.__path__ = [str(APP_SRC)]  # type: ignore[attr-defined]
    sys.modules["src"] = src_pkg

    spec = importlib.util.spec_from_file_location("app_integration_handler_module", HANDLER_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    for key in list(sys.modules.keys()):
        if key == "src" or key.startswith("src."):
            sys.modules.pop(key)
    sys.modules.update(preserved)
    return module


def _load_update_mapper_module():
    preserved: dict[str, Any] = {}
    for key in list(sys.modules.keys()):
        if key == "src" or key.startswith("src."):
            preserved[key] = sys.modules.pop(key)

    src_pkg = types.ModuleType("src")
    src_pkg.__path__ = [str(APP_SRC)]  # type: ignore[attr-defined]
    sys.modules["src"] = src_pkg

    spec = importlib.util.spec_from_file_location("app_integration_update_mapper_for_handler_tests", UPDATE_MAPPER_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    for key in list(sys.modules.keys()):
        if key == "src" or key.startswith("src."):
            sys.modules.pop(key)
    sys.modules.update(preserved)
    return module


class _NeverCalledLLM:
    def parse(self, text: str) -> dict:  # noqa: ARG002
        raise AssertionError("LLM must not be called for clarification continuation")


class _NoopAsr:
    def transcribe(self, audio_bytes: bytes, request_id: str | None = None) -> str:  # noqa: ARG002
        raise AssertionError("ASR must not be called for text clarification flow")


class _RecProbe:
    def __init__(self, should_fail_on_call: bool = True) -> None:
        self.calls = 0
        self.should_fail_on_call = should_fail_on_call

    def query(self, payload: dict) -> dict:  # noqa: ARG002
        self.calls += 1
        if self.should_fail_on_call:
            raise AssertionError("REC must not be called in this scenario")
        return {"outcome": "ok", "hits": [{"id": "x-1", "type": "task"}]}


class _StaticLLM:
    def __init__(self, payload: dict) -> None:
        self.payload = payload

    def parse(self, text: str) -> dict:  # noqa: ARG002
        return dict(self.payload)


class _FakeDb:
    def __init__(self, session: dict | None) -> None:
        self.session = session
        self.deleted_contexts: list[str] = []
        self.upserts: list[dict] = []
        self.finalized: list[dict] = []

    def get_active_clarification_session(self, context_key: str, ttl_seconds: int) -> dict | None:  # noqa: ARG002
        return self.session

    def upsert_clarification_session(self, **kwargs) -> None:
        self.upserts.append(kwargs)

    def delete_clarification_session(self, context_key: str) -> None:
        self.deleted_contexts.append(context_key)

    def get_command_dedup(self, idempotency_key: str) -> dict | None:  # noqa: ARG002
        return None

    def reserve_command_dedup(self, idempotency_key: str, intent: str) -> bool:  # noqa: ARG002
        return True

    def delete_command_dedup(self, idempotency_key: str) -> None:  # noqa: ARG002
        return None

    def finalize_command_dedup(self, **kwargs) -> None:
        self.finalized.append(kwargs)

    def is_command_dedup_stale(self, existing: dict, ttl_seconds: int) -> bool:  # noqa: ARG002
        return False

    def reclaim_command_dedup(self, idempotency_key: str, intent: str) -> bool:  # noqa: ARG002
        return False


def test_clarification_goes_directly_to_execution_when_command_complete() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(
        session={
            "intent": "timeblock.create",
            "missing_field": "duration_minutes",
            "idempotency_key": "idem-1",
            "payload": {
                "intent": "timeblock.create",
                "start_at": _dt_iso(1, 10),
                "start_at_date": _date_iso(1),
            },
        }
    )

    result = handler.handle_user_attempt(
        request_id="r1",
        user_id="u1",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="45",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-1",
        source_message_id="msg-1",
    )

    assert result["outcome"] == "success"
    assert result["rec"]["outcome"] == "skipped"
    assert result["command"]["duration_minutes"] == 45
    assert rec.calls == 0
    assert db.deleted_contexts


def test_schedule_call_without_duration_uses_default_and_shows_summary() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(session=None)
    llm = _StaticLLM(
        {
            "intent": "schedule_call",
            "entities": {
                "date": _date_iso(1),
                "time": "15:00",
                "text": "запланируй созвон с Иваном завтра в 15",
            },
        }
    )

    result = handler.handle_user_attempt(
        request_id="r-schedule-call-duration",
        user_id="u-schedule-call-duration",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=llm,
        rec_client=rec,
        text="запланируй созвон с Иваном завтра в 15",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-schedule-call-duration",
        source_message_id="msg-schedule-call-duration",
    )

    assert result["outcome"] == "rec_needs_clarification"
    assert result["rec"]["missing_field"] == "temporal_commit_confirm"
    question = str(result.get("clarifying_question") or "")
    assert "Тип: созвон" in question
    assert "Длительность: 30 минут" in question
    assert str(result.get("command", {}).get("intent") or "") == "meeting.create"
    entities = result.get("command", {}).get("entities") if isinstance(result.get("command"), dict) else {}
    entities = entities if isinstance(entities, dict) else {}
    assert str(entities.get("meeting_kind") or "") == "созвон"
    assert int(entities.get("duration_minutes") or 0) == 30
    assert rec.calls == 0


def test_schedule_meeting_without_duration_uses_default_and_shows_summary() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(session=None)
    llm = _StaticLLM(
        {
            "intent": "schedule_meeting",
            "entities": {
                "date": _date_iso(1),
                "time": "14",
                "text": "запланируй встречу завтра в 14",
            },
        }
    )

    result = handler.handle_user_attempt(
        request_id="r-schedule-meeting-duration",
        user_id="u-schedule-meeting-duration",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=llm,
        rec_client=rec,
        text="запланируй встречу завтра в 14",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-schedule-meeting-duration",
        source_message_id="msg-schedule-meeting-duration",
    )

    assert result["outcome"] == "rec_needs_clarification"
    assert result["rec"]["missing_field"] == "temporal_commit_confirm"
    question = str(result.get("clarifying_question") or "")
    assert "Время: 14:00" in question
    assert "Длительность: 30 минут" in question
    assert "На сколько минут" not in question
    assert str(result.get("command", {}).get("intent") or "") == "meeting.create"
    assert int(result.get("command", {}).get("duration_minutes") or 0) == 30
    assert rec.calls == 0


def test_temporal_edit_duration_numeric_input_updates_only_duration() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    future_date = _date_iso(2)
    base_payload = {
        "intent": "meeting.create",
        "start_at": f"{future_date} 14:00",
        "start_at_date": future_date,
        "start_at_time": "14:00",
        "duration_minutes": 60,
        "__source_text": f"назначь встречу {future_date} в 14:00 на 60 минут",
        "entities": {
            "text": f"назначь встречу {future_date} в 14:00 на 60 минут",
            "start_at_date": future_date,
            "start_at_time": "14:00",
            "duration_minutes": 60,
        },
    }
    db = _FakeDb(
        session={
            "intent": "meeting.create",
            "missing_field": "temporal_edit_field",
            "idempotency_key": "idem-temporal-edit-duration-active",
            "payload": dict(base_payload),
        }
    )

    pick = handler.handle_user_attempt(
        request_id="r-temporal-edit-duration-pick",
        user_id="u-temporal-edit-duration",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="длительность",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-temporal-edit-duration",
        source_message_id="msg-temporal-edit-duration-pick",
    )
    assert pick["outcome"] == "rec_needs_clarification"
    assert pick["rec"]["missing_field"] == "duration_minutes"

    db.session = {
        "intent": "meeting.create",
        "missing_field": "duration_minutes",
        "idempotency_key": "idem-temporal-edit-duration-active",
        "payload": dict(pick["command"]),
    }
    updated = handler.handle_user_attempt(
        request_id="r-temporal-edit-duration-apply",
        user_id="u-temporal-edit-duration",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="15",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-temporal-edit-duration",
        source_message_id="msg-temporal-edit-duration-apply",
    )
    assert updated["outcome"] == "rec_needs_clarification"
    assert updated["rec"]["missing_field"] == "temporal_commit_confirm"
    cmd = updated["command"]
    assert int(cmd.get("duration_minutes") or 0) == 15
    assert str(cmd.get("start_at_time") or "") == "14:00"
    question = str(updated.get("clarifying_question") or "")
    assert "Время: 14:00" in question
    assert "Длительность: 15 минут" in question
    assert rec.calls == 0


def test_temporal_confirm_yes_with_active_session_routes_to_execution() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(
        session={
            "intent": "timeblock.create",
            "missing_field": "temporal_commit_confirm",
            "idempotency_key": "idem-temporal-confirm-yes",
            "payload": {
                "intent": "timeblock.create",
                "start_at": _dt_iso(1, 14),
                "start_at_date": _date_iso(1),
                "start_at_time": "14:00",
                "duration_minutes": 60,
                "__source_text": f"запланируй встречу {_date_iso(1)} в 14:00 на 60 минут",
                "entities": {
                    "text": f"запланируй встречу {_date_iso(1)} в 14:00 на 60 минут",
                    "start_at_date": _date_iso(1),
                    "start_at_time": "14:00",
                    "duration_minutes": 60,
                },
            },
        }
    )

    result = handler.handle_user_attempt(
        request_id="r-temporal-confirm-yes",
        user_id="u-temporal-confirm-yes",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="yes",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-temporal-confirm-yes",
        source_message_id="msg-temporal-confirm-yes",
    )

    assert result["outcome"] == "success"
    assert result["rec"]["outcome"] == "skipped"
    assert bool(result["command"].get("__temporal_confirmed")) is True
    assert rec.calls == 0
    assert db.deleted_contexts


def test_temporal_confirm_callback_yes_routes_to_execution() -> None:
    handler = _load_handler_module()
    mapper = _load_update_mapper_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(
        session={
            "intent": "timeblock.create",
            "missing_field": "temporal_commit_confirm",
            "idempotency_key": "idem-temporal-confirm-callback-yes",
            "payload": {
                "intent": "timeblock.create",
                "start_at": _dt_iso(1, 14),
                "start_at_date": _date_iso(1),
                "start_at_time": "14:00",
                "duration_minutes": 60,
                "__source_text": f"назначь встречу {_date_iso(1)} в 14:00 на 60 минут",
                "entities": {
                    "text": f"назначь встречу {_date_iso(1)} в 14:00 на 60 минут",
                    "start_at_date": _date_iso(1),
                    "start_at_time": "14:00",
                    "duration_minutes": 60,
                },
            },
        }
    )
    update = {
        "update_id": 901,
        "callback_query": {
            "id": "cb-temporal-yes",
            "data": "clarify:v1:temporal:yes",
            "from": {"id": 5001},
            "message": {"message_id": 777, "chat": {"id": 9001}},
        },
    }
    req = mapper.map_update_to_request(update, app_id="app", tenant_id="tenant", timezone="Europe/Moscow")
    assert req.text == "yes"

    result = handler.handle_user_attempt(
        request_id=req.request_id,
        user_id="u-temporal-confirm-callback-yes",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text=req.text,
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-temporal-confirm-callback-yes",
        source_message_id="msg-temporal-confirm-callback-yes",
    )
    assert result["outcome"] == "success"
    assert result["rec"]["outcome"] == "skipped"
    assert "уточните, какую команду выполнить" not in str(result.get("user_message") or "").lower()
    assert rec.calls == 0


def test_temporal_confirm_callback_no_routes_to_edit_flow() -> None:
    handler = _load_handler_module()
    mapper = _load_update_mapper_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(
        session={
            "intent": "timeblock.create",
            "missing_field": "temporal_commit_confirm",
            "idempotency_key": "idem-temporal-confirm-callback-no",
            "payload": {
                "intent": "timeblock.create",
                "start_at": _dt_iso(1, 14),
                "start_at_date": _date_iso(1),
                "start_at_time": "14:00",
                "duration_minutes": 60,
                "__source_text": f"назначь встречу {_date_iso(1)} в 14:00 на 60 минут",
                "entities": {
                    "text": f"назначь встречу {_date_iso(1)} в 14:00 на 60 минут",
                    "start_at_date": _date_iso(1),
                    "start_at_time": "14:00",
                    "duration_minutes": 60,
                },
            },
        }
    )
    update = {
        "update_id": 902,
        "callback_query": {
            "id": "cb-temporal-no",
            "data": "clarify:v1:temporal:no",
            "from": {"id": 5002},
            "message": {"message_id": 778, "chat": {"id": 9002}},
        },
    }
    req = mapper.map_update_to_request(update, app_id="app", tenant_id="tenant", timezone="Europe/Moscow")
    assert req.text == "no"

    result = handler.handle_user_attempt(
        request_id=req.request_id,
        user_id="u-temporal-confirm-callback-no",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text=req.text,
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-temporal-confirm-callback-no",
        source_message_id="msg-temporal-confirm-callback-no",
    )
    assert result["outcome"] == "rec_needs_clarification"
    assert result["rec"]["missing_field"] == "temporal_edit_field"
    assert "Что исправить" in str(result.get("clarifying_question") or "")
    assert rec.calls == 0


def test_temporal_confirm_no_keeps_draft_and_moves_to_edit_field() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    payload = {
        "intent": "timeblock.create",
        "start_at": _dt_iso(1, 9),
        "start_at_date": _date_iso(1),
        "duration_minutes": 30,
        "entities": {"text": "запланируй блок"},
    }
    db = _FakeDb(
        session={
            "intent": "timeblock.create",
            "missing_field": "temporal_commit_confirm",
            "idempotency_key": "idem-temporal-confirm-no",
            "payload": dict(payload),
        }
    )

    result = handler.handle_user_attempt(
        request_id="r-temporal-confirm-no",
        user_id="u-temporal-confirm-no",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="no",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-temporal-confirm-no",
        source_message_id="msg-temporal-confirm-no",
    )

    assert result["outcome"] == "rec_needs_clarification"
    assert result["rec"]["missing_field"] == "temporal_edit_field"
    assert "Что исправить" in str(result.get("clarifying_question") or "")
    assert rec.calls == 0
    assert db.upserts
    assert db.upserts[-1]["missing_field"] == "temporal_edit_field"
    persisted_payload = db.upserts[-1]["payload"]
    assert persisted_payload["start_at"] == payload["start_at"]
    assert persisted_payload["duration_minutes"] == payload["duration_minutes"]


def test_temporal_confirm_allows_duration_override_and_redisplays_summary() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    payload = {
        "intent": "meeting.create",
        "start_at": _dt_iso(1, 15),
        "start_at_date": _date_iso(1),
        "start_at_time": "15:00",
        "duration_minutes": 30,
        "meeting_kind": "созвон",
        "entities": {
            "text": "запланируй созвон завтра в 15",
            "start_at_date": _date_iso(1),
            "start_at_time": "15:00",
            "duration_minutes": 30,
            "meeting_kind": "созвон",
        },
    }
    db = _FakeDb(
        session={
            "intent": "meeting.create",
            "missing_field": "temporal_commit_confirm",
            "idempotency_key": "idem-temporal-duration-override",
            "payload": dict(payload),
        }
    )

    result = handler.handle_user_attempt(
        request_id="r-temporal-duration-override",
        user_id="u-temporal-duration-override",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="60",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-temporal-duration-override",
        source_message_id="msg-temporal-duration-override",
    )

    assert result["outcome"] == "rec_needs_clarification"
    assert result["rec"]["missing_field"] == "temporal_commit_confirm"
    assert int(result["command"].get("duration_minutes") or 0) == 60
    assert "Длительность: 1 час" in str(result.get("clarifying_question") or "")
    assert rec.calls == 0


def test_temporal_edit_field_routes_to_date_time_duration_states_without_draft_reset() -> None:
    handler = _load_handler_module()
    base_payload = {
        "intent": "timeblock.create",
        "start_at": _dt_iso(1, 12),
        "start_at_date": _date_iso(1),
        "start_at_time": "12:00",
        "duration_minutes": 45,
        "entities": {
            "text": "назначь встречу",
            "start_at_date": _date_iso(1),
            "start_at_time": "12:00",
            "duration_minutes": 45,
        },
    }
    expectations = {
        "дата": "start_at_date",
        "время": "start_at_time",
        "длительность": "duration_minutes",
    }

    for text, expected_missing in expectations.items():
        rec = _RecProbe(should_fail_on_call=True)
        db = _FakeDb(
            session={
                "intent": "timeblock.create",
                "missing_field": "temporal_edit_field",
                "idempotency_key": f"idem-temporal-edit-{text}",
                "payload": dict(base_payload),
            }
        )

        result = handler.handle_user_attempt(
            request_id=f"r-temporal-edit-{text}",
            user_id=f"u-temporal-edit-{text}",
            channel="telegram",
            asr_client=_NoopAsr(),
            llm_client=_NeverCalledLLM(),
            rec_client=rec,
            text=text,
            local_db=db,
            app_id="app",
            tenant_id="tenant",
            source_chat_id="chat-temporal-edit",
            source_message_id=f"msg-temporal-edit-{text}",
        )

        assert result["outcome"] == "rec_needs_clarification"
        assert result["rec"]["missing_field"] == expected_missing
        assert db.upserts
        persisted = db.upserts[-1]["payload"]
        assert persisted["start_at"] == base_payload["start_at"]
        assert persisted["duration_minutes"] == base_payload["duration_minutes"]


def test_temporal_edit_date_then_new_summary_preserves_time_and_duration() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    base_payload = {
        "intent": "timeblock.create",
        "start_at": _dt_iso(1, 14),
        "start_at_date": _date_iso(1),
        "start_at_time": "14:00",
        "duration_minutes": 60,
        "__source_text": f"назначь встречу {_date_iso(1)} в 14:00 на 60 минут",
        "entities": {
            "text": f"назначь встречу {_date_iso(1)} в 14:00 на 60 минут",
            "start_at_date": _date_iso(1),
            "start_at_time": "14:00",
            "duration_minutes": 60,
        },
    }
    db = _FakeDb(
        session={
            "intent": "timeblock.create",
            "missing_field": "temporal_commit_confirm",
            "idempotency_key": "idem-edit-summary-1",
            "payload": dict(base_payload),
        }
    )

    step_no = handler.handle_user_attempt(
        request_id="r-edit-summary-no",
        user_id="u-edit-summary",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="нет",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-edit-summary",
        source_message_id="msg-edit-summary-no",
    )
    assert step_no["outcome"] == "rec_needs_clarification"
    assert step_no["rec"]["missing_field"] == "temporal_edit_field"
    db.session = {
        "intent": "timeblock.create",
        "missing_field": "temporal_edit_field",
        "idempotency_key": "idem-edit-summary-1",
        "payload": dict(step_no["command"]),
    }

    step_pick_field = handler.handle_user_attempt(
        request_id="r-edit-summary-pick-date",
        user_id="u-edit-summary",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="дата",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-edit-summary",
        source_message_id="msg-edit-summary-pick-date",
    )
    assert step_pick_field["outcome"] == "rec_needs_clarification"
    assert step_pick_field["rec"]["missing_field"] == "start_at_date"
    db.session = {
        "intent": "timeblock.create",
        "missing_field": "start_at_date",
        "idempotency_key": "idem-edit-summary-1",
        "payload": dict(step_pick_field["command"]),
    }

    step_new_date = handler.handle_user_attempt(
        request_id="r-edit-summary-new-date",
        user_id="u-edit-summary",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="15.04",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-edit-summary",
        source_message_id="msg-edit-summary-new-date",
    )
    assert step_new_date["outcome"] == "rec_needs_clarification"
    assert step_new_date["rec"]["missing_field"] == "temporal_commit_confirm"
    question = str(step_new_date.get("clarifying_question") or "")
    assert "Дата: 15.04." in question
    assert "Время: 14:00" in question
    assert "Длительность:" in question
    updated_command = step_new_date["command"]
    assert str(updated_command.get("start_at_date")) == f"{date.today().year}-04-15"
    assert str(updated_command.get("start_at_time")) == "14:00"
    assert int(updated_command.get("duration_minutes")) == 60


def test_temporal_edit_time_short_hour_updates_only_time() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    base_payload = {
        "intent": "timeblock.create",
        "start_at": "2026-04-12 14:00",
        "start_at_date": "2026-04-12",
        "start_at_time": "14:00",
        "duration_minutes": 60,
        "__source_text": "назначь встречу 12.04.2026 в 14:00 на 60 минут",
        "entities": {
            "text": "назначь встречу 12.04.2026 в 14:00 на 60 минут",
            "start_at_date": "2026-04-12",
            "start_at_time": "14:00",
            "duration_minutes": 60,
        },
    }
    db = _FakeDb(
        session={
            "intent": "timeblock.create",
            "missing_field": "temporal_edit_field",
            "idempotency_key": "idem-temporal-edit-time-short",
            "payload": dict(base_payload),
        }
    )

    pick = handler.handle_user_attempt(
        request_id="r-temporal-edit-time-pick-short",
        user_id="u-temporal-edit-time-short",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="время",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-temporal-edit-time-short",
        source_message_id="msg-temporal-edit-time-pick-short",
    )
    assert pick["outcome"] == "rec_needs_clarification"
    assert pick["rec"]["missing_field"] == "start_at_time"
    assert "во сколько" in str(pick.get("clarifying_question") or "").lower()

    db.session = {
        "intent": "timeblock.create",
        "missing_field": "start_at_time",
        "idempotency_key": "idem-temporal-edit-time-short",
        "payload": dict(pick["command"]),
    }
    updated = handler.handle_user_attempt(
        request_id="r-temporal-edit-time-update-short",
        user_id="u-temporal-edit-time-short",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="12",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-temporal-edit-time-short",
        source_message_id="msg-temporal-edit-time-update-short",
    )
    assert updated["outcome"] == "rec_needs_clarification"
    assert updated["rec"]["missing_field"] == "temporal_commit_confirm"
    cmd = updated["command"]
    assert str(cmd.get("start_at_date") or "") == "2026-04-12"
    assert str(cmd.get("start_at_time") or "") == "12:00"
    assert int(cmd.get("duration_minutes") or 0) == 60
    assert str(cmd.get("start_at") or "") == "2026-04-12 12:00"
    question = str(updated.get("clarifying_question") or "")
    assert "Дата: 12.04.2026" in question
    assert "Время: 12:00" in question
    assert "Длительность: 1 час" in question
    assert rec.calls == 0


def test_clarification_date_without_year_uses_current_year_for_ddmm() -> None:
    handler = _load_handler_module()
    base = date(2026, 4, 12)
    parsed = handler._normalize_clarification_date_to_iso("13.04", today=base)
    assert parsed == "2026-04-13"
    assert parsed != "2023-04-13"


def test_clarification_date_without_year_uses_current_year_for_month_name() -> None:
    handler = _load_handler_module()
    base = date(2026, 4, 12)
    parsed = handler._normalize_clarification_date_to_iso("13 апреля", today=base)
    assert parsed == "2026-04-13"
    assert parsed != "2023-04-13"


def test_clarification_date_month_name_without_year_uses_current_year_2026() -> None:
    handler = _load_handler_module()
    base = date(2026, 4, 20)
    parsed = handler._normalize_clarification_date_to_iso("21 апреля", today=base)
    assert parsed == "2026-04-21"


def test_clarification_date_ddmm_without_year_uses_current_year_2026() -> None:
    handler = _load_handler_module()
    base = date(2026, 4, 20)
    parsed = handler._normalize_clarification_date_to_iso("13.04", today=base)
    assert parsed == "2026-04-13"


def test_clarification_date_month_name_with_explicit_year_keeps_that_year() -> None:
    handler = _load_handler_module()
    base = date(2026, 4, 20)
    parsed = handler._normalize_clarification_date_to_iso("21 апреля 2021", today=base)
    assert parsed == "2021-04-21"


def test_temporal_edit_date_month_name_updates_to_current_year() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(
        session={
            "intent": "meeting.create",
            "missing_field": "start_at_date",
            "idempotency_key": "idem-date-month-name",
            "payload": {
                "intent": "meeting.create",
                "start_at_time": "14:00",
                "duration_minutes": 60,
                "__source_text": "назначь встречу в 14 на 60 минут",
                "entities": {
                    "text": "назначь встречу в 14 на 60 минут",
                    "start_at_time": "14:00",
                    "duration_minutes": 60,
                },
            },
        }
    )

    updated = handler.handle_user_attempt(
        request_id="r-date-month-name",
        user_id="u-date-month-name",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="13 апреля",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-date-month-name",
        source_message_id="msg-date-month-name",
    )
    assert updated["outcome"] == "rec_needs_clarification"
    assert updated["rec"]["missing_field"] == "temporal_commit_confirm"
    cmd = updated["command"]
    expected = f"{date.today().year}-04-13"
    assert str(cmd.get("start_at_date") or "") == expected
    assert "Дата: 13.04." in str(updated.get("clarifying_question") or "")
    assert rec.calls == 0


def test_date_from_text_is_materialized_to_entities_and_not_reasked() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(session=None)
    llm = _StaticLLM(
        {
                "intent": "meeting.create",
                "entities": {
                    "text": "запланируй встречу 22 апреля в 14:00 на 30 минут",
                    "start_at_time": "14:00",
                    "duration_minutes": 30,
                },
            }
        )

    out = handler.handle_user_attempt(
        request_id="r-date-materialized-entities",
        user_id="u-date-materialized-entities",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=llm,
        rec_client=rec,
        text="запланируй встречу 22 апреля в 14:00 на 30 минут",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-date-materialized-entities",
        source_message_id="msg-date-materialized-entities",
    )

    assert out["outcome"] == "rec_needs_clarification"
    assert out["rec"]["missing_field"] == "temporal_commit_confirm"
    entities = out["command"].get("entities") if isinstance(out.get("command"), dict) else {}
    entities = entities if isinstance(entities, dict) else {}
    expected_date = f"{date.today().year}-04-22"
    assert str(entities.get("start_at_date") or "") == expected_date
    assert str(out["command"].get("start_at_date") or "") == expected_date
    q = str(out.get("clarifying_question") or "").lower()
    assert "какая дата" not in q
    assert rec.calls == 0


def test_meeting_kind_sobranie_materializes_date_and_does_not_reask() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(session=None)
    llm = _StaticLLM(
        {
            "intent": "meeting.create",
            "entities": {
                "text": "запланируй собрание 22 апреля в 10 на 30 минут",
                "meeting_kind": "собрание",
                "start_at_time": "10:00",
                "duration_minutes": 30,
            },
        }
    )

    out = handler.handle_user_attempt(
        request_id="r-sobranie-date-materialized",
        user_id="u-sobranie-date-materialized",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=llm,
        rec_client=rec,
        text="запланируй собрание 22 апреля в 10 на 30 минут",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-sobranie-date-materialized",
        source_message_id="msg-sobranie-date-materialized",
    )

    assert out["outcome"] == "rec_needs_clarification"
    assert out["rec"]["missing_field"] == "temporal_commit_confirm"
    entities = out["command"].get("entities") if isinstance(out.get("command"), dict) else {}
    entities = entities if isinstance(entities, dict) else {}
    expected_date = f"{date.today().year}-04-22"
    assert str(entities.get("start_at_date") or "") == expected_date
    q = str(out.get("clarifying_question") or "").lower()
    assert "на какую дату" not in q
    assert rec.calls == 0


def test_date_without_year_overrides_stale_entity_year_from_parser() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(session=None)
    llm = _StaticLLM(
        {
            "intent": "meeting.create",
            "entities": {
                "text": "запланируй встречу 21 апреля в 14",
                "start_at_date": "2023-04-21",
                "start_at_time": "14:00",
                "duration_minutes": 30,
            },
        }
    )

    out = handler.handle_user_attempt(
        request_id="r-date-overrides-stale-parser-year",
        user_id="u-date-overrides-stale-parser-year",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=llm,
        rec_client=rec,
        text="запланируй встречу 21 апреля в 14",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-date-overrides-stale-parser-year",
        source_message_id="msg-date-overrides-stale-parser-year",
    )

    assert out["outcome"] == "rec_needs_clarification"
    expected_date = f"{date.today().year}-04-21"
    assert str(out["command"].get("start_at_date") or "") == expected_date
    entities = out["command"].get("entities") if isinstance(out.get("command"), dict) else {}
    entities = entities if isinstance(entities, dict) else {}
    assert str(entities.get("start_at_date") or "") == expected_date


def test_event_create_day_month_without_year_uses_current_year_and_materializes_entities() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(session=None)
    llm = _StaticLLM(
        {
            "intent": "meeting.create",
            "entities": {
                "text": "запланируй встречу 21 апреля",
                "meeting_kind": "встреча",
            },
        }
    )

    out = handler.handle_user_attempt(
        request_id="r-event-create-day-month-no-year",
        user_id="u-event-create-day-month-no-year",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=llm,
        rec_client=rec,
        text="запланируй встречу 21 апреля",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-event-create-day-month-no-year",
        source_message_id="msg-event-create-day-month-no-year",
    )
    expected = f"{date.today().year}-04-21"
    assert str(out["command"].get("start_at_date") or "") == expected
    entities = out["command"].get("entities") if isinstance(out.get("command"), dict) else {}
    entities = entities if isinstance(entities, dict) else {}
    assert str(entities.get("start_at_date") or "") == expected
    assert out["rec"]["missing_field"] == "start_at_time"


def test_event_create_day_month_and_time_does_not_reask_date() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(session=None)
    llm = _StaticLLM(
        {
            "intent": "meeting.create",
            "entities": {
                "text": "запланируй собрание 22 апреля в 11:00",
                "meeting_kind": "собрание",
                "start_at_time": "11:00",
            },
        }
    )

    out = handler.handle_user_attempt(
        request_id="r-event-create-day-month-time",
        user_id="u-event-create-day-month-time",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=llm,
        rec_client=rec,
        text="запланируй собрание 22 апреля в 11:00",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-event-create-day-month-time",
        source_message_id="msg-event-create-day-month-time",
    )
    assert out["rec"]["missing_field"] == "temporal_commit_confirm"
    q = str(out.get("clarifying_question") or "").lower()
    assert "на какую дату" not in q


def test_event_create_only_date_asks_time_not_date() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(session=None)
    llm = _StaticLLM(
        {
            "intent": "meeting.create",
            "entities": {
                "text": "созвон 25 апреля",
                "meeting_kind": "созвон",
            },
        }
    )

    out = handler.handle_user_attempt(
        request_id="r-event-create-only-date",
        user_id="u-event-create-only-date",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=llm,
        rec_client=rec,
        text="созвон 25 апреля",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-event-create-only-date",
        source_message_id="msg-event-create-only-date",
    )
    assert out["rec"]["missing_field"] == "start_at_time"
    q = str(out.get("clarifying_question") or "").lower()
    assert "на какую дату" not in q


def test_event_create_only_time_does_not_invent_date_and_asks_date() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(session=None)
    llm = _StaticLLM(
        {
            "intent": "meeting.create",
            "entities": {
                "text": "встреча в 15:00",
                "meeting_kind": "встреча",
                "start_at_time": "15:00",
            },
        }
    )

    out = handler.handle_user_attempt(
        request_id="r-event-create-only-time",
        user_id="u-event-create-only-time",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=llm,
        rec_client=rec,
        text="встреча в 15:00",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-event-create-only-time",
        source_message_id="msg-event-create-only-time",
    )
    assert out["rec"]["missing_field"] == "start_at_date"
    assert str(out["command"].get("start_at_date") or "").strip() == ""


def test_event_create_explicit_year_is_preserved() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(session=None)
    llm = _StaticLLM(
        {
            "intent": "meeting.create",
            "entities": {
                "text": "встреча 21 апреля 2027 в 11:00",
                "meeting_kind": "встреча",
                "start_at_time": "11:00",
            },
        }
    )

    out = handler.handle_user_attempt(
        request_id="r-event-create-explicit-year",
        user_id="u-event-create-explicit-year",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=llm,
        rec_client=rec,
        text="встреча 21 апреля 2027 в 11:00",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-event-create-explicit-year",
        source_message_id="msg-event-create-explicit-year",
    )
    assert out["rec"]["missing_field"] == "temporal_commit_confirm"
    assert str(out["command"].get("start_at_date") or "") == "2027-04-21"


def test_event_create_no_regression_to_2021_or_2023_for_day_month_without_year() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(session=None)
    llm = _StaticLLM(
        {
            "intent": "meeting.create",
            "entities": {
                "text": "мероприятие 21 апреля на 2 часа",
                "meeting_kind": "мероприятие",
                "duration_minutes": 120,
            },
        }
    )

    out = handler.handle_user_attempt(
        request_id="r-event-create-no-2021-2023",
        user_id="u-event-create-no-2021-2023",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=llm,
        rec_client=rec,
        text="мероприятие 21 апреля на 2 часа",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-event-create-no-2021-2023",
        source_message_id="msg-event-create-no-2021-2023",
    )
    result_date = str(out["command"].get("start_at_date") or "")
    assert result_date == f"{date.today().year}-04-21"
    assert result_date != "2021-04-21"
    assert result_date != "2023-04-21"


def test_existing_date_in_entities_is_not_reasked_when_session_missing_start_at_date() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    future = date.today() + timedelta(days=1)
    iso = future.isoformat()
    payload = {
        "intent": "meeting.create",
        "__source_text": f"запланируй встречу {iso} в 14:00 на 30 минут",
        "start_at_date": iso,
        "start_at_time": "14:00",
        "duration_minutes": 30,
        "entities": {
            "text": f"запланируй встречу {iso} в 14:00 на 30 минут",
            "start_at_date": iso,
            "start_at_time": "14:00",
            "duration_minutes": 30,
        },
    }
    db = _FakeDb(
        session={
            "intent": "meeting.create",
            "missing_field": "start_at_date",
            "idempotency_key": "idem-date-no-reask-session",
            "payload": dict(payload),
        }
    )

    out = handler.handle_user_attempt(
        request_id="r-date-no-reask-session",
        user_id="u-date-no-reask-session",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="ок",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-date-no-reask-session",
        source_message_id="msg-date-no-reask-session",
    )

    assert out["outcome"] == "rec_needs_clarification"
    assert out["rec"]["missing_field"] != "start_at_date"
    assert "какую дату" not in str(out.get("clarifying_question") or "").lower()
    assert rec.calls == 0


def test_temporal_edit_time_hhmm_updates_only_time() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    base_payload = {
        "intent": "timeblock.create",
        "start_at": "2026-04-12 14:00",
        "start_at_date": "2026-04-12",
        "start_at_time": "14:00",
        "duration_minutes": 60,
        "__source_text": "назначь встречу 12.04.2026 в 14:00 на 60 минут",
        "entities": {
            "text": "назначь встречу 12.04.2026 в 14:00 на 60 минут",
            "start_at_date": "2026-04-12",
            "start_at_time": "14:00",
            "duration_minutes": 60,
        },
    }
    db = _FakeDb(
        session={
            "intent": "timeblock.create",
            "missing_field": "start_at_time",
            "idempotency_key": "idem-temporal-edit-time-hhmm",
            "payload": dict(base_payload),
        }
    )

    updated = handler.handle_user_attempt(
        request_id="r-temporal-edit-time-update-hhmm",
        user_id="u-temporal-edit-time-hhmm",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="18:30",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-temporal-edit-time-hhmm",
        source_message_id="msg-temporal-edit-time-update-hhmm",
    )
    assert updated["outcome"] == "rec_needs_clarification"
    assert updated["rec"]["missing_field"] == "temporal_commit_confirm"
    cmd = updated["command"]
    assert str(cmd.get("start_at_date") or "") == "2026-04-12"
    assert str(cmd.get("start_at_time") or "") == "18:30"
    assert int(cmd.get("duration_minutes") or 0) == 60
    assert str(cmd.get("start_at") or "") == "2026-04-12 18:30"
    question = str(updated.get("clarifying_question") or "")
    assert "Дата: 12.04.2026" in question
    assert "Время: 18:30" in question
    assert "Длительность: 1 час" in question
    assert rec.calls == 0


def test_temporal_confirm_yes_after_time_edit_routes_to_execution() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    base_payload = {
        "intent": "timeblock.create",
        "start_at": "2026-04-12 14:00",
        "start_at_date": "2026-04-12",
        "start_at_time": "14:00",
        "duration_minutes": 60,
        "__source_text": "назначь встречу 12.04.2026 в 14:00 на 60 минут",
        "entities": {
            "text": "назначь встречу 12.04.2026 в 14:00 на 60 минут",
            "start_at_date": "2026-04-12",
            "start_at_time": "14:00",
            "duration_minutes": 60,
        },
    }
    db = _FakeDb(
        session={
            "intent": "timeblock.create",
            "missing_field": "temporal_commit_confirm",
            "idempotency_key": "idem-temporal-confirm-after-time-edit",
            "payload": dict(base_payload),
        }
    )

    step_no = handler.handle_user_attempt(
        request_id="r-time-edit-no",
        user_id="u-time-edit",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="нет",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-time-edit",
        source_message_id="msg-time-edit-no",
    )
    assert step_no["outcome"] == "rec_needs_clarification"
    assert step_no["rec"]["missing_field"] == "temporal_edit_field"
    db.session = {
        "intent": "timeblock.create",
        "missing_field": "temporal_edit_field",
        "idempotency_key": "idem-temporal-confirm-after-time-edit",
        "payload": dict(step_no["command"]),
    }

    step_pick_time = handler.handle_user_attempt(
        request_id="r-time-edit-pick",
        user_id="u-time-edit",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="время",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-time-edit",
        source_message_id="msg-time-edit-pick",
    )
    assert step_pick_time["outcome"] == "rec_needs_clarification"
    assert step_pick_time["rec"]["missing_field"] == "start_at_time"
    db.session = {
        "intent": "timeblock.create",
        "missing_field": "start_at_time",
        "idempotency_key": "idem-temporal-confirm-after-time-edit",
        "payload": dict(step_pick_time["command"]),
    }

    step_new_time = handler.handle_user_attempt(
        request_id="r-time-edit-set",
        user_id="u-time-edit",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="16",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-time-edit",
        source_message_id="msg-time-edit-set",
    )
    assert step_new_time["outcome"] == "rec_needs_clarification"
    assert step_new_time["rec"]["missing_field"] == "temporal_commit_confirm"
    assert "Время: 16:00" in str(step_new_time.get("clarifying_question") or "")
    db.session = {
        "intent": "timeblock.create",
        "missing_field": "temporal_commit_confirm",
        "idempotency_key": "idem-temporal-confirm-after-time-edit",
        "payload": dict(step_new_time["command"]),
    }

    step_yes = handler.handle_user_attempt(
        request_id="r-time-edit-yes",
        user_id="u-time-edit",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="да",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-time-edit",
        source_message_id="msg-time-edit-yes",
    )
    assert step_yes["outcome"] == "success"
    assert step_yes["rec"]["outcome"] == "skipped"
    assert "уточните, какую команду выполнить" not in str(step_yes.get("user_message") or "").lower()
    assert rec.calls == 0


def test_temporal_confirm_yes_after_date_edit_routes_to_execution() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    base_payload = {
        "intent": "timeblock.create",
        "start_at": "2026-04-12 14:00",
        "start_at_date": "2026-04-12",
        "start_at_time": "14:00",
        "duration_minutes": 60,
        "__source_text": "назначь встречу 12.04.2026 в 14:00 на 60 минут",
        "entities": {
            "text": "назначь встречу 12.04.2026 в 14:00 на 60 минут",
            "start_at_date": "2026-04-12",
            "start_at_time": "14:00",
            "duration_minutes": 60,
        },
    }
    db = _FakeDb(
        session={
            "intent": "timeblock.create",
            "missing_field": "temporal_commit_confirm",
            "idempotency_key": "idem-temporal-confirm-after-date-edit",
            "payload": dict(base_payload),
        }
    )

    step_no = handler.handle_user_attempt(
        request_id="r-date-edit-no",
        user_id="u-date-edit",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="нет",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-date-edit",
        source_message_id="msg-date-edit-no",
    )
    assert step_no["outcome"] == "rec_needs_clarification"
    assert step_no["rec"]["missing_field"] == "temporal_edit_field"
    db.session = {
        "intent": "timeblock.create",
        "missing_field": "temporal_edit_field",
        "idempotency_key": "idem-temporal-confirm-after-date-edit",
        "payload": dict(step_no["command"]),
    }

    step_pick_date = handler.handle_user_attempt(
        request_id="r-date-edit-pick",
        user_id="u-date-edit",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="дата",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-date-edit",
        source_message_id="msg-date-edit-pick",
    )
    assert step_pick_date["outcome"] == "rec_needs_clarification"
    assert step_pick_date["rec"]["missing_field"] == "start_at_date"
    db.session = {
        "intent": "timeblock.create",
        "missing_field": "start_at_date",
        "idempotency_key": "idem-temporal-confirm-after-date-edit",
        "payload": dict(step_pick_date["command"]),
    }

    step_new_date = handler.handle_user_attempt(
        request_id="r-date-edit-set",
        user_id="u-date-edit",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="15.04.2026",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-date-edit",
        source_message_id="msg-date-edit-set",
    )
    assert step_new_date["outcome"] == "rec_needs_clarification"
    assert step_new_date["rec"]["missing_field"] == "temporal_commit_confirm"
    assert "Дата: 15.04.2026" in str(step_new_date.get("clarifying_question") or "")
    db.session = {
        "intent": "timeblock.create",
        "missing_field": "temporal_commit_confirm",
        "idempotency_key": "idem-temporal-confirm-after-date-edit",
        "payload": dict(step_new_date["command"]),
    }

    step_yes = handler.handle_user_attempt(
        request_id="r-date-edit-yes",
        user_id="u-date-edit",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="да",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-date-edit",
        source_message_id="msg-date-edit-yes",
    )
    assert step_yes["outcome"] == "success"
    assert step_yes["rec"]["outcome"] == "skipped"
    assert "уточните, какую команду выполнить" not in str(step_yes.get("user_message") or "").lower()
    assert rec.calls == 0


def test_executable_task_intent_after_clarification_does_not_call_rec() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(
        session={
            "intent": "task.create",
            "missing_field": "title",
            "idempotency_key": "idem-2",
            "payload": {"intent": "task.create"},
        }
    )

    result = handler.handle_user_attempt(
        request_id="r2",
        user_id="u2",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="Купить молоко",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-2",
        source_message_id="msg-2",
    )

    assert result["outcome"] == "success"
    assert result["rec"]["outcome"] == "skipped"
    assert result["command"]["title"] == "Купить молоко"
    assert rec.calls == 0


def test_incomplete_executable_command_continues_clarification_without_rec() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(
        session={
            "intent": "task.create",
            "missing_field": "clarification_value",
            "idempotency_key": "idem-3",
            "payload": {"intent": "task.create"},
        }
    )

    result = handler.handle_user_attempt(
        request_id="r3",
        user_id="u3",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="да",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-3",
        source_message_id="msg-3",
    )

    assert result["outcome"] == "rec_needs_clarification"
    assert result["rec"]["outcome"] == "needs_clarification"
    assert rec.calls == 0
    assert db.upserts


def test_schedule_meeting_complete_never_routes_to_rec() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(session=None)

    llm = _StaticLLM(
        {
            "intent": "schedule_meeting",
            "start_at": _dt_iso(1, 11),
            "duration_minutes": 30,
            "entities": {"text": "Встреча завтра в 11 на 30 минут"},
        }
    )

    result = handler.handle_user_attempt(
        request_id="r4",
        user_id="u4",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=llm,
        rec_client=rec,
        text="Встреча завтра в 11 на 30 минут",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-4",
        source_message_id="msg-4",
    )

    assert result["outcome"] == "success"
    assert result["rec"]["outcome"] == "skipped"
    assert result["command"]["intent"] == "schedule_meeting"
    assert rec.calls == 0


def test_schedule_meeting_missing_fields_starts_clarification_without_rec() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(session=None)

    llm = _StaticLLM(
        {
            "intent": "schedule_meeting",
            "entities": {"text": "Встреча завтра в 11"},
        }
    )

    result = handler.handle_user_attempt(
        request_id="r5",
        user_id="u5",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=llm,
        rec_client=rec,
        text="Встреча завтра в 11",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-5",
        source_message_id="msg-5",
    )

    assert result["outcome"] == "rec_needs_clarification"
    assert result["rec"]["outcome"] == "needs_clarification"
    assert rec.calls == 0
    assert db.upserts
    assert db.upserts[-1]["missing_field"] in {"duration_minutes", "start_at", "start_at_date", "start_at_time"}


def test_schedule_meeting_without_datetime_asks_date_first() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(session=None)

    llm = _StaticLLM(
        {
            "intent": "schedule_meeting",
            "entities": {"text": "назначу встречу"},
        }
    )

    result = handler.handle_user_attempt(
        request_id="r5-date-first",
        user_id="u5",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=llm,
        rec_client=rec,
        text="назначу встречу",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-5",
        source_message_id="msg-5-date-first",
    )

    assert result["outcome"] == "rec_needs_clarification"
    assert result["rec"]["outcome"] == "needs_clarification"
    assert result["rec"]["missing_field"] == "start_at_date"
    assert "дат" in str(result["clarifying_question"]).lower()
    assert rec.calls == 0


def test_schedule_meeting_time_reply_hour_number_sets_time_and_moves_to_duration() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(
        session={
            "intent": "schedule_meeting",
            "missing_field": "start_at_time",
            "idempotency_key": "idem-5-time",
            "payload": {
                "intent": "schedule_meeting",
                "start_at": "завтра",
                "__source_text": "назначь встречу на завтра",
            },
        }
    )

    result = handler.handle_user_attempt(
        request_id="r5-time-hour",
        user_id="u5",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="12",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-5",
        source_message_id="msg-5-time-hour",
    )

    assert result["outcome"] == "rec_needs_clarification"
    assert result["rec"]["outcome"] == "needs_clarification"
    assert result["rec"]["missing_field"] == "duration_minutes"
    assert str(result["command"].get("start_at_time") or "") == "12:00"
    entities = result["command"].get("entities")
    entities = entities if isinstance(entities, dict) else {}
    assert str(entities.get("start_at_time") or "") == "12:00"
    assert "12:00" in str(result["command"].get("start_at") or "")
    assert "сколько минут" in str(result["clarifying_question"]).lower()
    assert rec.calls == 0


def test_awaiting_time_alias_short_hour_updates_time_without_new_intent_flow() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    payload = {
        "intent": "schedule_meeting",
        "start_at_date": _date_iso(1),
        "duration_minutes": 45,
        "__source_text": f"встреча {_date_iso(1)} на 45 минут",
        "entities": {
            "text": f"встреча {_date_iso(1)} на 45 минут",
            "start_at_date": _date_iso(1),
            "duration_minutes": 45,
        },
    }
    db = _FakeDb(
        session={
            "intent": "schedule_meeting",
            "missing_field": "awaiting_time",
            "idempotency_key": "idem-awaiting-time-short-hour",
            "payload": dict(payload),
        }
    )

    result = handler.handle_user_attempt(
        request_id="r-awaiting-time-short-hour",
        user_id="u-awaiting-time-short-hour",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="12",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-awaiting-time-short-hour",
        source_message_id="msg-awaiting-time-short-hour",
    )

    assert result["outcome"] == "rec_needs_clarification"
    assert result["rec"]["missing_field"] == "temporal_commit_confirm"
    cmd = result["command"]
    assert str(cmd.get("start_at_time") or "") == "12:00"
    assert str(cmd.get("start_at_date") or "") == _date_iso(1)
    assert int(cmd.get("duration_minutes") or 0) == 45
    assert "12:00" in str(cmd.get("start_at") or "")
    assert rec.calls == 0


def test_start_at_time_short_hour_is_accepted_as_hh00() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    payload = {
        "intent": "meeting.create",
        "start_at_date": _date_iso(1),
        "duration_minutes": 60,
        "__source_text": f"встреча {_date_iso(1)} на 60 минут",
        "entities": {
            "text": f"встреча {_date_iso(1)} на 60 минут",
            "start_at_date": _date_iso(1),
            "duration_minutes": 60,
        },
    }
    db = _FakeDb(
        session={
            "intent": "meeting.create",
            "missing_field": "start_at_time",
            "idempotency_key": "idem-start-at-time-short-hour",
            "payload": dict(payload),
        }
    )

    result = handler.handle_user_attempt(
        request_id="r-start-at-time-short-hour",
        user_id="u-start-at-time-short-hour",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="18",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-start-at-time-short-hour",
        source_message_id="msg-start-at-time-short-hour",
    )

    assert result["outcome"] == "rec_needs_clarification"
    assert result["rec"]["missing_field"] == "temporal_commit_confirm"
    cmd = result["command"]
    assert str(cmd.get("start_at_time") or "") == "18:00"
    entities = cmd.get("entities")
    entities = entities if isinstance(entities, dict) else {}
    assert str(entities.get("start_at_time") or "") == "18:00"
    assert str(cmd.get("start_at_date") or "") == _date_iso(1)
    assert int(cmd.get("duration_minutes") or 0) == 60
    assert rec.calls == 0


def test_schedule_meeting_followup_uses_from_clarification_path_and_skips_rec() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(
        session={
            "intent": "schedule_meeting",
            "missing_field": "duration_minutes",
            "idempotency_key": "idem-6",
            "payload": {
                "intent": "schedule_meeting",
                "start_at": _dt_iso(1, 11),
                "__source_text": "Поставь блок на завтра в 11",
            },
        }
    )

    result = handler.handle_user_attempt(
        request_id="r6",
        user_id="u6",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="30",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-6",
        source_message_id="msg-6",
    )

    assert result["outcome"] == "success"
    assert result["rec"]["outcome"] == "skipped"
    assert result["command"]["intent"] == "schedule_meeting"
    assert result["command"]["duration_minutes"] == 30
    assert rec.calls == 0
    assert db.deleted_contexts


def test_schedule_block_complete_never_routes_to_rec() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(session=None)

    llm = _StaticLLM(
        {
            "intent": "schedule_block",
            "start_at": _dt_iso(1, 10),
            "duration_minutes": 30,
            "entities": {"text": "поставь блок завтра в 10 на 30 минут"},
        }
    )

    result = handler.handle_user_attempt(
        request_id="r7",
        user_id="u7",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=llm,
        rec_client=rec,
        text="поставь блок завтра в 10 на 30 минут",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-7",
        source_message_id="msg-7",
    )

    assert result["outcome"] == "success"
    assert result["rec"]["outcome"] == "skipped"
    assert rec.calls == 0


def test_schedule_block_missing_fields_starts_clarification_without_rec() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(session=None)

    llm = _StaticLLM(
        {
            "intent": "schedule_block",
            "entities": {"text": "поставь блок на завтра"},
        }
    )

    result = handler.handle_user_attempt(
        request_id="r8",
        user_id="u8",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=llm,
        rec_client=rec,
        text="поставь блок на завтра",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-8",
        source_message_id="msg-8",
    )

    assert result["outcome"] == "rec_needs_clarification"
    assert result["rec"]["outcome"] == "needs_clarification"
    assert rec.calls == 0
    assert db.upserts


def test_early_clarification_return_emits_structured_log(caplog) -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(session=None)

    llm = _StaticLLM(
        {
            "intent": "schedule_block",
            "entities": {"text": "поставь блок на завтра"},
        }
    )

    caplog.set_level("INFO")
    result = handler.handle_user_attempt(
        request_id="r8-log",
        user_id="u8",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=llm,
        rec_client=rec,
        text="поставь блок на завтра",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-8",
        source_message_id="msg-8",
    )

    assert result["outcome"] == "rec_needs_clarification"
    record = next((r for r in caplog.records if r.message == "runtime_early_clarification_return"), None)
    assert record is not None
    assert getattr(record, "request_id") == "r8-log"
    assert getattr(record, "query_text") == "поставь блок на завтра"
    assert getattr(record, "intent") in {"schedule_block", "create_timeblock"}
    assert getattr(record, "missing_field") in {"duration_minutes", "start_at", "start_at_date", "start_at_time"}
    assert getattr(record, "outcome") == "rec_needs_clarification"


def test_schedule_block_followup_uses_from_clarification_path_and_skips_rec() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(
        session={
            "intent": "schedule_block",
            "missing_field": "duration_minutes",
            "idempotency_key": "idem-9",
            "payload": {
                "intent": "schedule_block",
                "start_at": _dt_iso(1, 10),
                "__source_text": "Поставь блок на завтра в 10",
            },
        }
    )

    result = handler.handle_user_attempt(
        request_id="r9",
        user_id="u9",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="30",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-9",
        source_message_id="msg-9",
    )

    assert result["outcome"] == "success"
    assert result["rec"]["outcome"] == "skipped"
    assert result["command"]["intent"] in {"schedule_block", "create_timeblock"}
    assert result["command"]["duration_minutes"] == 30
    assert rec.calls == 0
    assert db.deleted_contexts


def test_put_block_without_time_unknown_intent_still_starts_temporal_clarification() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(session=None)

    llm = _StaticLLM(
        {
            "intent": "unknown",
            "entities": {"text": "поставь блок"},
        }
    )

    result = handler.handle_user_attempt(
        request_id="r10",
        user_id="u10",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=llm,
        rec_client=rec,
        text="поставь блок",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-10",
        source_message_id="msg-10",
    )

    assert result["outcome"] == "rec_needs_clarification"
    assert result["rec"]["outcome"] == "needs_clarification"
    assert result["rec"]["missing_field"] == "start_at_date"
    assert result["command"]["intent"] == "create_timeblock"
    assert rec.calls == 0


def test_schedule_meeting_unknown_intent_routes_to_meeting_create_flow() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(session=None)

    llm = _StaticLLM(
        {
            "intent": "unknown",
            "entities": {"text": "назначь встречу"},
        }
    )

    result = handler.handle_user_attempt(
        request_id="r10-meeting-alias",
        user_id="u10-meeting-alias",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=llm,
        rec_client=rec,
        text="назначь встречу",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-10-meeting-alias",
        source_message_id="msg-10-meeting-alias",
    )

    assert result["outcome"] == "rec_needs_clarification"
    assert result["rec"]["outcome"] == "needs_clarification"
    assert result["rec"]["missing_field"] == "start_at_date"
    assert result["command"]["intent"] == "meeting.create"
    assert rec.calls == 0


def test_meeting_create_summary_uses_detected_meeting_kind() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(session=None)

    llm = _StaticLLM(
        {
            "intent": "meeting.create",
            "entities": {
                "text": "запланируй собрание завтра в 14 на 60 минут",
                "start_at_date": _date_iso(1),
                "start_at_time": "14:00",
                "duration_minutes": 60,
            },
        }
    )

    result = handler.handle_user_attempt(
        request_id="r10-meeting-kind-summary",
        user_id="u10-meeting-kind-summary",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=llm,
        rec_client=rec,
        text="запланируй собрание завтра в 14 на 60 минут",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-10-meeting-kind-summary",
        source_message_id="msg-10-meeting-kind-summary",
    )

    assert result["outcome"] == "rec_needs_clarification"
    assert result["rec"]["missing_field"] == "temporal_commit_confirm"
    assert "Тип: собрание" in str(result.get("clarifying_question") or "")
    assert str(result["command"].get("meeting_kind") or "") == "собрание"
    entities = result["command"].get("entities")
    entities = entities if isinstance(entities, dict) else {}
    assert str(entities.get("meeting_kind") or "") == "собрание"


def test_meeting_update_keeps_new_date_before_target_identification() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(session=None)
    future_day = date.today() + timedelta(days=7)
    date_token = f"{future_day.day:02d}.{future_day.month:02d}"
    llm = _StaticLLM(
        {
            "intent": "meeting.update",
            "entities": {
                "text": f"перенеси встречу на {date_token}",
            },
        }
    )
    original_search = handler._meeting_update_repeat_search
    handler._meeting_update_repeat_search = lambda _user_id, _target_hint, _meeting_kind="": {"ok": False, "reason": "not_found"}
    try:
        result = handler.handle_user_attempt(
            request_id="r-update-date-keep",
            user_id="u-update-date-keep",
            channel="telegram",
            asr_client=_NoopAsr(),
                llm_client=llm,
                rec_client=rec,
                text=f"перенеси встречу на {date_token}",
                local_db=db,
                app_id="app",
                tenant_id="tenant",
            source_chat_id="chat-update-date-keep",
            source_message_id="msg-update-date-keep",
        )
    finally:
        handler._meeting_update_repeat_search = original_search

    assert result["outcome"] == "rec_needs_clarification"
    assert result["rec"]["missing_field"] == "meeting_update_target_ref"
    expected = future_day.isoformat()
    command = result.get("command", {})
    assert str(command.get("start_at_date") or "") == expected
    assert str(command.get("meeting_update_new_date") or "") == expected
    assert db.upserts
    persisted_payload = db.upserts[-1]["payload"]
    assert str(persisted_payload.get("meeting_update_new_date") or "") == expected
    assert rec.calls == 0


def test_reschedule_phrase_unknown_intent_routes_to_meeting_update_flow() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(session=None)

    llm = _StaticLLM(
        {
            "intent": "unknown",
            "entities": {"text": "перенеси встречу"},
        }
    )

    result = handler.handle_user_attempt(
        request_id="r10-meeting-update-alias",
        user_id="u10-meeting-update-alias",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=llm,
        rec_client=rec,
        text="перенеси встречу",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-10-meeting-update-alias",
        source_message_id="msg-10-meeting-update-alias",
    )

    assert result["outcome"] == "rec_needs_clarification"
    assert result["rec"]["outcome"] == "needs_clarification"
    assert result["rec"]["missing_field"] == "meeting_update_target_ref"
    assert result["command"]["intent"] == "meeting.update"
    assert rec.calls == 0


def test_meeting_update_guard_blocks_vague_phrase_izmeni_sobranie() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(session=None)
    calls: list[dict[str, str]] = []

    def _fake_repeat_search(user_id: str, target_hint: str, meeting_kind: str = "") -> dict:
        calls.append({"user_id": user_id, "target_hint": target_hint, "meeting_kind": meeting_kind})
        return {"ok": False, "reason": "not_found"}

    handler._meeting_update_repeat_search = _fake_repeat_search  # type: ignore[attr-defined]
    llm = _StaticLLM({"intent": "unknown", "entities": {"text": "измени собрание"}})

    result = handler.handle_user_attempt(
        request_id="r-guard-vague-izmeni-sobranie",
        user_id="u-guard-vague-izmeni-sobranie",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=llm,
        rec_client=rec,
        text="измени собрание",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-guard-vague-izmeni-sobranie",
        source_message_id="msg-guard-vague-izmeni-sobranie",
    )

    assert result["outcome"] == "rec_needs_clarification"
    assert result["rec"]["missing_field"] == "meeting_update_target_ref"
    assert str(result.get("clarifying_question") or "") == "Какое именно событие нужно изменить? Укажите дату/время или другой ориентир."
    assert calls == []
    assert rec.calls == 0


def test_meeting_update_guard_with_anchor_starts_search_izmeni_sobranie_with_date_time() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(session=None)
    calls: list[dict[str, str]] = []

    def _fake_repeat_search(user_id: str, target_hint: str, meeting_kind: str = "") -> dict:
        calls.append({"user_id": user_id, "target_hint": target_hint, "meeting_kind": meeting_kind})
        return {"ok": False, "reason": "not_found"}

    handler._meeting_update_repeat_search = _fake_repeat_search  # type: ignore[attr-defined]
    llm = _StaticLLM({"intent": "unknown", "entities": {"text": "измени собрание завтра в 13:00"}})

    result = handler.handle_user_attempt(
        request_id="r-guard-anchor-izmeni-sobranie",
        user_id="u-guard-anchor-izmeni-sobranie",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=llm,
        rec_client=rec,
        text="измени собрание завтра в 13:00",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-guard-anchor-izmeni-sobranie",
        source_message_id="msg-guard-anchor-izmeni-sobranie",
    )

    assert result["outcome"] == "rec_needs_clarification"
    assert result["rec"]["missing_field"] == "meeting_update_target_refine"
    assert calls
    assert calls[0]["user_id"] == "u-guard-anchor-izmeni-sobranie"
    assert calls[0]["meeting_kind"] == "собрание"
    assert rec.calls == 0


def test_meeting_update_guard_blocks_vague_phrase_perenesi_vstrechu() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(session=None)
    calls: list[dict[str, str]] = []

    def _fake_repeat_search(user_id: str, target_hint: str, meeting_kind: str = "") -> dict:
        calls.append({"user_id": user_id, "target_hint": target_hint, "meeting_kind": meeting_kind})
        return {"ok": False, "reason": "not_found"}

    handler._meeting_update_repeat_search = _fake_repeat_search  # type: ignore[attr-defined]
    llm = _StaticLLM({"intent": "unknown", "entities": {"text": "перенеси встречу"}})

    result = handler.handle_user_attempt(
        request_id="r-guard-vague-perenesi-vstrechu",
        user_id="u-guard-vague-perenesi-vstrechu",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=llm,
        rec_client=rec,
        text="перенеси встречу",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-guard-vague-perenesi-vstrechu",
        source_message_id="msg-guard-vague-perenesi-vstrechu",
    )

    assert result["outcome"] == "rec_needs_clarification"
    assert result["rec"]["missing_field"] == "meeting_update_target_ref"
    assert str(result.get("clarifying_question") or "") == "Какое именно событие нужно изменить? Укажите дату/время или другой ориентир."
    assert calls == []
    assert rec.calls == 0


def test_meeting_update_guard_with_anchor_starts_search_perenesi_vstrechu_tomorrow_14() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(session=None)
    calls: list[dict[str, str]] = []

    def _fake_repeat_search(user_id: str, target_hint: str, meeting_kind: str = "") -> dict:
        calls.append({"user_id": user_id, "target_hint": target_hint, "meeting_kind": meeting_kind})
        return {"ok": False, "reason": "not_found"}

    handler._meeting_update_repeat_search = _fake_repeat_search  # type: ignore[attr-defined]
    llm = _StaticLLM({"intent": "unknown", "entities": {"text": "перенеси встречу завтра в 14"}})

    result = handler.handle_user_attempt(
        request_id="r-guard-anchor-perenesi-vstrechu",
        user_id="u-guard-anchor-perenesi-vstrechu",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=llm,
        rec_client=rec,
        text="перенеси встречу завтра в 14",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-guard-anchor-perenesi-vstrechu",
        source_message_id="msg-guard-anchor-perenesi-vstrechu",
    )

    assert result["outcome"] == "rec_needs_clarification"
    assert result["rec"]["missing_field"] == "meeting_update_target_refine"
    assert calls
    assert calls[0]["user_id"] == "u-guard-anchor-perenesi-vstrechu"
    assert rec.calls == 0


def test_meeting_update_guard_with_participant_anchor_starts_search_izmeni_sozvon_s_ivanom() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(session=None)
    calls: list[dict[str, str]] = []

    def _fake_repeat_search(user_id: str, target_hint: str, meeting_kind: str = "") -> dict:
        calls.append({"user_id": user_id, "target_hint": target_hint, "meeting_kind": meeting_kind})
        return {"ok": False, "reason": "not_found"}

    handler._meeting_update_repeat_search = _fake_repeat_search  # type: ignore[attr-defined]
    llm = _StaticLLM({"intent": "unknown", "entities": {"text": "измени созвон с Иваном"}})

    result = handler.handle_user_attempt(
        request_id="r-guard-anchor-izmeni-sozvon-s-ivanom",
        user_id="u-guard-anchor-izmeni-sozvon-s-ivanom",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=llm,
        rec_client=rec,
        text="измени созвон с Иваном",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-guard-anchor-izmeni-sozvon-s-ivanom",
        source_message_id="msg-guard-anchor-izmeni-sozvon-s-ivanom",
    )

    assert result["outcome"] == "rec_needs_clarification"
    assert result["rec"]["missing_field"] == "meeting_update_target_refine"
    assert calls
    assert calls[0]["user_id"] == "u-guard-anchor-izmeni-sozvon-s-ivanom"
    assert calls[0]["meeting_kind"] == "созвон"
    assert rec.calls == 0


def test_reschedule_phrase_forces_meeting_update_even_if_llm_returns_create_timeblock() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(session=None)

    llm = _StaticLLM(
        {
            "intent": "create_timeblock",
            "entities": {"text": "перенеси собрание"},
        }
    )

    def _fake_repeat_search(user_id: str, target_hint: str, meeting_kind: str = "") -> dict:
        return {
            "ok": False,
            "reason": "multiple",
            "candidates": [
                {
                    "calendar_event_id": "evt-1",
                    "title": "Собрание",
                    "meeting_kind": "собрание",
                    "start_at_date": _date_iso(1),
                    "start_at_time": "10:00",
                    "duration_minutes": 30,
                },
                {
                    "calendar_event_id": "evt-2",
                    "title": "Собрание с Валентином",
                    "meeting_kind": "собрание",
                    "start_at_date": _date_iso(1),
                    "start_at_time": "15:00",
                    "duration_minutes": 30,
                },
            ],
        }

    handler._meeting_update_repeat_search = _fake_repeat_search  # type: ignore[attr-defined]

    result = handler.handle_user_attempt(
        request_id="r10-meeting-update-force",
        user_id="u10-meeting-update-force",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=llm,
        rec_client=rec,
        text="перенеси собрание",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-10-meeting-update-force",
        source_message_id="msg-10-meeting-update-force",
    )

    assert result["outcome"] == "rec_needs_clarification"
    assert result["rec"]["missing_field"] == "meeting_update_target_select"
    assert result["command"]["intent"] == "meeting.update"
    entities = result["command"].get("entities")
    entities = entities if isinstance(entities, dict) else {}
    assert str(entities.get("meeting_kind") or "") == "собрание"
    assert "Нашёл несколько встреч" in str(result.get("clarifying_question") or "")
    assert rec.calls == 0


def test_meeting_update_with_source_fields_shows_summary_without_reasking_date_duration() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(session=None)
    llm = _StaticLLM(
        {
            "intent": "meeting.update",
            "calendar_event_id": "evt-1",
            "start_at_date": "2026-04-13",
            "start_at_time": "16:00",
            "duration_minutes": 60,
            "entities": {
                "text": "перенеси встречу на 16",
                "calendar_event_id": "evt-1",
                "meeting_update_source_event_id": "evt-1",
                "meeting_update_source_date": "2026-04-13",
                "meeting_update_source_time": "14:00",
                "meeting_update_source_duration": 60,
                "meeting_update_new_time": "16:00",
                "start_at_date": "2026-04-13",
                "start_at_time": "16:00",
                "duration_minutes": 60,
                "meeting_update_has_changes": True,
            },
        }
    )

    result = handler.handle_user_attempt(
        request_id="r-meeting-update-summary",
        user_id="u-meeting-update-summary",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=llm,
        rec_client=rec,
        text="перенеси встречу на 16",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-meeting-update-summary",
        source_message_id="msg-meeting-update-summary",
    )

    assert result["outcome"] == "rec_needs_clarification"
    assert result["rec"]["missing_field"] == "awaiting_target_confirm"
    q = str(result.get("clarifying_question") or "")
    assert "Вы имеете в виду встречу" in q
    assert "Перенести её на 16:00?" in q
    assert rec.calls == 0


def test_meeting_update_target_confirm_yes_does_not_reask_time_when_time_already_parsed() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    payload = {
        "intent": "meeting.update",
        "calendar_event_id": "evt-1",
        "start_at_date": "2026-04-13",
        "start_at_time": "16:00",
        "duration_minutes": 60,
        "__source_text": "перенеси встречу на 16",
        "entities": {
            "text": "перенеси встречу на 16",
            "calendar_event_id": "evt-1",
            "meeting_update_source_event_id": "evt-1",
            "meeting_update_source_date": "2026-04-13",
            "meeting_update_source_time": "14:00",
            "meeting_update_source_duration": 60,
            "meeting_update_new_time": "16:00",
            "meeting_update_has_changes": True,
            "start_at_date": "2026-04-13",
            "start_at_time": "16:00",
            "duration_minutes": 60,
        },
    }
    db = _FakeDb(
        session={
            "intent": "meeting.update",
            "missing_field": "awaiting_target_confirm",
            "idempotency_key": "idem-meeting-update-time-target",
            "payload": dict(payload),
        }
    )

    yes = handler.handle_user_attempt(
        request_id="r-meeting-update-time-target-yes",
        user_id="u-meeting-update-time-target",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="да",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-meeting-update-time-target",
        source_message_id="msg-meeting-update-time-target-yes",
    )
    assert yes["outcome"] == "rec_needs_clarification"
    assert yes["rec"]["missing_field"] == "temporal_commit_confirm"
    q = str(yes.get("clarifying_question") or "")
    assert "Время: 16:00" in q
    assert "Во сколько" not in q
    assert rec.calls == 0


def test_meeting_update_target_confirm_yes_does_not_reask_date_when_date_already_parsed() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    payload = {
        "intent": "meeting.update",
        "calendar_event_id": "evt-1",
        "start_at_date": "2026-04-14",
        "start_at_time": "14:00",
        "duration_minutes": 60,
        "__source_text": "перенеси встречу на завтра",
        "entities": {
            "text": "перенеси встречу на завтра",
            "calendar_event_id": "evt-1",
            "meeting_update_source_event_id": "evt-1",
            "meeting_update_source_date": "2026-04-13",
            "meeting_update_source_time": "14:00",
            "meeting_update_source_duration": 60,
            "meeting_update_new_date": "2026-04-14",
            "meeting_update_has_changes": True,
            "start_at_date": "2026-04-14",
            "start_at_time": "14:00",
            "duration_minutes": 60,
        },
    }
    db = _FakeDb(
        session={
            "intent": "meeting.update",
            "missing_field": "awaiting_target_confirm",
            "idempotency_key": "idem-meeting-update-date-target",
            "payload": dict(payload),
        }
    )

    yes = handler.handle_user_attempt(
        request_id="r-meeting-update-date-target-yes",
        user_id="u-meeting-update-date-target",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="yes",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-meeting-update-date-target",
        source_message_id="msg-meeting-update-date-target-yes",
    )
    assert yes["outcome"] == "rec_needs_clarification"
    assert yes["rec"]["missing_field"] == "temporal_commit_confirm"
    q = str(yes.get("clarifying_question") or "")
    assert "Дата: 14.04.2026" in q
    assert "На какую дату" not in q
    assert rec.calls == 0


def test_meeting_update_without_changes_after_target_confirm_asks_only_missing_time() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    payload = {
        "intent": "meeting.update",
        "calendar_event_id": "evt-1",
        "start_at_date": "2026-04-13",
        "start_at_time": "14:00",
        "duration_minutes": 60,
        "__source_text": "перенеси встречу",
        "entities": {
            "text": "перенеси встречу",
            "calendar_event_id": "evt-1",
            "meeting_update_source_event_id": "evt-1",
            "meeting_update_source_date": "2026-04-13",
            "meeting_update_source_time": "14:00",
            "meeting_update_source_duration": 60,
            "meeting_update_has_changes": False,
            "start_at_date": "2026-04-13",
            "start_at_time": "14:00",
            "duration_minutes": 60,
        },
    }
    db = _FakeDb(
        session={
            "intent": "meeting.update",
            "missing_field": "awaiting_target_confirm",
            "idempotency_key": "idem-meeting-update-nochange-target",
            "payload": dict(payload),
        }
    )

    yes = handler.handle_user_attempt(
        request_id="r-meeting-update-nochange-target-yes",
        user_id="u-meeting-update-nochange-target",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="да",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-meeting-update-nochange-target",
        source_message_id="msg-meeting-update-nochange-target-yes",
    )
    assert yes["outcome"] == "rec_needs_clarification"
    assert yes["rec"]["missing_field"] == "start_at_time"
    q = str(yes.get("clarifying_question") or "")
    assert "Во сколько" in q
    assert "На какую дату" not in q
    assert "На сколько минут" not in q
    assert rec.calls == 0


def test_meeting_update_target_confirm_no_requests_target_clarification() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    payload = {
        "intent": "meeting.update",
        "calendar_event_id": "evt-1",
        "start_at_date": "2026-04-13",
        "start_at_time": "16:00",
        "duration_minutes": 60,
        "__source_text": "перенеси встречу на 16",
        "entities": {
            "text": "перенеси встречу на 16",
            "calendar_event_id": "evt-1",
            "meeting_update_source_event_id": "evt-1",
            "meeting_update_source_date": "2026-04-13",
            "meeting_update_source_time": "14:00",
            "meeting_update_source_duration": 60,
            "meeting_update_new_time": "16:00",
            "meeting_update_has_changes": True,
            "start_at_date": "2026-04-13",
            "start_at_time": "16:00",
            "duration_minutes": 60,
        },
    }
    db = _FakeDb(
        session={
            "intent": "meeting.update",
            "missing_field": "awaiting_target_confirm",
            "idempotency_key": "idem-meeting-update-target-no",
            "payload": dict(payload),
        }
    )

    no = handler.handle_user_attempt(
        request_id="r-meeting-update-target-no",
        user_id="u-meeting-update-target-no",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="нет",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-meeting-update-target-no",
        source_message_id="msg-meeting-update-target-no",
    )
    assert no["outcome"] == "rec_needs_clarification"
    assert no["rec"]["missing_field"] == "meeting_update_target_ref"
    assert "уточните, какую встречу нужно перенести" in str(no.get("clarifying_question") or "").lower()
    assert rec.calls == 0


def test_meeting_update_target_identification_keeps_parsed_time_change_and_calls_repeat_search() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    calls: list[dict[str, str]] = []

    def _fake_repeat_search(user_id: str, target_hint: str) -> dict:
        calls.append({"user_id": user_id, "target_hint": target_hint})
        return {
            "ok": True,
            "candidate": {
                "calendar_event_id": "evt-found-14",
                "title": "Встреча с Иваном",
                "start_at_date": "2026-04-13",
                "start_at_time": "14:00",
                "duration_minutes": 60,
            },
        }

    handler._meeting_update_repeat_search = _fake_repeat_search  # type: ignore[attr-defined]
    payload = {
        "intent": "meeting.update",
        "start_at_date": "",
        "start_at_time": "16:00",
        "duration_minutes": "",
        "__source_text": "перенеси встречу с Иваном на 16",
        "entities": {
            "text": "перенеси встречу с Иваном на 16",
            "meeting_update_new_time": "16:00",
            "meeting_update_has_changes": True,
            "start_at_date": "",
            "start_at_time": "16:00",
            "duration_minutes": "",
        },
    }
    db = _FakeDb(
        session={
            "intent": "meeting.update",
            "missing_field": "meeting_update_target_ref",
            "idempotency_key": "idem-meeting-update-target-ref",
            "payload": dict(payload),
        }
    )

    out = handler.handle_user_attempt(
        request_id="r-meeting-update-target-ref",
        user_id="u-meeting-update-target-ref",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="встречу в 14",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-meeting-update-target-ref",
        source_message_id="msg-meeting-update-target-ref",
    )

    assert out["outcome"] == "rec_needs_clarification"
    assert calls == [{"user_id": "u-meeting-update-target-ref", "target_hint": "14:00"}]
    assert out["rec"]["missing_field"] == "awaiting_target_confirm"
    assert "Вы имеете в виду встречу" in str(out.get("clarifying_question") or "")
    assert str(out["command"].get("intent") or "") == "meeting.update"
    assert str(out["command"].get("start_at_time") or "") == "16:00"
    assert str(out["command"].get("calendar_event_id") or "") == "evt-found-14"
    assert str(out["command"].get("start_at_date") or "") == "2026-04-13"
    assert str(out["command"].get("duration_minutes") or "") == "60"
    assert str(out["command"].get("meeting_update_new_time") or "") == "16:00"
    assert str(out["command"].get("__meeting_update_stage") or "") == "awaiting_target_confirm"
    assert rec.calls == 0


def test_meeting_update_target_identification_by_text_hint_uses_normalized_query() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    calls: list[dict[str, str]] = []

    def _fake_repeat_search(user_id: str, target_hint: str) -> dict:
        calls.append({"user_id": user_id, "target_hint": target_hint})
        return {
            "ok": True,
            "candidate": {
                "calendar_event_id": "evt-ivan",
                "title": "Встреча с Иваном",
                "start_at_date": "2026-04-13",
                "start_at_time": "14:00",
                "duration_minutes": 60,
            },
        }

    handler._meeting_update_repeat_search = _fake_repeat_search  # type: ignore[attr-defined]
    payload = {
        "intent": "meeting.update",
        "start_at_time": "16:00",
        "__source_text": "перенеси встречу на 16",
        "entities": {
            "text": "перенеси встречу на 16",
            "meeting_update_new_time": "16:00",
            "meeting_update_has_changes": True,
            "start_at_time": "16:00",
        },
    }
    db = _FakeDb(
        session={
            "intent": "meeting.update",
            "missing_field": "meeting_update_target_ref",
            "idempotency_key": "idem-meeting-update-target-ivan",
            "payload": dict(payload),
        }
    )

    out = handler.handle_user_attempt(
        request_id="r-meeting-update-target-ivan",
        user_id="u-meeting-update-target-ivan",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="с Иваном",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-meeting-update-target-ivan",
        source_message_id="msg-meeting-update-target-ivan",
    )

    assert out["outcome"] == "rec_needs_clarification"
    assert out["rec"]["missing_field"] == "awaiting_target_confirm"
    assert calls == [{"user_id": "u-meeting-update-target-ivan", "target_hint": "иваном"}]
    assert str(out["command"].get("intent") or "") == "meeting.update"
    assert str(out["command"].get("calendar_event_id") or "") == "evt-ivan"
    assert str(out["command"].get("meeting_update_new_time") or "") == "16:00"
    assert rec.calls == 0


def test_meeting_update_same_date_phrase_reuses_existing_date_without_reask() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    payload = {
        "intent": "meeting.update",
        "calendar_event_id": "evt-1",
        "__meeting_update_target_confirmed": True,
        "start_at_date": "2026-04-13",
        "start_at_time": "16:00",
        "duration_minutes": 60,
        "__source_text": "перенеси встречу",
        "entities": {
            "text": "перенеси встречу",
            "calendar_event_id": "evt-1",
            "meeting_update_source_event_id": "evt-1",
            "meeting_update_source_date": "2026-04-13",
            "meeting_update_source_time": "14:00",
            "meeting_update_source_duration": 60,
            "start_at_date": "2026-04-13",
            "start_at_time": "16:00",
            "duration_minutes": 60,
            "meeting_update_has_changes": True,
        },
    }
    db = _FakeDb(
        session={
            "intent": "meeting.update",
            "missing_field": "start_at_date",
            "idempotency_key": "idem-meeting-update-same-date",
            "payload": dict(payload),
        }
    )

    out = handler.handle_user_attempt(
        request_id="r-meeting-update-same-date",
        user_id="u-meeting-update-same-date",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="эта же дата",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-meeting-update-same-date",
        source_message_id="msg-meeting-update-same-date",
    )

    assert out["outcome"] == "rec_needs_clarification"
    assert out["rec"]["missing_field"] == "temporal_commit_confirm"
    assert "На какую дату" not in str(out.get("clarifying_question") or "")
    assert str(out["command"].get("start_at_date") or "") == "2026-04-13"
    assert rec.calls == 0


def test_meeting_update_target_repeat_search_not_found_does_not_show_final_summary() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)

    def _fake_repeat_search(user_id: str, target_hint: str) -> dict:  # noqa: ARG001
        return {"ok": False, "reason": "not_found", "candidates": []}

    handler._meeting_update_repeat_search = _fake_repeat_search  # type: ignore[attr-defined]
    payload = {
        "intent": "meeting.update",
        "start_at_time": "16:00",
        "__source_text": "перенеси встречу на 16",
        "entities": {
            "text": "перенеси встречу на 16",
            "meeting_update_new_time": "16:00",
            "meeting_update_has_changes": True,
            "start_at_time": "16:00",
        },
    }
    db = _FakeDb(
        session={
            "intent": "meeting.update",
            "missing_field": "meeting_update_target_ref",
            "idempotency_key": "idem-meeting-update-target-not-found",
            "payload": dict(payload),
        }
    )

    out = handler.handle_user_attempt(
        request_id="r-meeting-update-target-not-found",
        user_id="u-meeting-update-target-not-found",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="встречу в 14",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-meeting-update-target-not-found",
        source_message_id="msg-meeting-update-target-not-found",
    )

    assert out["outcome"] == "rec_needs_clarification"
    assert out["rec"]["missing_field"] == "meeting_update_target_ref"
    q = str(out.get("clarifying_question") or "")
    assert "уточните, какую встречу" in q.lower()
    assert "Дата: -" not in q
    assert "Время: -" not in q
    assert "Длительность: -" not in q
    assert rec.calls == 0


def test_meeting_update_multiple_candidates_shows_numbered_shortlist() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)

    def _fake_repeat_search(user_id: str, target_hint: str) -> dict:  # noqa: ARG001
        return {
            "ok": False,
            "reason": "multiple",
            "candidates": [
                {
                    "calendar_event_id": "evt-1",
                    "title": "Встреча с Иваном",
                    "description": "Проект А",
                    "start_at_date": "2026-04-13",
                    "start_at_time": "14:00",
                    "duration_minutes": 60,
                },
                {
                    "calendar_event_id": "evt-2",
                    "title": "Встреча с Анной",
                    "description": "Проект Б",
                    "start_at_date": "2026-04-13",
                    "start_at_time": "16:00",
                    "duration_minutes": 30,
                },
            ],
        }

    handler._meeting_update_repeat_search = _fake_repeat_search  # type: ignore[attr-defined]
    payload = {
        "intent": "meeting.update",
        "start_at_time": "16:00",
        "__source_text": "перенеси встречу на 16",
        "entities": {"meeting_update_new_time": "16:00", "start_at_time": "16:00"},
    }
    db = _FakeDb(
        session={
            "intent": "meeting.update",
            "missing_field": "meeting_update_target_ref",
            "idempotency_key": "idem-meeting-update-multiple",
            "payload": dict(payload),
        }
    )

    out = handler.handle_user_attempt(
        request_id="r-meeting-update-multiple",
        user_id="u-meeting-update-multiple",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="встречу в 14",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-meeting-update-multiple",
        source_message_id="msg-meeting-update-multiple",
    )

    assert out["outcome"] == "rec_needs_clarification"
    assert out["rec"]["missing_field"] == "meeting_update_target_select"
    q = str(out.get("clarifying_question") or "")
    assert "1. Встреча с Иваном" in q
    assert "2. Встреча с Анной" in q
    assert rec.calls == 0


def test_meeting_update_shortlist_titles_are_humanized_without_duplicate_event_words() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)

    def _fake_repeat_search(user_id: str, target_hint: str) -> dict:  # noqa: ARG001
        return {
            "ok": False,
            "reason": "multiple",
            "candidates": [
                {
                    "calendar_event_id": "evt-1",
                    "title": "Встреча встречу с Иваном",
                    "description": "Проект А",
                    "meeting_kind": "встреча",
                    "start_at_date": "2026-04-13",
                    "start_at_time": "14:00",
                    "duration_minutes": 60,
                },
                {
                    "calendar_event_id": "evt-2",
                    "title": "Собрание собрание по найму",
                    "description": "Проект Б",
                    "meeting_kind": "собрание",
                    "start_at_date": "2026-04-13",
                    "start_at_time": "16:00",
                    "duration_minutes": 30,
                },
            ],
        }

    handler._meeting_update_repeat_search = _fake_repeat_search  # type: ignore[attr-defined]
    payload = {
        "intent": "meeting.update",
        "start_at_time": "16:00",
        "__source_text": "перенеси встречу на 16",
        "entities": {"meeting_update_new_time": "16:00", "start_at_time": "16:00"},
    }
    db = _FakeDb(
        session={
            "intent": "meeting.update",
            "missing_field": "meeting_update_target_ref",
            "idempotency_key": "idem-meeting-update-multiple-title-clean",
            "payload": dict(payload),
        }
    )

    out = handler.handle_user_attempt(
        request_id="r-meeting-update-multiple-title-clean",
        user_id="u-meeting-update-multiple-title-clean",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="встречу в 14",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-meeting-update-multiple-title-clean",
        source_message_id="msg-meeting-update-multiple-title-clean",
    )

    assert out["outcome"] == "rec_needs_clarification"
    q = str(out.get("clarifying_question") or "")
    assert "Встреча встречу" not in q
    assert "Собрание собрание" not in q
    assert "1. Встреча с Иваном" in q
    assert "2. Собрание по найму" in q
    assert rec.calls == 0


def test_meeting_update_participant_anchor_with_single_candidate_goes_to_target_confirm() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)

    def _fake_repeat_search(user_id: str, target_hint: str, meeting_kind: str = "") -> dict:  # noqa: ARG001
        return {
            "ok": True,
            "candidate": {
                "calendar_event_id": "evt-ivan-1",
                "title": "Созвон с Иваном",
                "description": "Статус",
                "meeting_kind": "созвон",
                "start_at_date": "2026-04-16",
                "start_at_time": "17:00",
                "duration_minutes": 30,
            },
            "candidates": [
                {
                    "calendar_event_id": "evt-ivan-1",
                    "title": "Созвон с Иваном",
                    "description": "Статус",
                    "meeting_kind": "созвон",
                    "start_at_date": "2026-04-16",
                    "start_at_time": "17:00",
                    "duration_minutes": 30,
                }
            ],
        }

    handler._meeting_update_repeat_search = _fake_repeat_search  # type: ignore[attr-defined]
    db = _FakeDb(session=None)
    llm = _StaticLLM({"intent": "unknown", "entities": {"text": "измени созвон с Иваном"}})

    result = handler.handle_user_attempt(
        request_id="r-guard-anchor-single-ivan",
        user_id="u-guard-anchor-single-ivan",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=llm,
        rec_client=rec,
        text="измени созвон с Иваном",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-guard-anchor-single-ivan",
        source_message_id="msg-guard-anchor-single-ivan",
    )

    assert result["outcome"] == "rec_needs_clarification"
    assert result["rec"]["missing_field"] == "awaiting_target_confirm"
    assert str(result["command"].get("calendar_event_id") or "") == "evt-ivan-1"
    assert rec.calls == 0


def test_meeting_update_candidate_selection_by_index_selects_candidate() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    payload = {
        "intent": "meeting.update",
        "start_at_time": "16:00",
        "meeting_update_new_time": "16:00",
        "__source_text": "перенеси встречу на 16",
        "__meeting_update_candidates": [
            {
                "calendar_event_id": "evt-1",
                "title": "Встреча с Иваном",
                "description": "Проект А",
                "start_at_date": "2026-04-13",
                "start_at_time": "14:00",
                "duration_minutes": 60,
            },
            {
                "calendar_event_id": "evt-2",
                "title": "Встреча с Анной",
                "description": "Проект Б",
                "start_at_date": "2026-04-13",
                "start_at_time": "16:00",
                "duration_minutes": 30,
            },
        ],
        "entities": {"meeting_update_new_time": "16:00", "start_at_time": "16:00"},
    }
    db = _FakeDb(
        session={
            "intent": "meeting.update",
            "missing_field": "meeting_update_target_select",
            "idempotency_key": "idem-meeting-update-select-index",
            "payload": dict(payload),
        }
    )

    out = handler.handle_user_attempt(
        request_id="r-meeting-update-select-index",
        user_id="u-meeting-update-select-index",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="1",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-meeting-update-select-index",
        source_message_id="msg-meeting-update-select-index",
    )

    assert out["outcome"] == "rec_needs_clarification"
    assert out["rec"]["missing_field"] == "awaiting_target_confirm"
    assert str(out["command"].get("calendar_event_id") or "") == "evt-1"
    assert str(out["command"].get("meeting_update_new_time") or "") == "16:00"
    assert rec.calls == 0


def test_meeting_update_candidate_selection_by_text_and_time_selects_candidate() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    base_payload = {
        "intent": "meeting.update",
        "start_at_time": "16:00",
        "meeting_update_new_time": "16:00",
        "__source_text": "перенеси встречу на 16",
        "__meeting_update_candidates": [
            {
                "calendar_event_id": "evt-1",
                "title": "Встреча с Иваном",
                "description": "Проект А",
                "start_at_date": "2026-04-13",
                "start_at_time": "14:00",
                "duration_minutes": 60,
            },
            {
                "calendar_event_id": "evt-2",
                "title": "Встреча с Анной",
                "description": "Проект Б",
                "start_at_date": "2026-04-13",
                "start_at_time": "16:00",
                "duration_minutes": 30,
            },
        ],
        "entities": {"meeting_update_new_time": "16:00", "start_at_time": "16:00"},
    }

    db_text = _FakeDb(
        session={
            "intent": "meeting.update",
            "missing_field": "meeting_update_target_select",
            "idempotency_key": "idem-meeting-update-select-text",
            "payload": dict(base_payload),
        }
    )
    by_text = handler.handle_user_attempt(
        request_id="r-meeting-update-select-text",
        user_id="u-meeting-update-select-text",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="с Иваном",
        local_db=db_text,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-meeting-update-select-text",
        source_message_id="msg-meeting-update-select-text",
    )
    assert by_text["rec"]["missing_field"] == "awaiting_target_confirm"
    assert str(by_text["command"].get("calendar_event_id") or "") == "evt-1"

    db_time = _FakeDb(
        session={
            "intent": "meeting.update",
            "missing_field": "meeting_update_target_select",
            "idempotency_key": "idem-meeting-update-select-time",
            "payload": dict(base_payload),
        }
    )
    by_time = handler.handle_user_attempt(
        request_id="r-meeting-update-select-time",
        user_id="u-meeting-update-select-time",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="в 14",
        local_db=db_time,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-meeting-update-select-time",
        source_message_id="msg-meeting-update-select-time",
    )
    assert by_time["rec"]["missing_field"] == "awaiting_target_confirm"
    assert str(by_time["command"].get("calendar_event_id") or "") == "evt-1"
    assert rec.calls == 0


def test_meeting_update_no_candidates_enters_refine_state_with_explicit_hint_prompt() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    calls: list[tuple[str, str, str]] = []

    def _fake_repeat_search(user_id: str, target_hint: str, meeting_kind: str = "") -> dict:
        calls.append((user_id, target_hint, meeting_kind))
        return {"ok": False, "reason": "not_found", "candidates": []}

    handler._meeting_update_repeat_search = _fake_repeat_search  # type: ignore[attr-defined]
    payload = {
        "intent": "meeting.update",
        "__source_text": "измени созвон",
        "entities": {"text": "измени созвон"},
    }
    db = _FakeDb(
        session={
            "intent": "meeting.update",
            "missing_field": "meeting_update_target_ref",
            "idempotency_key": "idem-meeting-update-refine-not-found",
            "payload": dict(payload),
        }
    )

    out = handler.handle_user_attempt(
        request_id="r-meeting-update-refine-not-found",
        user_id="u-meeting-update-refine-not-found",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="созвон с Иваном",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-meeting-update-refine-not-found",
        source_message_id="msg-meeting-update-refine-not-found",
    )

    assert out["outcome"] == "rec_needs_clarification"
    assert out["rec"]["missing_field"] == "meeting_update_target_refine"
    q = str(out.get("clarifying_question") or "").lower()
    assert "не нашёл подходящее событие" in q
    assert "уточните дату и время" in q
    assert calls
    assert rec.calls == 0


def test_meeting_update_shortlist_rejected_by_user_enters_refine_state() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    payload = {
        "intent": "meeting.update",
        "__source_text": "измени собрание",
        "__meeting_update_candidates": [
            {
                "calendar_event_id": "evt-1",
                "title": "Собрание по плану",
                "description": "",
                "start_at_date": "2026-04-16",
                "start_at_time": "13:00",
                "duration_minutes": 30,
            },
            {
                "calendar_event_id": "evt-2",
                "title": "Собрание по бюджету",
                "description": "",
                "start_at_date": "2026-04-17",
                "start_at_time": "15:00",
                "duration_minutes": 30,
            },
        ],
        "entities": {"text": "измени собрание"},
    }
    db = _FakeDb(
        session={
            "intent": "meeting.update",
            "missing_field": "meeting_update_target_select",
            "idempotency_key": "idem-meeting-update-refine-select-reject",
            "payload": dict(payload),
        }
    )

    out = handler.handle_user_attempt(
        request_id="r-meeting-update-refine-select-reject",
        user_id="u-meeting-update-refine-select-reject",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="ни один",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-meeting-update-refine-select-reject",
        source_message_id="msg-meeting-update-refine-select-reject",
    )

    assert out["outcome"] == "rec_needs_clarification"
    assert out["rec"]["missing_field"] == "meeting_update_target_refine"
    q = str(out.get("clarifying_question") or "").lower()
    assert "не нашёл подходящее событие" in q
    assert rec.calls == 0


def test_meeting_update_refine_state_restarts_repeat_search_with_new_hint() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    calls: list[tuple[str, str, str]] = []

    def _fake_repeat_search(user_id: str, target_hint: str, meeting_kind: str = "") -> dict:
        calls.append((user_id, target_hint, meeting_kind))
        return {
            "ok": True,
            "candidate": {
                "calendar_event_id": "evt-13",
                "title": "Собрание статуса",
                "start_at_date": "2026-04-16",
                "start_at_time": "13:00",
                "duration_minutes": 30,
                "meeting_kind": "собрание",
            },
            "candidates": [
                {
                    "calendar_event_id": "evt-13",
                    "title": "Собрание статуса",
                    "start_at_date": "2026-04-16",
                    "start_at_time": "13:00",
                    "duration_minutes": 30,
                    "meeting_kind": "собрание",
                }
            ],
        }

    handler._meeting_update_repeat_search = _fake_repeat_search  # type: ignore[attr-defined]
    payload = {
        "intent": "meeting.update",
        "__source_text": "измени собрание",
        "entities": {"text": "измени собрание"},
    }
    db = _FakeDb(
        session={
            "intent": "meeting.update",
            "missing_field": "meeting_update_target_refine",
            "idempotency_key": "idem-meeting-update-refine-retry",
            "payload": dict(payload),
        }
    )

    out = handler.handle_user_attempt(
        request_id="r-meeting-update-refine-retry",
        user_id="u-meeting-update-refine-retry",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="16 числа в 13:00",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-meeting-update-refine-retry",
        source_message_id="msg-meeting-update-refine-retry",
    )

    assert out["outcome"] == "rec_needs_clarification"
    assert out["rec"]["missing_field"] == "awaiting_target_confirm"
    assert calls
    assert "16" in calls[-1][1]
    assert rec.calls == 0


def test_meeting_update_continuation_keeps_intent_and_never_switches_to_create() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    payload = {
        "intent": "meeting.create",
        "calendar_event_id": "evt-1",
        "__meeting_update_target_confirmed": True,
        "start_at_date": "2026-04-13",
        "duration_minutes": 60,
        "__source_text": "перенеси встречу на 16",
        "entities": {
            "text": "перенеси встречу на 16",
            "calendar_event_id": "evt-1",
            "meeting_update_source_event_id": "evt-1",
            "meeting_update_source_date": "2026-04-13",
            "meeting_update_source_time": "14:00",
            "meeting_update_source_duration": 60,
            "start_at_date": "2026-04-13",
            "duration_minutes": 60,
        },
    }
    db = _FakeDb(
        session={
            "intent": "meeting.update",
            "missing_field": "start_at_time",
            "idempotency_key": "idem-meeting-update-intent-lock",
            "payload": dict(payload),
        }
    )

    out = handler.handle_user_attempt(
        request_id="r-meeting-update-intent-lock",
        user_id="u-meeting-update-intent-lock",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="16",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-meeting-update-intent-lock",
        source_message_id="msg-meeting-update-intent-lock",
    )

    assert out["outcome"] == "rec_needs_clarification"
    assert str(out["command"].get("intent") or "") == "meeting.update"
    assert out["rec"]["missing_field"] in {"start_at_time", "temporal_commit_confirm"}
    assert rec.calls == 0


def test_meeting_update_confirm_yes_routes_to_execution_not_fallback() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(
        session={
            "intent": "meeting.update",
            "missing_field": "temporal_commit_confirm",
            "idempotency_key": "idem-meeting-update-confirm",
                "payload": {
                    "intent": "meeting.update",
                    "calendar_event_id": "evt-1",
                    "__meeting_update_target_confirmed": True,
                    "start_at_date": "2026-04-13",
                    "start_at_time": "16:00",
                    "duration_minutes": 60,
                    "__source_text": "перенеси встречу на 16",
                    "entities": {
                        "text": "перенеси встречу на 16",
                        "calendar_event_id": "evt-1",
                        "meeting_update_source_event_id": "evt-1",
                        "meeting_update_source_date": "2026-04-13",
                        "meeting_update_source_time": "14:00",
                        "meeting_update_source_duration": 60,
                        "start_at_date": "2026-04-13",
                        "start_at_time": "16:00",
                        "duration_minutes": 60,
                        "meeting_update_has_changes": True,
                    },
            },
        }
    )

    result = handler.handle_user_attempt(
        request_id="r-meeting-update-confirm-yes",
        user_id="u-meeting-update-confirm-yes",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="Да",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-meeting-update-confirm-yes",
        source_message_id="msg-meeting-update-confirm-yes",
    )

    assert result["outcome"] == "success"
    assert result["rec"]["outcome"] == "skipped"
    assert str(result["command"].get("intent") or "") == "meeting.update"
    assert "уточните, какую команду выполнить" not in str(result.get("user_message") or "").lower()
    assert rec.calls == 0


def test_confirm_intent_lock_keeps_meeting_create_when_payload_mutated_to_update() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(
        session={
            "intent": "meeting.create",
            "missing_field": "temporal_commit_confirm",
            "idempotency_key": "idem-confirm-intent-lock-create",
                "payload": {
                    "intent": "meeting.update",
                    "calendar_event_id": "evt-1",
                    "__source_text": "встреча 2026-04-13 в 16:00 на 60 минут",
                    "start_at_date": "2026-04-13",
                    "start_at_time": "16:00",
                    "duration_minutes": 60,
                    "entities": {
                        "text": "встреча 2026-04-13 в 16:00 на 60 минут",
                        "start_at_date": "2026-04-13",
                        "start_at_time": "16:00",
                        "duration_minutes": 60,
                        "meeting_update_has_changes": True,
                    },
            },
        }
    )

    result = handler.handle_user_attempt(
        request_id="r-confirm-intent-lock-create",
        user_id="u-confirm-intent-lock-create",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="да",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-confirm-intent-lock-create",
        source_message_id="msg-confirm-intent-lock-create",
    )

    assert result["outcome"] == "success"
    assert result["rec"]["outcome"] == "skipped"
    assert str(result["command"].get("intent") or "") == "meeting.create"
    assert rec.calls == 0


def test_confirm_intent_lock_keeps_meeting_update_when_payload_mutated_to_create() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(
        session={
            "intent": "meeting.update",
            "missing_field": "temporal_commit_confirm",
            "idempotency_key": "idem-confirm-intent-lock-update",
            "payload": {
                "intent": "meeting.create",
                "calendar_event_id": "evt-1",
                "__meeting_update_target_confirmed": True,
                "start_at_date": "2026-04-13",
                "start_at_time": "16:00",
                "duration_minutes": 60,
                "entities": {
                    "text": "подтверждаю",
                    "calendar_event_id": "evt-1",
                    "meeting_update_source_event_id": "evt-1",
                    "meeting_update_source_date": "2026-04-13",
                    "meeting_update_source_time": "14:00",
                    "meeting_update_source_duration": 60,
                    "start_at_date": "2026-04-13",
                    "start_at_time": "16:00",
                    "duration_minutes": 60,
                    "meeting_update_has_changes": True,
                },
            },
        }
    )

    result = handler.handle_user_attempt(
        request_id="r-confirm-intent-lock-update",
        user_id="u-confirm-intent-lock-update",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="yes",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-confirm-intent-lock-update",
        source_message_id="msg-confirm-intent-lock-update",
    )

    assert result["outcome"] == "success"
    assert result["rec"]["outcome"] == "skipped"
    assert str(result["command"].get("intent") or "") == "meeting.update"
    assert rec.calls == 0


def test_place_block_clarification_chain_canonicalizes_to_create_timeblock_and_executes() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(
        session={
            "intent": "place_block",
            "missing_field": "start_at_time",
            "idempotency_key": "idem-11",
            "payload": {
                "intent": "place_block",
                "__source_text": "Поставь блок на завтра",
                "start_at": "завтра",
            },
        }
    )

    first = handler.handle_user_attempt(
        request_id="r11-1",
        user_id="u11",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="в 15:00",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-11",
        source_message_id="msg-11-1",
    )

    assert first["outcome"] == "rec_needs_clarification"
    assert first["rec"]["outcome"] == "needs_clarification"
    assert first["rec"]["missing_field"] == "duration_minutes"
    assert first["command"]["intent"] == "create_timeblock"

    db.session = {
        "intent": str(first["command"].get("intent") or ""),
        "missing_field": str(first["rec"].get("missing_field") or ""),
        "idempotency_key": "idem-11",
        "payload": dict(first["command"]),
    }

    second = handler.handle_user_attempt(
        request_id="r11-2",
        user_id="u11",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="30",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-11",
        source_message_id="msg-11-2",
    )

    assert second["outcome"] == "success"
    assert second["rec"]["outcome"] == "skipped"
    assert second["command"]["intent"] == "create_timeblock"
    assert second["command"]["duration_minutes"] == 30
    assert rec.calls == 0


def test_meeting_comment_update_with_comment_text_skips_comment_reask() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)

    def _fake_search(user_id: str, target_hint: str, meeting_kind: str = "") -> dict:  # noqa: ARG001
        return {
            "ok": True,
            "candidate": {
                "calendar_event_id": "evt-comment-1",
                "title": "Созвон с Иваном",
                "meeting_kind": "созвон",
                "start_at_date": "2026-04-13",
                "start_at_time": "15:00",
                "duration_minutes": 30,
            },
        }

    handler._meeting_comment_repeat_search = _fake_search  # type: ignore[attr-defined]
    llm = _StaticLLM(
        {
            "intent": "meeting.comment.update",
            "entities": {
                "text": "в созвоне с Иваном добавь комментарий взять цифры",
                "target_hint": "с Иваном",
                "comment_text": "взять цифры",
                "meeting_kind": "созвон",
            },
        }
    )
    db = _FakeDb(session=None)

    first = handler.handle_user_attempt(
        request_id="r-meeting-comment-1",
        user_id="u-meeting-comment-1",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=llm,
        rec_client=rec,
        text="в созвоне с Иваном добавь комментарий взять цифры",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-meeting-comment-1",
        source_message_id="msg-meeting-comment-1",
    )
    assert first["outcome"] == "rec_needs_clarification"
    assert first["rec"]["missing_field"] == "meeting_comment_target_confirm"
    assert str(first["command"].get("comment_text") or "") == "взять цифры"

    db.session = {
        "intent": "meeting.comment.update",
        "missing_field": "meeting_comment_target_confirm",
        "idempotency_key": "idem-meeting-comment-1",
        "payload": dict(first["command"]),
    }
    second = handler.handle_user_attempt(
        request_id="r-meeting-comment-2",
        user_id="u-meeting-comment-1",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="да",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-meeting-comment-1",
        source_message_id="msg-meeting-comment-2",
    )
    assert second["outcome"] == "rec_needs_clarification"
    assert second["rec"]["missing_field"] == "meeting_comment_final_confirm"
    assert "Комментарий: взять цифры" in str(second.get("clarifying_question") or "")


def test_meeting_comment_update_without_comment_text_asks_for_comment() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)

    def _fake_search(user_id: str, target_hint: str, meeting_kind: str = "") -> dict:  # noqa: ARG001
        return {
            "ok": True,
            "candidate": {
                "calendar_event_id": "evt-comment-2",
                "title": "Встреча по проекту",
                "meeting_kind": "встреча",
                "start_at_date": "2026-04-13",
                "start_at_time": "15:00",
                "duration_minutes": 60,
            },
        }

    handler._meeting_comment_repeat_search = _fake_search  # type: ignore[attr-defined]
    llm = _StaticLLM(
        {
            "intent": "meeting.comment.update",
            "entities": {
                "text": "в встрече по проекту измени комментарий",
                "target_hint": "по проекту",
                "meeting_kind": "встреча",
            },
        }
    )
    db = _FakeDb(session=None)

    first = handler.handle_user_attempt(
        request_id="r-meeting-comment-wo-1",
        user_id="u-meeting-comment-wo",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=llm,
        rec_client=rec,
        text="в встрече по проекту измени комментарий",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-meeting-comment-wo",
        source_message_id="msg-meeting-comment-wo-1",
    )
    assert first["rec"]["missing_field"] == "meeting_comment_target_confirm"

    db.session = {
        "intent": "meeting.comment.update",
        "missing_field": "meeting_comment_target_confirm",
        "idempotency_key": "idem-meeting-comment-wo",
        "payload": dict(first["command"]),
    }
    second = handler.handle_user_attempt(
        request_id="r-meeting-comment-wo-2",
        user_id="u-meeting-comment-wo",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="да",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-meeting-comment-wo",
        source_message_id="msg-meeting-comment-wo-2",
    )
    assert second["outcome"] == "rec_needs_clarification"
    assert second["rec"]["missing_field"] == "meeting_comment_text"


def test_task_comment_update_flow_asks_final_confirm_after_target_and_comment() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)

    def _fake_task_search(user_id: str, target_hint: str) -> dict:  # noqa: ARG001
        return {
            "ok": True,
            "candidate": {"task_id": 42, "title": "Подготовить отчёт", "comment": ""},
        }

    handler._task_comment_repeat_search = _fake_task_search  # type: ignore[attr-defined]
    llm = _StaticLLM(
        {
            "intent": "task.comment.update",
            "entities": {
                "text": "в задаче подготовить отчёт добавь комментарий отправить до 18:00",
                "task_ref": "подготовить отчёт",
                "comment_text": "отправить до 18:00",
            },
        }
    )
    db = _FakeDb(session=None)

    first = handler.handle_user_attempt(
        request_id="r-task-comment-1",
        user_id="u-task-comment-1",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=llm,
        rec_client=rec,
        text="в задаче подготовить отчёт добавь комментарий отправить до 18:00",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-task-comment-1",
        source_message_id="msg-task-comment-1",
    )
    assert first["rec"]["missing_field"] == "task_comment_target_confirm"

    db.session = {
        "intent": "task.comment.update",
        "missing_field": "task_comment_target_confirm",
        "idempotency_key": "idem-task-comment-1",
        "payload": dict(first["command"]),
    }
    second = handler.handle_user_attempt(
        request_id="r-task-comment-2",
        user_id="u-task-comment-1",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="да",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-task-comment-1",
        source_message_id="msg-task-comment-2",
    )
    assert second["outcome"] == "rec_needs_clarification"
    assert second["rec"]["missing_field"] == "task_comment_final_confirm"

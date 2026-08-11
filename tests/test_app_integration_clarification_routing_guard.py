from __future__ import annotations

import importlib.util
import sys
import types
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pytest


ROOT = Path(__file__).resolve().parents[1]
APP_SRC = ROOT / "telegram-bot" / "app-integration" / "src"
HANDLER_PATH = APP_SRC / "app" / "handler.py"
UPDATE_MAPPER_PATH = APP_SRC / "integrations" / "telegram" / "update_mapper.py"
_RU_MONTH_NAMES = (
    "января",
    "февраля",
    "марта",
    "апреля",
    "мая",
    "июня",
    "июля",
    "августа",
    "сентября",
    "октября",
    "ноября",
    "декабря",
)


def _date_iso(days_from_today: int) -> str:
    return (date.today() + timedelta(days=days_from_today)).isoformat()


def _dt_iso(days_from_today: int, hour: int) -> str:
    return f"{_date_iso(days_from_today)}T{hour:02d}:00:00+03:00"


def _future_day_month_text(days_from_today: int) -> str:
    future_date = date.today() + timedelta(days=days_from_today)
    return f"{future_date.day} {_RU_MONTH_NAMES[future_date.month - 1]}"


def _date_ru(days_from_today: int) -> str:
    future_date = date.today() + timedelta(days=days_from_today)
    return future_date.strftime("%d.%m.%Y")


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


class _RecEmpty:
    def __init__(self) -> None:
        self.calls = 0

    def query(self, payload: dict) -> dict:  # noqa: ARG002
        self.calls += 1
        return {"outcome": "empty", "hits": []}


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

    assert result["outcome"] == "rec_needs_clarification"
    assert result["rec"]["outcome"] == "needs_clarification"
    assert result["rec"]["missing_field"] == "temporal_commit_confirm"
    assert result["command"]["duration_minutes"] == 45
    assert rec.calls == 0
    assert db.upserts


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
    assert pick["rec"]["missing_field"] == "temporal_edit_field"
    assert str(pick.get("clarifying_question") or "") == "На сколько минут запланировать?"

    db.session = {
        "intent": "meeting.create",
        "missing_field": "temporal_edit_field",
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


def test_temporal_confirm_invalid_answer_does_not_rerender_full_summary_for_all_event_types() -> None:
    handler = _load_handler_module()
    kinds = ("встреча", "собрание", "созвон", "мероприятие")
    for idx, meeting_kind in enumerate(kinds, start=1):
        rec = _RecProbe(should_fail_on_call=True)
        payload = {
            "intent": "meeting.create",
            "start_at": _dt_iso(2, 15),
            "start_at_date": _date_iso(2),
            "start_at_time": "15:00",
            "duration_minutes": 30,
            "meeting_kind": meeting_kind,
            "entities": {
                "text": f"запланируй {meeting_kind} завтра в 15",
                "start_at_date": _date_iso(2),
                "start_at_time": "15:00",
                "duration_minutes": 30,
                "meeting_kind": meeting_kind,
            },
        }
        payload["__temporal_summary_signature"] = handler._temporal_summary_signature(payload)
        db = _FakeDb(
            session={
                "intent": "meeting.create",
                "missing_field": "temporal_commit_confirm",
                "idempotency_key": f"idem-temporal-confirm-invalid-{idx}",
                "payload": dict(payload),
            }
        )
        result = handler.handle_user_attempt(
            request_id=f"r-temporal-confirm-invalid-{idx}",
            user_id=f"u-temporal-confirm-invalid-{idx}",
            channel="telegram",
            asr_client=_NoopAsr(),
            llm_client=_NeverCalledLLM(),
            rec_client=rec,
            text="может быть",
            local_db=db,
            app_id="app",
            tenant_id="tenant",
            source_chat_id=f"chat-temporal-confirm-invalid-{idx}",
            source_message_id=f"msg-temporal-confirm-invalid-{idx}",
        )
        assert result["outcome"] == "rec_needs_clarification"
        assert result["rec"]["missing_field"] == "temporal_commit_confirm"
        question = str(result.get("clarifying_question") or "")
        assert "подтверд" in question.lower()
        assert "Тип:" not in question
        assert "Дата:" not in question
        assert "Время:" not in question
        assert rec.calls == 0


def test_past_date_confirm_yes_with_same_draft_does_not_rerender_full_summary() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    payload = {
        "intent": "meeting.create",
        "start_at": _dt_iso(1, 14),
        "start_at_date": _date_iso(1),
        "start_at_time": "14:00",
        "duration_minutes": 30,
        "meeting_kind": "созвон",
        "entities": {
            "text": "запланируй созвон завтра в 14",
            "start_at_date": _date_iso(1),
            "start_at_time": "14:00",
            "duration_minutes": 30,
            "meeting_kind": "созвон",
        },
    }
    payload["__temporal_summary_signature"] = handler._temporal_summary_signature(payload)
    db = _FakeDb(
        session={
            "intent": "meeting.create",
            "missing_field": "start_at_date_past_confirm",
            "idempotency_key": "idem-past-confirm-same-draft",
            "payload": dict(payload),
        }
    )
    result = handler.handle_user_attempt(
        request_id="r-past-confirm-same-draft",
        user_id="u-past-confirm-same-draft",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="да",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-past-confirm-same-draft",
        source_message_id="msg-past-confirm-same-draft",
    )
    assert result["outcome"] == "rec_needs_clarification"
    assert result["rec"]["missing_field"] == "temporal_commit_confirm"
    question = str(result.get("clarifying_question") or "")
    assert "подтверд" in question.lower()
    assert "Тип:" not in question
    assert "Дата:" not in question
    assert "Время:" not in question


def test_temporal_confirm_changed_draft_rerenders_full_summary_for_all_event_types() -> None:
    handler = _load_handler_module()
    kinds = ("встреча", "собрание", "созвон", "мероприятие")
    for idx, meeting_kind in enumerate(kinds, start=1):
        rec = _RecProbe(should_fail_on_call=True)
        payload = {
            "intent": "meeting.create",
            "start_at": _dt_iso(2, 16),
            "start_at_date": _date_iso(2),
            "start_at_time": "16:00",
            "duration_minutes": 30,
            "meeting_kind": meeting_kind,
            "entities": {
                "text": f"запланируй {meeting_kind} завтра в 16",
                "start_at_date": _date_iso(2),
                "start_at_time": "16:00",
                "duration_minutes": 30,
                "meeting_kind": meeting_kind,
            },
        }
        payload["__temporal_summary_signature"] = handler._temporal_summary_signature(payload)
        db = _FakeDb(
            session={
                "intent": "meeting.create",
                "missing_field": "temporal_commit_confirm",
                "idempotency_key": f"idem-temporal-confirm-changed-{idx}",
                "payload": dict(payload),
            }
        )
        result = handler.handle_user_attempt(
            request_id=f"r-temporal-confirm-changed-{idx}",
            user_id=f"u-temporal-confirm-changed-{idx}",
            channel="telegram",
            asr_client=_NoopAsr(),
            llm_client=_NeverCalledLLM(),
            rec_client=rec,
            text="45",
            local_db=db,
            app_id="app",
            tenant_id="tenant",
            source_chat_id=f"chat-temporal-confirm-changed-{idx}",
            source_message_id=f"msg-temporal-confirm-changed-{idx}",
        )
        assert result["outcome"] == "rec_needs_clarification"
        assert result["rec"]["missing_field"] == "temporal_commit_confirm"
        question = str(result.get("clarifying_question") or "")
        assert f"Тип: {meeting_kind}" in question
        assert "Длительность: 45 минут" in question
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
        "дата": "На какую дату запланировать?",
        "время": "На какое время запланировать?",
        "длительность": "На сколько минут запланировать?",
    }

    for text, expected_question in expectations.items():
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
        assert result["rec"]["missing_field"] == "temporal_edit_field"
        assert str(result.get("clarifying_question") or "") == expected_question
        assert db.upserts
        persisted = db.upserts[-1]["payload"]
        assert persisted["start_at"] == base_payload["start_at"]
        assert persisted["duration_minutes"] == base_payload["duration_minutes"]


def test_temporal_edit_date_then_new_summary_preserves_time_and_duration() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    future_date = date.today() + timedelta(days=5)
    future_token = f"{future_date.day:02d}.{future_date.month:02d}"
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
    assert step_pick_field["rec"]["missing_field"] == "temporal_edit_field"
    assert str(step_pick_field.get("clarifying_question") or "") == "На какую дату запланировать?"
    db.session = {
        "intent": "timeblock.create",
        "missing_field": "temporal_edit_field",
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
        text=future_token,
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-edit-summary",
        source_message_id="msg-edit-summary-new-date",
    )
    assert step_new_date["outcome"] == "rec_needs_clarification"
    assert step_new_date["rec"]["missing_field"] == "temporal_commit_confirm"
    question = str(step_new_date.get("clarifying_question") or "")
    assert f"Дата: {future_date.day:02d}.{future_date.month:02d}." in question
    assert "Время: 14:00" in question
    assert "Длительность:" in question
    updated_command = step_new_date["command"]
    assert str(updated_command.get("start_at_date")) == future_date.isoformat()
    assert str(updated_command.get("start_at_time")) == "14:00"
    assert int(updated_command.get("duration_minutes")) == 60


def test_temporal_edit_time_short_hour_updates_only_time() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    base_date = date.today() + timedelta(days=5)
    base_iso = base_date.isoformat()
    base_human = base_date.strftime("%d.%m.%Y")
    base_payload = {
        "intent": "timeblock.create",
        "start_at": f"{base_iso} 14:00",
        "start_at_date": base_iso,
        "start_at_time": "14:00",
        "duration_minutes": 60,
        "__source_text": f"назначь встречу {base_human} в 14:00 на 60 минут",
        "entities": {
            "text": f"назначь встречу {base_human} в 14:00 на 60 минут",
            "start_at_date": base_iso,
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
    assert pick["rec"]["missing_field"] == "temporal_edit_field"
    assert str(pick.get("clarifying_question") or "") == "На какое время запланировать?"

    db.session = {
        "intent": "timeblock.create",
        "missing_field": "temporal_edit_field",
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
    assert str(cmd.get("start_at_date") or "") == base_iso
    assert str(cmd.get("start_at_time") or "") == "12:00"
    assert int(cmd.get("duration_minutes") or 0) == 60
    assert str(cmd.get("start_at") or "") in {f"{base_iso} 12:00", f"{base_iso}T12:00"}
    question = str(updated.get("clarifying_question") or "")
    assert f"Дата: {base_human}" in question
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


def test_short_day_clarification_reply_materializes_to_current_month_and_year() -> None:
    handler = _load_handler_module()
    class _FrozenDate(date):
        @classmethod
        def today(cls) -> "_FrozenDate":
            return cls(2026, 4, 20)
    handler.date = _FrozenDate
    rec = _RecProbe(should_fail_on_call=True)
    today = _FrozenDate.today()
    expected = _FrozenDate(today.year, today.month, 23).isoformat()
    db = _FakeDb(
        session={
            "intent": "meeting.create",
            "missing_field": "start_at_date",
            "idempotency_key": "idem-short-day-clarification-23",
            "payload": {
                "intent": "meeting.create",
                "start_at_time": "15:00",
                "duration_minutes": 30,
                "__source_text": "запланируй встречу в 15",
                "entities": {
                    "text": "запланируй встречу в 15",
                    "start_at_time": "15:00",
                    "duration_minutes": 30,
                    "meeting_kind": "встреча",
                },
            },
        }
    )

    out = handler.handle_user_attempt(
        request_id="r-short-day-clarification-23",
        user_id="u-short-day-clarification-23",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="23",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-short-day-clarification-23",
        source_message_id="msg-short-day-clarification-23",
    )
    assert out["outcome"] == "rec_needs_clarification"
    assert out["rec"]["missing_field"] == "temporal_commit_confirm"
    cmd = out["command"]
    assert str(cmd.get("start_at_date") or "") == expected
    assert str(cmd.get("start_at_date") or "") != "2022-04-23"
    assert str(cmd.get("start_at_date") or "") != "2023-04-23"


def test_schedule_sobranie_22_aprelya_then_time_reply_keeps_current_year_in_summary() -> None:
    handler = _load_handler_module()
    class _FrozenDate(date):
        @classmethod
        def today(cls) -> "_FrozenDate":
            return cls(2026, 4, 20)
    handler.date = _FrozenDate
    llm = _StaticLLM(
        {
            "intent": "meeting.create",
            "entities": {
                "text": "запланируй собрание 22 апреля",
                "meeting_kind": "собрание",
                "start_at_date": "2022-04-22",
                "duration_minutes": 30,
            },
        }
    )
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(session=None)

    first = handler.handle_user_attempt(
        request_id="r-sobranie-22-aprelya-ask-time",
        user_id="u-sobranie-22-aprelya-ask-time",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=llm,
        rec_client=rec,
        text="запланируй собрание 22 апреля",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-sobranie-22-aprelya",
        source_message_id="msg-sobranie-22-aprelya",
    )
    assert first["outcome"] == "rec_needs_clarification"
    assert first["rec"]["missing_field"] == "start_at_time"

    db.session = {
        "intent": "meeting.create",
        "missing_field": "start_at_time",
        "idempotency_key": "idem-sobranie-22-aprelya-ask-time",
        "payload": dict(first["command"]),
    }
    second = handler.handle_user_attempt(
        request_id="r-sobranie-22-aprelya-time-15",
        user_id="u-sobranie-22-aprelya-ask-time",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="15",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-sobranie-22-aprelya",
        source_message_id="msg-sobranie-22-aprelya-2",
    )
    expected_iso = f"{_FrozenDate.today().year}-04-22"
    assert second["outcome"] == "rec_needs_clarification"
    assert second["rec"]["missing_field"] == "temporal_commit_confirm"
    cmd = second["command"]
    assert str(cmd.get("start_at_date") or "") == expected_iso
    summary = str(second.get("clarifying_question") or "")
    assert f"Дата: 22.04.{_FrozenDate.today().year}" in summary
    assert "22.04.2022" not in summary


def test_short_numeric_clarification_date_has_no_2022_2023_regression() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    today = date.today()
    expected = date(today.year, today.month, 22).isoformat()
    db = _FakeDb(
        session={
            "intent": "meeting.create",
            "missing_field": "start_at_date",
            "idempotency_key": "idem-short-day-no-2022-2023",
            "payload": {
                "intent": "meeting.create",
                "start_at_time": "11:00",
                "duration_minutes": 30,
                "__source_text": "запланируй созвон",
                "entities": {
                    "text": "запланируй созвон",
                    "meeting_kind": "созвон",
                    "start_at_time": "11:00",
                    "duration_minutes": 30,
                },
            },
        }
    )

    out = handler.handle_user_attempt(
        request_id="r-short-day-no-2022-2023",
        user_id="u-short-day-no-2022-2023",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="22",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-short-day-no-2022-2023",
        source_message_id="msg-short-day-no-2022-2023",
    )
    result_date = str(out["command"].get("start_at_date") or "")
    assert result_date == expected
    assert result_date != "2022-04-22"
    assert result_date != "2023-04-22"


def test_clarification_date_month_name_with_explicit_year_keeps_that_year() -> None:
    handler = _load_handler_module()
    base = date(2026, 4, 20)
    parsed = handler._normalize_clarification_date_to_iso("21 апреля 2021", today=base)
    assert parsed == "2021-04-21"


def test_temporal_edit_date_override_wins_over_stale_source_text_date() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    future_date = date.today() + timedelta(days=5)
    future_token = f"{future_date.day:02d}.{future_date.month:02d}"
    payload = {
        "intent": "meeting.create",
        "start_at": "2026-04-21T14:00",
        "start_at_date": "2026-04-21",
        "start_at_time": "14:00",
        "duration_minutes": 60,
        "__source_text": "запланируй встречу 21 апреля в 14:00 на 60 минут",
        "entities": {
            "text": "запланируй встречу 21 апреля в 14:00 на 60 минут",
            "start_at_date": "2026-04-21",
            "start_at_time": "14:00",
            "duration_minutes": 60,
            "meeting_kind": "встреча",
        },
    }
    db = _FakeDb(
        session={
            "intent": "meeting.create",
            "missing_field": "start_at_date",
            "idempotency_key": "idem-date-edit-override-wins",
            "payload": dict(payload),
        }
    )

    out = handler.handle_user_attempt(
        request_id="r-date-edit-override-wins",
        user_id="u-date-edit-override-wins",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text=future_token,
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-date-edit-override-wins",
        source_message_id="msg-date-edit-override-wins",
    )

    assert out["outcome"] == "rec_needs_clarification"
    assert out["rec"]["missing_field"] == "temporal_commit_confirm"
    q = str(out.get("clarifying_question") or "")
    assert f"Дата: {future_date.day:02d}.{future_date.month:02d}.{future_date.year}" in q
    assert "Время: 14:00" in q
    assert "Длительность: 1 час" in q
    cmd = out.get("command") if isinstance(out.get("command"), dict) else {}
    assert str(cmd.get("start_at_date") or "") == future_date.isoformat()
    assert str(cmd.get("start_at_time") or "") == "14:00"
    assert int(cmd.get("duration_minutes") or 0) == 60


def test_temporal_edit_date_month_name_updates_to_current_year() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    month_names = (
        "января",
        "февраля",
        "марта",
        "апреля",
        "мая",
        "июня",
        "июля",
        "августа",
        "сентября",
        "октября",
        "ноября",
        "декабря",
    )
    future_date = date.today() + timedelta(days=7)
    date_text = f"{future_date.day} {month_names[future_date.month - 1]}"
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
        text=date_text,
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-date-month-name",
        source_message_id="msg-date-month-name",
    )
    assert updated["outcome"] == "rec_needs_clarification"
    assert updated["rec"]["missing_field"] == "temporal_commit_confirm"
    cmd = updated["command"]
    expected = future_date.isoformat()
    assert str(cmd.get("start_at_date") or "") == expected
    assert f"Дата: {future_date.day:02d}.{future_date.month:02d}.{future_date.year}" in str(updated.get("clarifying_question") or "")
    assert rec.calls == 0


def test_date_from_text_is_materialized_to_entities_and_not_reasked() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(session=None)
    future_date = date.today() + timedelta(days=21)
    future_text = f"запланируй встречу {_future_day_month_text(21)} в 14:00 на 30 минут"
    llm = _StaticLLM(
        {
                "intent": "meeting.create",
                "entities": {
                        "text": future_text,
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
        text=future_text,
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
    expected_date = future_date.isoformat()
    assert str(entities.get("start_at_date") or "") == expected_date
    assert str(out["command"].get("start_at_date") or "") == expected_date
    q = str(out.get("clarifying_question") or "").lower()
    assert "какая дата" not in q
    assert rec.calls == 0


def test_meeting_kind_sobranie_materializes_date_and_does_not_reask() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(session=None)
    future_date = date.today() + timedelta(days=22)
    future_text = f"запланируй собрание {_future_day_month_text(22)} в 10 на 30 минут"
    llm = _StaticLLM(
        {
            "intent": "meeting.create",
            "entities": {
                "text": future_text,
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
        text=future_text,
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
    expected_date = future_date.isoformat()
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
    future_date = date.today() + timedelta(days=23)
    future_text = f"запланируй встречу {_future_day_month_text(23)}"
    llm = _StaticLLM(
        {
            "intent": "meeting.create",
            "entities": {
                "text": future_text,
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
        text=future_text,
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-event-create-day-month-no-year",
        source_message_id="msg-event-create-day-month-no-year",
    )
    expected = future_date.isoformat()
    assert str(out["command"].get("start_at_date") or "") == expected
    entities = out["command"].get("entities") if isinstance(out.get("command"), dict) else {}
    entities = entities if isinstance(entities, dict) else {}
    assert str(entities.get("start_at_date") or "") == expected
    assert out["rec"]["missing_field"] == "start_at_time"


def test_event_create_day_month_and_time_does_not_reask_date() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(session=None)
    future_text = f"запланируй собрание {_future_day_month_text(24)} в 11:00"
    llm = _StaticLLM(
        {
            "intent": "meeting.create",
            "entities": {
                "text": future_text,
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
        text=future_text,
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
    future_text = f"созвон {_future_day_month_text(25)}"
    llm = _StaticLLM(
        {
            "intent": "meeting.create",
            "entities": {
                "text": future_text,
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
        text=future_text,
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
    base_date = date.today() + timedelta(days=5)
    base_iso = base_date.isoformat()
    base_human = base_date.strftime("%d.%m.%Y")
    base_payload = {
        "intent": "timeblock.create",
        "start_at": f"{base_iso} 14:00",
        "start_at_date": base_iso,
        "start_at_time": "14:00",
        "duration_minutes": 60,
        "__source_text": f"назначь встречу {base_human} в 14:00 на 60 минут",
        "entities": {
            "text": f"назначь встречу {base_human} в 14:00 на 60 минут",
            "start_at_date": base_iso,
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
    assert str(cmd.get("start_at_date") or "") == base_iso
    assert str(cmd.get("start_at_time") or "") == "18:30"
    assert int(cmd.get("duration_minutes") or 0) == 60
    assert str(cmd.get("start_at") or "") in {f"{base_iso} 18:30", f"{base_iso}T18:30"}
    question = str(updated.get("clarifying_question") or "")
    assert f"Дата: {base_human}" in question
    assert "Время: 18:30" in question
    assert "Длительность: 1 час" in question
    assert rec.calls == 0


def test_temporal_confirm_yes_after_time_edit_routes_to_execution() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    base_date = date.today() + timedelta(days=5)
    base_iso = base_date.isoformat()
    base_human = base_date.strftime("%d.%m.%Y")
    base_payload = {
        "intent": "timeblock.create",
        "start_at": f"{base_iso} 14:00",
        "start_at_date": base_iso,
        "start_at_time": "14:00",
        "duration_minutes": 60,
        "__source_text": f"назначь встречу {base_human} в 14:00 на 60 минут",
        "entities": {
            "text": f"назначь встречу {base_human} в 14:00 на 60 минут",
            "start_at_date": base_iso,
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
    assert step_pick_time["rec"]["missing_field"] == "temporal_edit_field"
    assert str(step_pick_time.get("clarifying_question") or "") == "На какое время запланировать?"
    db.session = {
        "intent": "timeblock.create",
        "missing_field": "temporal_edit_field",
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
    base_date = date.today() + timedelta(days=5)
    base_iso = base_date.isoformat()
    base_human = base_date.strftime("%d.%m.%Y")
    edited_date = date.today() + timedelta(days=9)
    edited_token = edited_date.strftime("%d.%m.%Y")
    base_payload = {
        "intent": "timeblock.create",
        "start_at": f"{base_iso} 14:00",
        "start_at_date": base_iso,
        "start_at_time": "14:00",
        "duration_minutes": 60,
        "__source_text": f"назначь встречу {base_human} в 14:00 на 60 минут",
        "entities": {
            "text": f"назначь встречу {base_human} в 14:00 на 60 минут",
            "start_at_date": base_iso,
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
    assert step_pick_date["rec"]["missing_field"] == "temporal_edit_field"
    assert str(step_pick_date.get("clarifying_question") or "") == "На какую дату запланировать?"
    db.session = {
        "intent": "timeblock.create",
        "missing_field": "temporal_edit_field",
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
        text=edited_token,
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-date-edit",
        source_message_id="msg-date-edit-set",
    )
    assert step_new_date["outcome"] == "rec_needs_clarification"
    assert step_new_date["rec"]["missing_field"] == "temporal_commit_confirm"
    assert f"Дата: {edited_token}" in str(step_new_date.get("clarifying_question") or "")
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

    assert result["outcome"] == "rec_needs_clarification"
    assert result["rec"]["outcome"] == "needs_clarification"
    assert result["rec"]["missing_field"] == "task_create_confirm"
    assert result["command"]["title"] == "Купить молоко"
    assert rec.calls == 0


def test_task_create_phrase_extracts_title_and_due_date_and_shows_confirm(caplog) -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(session=None)
    llm = _StaticLLM(
        {
            "intent": "task.create",
            "entities": {
                "text": "создай задачу купить молоко завтра",
            },
        }
    )

    with caplog.at_level("INFO"):
        result = handler.handle_user_attempt(
            request_id="r-task-create-extract-title-date",
            user_id="u-task-create-extract-title-date",
            channel="telegram",
            asr_client=_NoopAsr(),
            llm_client=llm,
            rec_client=rec,
            text="создай задачу купить молоко завтра",
            local_db=db,
            app_id="app",
            tenant_id="tenant",
            source_chat_id="chat-task-create-extract-title-date",
            source_message_id="msg-task-create-extract-title-date",
        )

    assert result["outcome"] == "rec_needs_clarification"
    assert result["rec"]["missing_field"] == "task_create_confirm"
    question = str(result.get("clarifying_question") or "")
    assert "проверь задачу перед созданием" in question.lower()
    assert "завтра" not in question.lower()
    assert _date_ru(1) in question
    cmd = result.get("command") if isinstance(result.get("command"), dict) else {}
    assert str(cmd.get("intent") or "").strip().lower() == "task.create"
    assert str(cmd.get("title") or "") == "Купить молоко"
    assert str(cmd.get("due_date") or "") == _date_iso(1)
    assert str(cmd.get("planned_at") or "") == _date_iso(1)
    assert cmd.get("parent_task_id") is None
    assert cmd.get("parent_task_explicit") is False
    assert rec.calls == 0
    date_records = [r for r in caplog.records if r.getMessage() == "task_create_date_resolution"]
    assert date_records
    record = date_records[-1]
    assert getattr(record, "raw_text") == "создай задачу купить молоко завтра"
    assert getattr(record, "raw_due_date") in {"", "завтра"}
    assert getattr(record, "extracted_date") == _date_iso(1)
    assert getattr(record, "resolved_planned_at") == _date_iso(1)
    assert getattr(record, "planned_at") == _date_iso(1)


@pytest.mark.parametrize(
    ("text", "expected_title", "expected_days"),
    [
        ("купить хлеб завтра", "Купить хлеб", 1),
        ("купить цветы завтра", "Купить цветы", 1),
        ("позвонить врачу завтра", "Позвонить врачу", 1),
        ("отправить документы завтра", "Отправить документы", 1),
        ("создай задачу купить хлеб завтра", "Купить хлеб", 1),
        ("поставь задачу купить хлеб завтра", "Купить хлеб", 1),
    ],
)
def test_generic_task_phrases_with_date_are_normalized_into_task_create(
    text: str,
    expected_title: str,
    expected_days: int,
) -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(session=None)
    llm = _StaticLLM(
        {
            "intent": "unknown",
            "entities": {
                "text": text,
            },
        }
    )

    result = handler.handle_user_attempt(
        request_id=f"r-generic-task-{expected_title}",
        user_id="u-generic-task",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=llm,
        rec_client=rec,
        text=text,
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-generic-task",
        source_message_id=f"msg-generic-task-{expected_title}",
    )

    assert result["outcome"] == "rec_needs_clarification"
    assert result["rec"]["missing_field"] == "task_create_confirm"
    question = str(result.get("clarifying_question") or "")
    assert _date_ru(expected_days) in question
    assert "завтра" not in question.lower()
    cmd = result.get("command") if isinstance(result.get("command"), dict) else {}
    assert str(cmd.get("intent") or "").strip().lower() == "task.create"
    assert str(cmd.get("title") or "") == expected_title
    assert str(cmd.get("planned_at") or "") == _date_iso(expected_days)
    assert rec.calls == 0


def test_task_create_relative_due_date_from_llm_is_resolved_before_summary() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(session=None)
    llm = _StaticLLM(
        {
            "intent": "task.create",
            "entities": {
                "text": "создай задачу купить молоко завтра",
                "title": "Купить молоко",
                "due_date": "завтра",
            },
        }
    )

    result = handler.handle_user_attempt(
        request_id="r-task-create-relative-due-date",
        user_id="u-task-create-relative-due-date",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=llm,
        rec_client=rec,
        text="создай задачу купить молоко завтра",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-task-create-relative-due-date",
        source_message_id="msg-task-create-relative-due-date",
    )

    assert result["outcome"] == "rec_needs_clarification"
    assert result["rec"]["missing_field"] == "task_create_confirm"
    question = str(result.get("clarifying_question") or "")
    assert "завтра" not in question.lower()
    assert _date_ru(1) in question
    cmd = result.get("command") if isinstance(result.get("command"), dict) else {}
    assert str(cmd.get("due_date") or "") == _date_iso(1)
    assert str(cmd.get("planned_at") or "") == _date_iso(1)
    assert rec.calls == 0


def test_task_create_poslezavtra_is_resolved_before_summary() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(session=None)
    llm = _StaticLLM(
        {
            "intent": "task.create",
            "entities": {
                "text": "создай задачу купить молоко послезавтра",
                "title": "Купить молоко",
                "due_date": "послезавтра",
            },
        }
    )

    result = handler.handle_user_attempt(
        request_id="r-task-create-poslezavtra",
        user_id="u-task-create-poslezavtra",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=llm,
        rec_client=rec,
        text="создай задачу купить молоко послезавтра",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-task-create-poslezavtra",
        source_message_id="msg-task-create-poslezavtra",
    )

    assert result["outcome"] == "rec_needs_clarification"
    question = str(result.get("clarifying_question") or "")
    assert "послезавтра" not in question.lower()
    assert _date_ru(2) in question
    cmd = result.get("command") if isinstance(result.get("command"), dict) else {}
    assert str(cmd.get("planned_at") or "") == _date_iso(2)


def test_task_create_unresolved_due_date_asks_clarification_instead_of_showing_raw_summary() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(session=None)
    llm = _StaticLLM(
        {
            "intent": "task.create",
            "entities": {
                "text": "создай задачу купить молоко когда-нибудь потом",
                "title": "Купить молоко",
                "due_date": "когда-нибудь потом",
            },
        }
    )

    result = handler.handle_user_attempt(
        request_id="r-task-create-unresolved-due-date",
        user_id="u-task-create-unresolved-due-date",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=llm,
        rec_client=rec,
        text="создай задачу купить молоко когда-нибудь потом",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-task-create-unresolved-due-date",
        source_message_id="msg-task-create-unresolved-due-date",
    )

    assert result["outcome"] == "rec_needs_clarification"
    assert result["rec"]["missing_field"] == "task_create_due_date"
    question = str(result.get("clarifying_question") or "")
    assert "на какую дату" in question.lower()
    assert "когда-нибудь" not in question.lower()
    assert "создать задачу?" not in question.lower()


def test_telegram_normal_task_create_does_not_infer_subtask_parent() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(session=None)
    llm = _StaticLLM(
        {
            "intent": "task.create",
            "entities": {
                "text": "создай задачу купить молоко завтра",
                "parent_task_id": 17,
            },
        }
    )

    result = handler.handle_user_attempt(
        request_id="r-root-task-no-parent",
        user_id="u-root-task-no-parent",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=llm,
        rec_client=rec,
        text="создай задачу купить молоко завтра",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-root-task-no-parent",
        source_message_id="msg-root-task-no-parent",
    )

    assert result["outcome"] == "rec_needs_clarification"
    cmd = result.get("command") if isinstance(result.get("command"), dict) else {}
    assert str(cmd.get("intent") or "").strip().lower() == "task.create"
    assert cmd.get("parent_task_id") is None
    assert cmd.get("parent_task_explicit") is False
    assert rec.calls == 0


def test_telegram_explicit_subtask_create_sets_parent_task_id() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(session=None)
    llm = _StaticLLM(
        {
            "intent": "task.create",
            "entities": {
                "text": "создай подзадачу к задаче 17 купить молоко",
            },
        }
    )

    result = handler.handle_user_attempt(
        request_id="r-explicit-subtask-task-create",
        user_id="u-explicit-subtask-task-create",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=llm,
        rec_client=rec,
        text="создай подзадачу к задаче 17 купить молоко",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-explicit-subtask-task-create",
        source_message_id="msg-explicit-subtask-task-create",
    )

    assert result["outcome"] == "rec_needs_clarification"
    cmd = result.get("command") if isinstance(result.get("command"), dict) else {}
    assert str(cmd.get("intent") or "").strip().lower() == "task.create"
    assert int(cmd.get("parent_task_id") or 0) == 17
    assert cmd.get("parent_task_explicit") is True
    assert str(cmd.get("title") or "") == "Купить молоко"
    assert rec.calls == 0


def test_task_create_title_clarification_reply_rerenders_summary_without_memory_flow() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(
        session={
            "intent": "task.create",
            "missing_field": "title",
            "idempotency_key": "idem-task-title-rerender",
            "payload": {
                "intent": "task.create",
                "due_date": _date_iso(1),
                "planned_at": _date_iso(1),
            },
        }
    )

    result = handler.handle_user_attempt(
        request_id="r-task-title-rerender",
        user_id="u-task-title-rerender",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="Купить молоко",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-task-title-rerender",
        source_message_id="msg-task-title-rerender",
    )

    assert result["outcome"] == "rec_needs_clarification"
    assert result["rec"]["missing_field"] == "task_create_confirm"
    assert "в памяти нет данных по этому запросу" not in str(result.get("user_message") or "").lower()
    cmd = result.get("command") if isinstance(result.get("command"), dict) else {}
    assert str(cmd.get("title") or "") == "Купить молоко"
    assert str(cmd.get("due_date") or "") == _date_iso(1)
    assert rec.calls == 0


def test_task_create_edit_flow_preserves_canonical_planned_at() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(
        session={
            "intent": "task.create",
            "missing_field": "task_create_edit",
            "idempotency_key": "idem-task-edit-preserve-date",
            "payload": {
                "intent": "task.create",
                "title": "Купить",
                "due_date": _date_iso(1),
                "planned_at": _date_iso(1),
                "entities": {
                    "title": "Купить",
                    "due_date": _date_iso(1),
                    "planned_at": _date_iso(1),
                },
            },
        }
    )

    result = handler.handle_user_attempt(
        request_id="r-task-edit-preserve-date",
        user_id="u-task-edit-preserve-date",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="Купить молоко",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-task-edit-preserve-date",
        source_message_id="msg-task-edit-preserve-date",
    )

    assert result["outcome"] == "rec_needs_clarification"
    assert result["rec"]["missing_field"] == "task_create_confirm"
    question = str(result.get("clarifying_question") or "")
    assert _date_ru(1) in question
    cmd = result.get("command") if isinstance(result.get("command"), dict) else {}
    assert str(cmd.get("planned_at") or "") == _date_iso(1)
    assert str(cmd.get("due_date") or "") == _date_iso(1)
    assert rec.calls == 0


def test_memory_search_still_works_without_active_clarification(caplog) -> None:
    handler = _load_handler_module()
    rec = _RecEmpty()
    db = _FakeDb(session=None)
    llm = _StaticLLM({"intent": "unknown", "entities": {"text": "вспомни, что я говорил про старый список"}})

    with caplog.at_level("INFO"):
        result = handler.handle_user_attempt(
            request_id="r-memory-search-no-session",
            user_id="u-memory-search-no-session",
            channel="telegram",
            asr_client=_NoopAsr(),
            llm_client=llm,
            rec_client=rec,
            text="вспомни, что я говорил про старый список",
            local_db=db,
            app_id="app",
            tenant_id="tenant",
            source_chat_id="chat-memory-search-no-session",
            source_message_id="msg-memory-search-no-session",
        )

    assert result["outcome"] == "rec_empty"
    assert "в памяти нет данных по этому запросу" in str(result.get("user_message") or "").lower()
    assert rec.calls == 1
    assert any(r.getMessage() == "memory_fallback_selected" for r in caplog.records)


def test_final_rec_empty_branch_blocks_non_explicit_memory_query_and_returns_neutral_unknown(caplog) -> None:
    handler = _load_handler_module()

    with caplog.at_level("INFO"):
        result = handler._resolve_rec_empty_result(  # type: ignore[attr-defined]
            request_id="r-rec-empty-non-memory",
            query_text=".\\deploy_v2.ps1 -Services @(\"telegram-bot\")",
            rec_res={"outcome": "empty", "hits": []},
            command={"intent": "unknown", "entities": {"text": ".\\deploy_v2.ps1 -Services @(\"telegram-bot\")"}},
            explicit_memory_query=False,
            db=None,
            context_key="ctx",
            app_id="app",
            tenant_id="tenant",
            user_id="u1",
            source_message_id="m1",
            idempotency_key="idem-1",
        )

    assert result["outcome"] == "unrecognized"
    assert "Создать задачу или событие?" in str(result.get("user_message") or "")
    record = next((r for r in caplog.records if r.getMessage() == "memory_fallback_before_return"), None)
    assert record is not None
    assert getattr(record, "explicit_memory_query") in {False, "False"}
    assert getattr(record, "route_rules_version") == "stage67-hardguard-v2"


def test_final_rec_empty_branch_converts_generic_task_candidate_into_task_create(caplog) -> None:
    handler = _load_handler_module()

    with caplog.at_level("INFO"):
        result = handler._resolve_rec_empty_result(  # type: ignore[attr-defined]
            request_id="r-rec-empty-generic-task",
            query_text="купить хлеб завтра",
            rec_res={"outcome": "empty", "hits": []},
            command={"intent": "unknown", "entities": {"text": "купить хлеб завтра"}},
            explicit_memory_query=False,
            db=None,
            context_key="ctx",
            app_id="app",
            tenant_id="tenant",
            user_id="u1",
            source_message_id="m1",
            idempotency_key="idem-2",
        )

    assert result["outcome"] == "rec_needs_clarification"
    assert result["rec"]["missing_field"] == "task_create_confirm"
    assert "Купить хлеб" in str(result.get("clarifying_question") or "")
    assert "В памяти нет данных по этому запросу." not in str(result.get("user_message") or "")
    fallback_record = next((r for r in caplog.records if r.getMessage() == "generic_task_fallback_selected"), None)
    assert fallback_record is not None


def test_task_create_clarification_has_priority_over_memory_flow_even_without_payload_intent() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(
        session={
            "intent": "task.create",
            "missing_field": "title",
            "idempotency_key": "idem-task-title-priority",
            "payload": {},
        }
    )

    result = handler.handle_user_attempt(
        request_id="r-task-title-priority",
        user_id="u-task-title-priority",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="Купить молоко",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-task-title-priority",
        source_message_id="msg-task-title-priority",
    )

    assert result["outcome"] == "rec_needs_clarification"
    assert result["rec"]["missing_field"] == "task_create_confirm"
    cmd = result.get("command") if isinstance(result.get("command"), dict) else {}
    assert str(cmd.get("intent") or "").strip().lower() == "task.create"
    assert str(cmd.get("title") or "") == "Купить молоко"
    assert rec.calls == 0


def test_stale_previous_parent_context_does_not_leak_into_new_task_create() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(
        session={
            "intent": "task.create",
            "missing_field": "task_create_confirm",
            "idempotency_key": "idem-stale-parent-context",
            "payload": {
                "intent": "task.create",
                "title": "Старая задача",
                "parent_task_id": 17,
                "parent_task_explicit": True,
            },
        }
    )
    llm = _StaticLLM(
        {
            "intent": "task.create",
            "entities": {
                "text": "создай задачу купить молоко",
            },
        }
    )

    result = handler.handle_user_attempt(
        request_id="r-stale-parent-context",
        user_id="u-stale-parent-context",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=llm,
        rec_client=rec,
        text="создай задачу купить молоко",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-stale-parent-context",
        source_message_id="msg-stale-parent-context",
    )

    assert result["outcome"] == "rec_needs_clarification"
    assert result["rec"]["missing_field"] == "task_create_confirm"
    cmd = result.get("command") if isinstance(result.get("command"), dict) else {}
    assert str(cmd.get("intent") or "").strip().lower() == "task.create"
    assert cmd.get("parent_task_id") is None
    assert cmd.get("parent_task_explicit") is False
    assert str(cmd.get("title") or "") == "Купить молоко"
    assert rec.calls == 0


def test_task_create_duplicate_confirmation_reply_sets_override_and_stays_in_task_flow() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(
        session={
            "intent": "task.create",
            "missing_field": "task_create_duplicate_confirm",
            "idempotency_key": "idem-task-duplicate-confirm",
            "payload": {
                "intent": "task.create",
                "title": "Купить молоко",
                "planned_at": _date_iso(1),
                "entities": {
                    "title": "Купить молоко",
                    "planned_at": _date_iso(1),
                },
            },
        }
    )

    result = handler.handle_user_attempt(
        request_id="r-task-duplicate-confirm",
        user_id="u-task-duplicate-confirm",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="Да",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-task-duplicate-confirm",
        source_message_id="msg-task-duplicate-confirm",
    )

    cmd = result.get("command") if isinstance(result.get("command"), dict) else {}
    assert str(cmd.get("intent") or "").strip().lower() == "task.create"
    assert cmd.get("__task_create_confirmed") is True
    assert cmd.get("duplicate_check_override") is True
    assert cmd.get("entities", {}).get("duplicate_check_override") is True
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

    assert result["outcome"] == "rec_needs_clarification"
    assert result["rec"]["outcome"] == "needs_clarification"
    assert result["rec"]["missing_field"] == "temporal_commit_confirm"
    assert result["command"]["intent"] == "meeting.create"
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
    assert db.upserts[-1]["missing_field"] in {
        "temporal_commit_confirm",
        "duration_minutes",
        "start_at",
        "start_at_date",
        "start_at_time",
    }


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

    assert result["outcome"] == "rec_needs_clarification"
    assert result["rec"]["outcome"] == "needs_clarification"
    assert result["rec"]["missing_field"] == "temporal_commit_confirm"
    assert result["command"]["intent"] == "meeting.create"
    assert result["command"]["duration_minutes"] == 30
    assert rec.calls == 0
    assert db.upserts


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

    assert result["outcome"] == "rec_needs_clarification"
    assert result["rec"]["outcome"] == "needs_clarification"
    assert result["rec"]["missing_field"] == "temporal_commit_confirm"
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

    assert result["outcome"] == "rec_needs_clarification"
    assert result["rec"]["outcome"] == "needs_clarification"
    assert result["rec"]["missing_field"] == "temporal_commit_confirm"
    assert result["command"]["intent"] in {"schedule_block", "create_timeblock"}
    assert result["command"]["duration_minutes"] == 30
    assert rec.calls == 0
    assert db.upserts


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


def test_meeting_create_summary_kind_is_consistent_for_all_event_types() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(session=None)
    cases = [
        ("встреча", "запланируй встречу завтра в 11 на 30 минут"),
        ("собрание", "запланируй собрание завтра в 11 на 30 минут"),
        ("созвон", "запланируй созвон завтра в 11 на 30 минут"),
        ("мероприятие", "запланируй мероприятие завтра в 11 на 30 минут"),
    ]

    for idx, (kind, text) in enumerate(cases, start=1):
        llm = _StaticLLM(
            {
                "intent": "meeting.create",
                "entities": {
                    "text": text,
                    "start_at_date": _date_iso(1),
                    "start_at_time": "11:00",
                    "duration_minutes": 30,
                },
            }
        )
        result = handler.handle_user_attempt(
            request_id=f"r10-meeting-kind-consistency-{idx}",
            user_id=f"u10-meeting-kind-consistency-{idx}",
            channel="telegram",
            asr_client=_NoopAsr(),
            llm_client=llm,
            rec_client=rec,
            text=text,
            local_db=db,
            app_id="app",
            tenant_id="tenant",
            source_chat_id=f"chat-10-meeting-kind-consistency-{idx}",
            source_message_id=f"msg-10-meeting-kind-consistency-{idx}",
        )

        assert result["outcome"] == "rec_needs_clarification"
        assert result["rec"]["missing_field"] == "temporal_commit_confirm"
        assert f"Тип: {kind}" in str(result.get("clarifying_question") or "")
        assert str(result["command"].get("meeting_kind") or "") == kind
        entities = result["command"].get("entities")
        entities = entities if isinstance(entities, dict) else {}
        assert str(entities.get("meeting_kind") or "") == kind


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
    assert result["rec"]["missing_field"] == "meeting_update_target_refine"
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
    assert result["rec"]["missing_field"] == "meeting_update_target_ref"
    assert result["command"]["intent"] == "meeting.update"
    entities = result["command"].get("entities")
    entities = entities if isinstance(entities, dict) else {}
    assert str(entities.get("meeting_kind") or "") == "собрание"
    assert "Какое именно событие нужно изменить" in str(result.get("clarifying_question") or "")
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
    assert "Перенести её на 16:00?" not in q
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
    assert yes["rec"]["missing_field"] == "meeting_update_edit_choice"
    q = str(yes.get("clarifying_question") or "")
    assert "Что изменить?" in q
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
    assert yes["rec"]["missing_field"] == "meeting_update_edit_choice"
    q = str(yes.get("clarifying_question") or "")
    assert "Что изменить?" in q
    assert "На какую дату" not in q
    assert rec.calls == 0


def test_meeting_update_target_confirm_prompt_is_split_from_change_confirm() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)

    def _fake_repeat_search(user_id: str, target_hint: str, meeting_kind: str = "") -> dict:  # noqa: ARG001
        return {
            "ok": True,
            "candidate": {
                "calendar_event_id": "evt-split-confirm",
                "title": "Встреча с Иваном",
                "meeting_kind": "встреча",
                "start_at_date": "2026-04-29",
                "start_at_time": "16:00",
                "duration_minutes": 15,
            },
        }

    handler._meeting_update_repeat_search = _fake_repeat_search  # type: ignore[attr-defined]
    db = _FakeDb(session=None)
    llm = _StaticLLM(
        {
            "intent": "meeting.update",
            "entities": {
                "text": "перенеси встречу с Иваном на 15",
                "meeting_kind": "встреча",
                "meeting_update_new_time": "15:00",
            },
        }
    )

    first = handler.handle_user_attempt(
        request_id="r-update-split-confirm-1",
        user_id="u-update-split-confirm",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=llm,
        rec_client=rec,
        text="перенеси встречу с Иваном на 15",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-update-split-confirm",
        source_message_id="msg-update-split-confirm-1",
    )

    assert first["outcome"] == "rec_needs_clarification"
    assert first["rec"]["missing_field"] == "awaiting_target_confirm"
    first_q = str(first.get("clarifying_question") or "")
    assert "Вы имеете в виду" in first_q
    assert "Перенести" not in first_q

    db.session = {
        "intent": "meeting.update",
        "missing_field": "awaiting_target_confirm",
        "idempotency_key": "idem-update-split-confirm",
        "payload": dict(first["command"]),
    }
    second = handler.handle_user_attempt(
        request_id="r-update-split-confirm-2",
        user_id="u-update-split-confirm",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="да",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-update-split-confirm",
        source_message_id="msg-update-split-confirm-2",
    )
    assert second["outcome"] == "rec_needs_clarification"
    assert second["rec"]["missing_field"] == "meeting_update_edit_choice"
    second_q = str(second.get("clarifying_question") or "")
    assert "Что изменить?" in second_q
    assert rec.calls == 0


def test_meeting_comment_text_reply_is_isolated_from_temporal_clarification() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(
        session={
            "intent": "meeting.comment.update",
            "missing_field": "meeting_comment_text",
            "idempotency_key": "idem-comment-text-isolated",
            "payload": {
                "intent": "meeting.comment.update",
                "calendar_event_id": "evt-comment-isolated",
                "meeting_kind": "встреча",
                "meeting_title": "Встреча с Иваном",
                "start_at_date": "2026-04-29",
                "start_at_time": "15:00",
                "duration_minutes": 30,
                "__meeting_comment_target_confirmed": True,
                "entities": {
                    "text": "в встрече с Иваном добавь комментарий:",
                    "calendar_event_id": "evt-comment-isolated",
                    "meeting_kind": "встреча",
                    "meeting_title": "Встреча с Иваном",
                    "start_at_date": "2026-04-29",
                    "start_at_time": "15:00",
                    "duration_minutes": 30,
                },
            },
        }
    )

    out = handler.handle_user_attempt(
        request_id="r-comment-text-isolated",
        user_id="u-comment-text-isolated",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="обсудить рыбалку",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-comment-text-isolated",
        source_message_id="msg-comment-text-isolated",
    )

    assert out["outcome"] == "rec_needs_clarification"
    assert out["rec"]["missing_field"] == "meeting_comment_final_confirm"
    q_low = str(out.get("clarifying_question") or "").lower()
    assert "комментар" in q_low
    assert "на сколько минут" not in q_low
    assert "подтвердить планирование" not in q_low
    assert out["rec"]["missing_field"] != "temporal_commit_confirm"
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
    assert yes["rec"]["missing_field"] == "meeting_update_edit_choice"
    q = str(yes.get("clarifying_question") or "")
    assert "Что изменить?" in q
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
    future_iso = _date_iso(7)
    payload = {
        "intent": "meeting.update",
        "calendar_event_id": "evt-1",
        "__meeting_update_target_confirmed": True,
        "start_at_date": future_iso,
        "start_at_time": "16:00",
        "duration_minutes": 60,
        "__source_text": "перенеси встречу",
        "entities": {
            "text": "перенеси встречу",
            "calendar_event_id": "evt-1",
            "meeting_update_source_event_id": "evt-1",
            "meeting_update_source_date": future_iso,
            "meeting_update_source_time": "14:00",
            "meeting_update_source_duration": 60,
            "start_at_date": future_iso,
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
    assert str(out["command"].get("start_at_date") or "") == future_iso
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
    assert out["rec"]["missing_field"] == "meeting_update_target_refine"
    q = str(out.get("clarifying_question") or "")
    assert "не нашёл подходящее событие" in q.lower()
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


def test_meeting_update_shortlist_title_cleans_arrow_tail_and_duplicate_kind() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)

    def _fake_repeat_search(user_id: str, target_hint: str) -> dict:  # noqa: ARG001
        return {
            "ok": False,
            "reason": "multiple",
            "candidates": [
                {
                    "calendar_event_id": "evt-1",
                    "title": "Встреча в 11 → 29.04",
                    "description": "Проект А",
                    "meeting_kind": "встреча",
                    "start_at_date": "2026-04-13",
                    "start_at_time": "14:00",
                    "duration_minutes": 60,
                },
                {
                    "calendar_event_id": "evt-2",
                    "title": "Созвон Созвон",
                    "description": "Проект Б",
                    "meeting_kind": "созвон",
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
            "idempotency_key": "idem-meeting-update-title-clean-arrow-dup",
            "payload": dict(payload),
        }
    )

    out = handler.handle_user_attempt(
        request_id="r-meeting-update-title-clean-arrow-dup",
        user_id="u-meeting-update-title-clean-arrow-dup",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="встречу в 14",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-meeting-update-title-clean-arrow-dup",
        source_message_id="msg-meeting-update-title-clean-arrow-dup",
    )
    assert out["outcome"] == "rec_needs_clarification"
    q = str(out.get("clarifying_question") or "")
    assert "Встреча в 11" not in q
    assert "29.04" not in q
    assert "Созвон Созвон" not in q
    assert "1. Встреча" in q
    assert "2. Созвон" in q


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


def test_meeting_update_candidate_with_dirty_title_still_matches_clean_query() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    payload = {
        "intent": "meeting.update",
        "start_at_time": "15:00",
        "meeting_update_new_time": "15:00",
        "__source_text": "перенеси встречу с Иваном на 15",
        "__meeting_update_candidates": [
            {
                "calendar_event_id": "evt-1",
                "title": "Встреча встречу с Иваном → 29.04",
                "description": "Проект А",
                "start_at_date": "2026-04-29",
                "start_at_time": "14:00",
                "duration_minutes": 60,
                "meeting_kind": "встреча",
            },
            {
                "calendar_event_id": "evt-2",
                "title": "Встреча с Анной",
                "description": "Проект Б",
                "start_at_date": "2026-04-29",
                "start_at_time": "16:00",
                "duration_minutes": 30,
                "meeting_kind": "встреча",
            },
        ],
        "entities": {"meeting_update_new_time": "15:00", "start_at_time": "15:00"},
    }
    db = _FakeDb(
        session={
            "intent": "meeting.update",
            "missing_field": "meeting_update_target_select",
            "idempotency_key": "idem-meeting-update-select-dirty-title",
            "payload": dict(payload),
        }
    )

    out = handler.handle_user_attempt(
        request_id="r-meeting-update-select-dirty-title",
        user_id="u-meeting-update-select-dirty-title",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="с иваном",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-meeting-update-select-dirty-title",
        source_message_id="msg-meeting-update-select-dirty-title",
    )

    assert out["outcome"] == "rec_needs_clarification"
    assert out["rec"]["missing_field"] == "awaiting_target_confirm"
    assert str(out["command"].get("calendar_event_id") or "") == "evt-1"
    assert rec.calls == 0


def test_meeting_comment_update_with_participant_hint_reaches_target_and_final_confirm() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)

    def _fake_repeat_search(user_id: str, target_hint: str, meeting_kind: str = "") -> dict:  # noqa: ARG001
        return {
            "ok": True,
            "candidate": {
                "calendar_event_id": "evt-ivan-comment-1",
                "title": "Встреча встречу с Иваном → 29.04",
                "meeting_kind": "встреча",
                "start_at_date": "2026-04-29",
                "start_at_time": "14:00",
                "duration_minutes": 60,
            },
        }

    handler._meeting_comment_repeat_search = _fake_repeat_search  # type: ignore[attr-defined]

    first = handler.handle_user_attempt(
        request_id="r-meeting-comment-ivan-1",
        user_id="u-meeting-comment-ivan-1",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="в встрече с Иваном добавь комментарий: подтвердить смету",
        local_db=_FakeDb(
            session={
                "intent": "meeting.comment.update",
                "missing_field": "meeting_comment_target_ref",
                "idempotency_key": "idem-meeting-comment-ivan-1",
                "payload": {
                    "intent": "meeting.comment.update",
                    "comment_text": "подтвердить смету",
                    "entities": {"text": "в встрече с Иваном добавь комментарий: подтвердить смету", "comment_text": "подтвердить смету"},
                },
            }
        ),
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-meeting-comment-ivan-1",
        source_message_id="msg-meeting-comment-ivan-1",
    )
    assert first["outcome"] == "rec_needs_clarification"
    assert first["rec"]["missing_field"] == "meeting_comment_target_confirm"
    assert str(first["command"].get("calendar_event_id") or "") == "evt-ivan-comment-1"
    assert str(first["command"].get("comment_text") or "") == "подтвердить смету"

    second_db = _FakeDb(
        session={
            "intent": "meeting.comment.update",
            "missing_field": "meeting_comment_target_confirm",
            "idempotency_key": "idem-meeting-comment-ivan-1",
            "payload": dict(first["command"]),
        }
    )
    second = handler.handle_user_attempt(
        request_id="r-meeting-comment-ivan-2",
        user_id="u-meeting-comment-ivan-1",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="да",
        local_db=second_db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-meeting-comment-ivan-1",
        source_message_id="msg-meeting-comment-ivan-2",
    )
    assert second["outcome"] == "rec_needs_clarification"
    assert second["rec"]["missing_field"] == "meeting_comment_final_confirm"
    assert str(second["command"].get("comment_text") or "") == "подтвердить смету"
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
    future_iso = _date_iso(7)
    payload = {
        "intent": "meeting.create",
        "calendar_event_id": "evt-1",
        "__meeting_update_target_confirmed": True,
        "start_at_date": future_iso,
        "duration_minutes": 60,
        "__source_text": "перенеси встречу на 16",
        "entities": {
            "text": "перенеси встречу на 16",
            "calendar_event_id": "evt-1",
            "meeting_update_source_event_id": "evt-1",
            "meeting_update_source_date": future_iso,
            "meeting_update_source_time": "14:00",
            "meeting_update_source_duration": 60,
            "start_at_date": future_iso,
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
    future_iso = _date_iso(7)
    db = _FakeDb(
        session={
            "intent": "meeting.update",
            "missing_field": "temporal_commit_confirm",
            "idempotency_key": "idem-meeting-update-confirm",
                "payload": {
                    "intent": "meeting.update",
                    "calendar_event_id": "evt-1",
                    "__meeting_update_target_confirmed": True,
                    "start_at_date": future_iso,
                    "start_at_time": "16:00",
                    "duration_minutes": 60,
                    "__source_text": "перенеси встречу на 16",
                    "entities": {
                        "text": "перенеси встречу на 16",
                        "calendar_event_id": "evt-1",
                        "meeting_update_source_event_id": "evt-1",
                        "meeting_update_source_date": future_iso,
                        "meeting_update_source_time": "14:00",
                        "meeting_update_source_duration": 60,
                        "start_at_date": future_iso,
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
    future_iso = _date_iso(7)
    db = _FakeDb(
        session={
            "intent": "meeting.create",
            "missing_field": "temporal_commit_confirm",
            "idempotency_key": "idem-confirm-intent-lock-create",
                "payload": {
                    "intent": "meeting.update",
                    "calendar_event_id": "evt-1",
                    "__source_text": f"встреча {future_iso} в 16:00 на 60 минут",
                    "start_at_date": future_iso,
                    "start_at_time": "16:00",
                    "duration_minutes": 60,
                    "entities": {
                        "text": f"встреча {future_iso} в 16:00 на 60 минут",
                        "start_at_date": future_iso,
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
    future_iso = _date_iso(7)
    db = _FakeDb(
        session={
            "intent": "meeting.update",
            "missing_field": "temporal_commit_confirm",
            "idempotency_key": "idem-confirm-intent-lock-update",
            "payload": {
                "intent": "meeting.create",
                "calendar_event_id": "evt-1",
                "__meeting_update_target_confirmed": True,
                "start_at_date": future_iso,
                "start_at_time": "16:00",
                "duration_minutes": 60,
                "entities": {
                    "text": "подтверждаю",
                    "calendar_event_id": "evt-1",
                    "meeting_update_source_event_id": "evt-1",
                    "meeting_update_source_date": future_iso,
                    "meeting_update_source_time": "14:00",
                    "meeting_update_source_duration": 60,
                    "start_at_date": future_iso,
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

    assert second["outcome"] == "rec_needs_clarification"
    assert second["rec"]["outcome"] == "needs_clarification"
    assert second["rec"]["missing_field"] == "temporal_commit_confirm"
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


def test_meeting_comment_final_confirm_is_not_hijacked_by_past_date_gate() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(
        session={
            "intent": "meeting.comment.update",
            "missing_field": "meeting_comment_final_confirm",
            "idempotency_key": "idem-meeting-comment-final-not-hijacked",
            "payload": {
                "intent": "meeting.comment.update",
                "calendar_event_id": "evt-comment-old-1",
                "start_at_date": "2021-04-13",
                "start_at_time": "15:00",
                "duration_minutes": 30,
                "__meeting_comment_target_confirmed": True,
                "comment_text": "взять цифры",
                "__source_text": "в созвоне с Иваном добавь комментарий взять цифры",
                "entities": {
                    "text": "в созвоне с Иваном добавь комментарий взять цифры",
                    "calendar_event_id": "evt-comment-old-1",
                    "meeting_kind": "созвон",
                    "start_at_date": "2021-04-13",
                    "start_at_time": "15:00",
                    "duration_minutes": 30,
                    "comment_text": "взять цифры",
                },
            },
        }
    )

    out = handler.handle_user_attempt(
        request_id="r-meeting-comment-final-not-hijacked",
        user_id="u-meeting-comment-final-not-hijacked",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="может быть",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-meeting-comment-final-not-hijacked",
        source_message_id="msg-meeting-comment-final-not-hijacked",
    )

    assert out["outcome"] == "rec_needs_clarification"
    assert out["rec"]["missing_field"] == "meeting_comment_final_confirm"
    assert "подтвердить изменение комментария" in str(out.get("clarifying_question") or "").lower()


def test_meeting_update_temporal_confirm_still_uses_past_date_safety() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(
        session={
            "intent": "meeting.update",
            "missing_field": "temporal_commit_confirm",
            "idempotency_key": "idem-meeting-update-past-safety",
            "payload": {
                "intent": "meeting.update",
                "calendar_event_id": "evt-update-old-1",
                "__meeting_update_target_confirmed": True,
                "start_at_date": "2021-04-13",
                "start_at_time": "16:00",
                "duration_minutes": 60,
                "__source_text": "перенеси встречу на 16",
                "entities": {
                    "text": "перенеси встречу на 16",
                    "calendar_event_id": "evt-update-old-1",
                    "meeting_update_source_event_id": "evt-update-old-1",
                    "meeting_update_source_date": "2021-04-13",
                    "meeting_update_source_time": "14:00",
                    "meeting_update_source_duration": 60,
                    "start_at_date": "2021-04-13",
                    "start_at_time": "16:00",
                    "duration_minutes": 60,
                    "meeting_update_has_changes": True,
                },
            },
        }
    )

    out = handler.handle_user_attempt(
        request_id="r-meeting-update-past-safety",
        user_id="u-meeting-update-past-safety",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="да",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-meeting-update-past-safety",
        source_message_id="msg-meeting-update-past-safety",
    )

    assert out["outcome"] == "rec_needs_clarification"
    assert out["rec"]["missing_field"] == "start_at_date_past_confirm"


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

def test_live_regression_future_month_name_date_not_routed_to_past_confirm(monkeypatch) -> None:
    handler = _load_handler_module()

    class _FrozenDate(date):
        @classmethod
        def today(cls) -> "_FrozenDate":
            return cls(2026, 4, 28)

    monkeypatch.setattr(handler, "date", _FrozenDate)
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(session=None)
    llm = _StaticLLM(
        {
            "intent": "meeting.create",
            "entities": {
                "text": "запланируй собрание 29 апреля",
                "meeting_kind": "собрание",
                "start_at_date": "2023-04-29",
            },
        }
    )

    out = handler.handle_user_attempt(
        request_id="r-live-29-apr",
        user_id="u-live-29-apr",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=llm,
        rec_client=rec,
        text="запланируй собрание 29 апреля",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-live-29-apr",
        source_message_id="msg-live-29-apr",
    )
    assert out["outcome"] == "rec_needs_clarification"
    assert out["rec"]["missing_field"] == "start_at_time"
    cmd = out["command"]
    assert str(cmd.get("start_at_date") or "") == "2026-04-29"
    assert out["rec"]["missing_field"] != "start_at_date_past_confirm"


def test_live_regression_event_may_first_keeps_2026_and_duration_45(monkeypatch) -> None:
    handler = _load_handler_module()

    class _FrozenDate(date):
        @classmethod
        def today(cls) -> "_FrozenDate":
            return cls(2026, 4, 28)

    monkeypatch.setattr(handler, "date", _FrozenDate)
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(session=None)
    llm = _StaticLLM(
        {
            "intent": "meeting.create",
            "entities": {
                "text": "мероприятие 1 мая в 10 на 45 минут",
                "meeting_kind": "мероприятие",
                "start_at_date": "2023-05-01",
                "start_at_time": "10:00",
                "duration_minutes": 45,
            },
        }
    )

    out = handler.handle_user_attempt(
        request_id="r-live-1-may",
        user_id="u-live-1-may",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=llm,
        rec_client=rec,
        text="мероприятие 1 мая в 10 на 45 минут",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-live-1-may",
        source_message_id="msg-live-1-may",
    )
    assert out["outcome"] == "rec_needs_clarification"
    assert out["rec"]["missing_field"] == "temporal_commit_confirm"
    cmd = out["command"]
    assert str(cmd.get("start_at_date") or "") == "2026-05-01"
    assert str(cmd.get("start_at_time") or "") == "10:00"
    assert out["rec"]["missing_field"] != "start_at_date_past_confirm"


def test_live_regression_short_numeric_date_reply_29_is_accepted(monkeypatch) -> None:
    handler = _load_handler_module()

    class _FrozenDate(date):
        @classmethod
        def today(cls) -> "_FrozenDate":
            return cls(2026, 4, 28)

    monkeypatch.setattr(handler, "date", _FrozenDate)
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(
        session={
            "intent": "meeting.create",
            "missing_field": "start_at_date",
            "idempotency_key": "idem-live-short-29",
            "payload": {
                "intent": "meeting.create",
                "start_at_time": "15:00",
                "duration_minutes": 30,
                "__source_text": "созвон в 15:00",
                "entities": {"text": "созвон в 15:00", "meeting_kind": "созвон", "start_at_time": "15:00", "duration_minutes": 30},
            },
        }
    )

    out = handler.handle_user_attempt(
        request_id="r-live-short-29",
        user_id="u-live-short-29",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="29",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-live-short-29",
        source_message_id="msg-live-short-29",
    )
    assert out["outcome"] == "rec_needs_clarification"
    assert out["rec"]["missing_field"] == "temporal_commit_confirm"
    cmd = out["command"]
    assert str(cmd.get("start_at_date") or "") == "2026-04-29"
    assert str(cmd.get("start_at_time") or "") == "15:00"


def test_live_regression_past_date_stays_current_year_not_2022_2023(monkeypatch) -> None:
    handler = _load_handler_module()

    class _FrozenDate(date):
        @classmethod
        def today(cls) -> "_FrozenDate":
            return cls(2026, 4, 28)

    monkeypatch.setattr(handler, "date", _FrozenDate)
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(session=None)
    llm = _StaticLLM(
        {
            "intent": "meeting.create",
            "entities": {
                "text": "запланируй встречу 22 апреля в 11",
                "meeting_kind": "встреча",
                "start_at_date": "2023-04-22",
                "start_at_time": "11:00",
            },
        }
    )

    out = handler.handle_user_attempt(
        request_id="r-live-past-22-apr",
        user_id="u-live-past-22-apr",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=llm,
        rec_client=rec,
        text="запланируй встречу 22 апреля в 11",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-live-past-22-apr",
        source_message_id="msg-live-past-22-apr",
    )
    assert out["outcome"] == "rec_needs_clarification"
    assert out["rec"]["missing_field"] == "start_at_date_past_confirm"
    cmd = out["command"]
    assert str(cmd.get("start_at_date") or "") == "2026-04-22"
    assert str(cmd.get("start_at_date") or "") not in {"2022-04-22", "2023-04-22"}


def test_live_regression_prompt_priority_for_future_date_is_single_time_prompt(monkeypatch) -> None:
    handler = _load_handler_module()

    class _FrozenDate(date):
        @classmethod
        def today(cls) -> "_FrozenDate":
            return cls(2026, 4, 28)

    monkeypatch.setattr(handler, "date", _FrozenDate)
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(session=None)
    llm = _StaticLLM(
        {
            "intent": "meeting.create",
            "entities": {
                "text": "запланируй собрание 29 апреля",
                "meeting_kind": "собрание",
                "start_at_date": "2023-04-29",
            },
        }
    )

    out = handler.handle_user_attempt(
        request_id="r-live-priority-29-apr",
        user_id="u-live-priority-29-apr",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=llm,
        rec_client=rec,
        text="запланируй собрание 29 апреля",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-live-priority-29-apr",
        source_message_id="msg-live-priority-29-apr",
    )
    assert out["outcome"] == "rec_needs_clarification"
    assert out["rec"]["missing_field"] == "start_at_time"
    question = str(out.get("clarifying_question") or "").lower()
    assert "эта дата уже прошла" not in question
    assert ("во сколько" in question) or ("какое время" in question)

def test_live_duration_extraction_event_may_first_keeps_45_in_summary(monkeypatch) -> None:
    handler = _load_handler_module()

    class _FrozenDate(date):
        @classmethod
        def today(cls) -> "_FrozenDate":
            return cls(2026, 4, 28)

    monkeypatch.setattr(handler, "date", _FrozenDate)
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(session=None)
    llm = _StaticLLM(
        {
            "intent": "meeting.create",
            "entities": {
                "text": "мероприятие 1 мая в 10 на 45 минут",
                "meeting_kind": "мероприятие",
                "start_at_date": "2023-05-01",
                "start_at_time": "10:00",
            },
        }
    )

    out = handler.handle_user_attempt(
        request_id="r-live-dur-45",
        user_id="u-live-dur-45",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=llm,
        rec_client=rec,
        text="мероприятие 1 мая в 10 на 45 минут",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-live-dur-45",
        source_message_id="msg-live-dur-45",
    )

    assert out["outcome"] == "rec_needs_clarification"
    assert out["rec"]["missing_field"] == "temporal_commit_confirm"
    cmd = out["command"]
    assert str(cmd.get("start_at_date") or "") == "2026-05-01"
    assert str(cmd.get("start_at_time") or "") == "10:00"
    assert int(cmd.get("duration_minutes") or 0) == 45
    assert "Длительность: 45 минут" in str(out.get("clarifying_question") or "")


def test_live_duration_extraction_one_hour_normalizes_to_60(monkeypatch) -> None:
    handler = _load_handler_module()

    class _FrozenDate(date):
        @classmethod
        def today(cls) -> "_FrozenDate":
            return cls(2026, 4, 28)

    monkeypatch.setattr(handler, "date", _FrozenDate)
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(session=None)
    llm = _StaticLLM(
        {
            "intent": "meeting.create",
            "entities": {
                "text": "встреча 29 апреля в 11 на 1 час",
                "meeting_kind": "встреча",
                "start_at_date": "2023-04-29",
                "start_at_time": "11:00",
            },
        }
    )

    out = handler.handle_user_attempt(
        request_id="r-live-dur-1h",
        user_id="u-live-dur-1h",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=llm,
        rec_client=rec,
        text="встреча 29 апреля в 11 на 1 час",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-live-dur-1h",
        source_message_id="msg-live-dur-1h",
    )

    assert out["outcome"] == "rec_needs_clarification"
    assert out["rec"]["missing_field"] == "temporal_commit_confirm"
    cmd = out["command"]
    assert str(cmd.get("start_at_date") or "") == "2026-04-29"
    assert str(cmd.get("start_at_time") or "") == "11:00"
    assert int(cmd.get("duration_minutes") or 0) == 60
    assert "Длительность: 1 час" in str(out.get("clarifying_question") or "")


def test_live_duration_default_still_30_when_not_specified(monkeypatch) -> None:
    handler = _load_handler_module()

    class _FrozenDate(date):
        @classmethod
        def today(cls) -> "_FrozenDate":
            return cls(2026, 4, 28)

    monkeypatch.setattr(handler, "date", _FrozenDate)
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(session=None)
    llm = _StaticLLM(
        {
            "intent": "meeting.create",
            "entities": {
                "text": "созвон 29 апреля в 15",
                "meeting_kind": "созвон",
                "start_at_date": "2023-04-29",
                "start_at_time": "15:00",
            },
        }
    )

    out = handler.handle_user_attempt(
        request_id="r-live-dur-default-30",
        user_id="u-live-dur-default-30",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=llm,
        rec_client=rec,
        text="созвон 29 апреля в 15",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-live-dur-default-30",
        source_message_id="msg-live-dur-default-30",
    )

    assert out["outcome"] == "rec_needs_clarification"
    assert out["rec"]["missing_field"] == "temporal_commit_confirm"
    assert int(out["command"].get("duration_minutes") or 0) == 30


def test_live_duration_override_works_after_explicit_45_summary(monkeypatch) -> None:
    handler = _load_handler_module()

    class _FrozenDate(date):
        @classmethod
        def today(cls) -> "_FrozenDate":
            return cls(2026, 4, 28)

    monkeypatch.setattr(handler, "date", _FrozenDate)
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(
        session={
            "intent": "meeting.create",
            "missing_field": "temporal_commit_confirm",
            "idempotency_key": "idem-live-override-45-to-30",
            "payload": {
                "intent": "meeting.create",
                "start_at_date": "2026-05-01",
                "start_at_time": "10:00",
                "duration_minutes": 45,
                "__source_text": "мероприятие 1 мая в 10 на 45 минут",
                "entities": {
                    "text": "мероприятие 1 мая в 10 на 45 минут",
                    "meeting_kind": "мероприятие",
                    "start_at_date": "2026-05-01",
                    "start_at_time": "10:00",
                    "duration_minutes": 45,
                },
            },
        }
    )

    out = handler.handle_user_attempt(
        request_id="r-live-override-45-to-30",
        user_id="u-live-override-45-to-30",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="30",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-live-override-45-to-30",
        source_message_id="msg-live-override-45-to-30",
    )

    assert out["outcome"] == "rec_needs_clarification"
    assert out["rec"]["missing_field"] == "temporal_commit_confirm"
    cmd = out["command"]
    assert int(cmd.get("duration_minutes") or 0) == 30
    assert "Длительность: 30 минут" in str(out.get("clarifying_question") or "")

def test_date_clarification_then_repeated_same_draft_does_not_duplicate_full_summary() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    base_payload = {
        "intent": "meeting.create",
        "start_at_date": _date_iso(2),
        "duration_minutes": 30,
        "__source_text": f"запланируй встречу {_date_iso(2)}",
        "entities": {
            "text": f"запланируй встречу {_date_iso(2)}",
            "meeting_kind": "встреча",
            "start_at_date": _date_iso(2),
            "duration_minutes": 30,
        },
    }
    db = _FakeDb(
        session={
            "intent": "meeting.create",
            "missing_field": "start_at_time",
            "idempotency_key": "idem-no-dup-after-date-clar",
            "payload": dict(base_payload),
        }
    )

    first = handler.handle_user_attempt(
        request_id="r-no-dup-after-date-clar-1",
        user_id="u-no-dup-after-date-clar",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="11",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-no-dup-after-date-clar",
        source_message_id="msg-no-dup-after-date-clar-1",
    )
    assert first["outcome"] == "rec_needs_clarification"
    assert first["rec"]["missing_field"] == "temporal_commit_confirm"
    assert "Тип:" in str(first.get("clarifying_question") or "")

    db.session = {
        "intent": "meeting.create",
        "missing_field": "temporal_commit_confirm",
        "idempotency_key": "idem-no-dup-after-date-clar",
        "payload": dict(first["command"]),
    }
    second = handler.handle_user_attempt(
        request_id="r-no-dup-after-date-clar-2",
        user_id="u-no-dup-after-date-clar",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="может быть",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-no-dup-after-date-clar",
        source_message_id="msg-no-dup-after-date-clar-2",
    )
    q = str(second.get("clarifying_question") or "")
    assert second["outcome"] == "rec_needs_clarification"
    assert second["rec"]["missing_field"] == "temporal_commit_confirm"
    assert "подтверд" in q.lower()
    assert "Тип:" not in q
    assert "Дата:" not in q


def test_meeting_update_target_confirm_then_repeated_same_draft_does_not_duplicate_full_summary() -> None:
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
            "idempotency_key": "idem-update-target-confirm-no-dup",
            "payload": dict(payload),
        }
    )

    first = handler.handle_user_attempt(
        request_id="r-update-target-confirm-no-dup-1",
        user_id="u-update-target-confirm-no-dup",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="да",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-update-target-confirm-no-dup",
        source_message_id="msg-update-target-confirm-no-dup-1",
    )
    assert first["outcome"] == "rec_needs_clarification"
    assert first["rec"]["missing_field"] == "meeting_update_edit_choice"
    assert "Что изменить?" in str(first.get("clarifying_question") or "")

    db.session = {
        "intent": "meeting.update",
        "missing_field": "meeting_update_edit_choice",
        "idempotency_key": "idem-update-target-confirm-no-dup",
        "payload": dict(first["command"]),
    }
    second = handler.handle_user_attempt(
        request_id="r-update-target-confirm-no-dup-2",
        user_id="u-update-target-confirm-no-dup",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="не понял",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-update-target-confirm-no-dup",
        source_message_id="msg-update-target-confirm-no-dup-2",
    )
    q = str(second.get("clarifying_question") or "")
    assert second["outcome"] == "rec_needs_clarification"
    assert second["rec"]["missing_field"] == "meeting_update_edit_choice"
    assert "что изменить" in q.lower()
    assert "Текущие параметры:" not in q
    assert "Новые параметры:" not in q


def test_meeting_update_target_select_then_confirm_duplicate_summary_guard_applies() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    payload = {
        "intent": "meeting.update",
        "meeting_update_new_time": "15:00",
        "entities": {
            "text": "перенеси встречу с иваном на 15",
            "meeting_update_new_time": "15:00",
            "meeting_update_has_changes": True,
            "__meeting_update_candidates": [
                {
                    "calendar_event_id": "evt-1",
                    "title": "Встреча с Иваном",
                    "start_at_date": "2026-04-13",
                    "start_at_time": "14:00",
                    "duration_minutes": 60,
                },
                {
                    "calendar_event_id": "evt-2",
                    "title": "Встреча с Петром",
                    "start_at_date": "2026-04-13",
                    "start_at_time": "16:00",
                    "duration_minutes": 60,
                },
            ],
        },
    }
    db = _FakeDb(
        session={
            "intent": "meeting.update",
            "missing_field": "meeting_update_target_select",
            "idempotency_key": "idem-update-select-no-dup",
            "payload": dict(payload),
        }
    )

    pick = handler.handle_user_attempt(
        request_id="r-update-select-no-dup-pick",
        user_id="u-update-select-no-dup",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="1",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-update-select-no-dup",
        source_message_id="msg-update-select-no-dup-pick",
    )
    assert pick["outcome"] == "rec_needs_clarification"
    assert pick["rec"]["missing_field"] == "awaiting_target_confirm"

    db.session = {
        "intent": "meeting.update",
        "missing_field": "awaiting_target_confirm",
        "idempotency_key": "idem-update-select-no-dup",
        "payload": dict(pick["command"]),
    }
    first = handler.handle_user_attempt(
        request_id="r-update-select-no-dup-first-summary",
        user_id="u-update-select-no-dup",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="да",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-update-select-no-dup",
        source_message_id="msg-update-select-no-dup-first-summary",
    )
    assert first["outcome"] == "rec_needs_clarification"
    assert first["rec"]["missing_field"] == "meeting_update_edit_choice"
    assert "Что изменить?" in str(first.get("clarifying_question") or "")

    db.session = {
        "intent": "meeting.update",
        "missing_field": "meeting_update_edit_choice",
        "idempotency_key": "idem-update-select-no-dup",
        "payload": dict(first["command"]),
    }
    second = handler.handle_user_attempt(
        request_id="r-update-select-no-dup-second",
        user_id="u-update-select-no-dup",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="не уверен",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-update-select-no-dup",
        source_message_id="msg-update-select-no-dup-second",
    )
    q = str(second.get("clarifying_question") or "")
    assert second["outcome"] == "rec_needs_clarification"
    assert second["rec"]["missing_field"] == "meeting_update_edit_choice"
    assert "что изменить" in q.lower()
    assert "Текущие параметры:" not in q


def test_duplicate_summary_guard_preserves_comment_and_description_fields() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    payload = {
        "intent": "meeting.create",
        "start_at_date": _date_iso(2),
        "start_at_time": "11:00",
        "duration_minutes": 45,
        "comment": "не менять comment",
        "description": "не менять description",
        "entities": {
            "text": f"встреча {_date_iso(2)} в 11 на 45 минут",
            "meeting_kind": "встреча",
            "start_at_date": _date_iso(2),
            "start_at_time": "11:00",
            "duration_minutes": 45,
            "comment": "entity comment",
            "description": "entity description",
        },
    }
    payload["__temporal_summary_signature"] = handler._temporal_summary_signature(payload)
    db = _FakeDb(
        session={
            "intent": "meeting.create",
            "missing_field": "temporal_commit_confirm",
            "idempotency_key": "idem-preserve-comment-desc",
            "payload": dict(payload),
        }
    )

    out = handler.handle_user_attempt(
        request_id="r-preserve-comment-desc",
        user_id="u-preserve-comment-desc",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="может быть",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-preserve-comment-desc",
        source_message_id="msg-preserve-comment-desc",
    )

    assert out["outcome"] == "rec_needs_clarification"
    assert out["rec"]["missing_field"] == "temporal_commit_confirm"
    cmd = out["command"]
    assert str(cmd.get("comment") or "") == "не менять comment"
    assert str(cmd.get("description") or "") == "не менять description"
    entities = cmd.get("entities") if isinstance(cmd.get("entities"), dict) else {}
    assert str(entities.get("comment") or "") == "entity comment"
    assert str(entities.get("description") or "") == "entity description"


def test_duplicate_summary_same_request_is_suppressed_without_new_message() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    payload = {
        "intent": "meeting.create",
        "start_at_date": _date_iso(2),
        "start_at_time": "11:00",
        "duration_minutes": 45,
        "entities": {
            "text": f"запланируй встречу {_date_iso(2)} в 11",
            "meeting_kind": "встреча",
            "start_at_date": _date_iso(2),
            "start_at_time": "11:00",
            "duration_minutes": 45,
        },
    }
    payload["__temporal_summary_signature"] = handler._temporal_summary_signature(payload)
    payload["__temporal_summary_render_request_id"] = "tg:101:202:303"
    db = _FakeDb(
        session={
            "intent": "meeting.create",
            "missing_field": "temporal_commit_confirm",
            "idempotency_key": "idem-same-request-summary",
            "payload": dict(payload),
        }
    )

    out = handler.handle_user_attempt(
        request_id="tg:101:202:303",
        user_id="u-same-request-summary",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_StaticLLM(
            {
                "intent": "meeting.create",
                "start_at_date": _date_iso(2),
                "start_at_time": "11:00",
                "duration_minutes": 45,
                "entities": {
                    "text": f"запланируй встречу {_date_iso(2)} в 11",
                    "meeting_kind": "встреча",
                    "start_at_date": _date_iso(2),
                    "start_at_time": "11:00",
                    "duration_minutes": 45,
                },
            }
        ),
        rec_client=rec,
        text=f"запланируй встречу {_date_iso(2)} в 11",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-same-request-summary",
        source_message_id="303",
    )

    assert out["outcome"] == "rec_needs_clarification"
    assert out["rec"]["missing_field"] == "temporal_commit_confirm"
    assert str(out.get("clarifying_question") or "") == ""
    telemetry = out.get("telemetry") if isinstance(out.get("telemetry"), dict) else {}
    assert telemetry.get("suppress_send") is True
    assert rec.calls == 0


def test_meeting_comment_update_final_confirm_preserves_requested_comment_text() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    payload = {
        "intent": "meeting.comment.update",
        "calendar_event_id": "evt-1",
        "meeting_title": "Встреча с Иваном",
        "start_at_date": "2026-04-13",
        "start_at_time": "14:00",
        "comment_text": "взять цифры",
        "entities": {
            "text": "добавь комментарий",
            "calendar_event_id": "evt-1",
            "comment_text": "взять цифры",
        },
    }
    db = _FakeDb(
        session={
            "intent": "meeting.comment.update",
            "missing_field": "meeting_comment_final_confirm",
            "idempotency_key": "idem-meeting-comment-final-preserve",
            "payload": dict(payload),
        }
    )

    out = handler.handle_user_attempt(
        request_id="r-meeting-comment-final-preserve",
        user_id="u-meeting-comment-final-preserve",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="да",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-meeting-comment-final-preserve",
        source_message_id="msg-meeting-comment-final-preserve",
    )

    assert out["outcome"] in {"success", "rec_needs_clarification"}
    assert str(out["command"].get("comment_text") or "") == "взять цифры"


def test_meeting_comment_confirm_does_not_trigger_duration() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(
        session={
            "intent": "meeting.comment",
            "missing_field": "meeting_comment_final_confirm",
            "idempotency_key": "idem-meeting-comment-no-duration",
            "payload": {
                "intent": "meeting.comment",
                "calendar_event_id": "evt-comment-no-duration",
                "__meeting_comment_target_confirmed": True,
                "meeting_title": "Встреча с Иваном",
                "start_at_date": "2026-04-29",
                "start_at_time": "15:00",
                "duration_minutes": 30,
                "comment_text": "обсудить рыбалку",
                "entities": {
                    "text": "в встрече с иваном добавь комментарий",
                    "calendar_event_id": "evt-comment-no-duration",
                    "meeting_kind": "встреча",
                    "start_at_date": "2026-04-29",
                    "start_at_time": "15:00",
                    "duration_minutes": 30,
                    "comment_text": "обсудить рыбалку",
                },
            },
        }
    )

    out = handler.handle_user_attempt(
        request_id="r-meeting-comment-no-duration",
        user_id="u-meeting-comment-no-duration",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="да",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-meeting-comment-no-duration",
        source_message_id="msg-meeting-comment-no-duration",
    )

    assert out["outcome"] == "success"
    rec_info = out.get("rec") if isinstance(out.get("rec"), dict) else {}
    assert str(rec_info.get("outcome") or "") == "skipped"
    assert str(rec_info.get("reason") or "") == "clarification_executable_guard"
    assert str(out.get("clarifying_question") or "").strip() == ""
    user_msg = str(out.get("user_message") or "").lower()
    assert "на сколько минут" not in user_msg
    assert "подтвердить планирование" not in user_msg
    assert "тип:" not in user_msg
    cmd = out.get("command") if isinstance(out.get("command"), dict) else {}
    assert str(cmd.get("intent") or "").strip().lower() in {"meeting.comment", "meeting.comment.update", "meeting_comment_update"}
    assert str(cmd.get("comment_text") or "") == "обсудить рыбалку"
    assert rec.calls == 0


def test_task_comment_update_final_confirm_preserves_requested_comment_text() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    payload = {
        "intent": "task.comment.update",
        "task_id": "task-1",
        "task_title": "Подготовить отчет",
        "comment_text": "отправить до 18:00",
        "entities": {
            "text": "добавь комментарий",
            "task_id": "task-1",
            "comment_text": "отправить до 18:00",
        },
    }
    db = _FakeDb(
        session={
            "intent": "task.comment.update",
            "missing_field": "task_comment_final_confirm",
            "idempotency_key": "idem-task-comment-final-preserve",
            "payload": dict(payload),
        }
    )

    out = handler.handle_user_attempt(
        request_id="r-task-comment-final-preserve",
        user_id="u-task-comment-final-preserve",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="да",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-task-comment-final-preserve",
        source_message_id="msg-task-comment-final-preserve",
    )

    assert out["outcome"] in {"success", "rec_needs_clarification"}
    assert str(out["command"].get("comment_text") or "") == "отправить до 18:00"


def test_meeting_create_with_participant_phrase_keeps_name_in_title() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(session=None)
    llm = _StaticLLM(
        {
            "intent": "meeting.create",
            "entities": {
                "text": "встреча с Алексеем завтра в 12",
                "meeting_kind": "встреча",
                "start_at": _dt_iso(1, 12),
                "duration_minutes": 30,
            },
        }
    )

    out = handler.handle_user_attempt(
        request_id="r-meeting-title-participant",
        user_id="u-meeting-title-participant",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=llm,
        rec_client=rec,
        text="встреча с Алексеем завтра в 12",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-meeting-title-participant",
        source_message_id="msg-meeting-title-participant",
    )

    assert out["outcome"] == "rec_needs_clarification"
    title = str(out.get("command", {}).get("title") or "")
    assert "с Алексеем" in title
    assert rec.calls == 0


def test_summary_allows_comment_edit() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(session=None)
    llm = _StaticLLM(
        {
            "intent": "meeting.create",
            "entities": {
                "text": "встреча с Алексеем завтра в 12",
                "meeting_kind": "встреча",
                "start_at": _dt_iso(1, 12),
                "duration_minutes": 30,
            },
        }
    )

    first = handler.handle_user_attempt(
        request_id="r-summary-comment-edit-1",
        user_id="u-summary-comment-edit",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=llm,
        rec_client=rec,
        text="встреча с Алексеем завтра в 12",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-summary-comment-edit",
        source_message_id="msg-summary-comment-edit-1",
    )
    assert first["outcome"] == "rec_needs_clarification"
    assert first["rec"]["missing_field"] == "temporal_commit_confirm"

    db.session = {
        "intent": "meeting.create",
        "missing_field": "temporal_commit_confirm",
        "idempotency_key": "idem-summary-comment-edit",
        "payload": dict(first["command"]),
    }
    second = handler.handle_user_attempt(
        request_id="r-summary-comment-edit-2",
        user_id="u-summary-comment-edit",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="нет",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-summary-comment-edit",
        source_message_id="msg-summary-comment-edit-2",
    )
    assert second["outcome"] == "rec_needs_clarification"
    assert second["rec"]["missing_field"] == "temporal_edit_field"

    db.session = {
        "intent": "meeting.create",
        "missing_field": "temporal_edit_field",
        "idempotency_key": "idem-summary-comment-edit",
        "payload": dict(second["command"]),
    }
    third = handler.handle_user_attempt(
        request_id="r-summary-comment-edit-3",
        user_id="u-summary-comment-edit",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="Комментарий",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-summary-comment-edit",
        source_message_id="msg-summary-comment-edit-3",
    )
    assert third["outcome"] == "rec_needs_clarification"
    assert third["rec"]["missing_field"] == "temporal_edit_field"
    third_q = str(third.get("clarifying_question") or "")
    assert third_q.startswith("Введите новый комментарий.")
    assert "Текущий комментарий:\n-" in third_q

    db.session = {
        "intent": "meeting.create",
        "missing_field": "temporal_edit_field",
        "idempotency_key": "idem-summary-comment-edit",
        "payload": dict(third["command"]),
    }
    fourth = handler.handle_user_attempt(
        request_id="r-summary-comment-edit-4",
        user_id="u-summary-comment-edit",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="обсудить рыбалку",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-summary-comment-edit",
        source_message_id="msg-summary-comment-edit-4",
    )
    assert fourth["outcome"] == "rec_needs_clarification"
    assert fourth["rec"]["missing_field"] == "temporal_commit_confirm"
    q = str(fourth.get("clarifying_question") or "").lower()
    assert "на сколько минут" not in q
    assert "комментарий: обсудить рыбалку" in q
    assert "подтвердить планирование" in q
    cmd = fourth.get("command") if isinstance(fourth.get("command"), dict) else {}
    assert str(cmd.get("comment_text") or "") == "обсудить рыбалку"
    assert db.upserts
    persisted_payload = db.upserts[-1]["payload"]
    assert str(persisted_payload.get("comment_text") or "") == "обсудить рыбалку"
    persisted_draft = persisted_payload.get("__temporal_draft") if isinstance(persisted_payload.get("__temporal_draft"), dict) else {}
    assert str(persisted_draft.get("comment") or "") == "обсудить рыбалку"
    assert rec.calls == 0


def test_temporal_confirm_edit_buttons_route_into_field_specific_questions_without_summary_rerender() -> None:
    handler = _load_handler_module()
    base_payload = {
        "intent": "meeting.create",
        "start_at": _dt_iso(1, 11),
        "start_at_date": _date_iso(1),
        "start_at_time": "11:00",
        "duration_minutes": 30,
        "__source_text": f"запланируй встречу с Алексеем {_date_iso(1)} в 11",
        "entities": {
            "text": f"запланируй встречу с Алексеем {_date_iso(1)} в 11",
            "meeting_kind": "встреча",
            "start_at_date": _date_iso(1),
            "start_at_time": "11:00",
            "duration_minutes": 30,
        },
    }
    expectations = {
        "дата": "На какую дату запланировать?",
        "время": "На какое время запланировать?",
        "длительность": "На сколько минут запланировать?",
        "Комментарий": "Введите новый комментарий.\n\nТекущий комментарий:\n-\n\nЕсли передумали — нажмите «Оставить без изменений» или «Отмена».",
    }

    for text, expected_question in expectations.items():
        rec = _RecProbe(should_fail_on_call=True)
        db = _FakeDb(
            session={
                "intent": "meeting.create",
                "missing_field": "temporal_commit_confirm",
                "idempotency_key": f"idem-confirm-button-{text}",
                "payload": dict(base_payload),
            }
        )

        result = handler.handle_user_attempt(
            request_id=f"r-confirm-button-{text}",
            user_id=f"u-confirm-button-{text}",
            channel="telegram",
            asr_client=_NoopAsr(),
            llm_client=_NeverCalledLLM(),
            rec_client=rec,
            text=text,
            local_db=db,
            app_id="app",
            tenant_id="tenant",
            source_chat_id="chat-confirm-button",
            source_message_id=f"msg-confirm-button-{text}",
        )

        assert result["outcome"] == "rec_needs_clarification"
        assert result["rec"]["missing_field"] == "temporal_edit_field"
        assert str(result.get("clarifying_question") or "") == expected_question
        assert "Подтвердить планирование?" not in str(result.get("clarifying_question") or "")
        cmd = result.get("command") if isinstance(result.get("command"), dict) else {}
        assert str(cmd.get("__temporal_edit_active_field") or "") != ""
        assert rec.calls == 0


def test_temporal_edit_comment_callback_is_consumed_without_writing_label_into_draft() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    base_payload = {
        "intent": "meeting.create",
        "start_at": _dt_iso(1, 11),
        "start_at_date": _date_iso(1),
        "start_at_time": "11:00",
        "duration_minutes": 30,
        "__source_text": f"запланируй встречу с Алексеем {_date_iso(1)} в 11",
        "entities": {
            "text": f"запланируй встречу с Алексеем {_date_iso(1)} в 11",
            "meeting_kind": "встреча",
            "start_at_date": _date_iso(1),
            "start_at_time": "11:00",
            "duration_minutes": 30,
        },
    }
    db = _FakeDb(
        session={
            "intent": "meeting.create",
            "missing_field": "temporal_commit_confirm",
            "idempotency_key": "idem-comment-callback-consumed",
            "payload": dict(base_payload),
        }
    )

    first = handler.handle_user_attempt(
        request_id="r-comment-callback-consumed-1",
        user_id="u-comment-callback-consumed",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="Комментарий",
        metadata={"callback_query": {"id": "cb-1", "data": "clarify:v1:temporal_edit:comment"}},
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-comment-callback-consumed",
        source_message_id="msg-comment-callback-consumed-1",
    )
    assert first["outcome"] == "rec_needs_clarification"
    assert first["rec"]["missing_field"] == "temporal_edit_field"
    first_q = str(first.get("clarifying_question") or "")
    assert first_q.startswith("Введите новый комментарий.")
    assert "Текущий комментарий:\n-" in first_q
    first_cmd = first.get("command") if isinstance(first.get("command"), dict) else {}
    assert str(first_cmd.get("comment_text") or "") == ""
    assert str(first_cmd.get("comment") or "") == ""

    db.session = {
        "intent": "meeting.create",
        "missing_field": "temporal_edit_field",
        "idempotency_key": "idem-comment-callback-consumed",
        "payload": dict(first_cmd),
    }
    second = handler.handle_user_attempt(
        request_id="r-comment-callback-consumed-2",
        user_id="u-comment-callback-consumed",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="обсудить договор",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-comment-callback-consumed",
        source_message_id="msg-comment-callback-consumed-2",
    )
    assert second["outcome"] == "rec_needs_clarification"
    assert second["rec"]["missing_field"] == "temporal_commit_confirm"
    assert "Комментарий: обсудить договор" in str(second.get("clarifying_question") or "")


def test_temporal_edit_time_and_duration_callbacks_are_consumed_without_summary_rerender() -> None:
    handler = _load_handler_module()
    base_payload = {
        "intent": "meeting.create",
        "start_at": _dt_iso(1, 11),
        "start_at_date": _date_iso(1),
        "start_at_time": "11:00",
        "duration_minutes": 30,
        "__source_text": f"запланируй встречу с Алексеем {_date_iso(1)} в 11",
        "entities": {
            "text": f"запланируй встречу с Алексеем {_date_iso(1)} в 11",
            "meeting_kind": "встреча",
            "start_at_date": _date_iso(1),
            "start_at_time": "11:00",
            "duration_minutes": 30,
        },
    }
    expectations = {
        "clarify:v1:temporal_edit:time": "На какое время запланировать?",
        "clarify:v1:temporal_edit:duration": "На сколько минут запланировать?",
    }

    for callback_data, expected_question in expectations.items():
        rec = _RecProbe(should_fail_on_call=True)
        db = _FakeDb(
            session={
                "intent": "meeting.create",
                "missing_field": "temporal_commit_confirm",
                "idempotency_key": f"idem-{callback_data}",
                "payload": dict(base_payload),
            }
        )

        result = handler.handle_user_attempt(
            request_id=f"r-{callback_data}",
            user_id="u-temporal-edit-callbacks",
            channel="telegram",
            asr_client=_NoopAsr(),
            llm_client=_NeverCalledLLM(),
            rec_client=rec,
            text="время" if callback_data.endswith(":time") else "длительность",
            metadata={"callback_query": {"id": "cb-x", "data": callback_data}},
            local_db=db,
            app_id="app",
            tenant_id="tenant",
            source_chat_id="chat-temporal-edit-callbacks",
            source_message_id="msg-temporal-edit-callbacks",
        )
        assert result["outcome"] == "rec_needs_clarification"
        assert result["rec"]["missing_field"] == "temporal_edit_field"
        assert str(result.get("clarifying_question") or "") == expected_question
        assert "Подтвердить планирование?" not in str(result.get("clarifying_question") or "")
        assert rec.calls == 0


def test_temporal_edit_comment_callback_pressed_twice_does_not_send_second_prompt() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    payload = {
        "intent": "meeting.create",
        "start_at": _dt_iso(1, 11),
        "start_at_date": _date_iso(1),
        "start_at_time": "11:00",
        "duration_minutes": 30,
        "__temporal_edit_active_field": "meeting_comment_text",
        "entities": {
            "text": f"запланируй встречу {_date_iso(1)} в 11",
            "meeting_kind": "встреча",
            "start_at_date": _date_iso(1),
            "start_at_time": "11:00",
            "duration_minutes": 30,
        },
    }
    db = _FakeDb(
        session={
            "intent": "meeting.create",
            "missing_field": "temporal_edit_field",
            "source_message_id": "9001",
            "idempotency_key": "idem-comment-double",
            "payload": dict(payload),
        }
    )

    result = handler.handle_user_attempt(
        request_id="r-comment-double",
        user_id="u-comment-double",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="Комментарий",
        metadata={"callback_query": {"id": "cb-comment-double", "data": "clarify:v1:temporal_edit:comment"}},
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-comment-double",
        source_message_id="9001",
    )
    assert result["outcome"] == "rec_needs_clarification"
    assert result["rec"]["missing_field"] == "temporal_edit_field"
    q = str(result.get("clarifying_question") or "").lower()
    assert "комментар" in q
    assert "оставить без изменений" in q
    cmd = result.get("command") if isinstance(result.get("command"), dict) else {}
    assert str(cmd.get("__temporal_edit_active_field") or "") == "meeting_comment_text"


def test_temporal_edit_comment_then_time_callback_does_not_switch_active_field() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    payload = {
        "intent": "meeting.create",
        "start_at": _dt_iso(1, 11),
        "start_at_date": _date_iso(1),
        "start_at_time": "11:00",
        "duration_minutes": 30,
        "__temporal_edit_active_field": "meeting_comment_text",
        "entities": {
            "text": f"запланируй встречу {_date_iso(1)} в 11",
            "meeting_kind": "встреча",
            "start_at_date": _date_iso(1),
            "start_at_time": "11:00",
            "duration_minutes": 30,
        },
    }
    db = _FakeDb(
        session={
            "intent": "meeting.create",
            "missing_field": "temporal_edit_field",
            "source_message_id": "9002",
            "idempotency_key": "idem-comment-then-time",
            "payload": dict(payload),
        }
    )

    result = handler.handle_user_attempt(
        request_id="r-comment-then-time",
        user_id="u-comment-then-time",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="время",
        metadata={"callback_query": {"id": "cb-comment-then-time", "data": "clarify:v1:temporal_edit:time"}},
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-comment-then-time",
        source_message_id="9002",
    )
    assert result["outcome"] == "rec_needs_clarification"
    assert result["rec"]["missing_field"] == "temporal_edit_field"
    q = str(result.get("clarifying_question") or "").lower()
    assert "комментар" in q
    assert "оставить без изменений" in q
    cmd = result.get("command") if isinstance(result.get("command"), dict) else {}
    assert str(cmd.get("__temporal_edit_active_field") or "") == "meeting_comment_text"


def test_temporal_edit_stale_summary_callback_is_ignored() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    payload = {
        "intent": "meeting.create",
        "start_at": _dt_iso(1, 11),
        "start_at_date": _date_iso(1),
        "start_at_time": "11:00",
        "duration_minutes": 30,
        "entities": {
            "text": f"запланируй встречу {_date_iso(1)} в 11",
            "meeting_kind": "встреча",
            "start_at_date": _date_iso(1),
            "start_at_time": "11:00",
            "duration_minutes": 30,
        },
    }
    db = _FakeDb(
        session={
            "intent": "meeting.create",
            "missing_field": "temporal_commit_confirm",
            "source_message_id": "9100",
            "idempotency_key": "idem-stale-summary",
            "payload": dict(payload),
        }
    )

    result = handler.handle_user_attempt(
        request_id="r-stale-summary",
        user_id="u-stale-summary",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="Комментарий",
        metadata={"callback_query": {"id": "cb-stale-summary", "data": "clarify:v1:temporal_edit:comment"}},
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-stale-summary",
        source_message_id="9099",
    )
    assert result["outcome"] == "rec_needs_clarification"
    assert result["rec"]["missing_field"] == "temporal_commit_confirm"
    assert str(result.get("clarifying_question") or "") == ""
    telemetry = result.get("telemetry") if isinstance(result.get("telemetry"), dict) else {}
    assert telemetry.get("suppress_send") is True


def test_temporal_comment_prompt_shows_current_comment_and_keep_preserves_it() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    payload = {
        "intent": "meeting.create",
        "start_at": _dt_iso(1, 11),
        "start_at_date": _date_iso(1),
        "start_at_time": "11:00",
        "duration_minutes": 30,
        "comment_text": "старый комментарий",
        "comment": "старый комментарий",
        "entities": {
            "text": f"запланируй встречу {_date_iso(1)} в 11",
            "meeting_kind": "встреча",
            "start_at_date": _date_iso(1),
            "start_at_time": "11:00",
            "duration_minutes": 30,
            "comment_text": "старый комментарий",
            "comment": "старый комментарий",
        },
    }
    db = _FakeDb(
        session={
            "intent": "meeting.create",
            "missing_field": "temporal_commit_confirm",
            "source_message_id": "9201",
            "idempotency_key": "idem-comment-keep",
            "payload": dict(payload),
        }
    )

    first = handler.handle_user_attempt(
        request_id="r-comment-keep-1",
        user_id="u-comment-keep",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="Комментарий",
        metadata={"callback_query": {"id": "cb-comment-keep-1", "data": "clarify:v1:temporal_edit:comment"}},
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-comment-keep",
        source_message_id="9201",
    )
    q = str(first.get("clarifying_question") or "")
    assert "Текущий комментарий:\nстарый комментарий" in q
    first_cmd = first.get("command") if isinstance(first.get("command"), dict) else {}
    assert str(first_cmd.get("comment_text") or "") == "старый комментарий"

    db.session = {
        "intent": "meeting.create",
        "missing_field": "temporal_edit_field",
        "source_message_id": "9201",
        "idempotency_key": "idem-comment-keep",
        "payload": dict(first_cmd),
    }
    second = handler.handle_user_attempt(
        request_id="r-comment-keep-2",
        user_id="u-comment-keep",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="оставить без изменений",
        metadata={"callback_query": {"id": "cb-comment-keep-2", "data": "clarify:v1:temporal_comment:keep"}},
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-comment-keep",
        source_message_id="9201",
    )
    assert second["outcome"] == "rec_needs_clarification"
    assert second["rec"]["missing_field"] == "temporal_commit_confirm"
    assert "Комментарий: старый комментарий" in str(second.get("clarifying_question") or "")


def test_temporal_comment_waiting_old_time_button_returns_finish_comment_reminder() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    payload = {
        "intent": "meeting.create",
        "start_at": _dt_iso(1, 11),
        "start_at_date": _date_iso(1),
        "start_at_time": "11:00",
        "duration_minutes": 30,
        "__temporal_edit_active_field": "meeting_comment_text",
        "entities": {
            "text": f"запланируй встречу {_date_iso(1)} в 11",
            "meeting_kind": "встреча",
            "start_at_date": _date_iso(1),
            "start_at_time": "11:00",
            "duration_minutes": 30,
        },
    }
    db = _FakeDb(
        session={
            "intent": "meeting.create",
            "missing_field": "temporal_edit_field",
            "source_message_id": "9301",
            "idempotency_key": "idem-comment-old-time",
            "payload": dict(payload),
        }
    )

    result = handler.handle_user_attempt(
        request_id="r-comment-old-time",
        user_id="u-comment-old-time",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="время",
        metadata={"callback_query": {"id": "cb-comment-old-time", "data": "clarify:v1:temporal_edit:time"}},
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-comment-old-time",
        source_message_id="9301",
    )
    assert result["outcome"] == "rec_needs_clarification"
    assert result["rec"]["missing_field"] == "temporal_edit_field"
    assert str(result.get("clarifying_question") or "") == "Сначала введите новый комментарий или нажмите «Оставить без изменений»."


def test_temporal_comment_clear_resets_comment_and_rerenders_summary() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    payload = {
        "intent": "meeting.create",
        "start_at": _dt_iso(1, 11),
        "start_at_date": _date_iso(1),
        "start_at_time": "11:00",
        "duration_minutes": 30,
        "comment_text": "старый комментарий",
        "comment": "старый комментарий",
        "__temporal_edit_active_field": "meeting_comment_text",
        "entities": {
            "text": f"запланируй встречу {_date_iso(1)} в 11",
            "meeting_kind": "встреча",
            "start_at_date": _date_iso(1),
            "start_at_time": "11:00",
            "duration_minutes": 30,
            "comment_text": "старый комментарий",
            "comment": "старый комментарий",
        },
    }
    db = _FakeDb(
        session={
            "intent": "meeting.create",
            "missing_field": "temporal_edit_field",
            "source_message_id": "9401",
            "idempotency_key": "idem-comment-clear",
            "payload": dict(payload),
        }
    )

    result = handler.handle_user_attempt(
        request_id="r-comment-clear",
        user_id="u-comment-clear",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="очистить комментарий",
        metadata={"callback_query": {"id": "cb-comment-clear", "data": "clarify:v1:temporal_comment:clear"}},
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-comment-clear",
        source_message_id="9401",
    )
    assert result["outcome"] == "rec_needs_clarification"
    assert result["rec"]["missing_field"] == "temporal_commit_confirm"
    q = str(result.get("clarifying_question") or "")
    assert "Комментарий: -" in q
    cmd = result.get("command") if isinstance(result.get("command"), dict) else {}
    assert str(cmd.get("comment_text") or "") == ""


def test_voice_transcript_spoken_evening_time_materializes_to_20_00() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(session=None)

    class _VoiceAsr:
        def transcribe(  # noqa: ANN001
            self,
            audio_bytes,
            mime_type=None,
            filename=None,
            request_id=None,
        ):
            _ = (audio_bytes, mime_type, filename, request_id)
            return "запланируй собрание завтра в восемь вечера"

    llm = _StaticLLM(
        {
            "intent": "meeting.create",
            "entities": {
                "text": "запланируй собрание завтра в восемь вечера",
                "start_at_time": "запланируй собрание завтра в восемь вечера.",
            },
        }
    )

    out = handler.handle_user_attempt(
        request_id="r-voice-evening-20",
        user_id="u-voice-evening-20",
        channel="telegram",
        asr_client=_VoiceAsr(),
        llm_client=llm,
        rec_client=rec,
        audio_bytes=b"audio-bytes",
        audio_mime="audio/ogg",
        audio_filename="voice.ogg",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-voice-evening-20",
        source_message_id="msg-voice-evening-20",
    )

    assert out["outcome"] == "rec_needs_clarification"
    assert out["rec"]["missing_field"] == "temporal_commit_confirm"
    cmd = out.get("command") if isinstance(out.get("command"), dict) else {}
    assert str(cmd.get("meeting_kind") or "") == "собрание"
    assert str(cmd.get("start_at_time") or "") == "20:00"
    q = str(out.get("clarifying_question") or "")
    q_low = q.lower()
    assert "Время: 20:00" in q
    assert "время: запланируй собрание завтра" not in q_low
    assert rec.calls == 0


def test_text_spoken_evening_time_materializes_to_20_00() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(session=None)
    llm = _StaticLLM(
        {
            "intent": "meeting.create",
            "entities": {
                "text": "запланируй собрание завтра в 8 вечера",
                "start_at_time": "запланируй собрание завтра в 8 вечера.",
            },
        }
    )

    out = handler.handle_user_attempt(
        request_id="r-text-evening-20",
        user_id="u-text-evening-20",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=llm,
        rec_client=rec,
        text="запланируй собрание завтра в 8 вечера",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-text-evening-20",
        source_message_id="msg-text-evening-20",
    )

    assert out["outcome"] == "rec_needs_clarification"
    assert out["rec"]["missing_field"] == "temporal_commit_confirm"
    cmd = out.get("command") if isinstance(out.get("command"), dict) else {}
    assert str(cmd.get("meeting_kind") or "") == "собрание"
    assert str(cmd.get("start_at_time") or "") == "20:00"
    q = str(out.get("clarifying_question") or "")
    q_low = q.lower()
    assert "Время: 20:00" in q
    assert "время: запланируй собрание завтра" not in q_low
    assert rec.calls == 0


def test_new_task_command_during_temporal_confirm_is_not_treated_as_yes_no() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(
        session={
            "intent": "meeting.create",
            "missing_field": "temporal_commit_confirm",
            "idempotency_key": "idem-new-task-during-temporal-confirm",
            "payload": {
                "intent": "meeting.create",
                "start_at_date": _date_iso(1),
                "start_at_time": "10:00",
                "duration_minutes": 30,
                "entities": {
                    "text": f"встреча {_date_iso(1)} в 10",
                    "meeting_kind": "встреча",
                    "start_at_date": _date_iso(1),
                    "start_at_time": "10:00",
                    "duration_minutes": 30,
                },
            },
        }
    )
    llm = _StaticLLM(
        {
            "intent": "task.create",
            "entities": {
                "text": "создай задачу завтра в 12 часов купить молоко",
                "title": "купить молоко",
                "due_date": _date_iso(1),
            },
        }
    )

    out = handler.handle_user_attempt(
        request_id="r-new-task-during-temporal-confirm",
        user_id="u-new-task-during-temporal-confirm",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=llm,
        rec_client=rec,
        text="создай задачу завтра в 12 часов купить молоко",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-new-task-during-temporal-confirm",
        source_message_id="msg-new-task-during-temporal-confirm",
    )

    assert out["outcome"] == "rec_needs_clarification"
    assert out["rec"]["missing_field"] == "task_create_confirm"
    q = str(out.get("clarifying_question") or "").lower()
    assert "подтверди планирование: да или нет." not in q
    cmd = out.get("command") if isinstance(out.get("command"), dict) else {}
    assert str(cmd.get("intent") or "").strip().lower() in {"task.create", "task_create", "task"}
    assert str(cmd.get("intent") or "").strip().lower() != "meeting.create"
    assert rec.calls == 0


def test_non_command_text_during_temporal_confirm_reasks_yes_no() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(
        session={
            "intent": "meeting.create",
            "missing_field": "temporal_commit_confirm",
            "idempotency_key": "idem-non-command-during-temporal-confirm",
            "payload": {
                "intent": "meeting.create",
                "start_at_date": _date_iso(1),
                "start_at_time": "10:00",
                "duration_minutes": 30,
                "entities": {
                    "text": f"встреча {_date_iso(1)} в 10",
                    "meeting_kind": "встреча",
                    "start_at_date": _date_iso(1),
                    "start_at_time": "10:00",
                    "duration_minutes": 30,
                },
            },
        }
    )

    out = handler.handle_user_attempt(
        request_id="r-non-command-during-temporal-confirm",
        user_id="u-non-command-during-temporal-confirm",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="купить молоко",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-non-command-during-temporal-confirm",
        source_message_id="msg-non-command-during-temporal-confirm",
    )

    assert out["outcome"] == "rec_needs_clarification"
    assert out["rec"]["missing_field"] == "temporal_commit_confirm"
    q = str(out.get("clarifying_question") or "").lower()
    assert "подтверд" in q
    assert rec.calls == 0


def test_text_task_create_routes_to_task_intent_not_meeting() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(session=None)
    llm = _StaticLLM(
        {
            "intent": "task.create",
            "entities": {
                "text": "создай задачу завтра в 12 часов купить молоко",
                "title": "купить молоко",
                "due_date": _date_iso(1),
            },
        }
    )

    out = handler.handle_user_attempt(
        request_id="r-text-task-create-not-meeting",
        user_id="u-text-task-create-not-meeting",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=llm,
        rec_client=rec,
        text="создай задачу завтра в 12 часов купить молоко",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-text-task-create-not-meeting",
        source_message_id="msg-text-task-create-not-meeting",
    )

    assert out["outcome"] == "rec_needs_clarification"
    assert out["rec"]["missing_field"] == "task_create_confirm"
    cmd = out.get("command") if isinstance(out.get("command"), dict) else {}
    assert str(cmd.get("intent") or "").strip().lower() in {"task.create", "task_create", "task"}
    assert str(cmd.get("intent") or "").strip().lower() != "meeting.create"
    assert rec.calls == 0


def test_task_create_duplicate_precheck_shows_duplicate_warning_before_normal_summary() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(session=None)
    llm = _StaticLLM(
        {
            "intent": "task.create",
            "entities": {
                "text": "создай задачу купить молоко завтра",
                "title": "купить молоко",
                "due_date": _date_iso(1),
            },
        }
    )

    setattr(
        handler,
        "_task_create_duplicate_precheck",
        lambda user_id, command: {  # noqa: ARG005
            "ok": True,
            "duplicate_found": True,
            "existing_task": {
                "id": 17,
                "title": "Купить молоко",
                "planned_at": f"{_date_iso(1)}T09:00:00+03:00",
                "parent_task_id": None,
            },
        },
    )

    out = handler.handle_user_attempt(
        request_id="r-task-duplicate-precheck",
        user_id="u-task-duplicate-precheck",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=llm,
        rec_client=rec,
        text="создай задачу купить молоко завтра",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-task-duplicate-precheck",
        source_message_id="msg-task-duplicate-precheck",
    )

    assert out["outcome"] == "rec_needs_clarification"
    assert out["rec"]["missing_field"] == "task_create_duplicate_confirm"
    question = str(out.get("clarifying_question") or "")
    assert "Похоже, такая задача уже есть" in question
    assert "Создать ещё одну?" in question
    assert "Создать задачу?" not in question
    cmd = out.get("command") if isinstance(out.get("command"), dict) else {}
    entities = cmd.get("entities") if isinstance(cmd.get("entities"), dict) else {}
    assert str(cmd.get("title") or entities.get("title") or "").lower() == "купить молоко"
    assert str(cmd.get("planned_at") or entities.get("planned_at") or cmd.get("due_date") or entities.get("due_date") or "") == _date_iso(1)
    assert rec.calls == 0


def test_task_create_duplicate_precheck_resolves_relative_due_date_before_warning() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(session=None)
    llm = _StaticLLM(
        {
            "intent": "task.create",
            "entities": {
                "text": "создай задачу купить молоко завтра",
                "title": "купить молоко",
                "due_date": "завтра",
            },
        }
    )

    setattr(
        handler,
        "_task_create_duplicate_precheck",
        lambda user_id, command: {  # noqa: ARG005
            "ok": True,
            "duplicate_found": str(command.get("planned_at") or "") == _date_iso(1),
            "existing_task": {
                "id": 17,
                "title": "Купить молоко",
                "planned_at": f"{_date_iso(1)}T09:00:00+03:00",
                "parent_task_id": None,
            },
        },
    )

    out = handler.handle_user_attempt(
        request_id="r-task-duplicate-precheck-relative-due",
        user_id="u-task-duplicate-precheck-relative-due",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=llm,
        rec_client=rec,
        text="создай задачу купить молоко завтра",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-task-duplicate-precheck-relative-due",
        source_message_id="msg-task-duplicate-precheck-relative-due",
    )

    assert out["outcome"] == "rec_needs_clarification"
    assert out["rec"]["missing_field"] == "task_create_duplicate_confirm"
    question = str(out.get("clarifying_question") or "")
    assert "Похоже, такая задача уже есть" in question
    assert "Создать задачу?" not in question
    cmd = out.get("command") if isinstance(out.get("command"), dict) else {}
    assert str(cmd.get("planned_at") or "") == _date_iso(1)


def test_task_create_without_duplicate_shows_normal_confirmation_summary() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(session=None)
    llm = _StaticLLM(
        {
            "intent": "task.create",
            "entities": {
                "text": "создай задачу купить молоко завтра",
                "title": "купить молоко",
                "due_date": _date_iso(1),
            },
        }
    )

    setattr(
        handler,
        "_task_create_duplicate_precheck",
        lambda user_id, command: {"ok": True, "duplicate_found": False},  # noqa: ARG005
    )

    out = handler.handle_user_attempt(
        request_id="r-task-no-duplicate-precheck",
        user_id="u-task-no-duplicate-precheck",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=llm,
        rec_client=rec,
        text="создай задачу купить молоко завтра",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-task-no-duplicate-precheck",
        source_message_id="msg-task-no-duplicate-precheck",
    )

    assert out["outcome"] == "rec_needs_clarification"
    assert out["rec"]["missing_field"] == "task_create_confirm"
    question = str(out.get("clarifying_question") or "")
    assert "Проверь задачу перед созданием:" in question
    assert "Создать задачу?" in question
    assert "Создать ещё одну?" not in question
    assert rec.calls == 0


def test_task_create_duplicate_precheck_no_cancels_scenario() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(
        session={
            "intent": "task.create",
            "missing_field": "task_create_duplicate_confirm",
            "idempotency_key": "idem-task-duplicate-no",
            "payload": {
                "intent": "task.create",
                "title": "Купить молоко",
                "planned_at": _date_iso(1),
                "entities": {
                    "title": "Купить молоко",
                    "planned_at": _date_iso(1),
                },
            },
        }
    )

    result = handler.handle_user_attempt(
        request_id="r-task-duplicate-no",
        user_id="u-task-duplicate-no",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="Нет",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-task-duplicate-no",
        source_message_id="msg-task-duplicate-no",
    )

    assert result["outcome"] == "cancelled"
    assert "не создаю" in str(result.get("user_message") or "").lower()
    assert rec.calls == 0
    assert db.deleted_contexts


def test_task_create_duplicate_precheck_stop_cancels_scenario_without_memory_fallback() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(
        session={
            "intent": "task.create",
            "missing_field": "task_create_duplicate_confirm",
            "idempotency_key": "idem-task-duplicate-stop",
            "payload": {
                "intent": "task.create",
                "title": "Купить молоко",
                "planned_at": _date_iso(1),
                "entities": {
                    "title": "Купить молоко",
                    "planned_at": _date_iso(1),
                },
            },
        }
    )

    result = handler.handle_user_attempt(
        request_id="r-task-duplicate-stop",
        user_id="u-task-duplicate-stop",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="стоп",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-task-duplicate-stop",
        source_message_id="msg-task-duplicate-stop",
    )

    assert result["outcome"] == "cancelled"
    assert rec.calls == 0


def test_task_create_confirm_no_sooner_routes_to_edit_flow() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(
        session={
            "intent": "task.create",
            "missing_field": "task_create_confirm",
            "idempotency_key": "idem-task-confirm-no",
            "payload": {
                "intent": "task.create",
                "title": "Купить молоко",
                "planned_at": _date_iso(1),
                "entities": {
                    "title": "Купить молоко",
                    "planned_at": _date_iso(1),
                },
            },
        }
    )

    result = handler.handle_user_attempt(
        request_id="r-task-confirm-no",
        user_id="u-task-confirm-no",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="Нет",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-task-confirm-no",
        source_message_id="msg-task-confirm-no",
    )

    assert result["outcome"] == "rec_needs_clarification"
    assert result["rec"]["missing_field"] == "task_create_edit"
    assert rec.calls == 0


def test_task_create_confirm_explicit_cancel_cancels_scenario() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(
        session={
            "intent": "task.create",
            "missing_field": "task_create_confirm",
            "idempotency_key": "idem-task-confirm-cancel",
            "payload": {
                "intent": "task.create",
                "title": "Купить молоко",
                "planned_at": _date_iso(1),
                "entities": {
                    "title": "Купить молоко",
                    "planned_at": _date_iso(1),
                },
            },
        }
    )

    result = handler.handle_user_attempt(
        request_id="r-task-confirm-cancel",
        user_id="u-task-confirm-cancel",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="Не создавать",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-task-confirm-cancel",
        source_message_id="msg-task-confirm-cancel",
    )

    assert result["outcome"] == "cancelled"
    assert "задачу не создаю" in str(result.get("user_message") or "").lower()
    assert rec.calls == 0
    assert db.deleted_contexts
    assert db.deleted_contexts


def test_task_create_summary_shows_comment_placeholder_and_comment_button_routes_to_comment_state() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    llm = _StaticLLM(
        {
            "intent": "task.create",
            "entities": {
                "text": "создай задачу купить молоко завтра",
                "title": "Купить молоко",
                "due_date": _date_iso(1),
            },
        }
    )
    db = _FakeDb(session=None)

    first = handler.handle_user_attempt(
        request_id="r-task-comment-summary-1",
        user_id="u-task-comment-summary",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=llm,
        rec_client=rec,
        text="создай задачу купить молоко завтра",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-task-comment-summary",
        source_message_id="msg-task-comment-summary-1",
    )

    assert first["outcome"] == "rec_needs_clarification"
    assert first["rec"]["missing_field"] == "task_create_confirm"
    assert "Комментарий: -" in str(first.get("clarifying_question") or "")

    db.session = {
        "intent": "task.create",
        "missing_field": "task_create_confirm",
        "idempotency_key": "idem-task-comment-summary",
        "payload": dict(first["command"]),
    }
    second = handler.handle_user_attempt(
        request_id="r-task-comment-summary-2",
        user_id="u-task-comment-summary",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="Комментарий",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-task-comment-summary",
        source_message_id="msg-task-comment-summary-2",
    )

    assert second["outcome"] == "rec_needs_clarification"
    assert second["rec"]["missing_field"] == "task_create_comment"
    assert str(second.get("clarifying_question") or "") == "Введите комментарий к задаче."
    assert rec.calls == 0


def test_task_create_comment_entry_rerenders_summary_without_memory_fallback() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(
        session={
            "intent": "task.create",
            "missing_field": "task_create_comment",
            "idempotency_key": "idem-task-comment-entry",
            "payload": {
                "intent": "task.create",
                "title": "Купить молоко",
                "planned_at": _date_iso(1),
                "entities": {
                    "title": "Купить молоко",
                    "planned_at": _date_iso(1),
                },
            },
        }
    )

    out = handler.handle_user_attempt(
        request_id="r-task-comment-entry",
        user_id="u-task-comment-entry",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="взять 2 литра",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-task-comment-entry",
        source_message_id="msg-task-comment-entry",
    )

    assert out["outcome"] == "rec_needs_clarification"
    assert out["rec"]["missing_field"] == "task_create_confirm"
    assert "Комментарий: взять 2 литра" in str(out.get("clarifying_question") or "")
    cmd = out.get("command") if isinstance(out.get("command"), dict) else {}
    assert str(cmd.get("comment_text") or "") == "взять 2 литра"
    assert rec.calls == 0


def test_task_create_natural_language_comment_extraction_works() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    llm = _StaticLLM(
        {
            "intent": "task.create",
            "entities": {
                "text": "создай задачу купить молоко сегодня комментарий взять 2 литра",
                "title": "Задача",
                "due_date": _date_iso(0),
            },
        }
    )
    db = _FakeDb(session=None)

    out = handler.handle_user_attempt(
        request_id="r-task-comment-nl",
        user_id="u-task-comment-nl",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=llm,
        rec_client=rec,
        text="создай задачу купить молоко сегодня комментарий взять 2 литра",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-task-comment-nl",
        source_message_id="msg-task-comment-nl",
    )

    assert out["outcome"] == "rec_needs_clarification"
    assert out["rec"]["missing_field"] == "task_create_confirm"
    cmd = out.get("command") if isinstance(out.get("command"), dict) else {}
    assert str(cmd.get("title") or "") == "Купить молоко"
    assert str(cmd.get("comment_text") or "") == "взять 2 литра"
    assert "Комментарий: взять 2 литра" in str(out.get("clarifying_question") or "")


def test_task_create_comment_input_stop_cancels_cleanly() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(
        session={
            "intent": "task.create",
            "missing_field": "task_create_comment",
            "idempotency_key": "idem-task-comment-stop",
            "payload": {
                "intent": "task.create",
                "title": "Купить молоко",
                "planned_at": _date_iso(1),
                "entities": {
                    "title": "Купить молоко",
                    "planned_at": _date_iso(1),
                },
            },
        }
    )

    out = handler.handle_user_attempt(
        request_id="r-task-comment-stop",
        user_id="u-task-comment-stop",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="стоп",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-task-comment-stop",
        source_message_id="msg-task-comment-stop",
    )

    assert out["outcome"] == "cancelled"
    assert rec.calls == 0


def test_generic_undated_task_text_becomes_task_create_not_memory() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(session=None)
    llm = _StaticLLM({"intent": "unknown", "entities": {"text": "взять два батона"}})

    out = handler.handle_user_attempt(
        request_id="r-generic-undated-task",
        user_id="u-generic-undated-task",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=llm,
        rec_client=rec,
        text="взять два батона",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-generic-undated-task",
        source_message_id="msg-generic-undated-task",
    )

    assert out["outcome"] == "rec_needs_clarification"
    assert out["rec"]["missing_field"] == "task_create_confirm"
    cmd = out.get("command") if isinstance(out.get("command"), dict) else {}
    assert str(cmd.get("intent") or "").strip().lower() == "task.create"
    assert str(cmd.get("title") or "") == "Взять два батона"
    assert "в памяти нет данных по этому запросу" not in str(out.get("user_message") or "").lower()
    assert rec.calls == 0


def test_explicit_task_marker_phrase_routes_to_task_create() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(session=None)
    llm = _StaticLLM({"intent": "unknown", "entities": {"text": "создай действие проверить договор завтра"}})

    out = handler.handle_user_attempt(
        request_id="r-explicit-task-marker",
        user_id="u-explicit-task-marker",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=llm,
        rec_client=rec,
        text="создай действие проверить договор завтра",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-explicit-task-marker",
        source_message_id="msg-explicit-task-marker",
    )

    assert out["outcome"] == "rec_needs_clarification"
    cmd = out.get("command") if isinstance(out.get("command"), dict) else {}
    assert str(cmd.get("intent") or "").strip().lower() == "task.create"
    assert str(cmd.get("title") or "") == "Проверить договор"
    assert rec.calls == 0


def test_explicit_meeting_marker_routes_to_meeting_create() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(session=None)
    llm = _StaticLLM({"intent": "unknown", "entities": {"text": "создай встречу завтра в 10"}})

    out = handler.handle_user_attempt(
        request_id="r-explicit-meeting-marker",
        user_id="u-explicit-meeting-marker",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=llm,
        rec_client=rec,
        text="создай встречу завтра в 10",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-explicit-meeting-marker",
        source_message_id="msg-explicit-meeting-marker",
    )

    assert out["outcome"] == "rec_needs_clarification"
    assert out["rec"]["missing_field"] == "temporal_commit_confirm"
    cmd = out.get("command") if isinstance(out.get("command"), dict) else {}
    assert str(cmd.get("intent") or "").strip().lower() == "meeting.create"
    assert rec.calls == 0


def test_explicit_timeblock_marker_routes_to_timeblock_create() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(session=None)
    llm = _StaticLLM({"intent": "unknown", "entities": {"text": "выдели время завтра на договор"}})

    out = handler.handle_user_attempt(
        request_id="r-explicit-timeblock-marker",
        user_id="u-explicit-timeblock-marker",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=llm,
        rec_client=rec,
        text="выдели время завтра на договор",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-explicit-timeblock-marker",
        source_message_id="msg-explicit-timeblock-marker",
    )

    assert out["outcome"] == "rec_needs_clarification"
    cmd = out.get("command") if isinstance(out.get("command"), dict) else {}
    assert str(cmd.get("intent") or "").strip().lower() == "timeblock.create"
    assert rec.calls == 0


def test_meeting_markers_win_over_timeblock_markers() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(session=None)
    llm = _StaticLLM({"intent": "unknown", "entities": {"text": "забронируй встречу с Иваном"}})

    out = handler.handle_user_attempt(
        request_id="r-meeting-over-timeblock",
        user_id="u-meeting-over-timeblock",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=llm,
        rec_client=rec,
        text="забронируй встречу с Иваном",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-meeting-over-timeblock",
        source_message_id="msg-meeting-over-timeblock",
    )

    assert out["outcome"] == "rec_needs_clarification"
    cmd = out.get("command") if isinstance(out.get("command"), dict) else {}
    assert str(cmd.get("intent") or "").strip().lower() == "meeting.create"
    assert rec.calls == 0


def test_ambiguous_create_request_prompts_for_entity_choice(caplog) -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(session=None)
    llm = _StaticLLM({"intent": "unknown", "entities": {"text": "создай"}})

    with caplog.at_level("INFO"):
        out = handler.handle_user_attempt(
            request_id="r-ambiguous-create",
            user_id="u-ambiguous-create",
            channel="telegram",
            asr_client=_NoopAsr(),
            llm_client=llm,
            rec_client=rec,
            text="создай",
            local_db=db,
            app_id="app",
            tenant_id="tenant",
            source_chat_id="chat-ambiguous-create",
            source_message_id="msg-ambiguous-create",
        )

    assert out["outcome"] == "rec_needs_clarification"
    assert out["rec"]["missing_field"] == "ambiguous_create_route"
    assert "Что создать: событие в календаре, блок времени или задачу?" in str(out.get("clarifying_question") or "")
    assert rec.calls == 0
    assert any(r.getMessage() == "ambiguous_create_prompt_rendered" for r in caplog.records)


def test_ambiguous_create_task_choice_continues_task_flow() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(
        session={
            "intent": "unknown",
            "missing_field": "ambiguous_create_route",
            "idempotency_key": "idem-ambiguous-task",
            "payload": {"intent": "unknown"},
        }
    )

    out = handler.handle_user_attempt(
        request_id="r-ambiguous-task-choice",
        user_id="u-ambiguous-task-choice",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="Задачу",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-ambiguous-task-choice",
        source_message_id="msg-ambiguous-task-choice",
    )

    assert out["outcome"] == "rec_needs_clarification"
    assert out["rec"]["missing_field"] == "task_create_edit"
    assert str(out.get("clarifying_question") or "") == "Какую задачу создать?"
    assert rec.calls == 0


def test_ambiguous_create_meeting_choice_continues_meeting_flow() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(
        session={
            "intent": "unknown",
            "missing_field": "ambiguous_create_route",
            "idempotency_key": "idem-ambiguous-meeting",
            "payload": {"intent": "unknown"},
        }
    )

    out = handler.handle_user_attempt(
        request_id="r-ambiguous-meeting-choice",
        user_id="u-ambiguous-meeting-choice",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="Событие в календаре",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-ambiguous-meeting-choice",
        source_message_id="msg-ambiguous-meeting-choice",
    )

    assert out["outcome"] == "rec_needs_clarification"
    assert out["rec"]["missing_field"] == "meeting_create_details"
    assert str(out.get("clarifying_question") or "") == "Какое событие в календаре создать?"
    assert rec.calls == 0


def test_technical_command_text_does_not_route_to_memory() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(session=None)
    llm = _StaticLLM({"intent": "unknown", "entities": {"text": ".\\deploy_v2.ps1 -Services @(\"telegram-bot\",\"organizer-worker\",\"google-sync\")"}})

    out = handler.handle_user_attempt(
        request_id="r-technical-unknown",
        user_id="u-technical-unknown",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=llm,
        rec_client=rec,
        text=".\\deploy_v2.ps1 -Services @(\"telegram-bot\",\"organizer-worker\",\"google-sync\")",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-technical-unknown",
        source_message_id="msg-technical-unknown",
    )

    assert out["outcome"] == "unrecognized"
    assert "в памяти нет данных по этому запросу" not in str(out.get("user_message") or "").lower()
    assert rec.calls == 0


def test_active_task_comment_state_beats_explicit_memory_query() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(
        session={
            "intent": "task.create",
            "missing_field": "task_create_comment",
            "idempotency_key": "idem-task-comment-memory-priority",
            "payload": {
                "intent": "task.create",
                "title": "Купить хлеб",
                "planned_at": _date_iso(1),
                "entities": {
                    "title": "Купить хлеб",
                    "planned_at": _date_iso(1),
                },
            },
        }
    )

    out = handler.handle_user_attempt(
        request_id="r-task-comment-memory-priority",
        user_id="u-task-comment-memory-priority",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="вспомни, что я говорил про батоны",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-task-comment-memory-priority",
        source_message_id="msg-task-comment-memory-priority",
    )

    assert out["outcome"] == "rec_needs_clarification"
    assert out["rec"]["missing_field"] == "task_create_confirm"
    assert "Комментарий: вспомни, что я говорил про батоны" in str(out.get("clarifying_question") or "")
    assert rec.calls == 0


def test_temporal_text_never_falls_into_task_route(caplog) -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(session=None)
    llm = _StaticLLM(
        {
            "intent": "task.create",
            "entities": {
                "text": "запланируй встречу завтра в 12",
                "title": "встречу",
            },
        }
    )

    with caplog.at_level("ERROR"):
        out = handler.handle_user_attempt(
            request_id="r-temporal-not-task-fallback",
            user_id="u-temporal-not-task-fallback",
            channel="telegram",
            asr_client=_NoopAsr(),
            llm_client=llm,
            rec_client=rec,
            text="запланируй встречу завтра в 12",
            local_db=db,
            app_id="app",
            tenant_id="tenant",
            source_chat_id="chat-temporal-not-task-fallback",
            source_message_id="msg-temporal-not-task-fallback",
        )

    assert out["outcome"] == "rec_needs_clarification"
    assert out["rec"]["missing_field"] == "temporal_commit_confirm"
    cmd = out.get("command") if isinstance(out.get("command"), dict) else {}
    assert str(cmd.get("intent") or "").strip().lower() == "meeting.create"
    assert rec.calls == 0


def test_meeting_title_does_not_include_command_tail() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(session=None)
    llm = _StaticLLM(
        {
            "intent": "meeting.create",
            "entities": {
                "text": "запланируй собрание завтра в 9 вечера",
                "meeting_kind": "собрание",
                "start_at": _dt_iso(1, 21),
                "duration_minutes": 30,
                "title": "Собрание Запланирую собрание вечера",
            },
        }
    )

    out = handler.handle_user_attempt(
        request_id="r-meeting-title-clean-tail",
        user_id="u-meeting-title-clean-tail",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=llm,
        rec_client=rec,
        text="запланируй собрание завтра в 9 вечера",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-meeting-title-clean-tail",
        source_message_id="msg-meeting-title-clean-tail",
    )

    assert out["outcome"] == "rec_needs_clarification"
    title = str(out.get("command", {}).get("title") or "")
    assert title == "Собрание"
    assert rec.calls == 0


def test_meeting_intent_has_priority_over_task() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(session=None)
    llm = _StaticLLM(
        {
            "intent": "task.create",
            "entities": {
                "text": "запланируй встречу завтра в 9 вечера",
                "title": "что-то как задача",
            },
        }
    )

    out = handler.handle_user_attempt(
        request_id="r-meeting-intent-priority",
        user_id="u-meeting-intent-priority",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=llm,
        rec_client=rec,
        text="запланируй встречу завтра в 9 вечера",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-meeting-intent-priority",
        source_message_id="msg-meeting-intent-priority",
    )

    assert out["outcome"] == "rec_needs_clarification"
    cmd = out.get("command") if isinstance(out.get("command"), dict) else {}
    assert str(cmd.get("intent") or "").strip().lower() == "meeting.create"
    assert rec.calls == 0


def test_task_intent_without_meeting_keywords() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(session=None)
    llm = _StaticLLM(
        {
            "intent": "unknown",
            "entities": {
                "text": "создай задачу завтра в 12 купить молоко",
                "title": "купить молоко",
                "due_date": _date_iso(1),
            },
        }
    )

    out = handler.handle_user_attempt(
        request_id="r-task-intent-no-meeting-kw",
        user_id="u-task-intent-no-meeting-kw",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=llm,
        rec_client=rec,
        text="создай задачу завтра в 12 купить молоко",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-task-intent-no-meeting-kw",
        source_message_id="msg-task-intent-no-meeting-kw",
    )

    assert out["outcome"] == "rec_needs_clarification"
    cmd = out.get("command") if isinstance(out.get("command"), dict) else {}
    assert str(cmd.get("intent") or "").strip().lower() == "task.create"
    assert rec.calls == 0


def test_task_time_allocation_phrase_routes_to_timeblock_create_and_asks_start_time() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(session=None)
    llm = _StaticLLM(
        {
            "intent": "task.create",
            "entities": {
                "text": "задача на завтра выдели время на нее",
                "title": "задача",
            },
        }
    )

    out = handler.handle_user_attempt(
        request_id="r-task-time-allocation-tomorrow",
        user_id="u-task-time-allocation-tomorrow",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=llm,
        rec_client=rec,
        text="задача на завтра выдели время на нее",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-task-time-allocation-tomorrow",
        source_message_id="msg-task-time-allocation-tomorrow",
    )

    assert out["outcome"] == "rec_needs_clarification"
    assert out["rec"]["missing_field"] == "start_at_time"
    assert "Во сколько поставить блок?" in str(out.get("clarifying_question") or "")
    cmd = out.get("command") if isinstance(out.get("command"), dict) else {}
    assert str(cmd.get("intent") or "").strip().lower() == "timeblock.create"
    assert str(cmd.get("start_at_date") or "") == _date_iso(1)
    assert rec.calls == 0


def test_allocate_hour_for_task_tomorrow_routes_to_timeblock_create_with_duration() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(session=None)
    llm = _StaticLLM(
        {
            "intent": "task.create",
            "entities": {
                "text": "выдели завтра час на задачу",
                "title": "задача",
            },
        }
    )

    out = handler.handle_user_attempt(
        request_id="r-allocate-hour-task-tomorrow",
        user_id="u-allocate-hour-task-tomorrow",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=llm,
        rec_client=rec,
        text="выдели завтра час на задачу",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-allocate-hour-task-tomorrow",
        source_message_id="msg-allocate-hour-task-tomorrow",
    )

    assert out["outcome"] == "rec_needs_clarification"
    assert out["rec"]["missing_field"] == "start_at_time"
    assert "Во сколько поставить блок?" in str(out.get("clarifying_question") or "")
    cmd = out.get("command") if isinstance(out.get("command"), dict) else {}
    assert str(cmd.get("intent") or "").strip().lower() == "timeblock.create"
    assert str(cmd.get("start_at_date") or "") == _date_iso(1)
    assert int(cmd.get("duration_minutes") or 0) == 60
    assert rec.calls == 0


def test_allocate_30_minutes_for_task_tomorrow_routes_to_timeblock_create() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(session=None)
    llm = _StaticLLM(
        {
            "intent": "task.create",
            "entities": {
                "text": "завтра выдели 30 минут на задачу",
                "title": "задача",
            },
        }
    )

    out = handler.handle_user_attempt(
        request_id="r-allocate-30-task-tomorrow",
        user_id="u-allocate-30-task-tomorrow",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=llm,
        rec_client=rec,
        text="завтра выдели 30 минут на задачу",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-allocate-30-task-tomorrow",
        source_message_id="msg-allocate-30-task-tomorrow",
    )

    assert out["outcome"] == "rec_needs_clarification"
    assert out["rec"]["missing_field"] == "start_at_time"
    cmd = out.get("command") if isinstance(out.get("command"), dict) else {}
    assert str(cmd.get("intent") or "").strip().lower() == "timeblock.create"
    assert str(cmd.get("start_at_date") or "") == _date_iso(1)
    assert int(cmd.get("duration_minutes") or 0) == 30
    assert rec.calls == 0


def test_timeblock_allocation_comment_update_rerenders_summary_without_task_search_fallback() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(session=None)
    llm = _StaticLLM(
        {
            "intent": "task.create",
            "entities": {
                "text": "задача на завтра выдели время на нее",
                "title": "задача",
            },
        }
    )

    first = handler.handle_user_attempt(
        request_id="r-timeblock-allocation-comment-1",
        user_id="u-timeblock-allocation-comment",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=llm,
        rec_client=rec,
        text="задача на завтра выдели время на нее",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-timeblock-allocation-comment",
        source_message_id="msg-timeblock-allocation-comment-1",
    )
    assert first["outcome"] == "rec_needs_clarification"
    assert first["rec"]["missing_field"] == "start_at_time"
    first_cmd = first.get("command") if isinstance(first.get("command"), dict) else {}
    assert str(first_cmd.get("intent") or "").strip().lower() == "timeblock.create"
    assert bool(first_cmd.get("task_ref_optional")) is True

    db.session = {
        "intent": "timeblock.create",
        "missing_field": "start_at_time",
        "idempotency_key": "idem-timeblock-allocation-comment",
        "payload": dict(first_cmd),
    }
    second = handler.handle_user_attempt(
        request_id="r-timeblock-allocation-comment-2",
        user_id="u-timeblock-allocation-comment",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="15",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-timeblock-allocation-comment",
        source_message_id="msg-timeblock-allocation-comment-2",
    )
    assert second["outcome"] == "rec_needs_clarification"
    assert second["rec"]["missing_field"] == "duration_minutes"
    assert "На сколько минут поставить блок?" in str(second.get("clarifying_question") or "")

    db.session = {
        "intent": "timeblock.create",
        "missing_field": "duration_minutes",
        "idempotency_key": "idem-timeblock-allocation-comment",
        "payload": dict(second["command"]),
    }
    third = handler.handle_user_attempt(
        request_id="r-timeblock-allocation-comment-3",
        user_id="u-timeblock-allocation-comment",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="30",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-timeblock-allocation-comment",
        source_message_id="msg-timeblock-allocation-comment-3",
    )
    assert third["outcome"] == "rec_needs_clarification"
    assert third["rec"]["missing_field"] == "temporal_commit_confirm"
    third_q = str(third.get("clarifying_question") or "")
    assert "Время: 15:00" in third_q
    assert "Комментарий: -" in third_q

    db.session = {
        "intent": "timeblock.create",
        "missing_field": "temporal_commit_confirm",
        "idempotency_key": "idem-timeblock-allocation-comment",
        "payload": dict(third["command"]),
    }
    fourth = handler.handle_user_attempt(
        request_id="r-timeblock-allocation-comment-4",
        user_id="u-timeblock-allocation-comment",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="Комментарий",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-timeblock-allocation-comment",
        source_message_id="msg-timeblock-allocation-comment-4",
    )
    assert fourth["outcome"] == "rec_needs_clarification"
    assert fourth["rec"]["missing_field"] == "temporal_edit_field"
    assert str(fourth.get("clarifying_question") or "").startswith("Введите новый комментарий.")

    db.session = {
        "intent": "timeblock.create",
        "missing_field": "temporal_edit_field",
        "idempotency_key": "idem-timeblock-allocation-comment",
        "payload": dict(fourth["command"]),
    }
    fifth = handler.handle_user_attempt(
        request_id="r-timeblock-allocation-comment-5",
        user_id="u-timeblock-allocation-comment",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="купить молоко",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-timeblock-allocation-comment",
        source_message_id="msg-timeblock-allocation-comment-5",
    )
    assert fifth["outcome"] == "rec_needs_clarification"
    assert fifth["rec"]["missing_field"] == "temporal_commit_confirm"
    fifth_q = str(fifth.get("clarifying_question") or "")
    assert "Комментарий: купить молоко" in fifth_q
    assert "Не нашел подходящую задачу." not in fifth_q
    fifth_cmd = fifth.get("command") if isinstance(fifth.get("command"), dict) else {}
    assert str(fifth_cmd.get("comment_text") or "") == "купить молоко"
    assert bool(fifth_cmd.get("task_ref_optional")) is True
    assert rec.calls == 0


def test_explicit_task_create_stays_task_create_not_timeblock() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(session=None)
    llm = _StaticLLM(
        {
            "intent": "task.create",
            "entities": {
                "text": "создай задачу на завтра",
                "title": "Задача",
                "due_date": _date_iso(1),
            },
        }
    )

    out = handler.handle_user_attempt(
        request_id="r-explicit-task-create-stays-task",
        user_id="u-explicit-task-create-stays-task",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=llm,
        rec_client=rec,
        text="создай задачу на завтра",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-explicit-task-create-stays-task",
        source_message_id="msg-explicit-task-create-stays-task",
    )

    assert out["outcome"] == "rec_needs_clarification"
    assert out["rec"]["missing_field"] == "task_create_confirm"
    cmd = out.get("command") if isinstance(out.get("command"), dict) else {}
    assert str(cmd.get("intent") or "").strip().lower() == "task.create"
    assert rec.calls == 0


def test_meeting_create_phrase_stays_meeting_not_timeblock() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(session=None)
    llm = _StaticLLM(
        {
            "intent": "task.create",
            "entities": {
                "text": "назначь встречу завтра",
                "title": "задача",
            },
        }
    )

    out = handler.handle_user_attempt(
        request_id="r-meeting-create-stays-meeting-not-timeblock",
        user_id="u-meeting-create-stays-meeting-not-timeblock",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=llm,
        rec_client=rec,
        text="назначь встречу завтра",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-meeting-create-stays-meeting-not-timeblock",
        source_message_id="msg-meeting-create-stays-meeting-not-timeblock",
    )

    assert out["outcome"] == "rec_needs_clarification"
    assert out["rec"]["missing_field"] == "start_at_time"
    assert "Во сколько запланировать встречу?" in str(out.get("clarifying_question") or "")
    cmd = out.get("command") if isinstance(out.get("command"), dict) else {}
    assert str(cmd.get("intent") or "").strip().lower() == "meeting.create"
    assert rec.calls == 0


def test_sync_conflict_prompt_is_rendered_for_meeting_update_conflict() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    db = _FakeDb(session=None)
    llm = _StaticLLM(
        {
            "intent": "meeting.update",
            "entities": {
                "text": "перенеси встречу",
                "__sync_conflict": {
                    "id": "sc-1",
                    "title": "Встреча с Алексеем",
                    "start_at_date": _date_iso(1),
                    "start_at_time": "11:00",
                    "summary_text": f"Встреча с Алексеем\n{_date_iso(1)} 11:00",
                },
            },
        }
    )

    out = handler.handle_user_attempt(
        request_id="r-sync-conflict-prompt",
        user_id="u-sync-conflict-prompt",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=llm,
        rec_client=rec,
        text="перенеси встречу",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-sync-conflict-prompt",
        source_message_id="msg-sync-conflict-prompt",
    )

    assert out["outcome"] == "rec_needs_clarification"
    assert out["rec"]["missing_field"] == "sync_conflict_resolution"
    assert "Найдено событие в базе" in str(out.get("clarifying_question") or "")


def test_sync_conflict_delete_callback_resolves_without_reask() -> None:
    handler = _load_handler_module()
    original = handler._sync_conflict_apply_action
    handler._sync_conflict_apply_action = lambda conflict_id, action: {  # type: ignore[assignment]
        "ok": True,
        "conflict_id": conflict_id,
        "action": action,
        "user_message": "Событие удалено из внутренней базы и больше не будет предлагаться.",
    }
    try:
        db = _FakeDb(
            session={
                "intent": "meeting.update",
                "missing_field": "sync_conflict_resolution",
                "idempotency_key": "idem-sync-conflict-delete",
                "payload": {
                    "intent": "meeting.update",
                    "__sync_conflict": {"id": "sc-2", "title": "Встреча"},
                },
            }
        )
        out = handler.handle_user_attempt(
            request_id="r-sync-conflict-delete",
            user_id="u-sync-conflict-delete",
            channel="telegram",
            asr_client=_NoopAsr(),
            llm_client=_NeverCalledLLM(),
            rec_client=_RecProbe(should_fail_on_call=True),
            text="удалить из базы",
            metadata={"callback_query": {"id": "cb-sync-delete", "data": "sync_conflict:delete_db:sc-2"}},
            local_db=db,
            app_id="app",
            tenant_id="tenant",
            source_chat_id="chat-sync-conflict-delete",
            source_message_id="msg-sync-conflict-delete",
        )
    finally:
        handler._sync_conflict_apply_action = original  # type: ignore[assignment]

    assert out["outcome"] == "success"
    assert "удалено" in str(out.get("user_message") or "").lower()


def test_sync_conflict_edit_callback_without_active_session_opens_temporal_confirm() -> None:
    handler = _load_handler_module()
    original = handler._sync_conflict_apply_action
    handler._sync_conflict_apply_action = lambda conflict_id, action: {  # type: ignore[assignment]
        "ok": True,
        "conflict_id": conflict_id,
        "action": action,
        "conflict": {
            "id": conflict_id,
            "calendar_event_id": "evt-conflict-edit-1",
            "title": "Встреча с Алексеем",
            "start_at_date": _date_iso(1),
            "start_at_time": "11:00",
            "duration_minutes": 30,
            "comment_text": "обсудить договор",
            "meeting_kind": "встреча",
        },
    }
    try:
        out = handler.handle_user_attempt(
            request_id="r-sync-conflict-edit",
            user_id="u-sync-conflict-edit",
            channel="telegram",
            asr_client=_NoopAsr(),
            llm_client=_NeverCalledLLM(),
            rec_client=_RecProbe(should_fail_on_call=True),
            text="изменить",
            metadata={"callback_query": {"id": "cb-sync-edit", "data": "sync_conflict:edit:sc-3"}},
            local_db=_FakeDb(session=None),
            app_id="app",
            tenant_id="tenant",
            source_chat_id="chat-sync-conflict-edit",
            source_message_id="msg-sync-conflict-edit",
        )
    finally:
        handler._sync_conflict_apply_action = original  # type: ignore[assignment]

    assert out["outcome"] == "rec_needs_clarification"
    assert out["rec"]["missing_field"] == "temporal_commit_confirm"
    assert "Подтвердить планирование?" in str(out.get("clarifying_question") or "")


def test_meeting_update_unified_flow_after_target_confirm_asks_field_choice() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    payload = {
        "intent": "meeting.update",
        "calendar_event_id": "evt-u-flow-1",
        "start_at_date": "2026-05-10",
        "start_at_time": "11:00",
        "duration_minutes": 30,
        "__source_text": "измени встречу с алексеем",
        "entities": {
            "text": "измени встречу с алексеем",
            "calendar_event_id": "evt-u-flow-1",
            "meeting_update_source_event_id": "evt-u-flow-1",
            "meeting_update_source_date": "2026-05-10",
            "meeting_update_source_time": "11:00",
            "meeting_update_source_duration": 30,
            "meeting_update_source_title": "Встреча с Алексеем",
            "start_at_date": "2026-05-10",
            "start_at_time": "11:00",
            "duration_minutes": 30,
        },
    }
    db = _FakeDb(
        session={
            "intent": "meeting.update",
            "missing_field": "awaiting_target_confirm",
            "idempotency_key": "idem-meeting-update-unified",
            "payload": dict(payload),
        }
    )
    out = handler.handle_user_attempt(
        request_id="r-meeting-update-unified",
        user_id="u-meeting-update-unified",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="да",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-meeting-update-unified",
        source_message_id="msg-meeting-update-unified",
    )
    assert out["outcome"] == "rec_needs_clarification"
    assert out["rec"]["missing_field"] == "meeting_update_edit_choice"
    assert "Что изменить?" in str(out.get("clarifying_question") or "")
    assert "Во сколько" not in str(out.get("clarifying_question") or "")


def test_meeting_update_choose_time_and_reply_16_renders_final_confirmation() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    payload = {
        "intent": "meeting.update",
        "calendar_event_id": "evt-u-time-1",
        "start_at_date": "2026-05-10",
        "start_at_time": "11:00",
        "duration_minutes": 30,
        "meeting_update_source_event_id": "evt-u-time-1",
        "meeting_update_source_date": "2026-05-10",
        "meeting_update_source_time": "11:00",
        "meeting_update_source_duration": 30,
        "meeting_update_source_title": "Встреча с Алексеем",
        "__meeting_update_target_confirmed": True,
        "entities": {
            "calendar_event_id": "evt-u-time-1",
            "meeting_update_source_event_id": "evt-u-time-1",
            "meeting_update_source_date": "2026-05-10",
            "meeting_update_source_time": "11:00",
            "meeting_update_source_duration": 30,
            "meeting_update_source_title": "Встреча с Алексеем",
            "start_at_date": "2026-05-10",
            "start_at_time": "11:00",
            "duration_minutes": 30,
        },
    }
    db = _FakeDb(
        session={
            "intent": "meeting.update",
            "missing_field": "meeting_update_edit_choice",
            "idempotency_key": "idem-meeting-update-time-choice",
            "payload": dict(payload),
        }
    )
    picked = handler.handle_user_attempt(
        request_id="r-meeting-update-time-choice-1",
        user_id="u-meeting-update-time-choice",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="время",
        metadata={"callback_query": {"id": "cb-mu-time", "data": "clarify:v1:meeting_update_edit:time"}},
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-meeting-update-time-choice",
        source_message_id="msg-meeting-update-time-choice-1",
    )
    assert picked["rec"]["missing_field"] == "meeting_update_edit_time"
    assert "На какое время" in str(picked.get("clarifying_question") or "")

    db.session = {
        "intent": "meeting.update",
        "missing_field": "meeting_update_edit_time",
        "idempotency_key": "idem-meeting-update-time-choice",
        "payload": dict(picked["command"]),
    }
    out = handler.handle_user_attempt(
        request_id="r-meeting-update-time-choice-2",
        user_id="u-meeting-update-time-choice",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="16",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-meeting-update-time-choice",
        source_message_id="msg-meeting-update-time-choice-2",
    )
    assert out["rec"]["missing_field"] == "awaiting_final_confirm"
    q = str(out.get("clarifying_question") or "")
    assert "Новые параметры:" in q
    assert "Время: 16:00" in q
    assert "На какое время" not in q


def test_meeting_update_choose_duration_and_reply_45_minutes_renders_final_confirmation() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    payload = {
        "intent": "meeting.update",
        "calendar_event_id": "evt-u-dur-1",
        "start_at_date": "2026-05-10",
        "start_at_time": "11:00",
        "duration_minutes": 30,
        "meeting_update_source_event_id": "evt-u-dur-1",
        "meeting_update_source_date": "2026-05-10",
        "meeting_update_source_time": "11:00",
        "meeting_update_source_duration": 30,
        "meeting_update_source_title": "Встреча с Алексеем",
        "__meeting_update_target_confirmed": True,
        "entities": {"start_at_date": "2026-05-10", "start_at_time": "11:00", "duration_minutes": 30},
    }
    db = _FakeDb(session={"intent": "meeting.update", "missing_field": "meeting_update_edit_choice", "idempotency_key": "idem-mu-dur", "payload": dict(payload)})
    picked = handler.handle_user_attempt(
        request_id="r-mu-dur-1",
        user_id="u-mu-dur",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="длительность",
        metadata={"callback_query": {"id": "cb-mu-dur", "data": "clarify:v1:meeting_update_edit:duration"}},
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-mu-dur",
        source_message_id="msg-mu-dur-1",
    )
    db.session = {"intent": "meeting.update", "missing_field": "meeting_update_edit_duration", "idempotency_key": "idem-mu-dur", "payload": dict(picked["command"])}
    out = handler.handle_user_attempt(
        request_id="r-mu-dur-2",
        user_id="u-mu-dur",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="45 минут",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-mu-dur",
        source_message_id="msg-mu-dur-2",
    )
    assert out["rec"]["missing_field"] == "awaiting_final_confirm"
    assert "45 минут" in str(out.get("clarifying_question") or "")


def test_meeting_update_choose_comment_and_reply_updates_confirmation_summary() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    payload = {
        "intent": "meeting.update",
        "calendar_event_id": "evt-u-comment-1",
        "start_at_date": "2026-05-10",
        "start_at_time": "11:00",
        "duration_minutes": 30,
        "meeting_update_source_event_id": "evt-u-comment-1",
        "meeting_update_source_date": "2026-05-10",
        "meeting_update_source_time": "11:00",
        "meeting_update_source_duration": 30,
        "meeting_update_source_title": "Встреча с Алексеем",
        "__meeting_update_target_confirmed": True,
        "entities": {"start_at_date": "2026-05-10", "start_at_time": "11:00", "duration_minutes": 30},
    }
    db = _FakeDb(session={"intent": "meeting.update", "missing_field": "meeting_update_edit_choice", "idempotency_key": "idem-mu-comment", "payload": dict(payload)})
    picked = handler.handle_user_attempt(
        request_id="r-mu-comment-1",
        user_id="u-mu-comment",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="Комментарий",
        metadata={"callback_query": {"id": "cb-mu-comment", "data": "clarify:v1:meeting_update_edit:comment"}},
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-mu-comment",
        source_message_id="msg-mu-comment-1",
    )
    assert picked["rec"]["missing_field"] == "meeting_update_edit_comment"
    db.session = {"intent": "meeting.update", "missing_field": "meeting_update_edit_comment", "idempotency_key": "idem-mu-comment", "payload": dict(picked["command"])}
    out = handler.handle_user_attempt(
        request_id="r-mu-comment-2",
        user_id="u-mu-comment",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="обсудить договор",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-mu-comment",
        source_message_id="msg-mu-comment-2",
    )
    assert out["rec"]["missing_field"] == "awaiting_final_confirm"
    assert "Комментарий: обсудить договор" in str(out.get("clarifying_question") or "")


def test_meeting_update_choose_date_time_asks_date_then_time_then_confirmation() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    payload = {
        "intent": "meeting.update",
        "calendar_event_id": "evt-u-datetime-1",
        "start_at_date": "2026-05-10",
        "start_at_time": "11:00",
        "duration_minutes": 30,
        "meeting_update_source_event_id": "evt-u-datetime-1",
        "meeting_update_source_date": "2026-05-10",
        "meeting_update_source_time": "11:00",
        "meeting_update_source_duration": 30,
        "meeting_update_source_title": "Встреча с Алексеем",
        "__meeting_update_target_confirmed": True,
        "entities": {"start_at_date": "2026-05-10", "start_at_time": "11:00", "duration_minutes": 30},
    }
    db = _FakeDb(session={"intent": "meeting.update", "missing_field": "meeting_update_edit_choice", "idempotency_key": "idem-mu-datetime", "payload": dict(payload)})
    picked = handler.handle_user_attempt(
        request_id="r-mu-datetime-1",
        user_id="u-mu-datetime",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="дата и время",
        metadata={"callback_query": {"id": "cb-mu-datetime", "data": "clarify:v1:meeting_update_edit:date_time"}},
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-mu-datetime",
        source_message_id="msg-mu-datetime-1",
    )
    assert picked["rec"]["missing_field"] == "meeting_update_edit_date_time_date"
    assert "На какую дату" in str(picked.get("clarifying_question") or "")
    db.session = {"intent": "meeting.update", "missing_field": "meeting_update_edit_date_time_date", "idempotency_key": "idem-mu-datetime", "payload": dict(picked["command"])}
    date_reply = handler.handle_user_attempt(
        request_id="r-mu-datetime-2",
        user_id="u-mu-datetime",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="11.05.2026",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-mu-datetime",
        source_message_id="msg-mu-datetime-2",
    )
    assert date_reply["rec"]["missing_field"] == "meeting_update_edit_date_time_time"
    assert "На какое время" in str(date_reply.get("clarifying_question") or "")
    db.session = {"intent": "meeting.update", "missing_field": "meeting_update_edit_date_time_time", "idempotency_key": "idem-mu-datetime", "payload": dict(date_reply["command"])}
    time_reply = handler.handle_user_attempt(
        request_id="r-mu-datetime-3",
        user_id="u-mu-datetime",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="в 16",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-mu-datetime",
        source_message_id="msg-mu-datetime-3",
    )
    assert time_reply["rec"]["missing_field"] == "awaiting_final_confirm"
    assert "Время: 16:00" in str(time_reply.get("clarifying_question") or "")


def test_meeting_update_choose_date_and_reply_renders_final_confirmation() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    payload = {
        "intent": "meeting.update",
        "calendar_event_id": "evt-u-date-1",
        "start_at_date": "2026-05-10",
        "start_at_time": "11:00",
        "duration_minutes": 30,
        "meeting_update_source_event_id": "evt-u-date-1",
        "meeting_update_source_date": "2026-05-10",
        "meeting_update_source_time": "11:00",
        "meeting_update_source_duration": 30,
        "meeting_update_source_title": "Встреча с Алексеем",
        "__meeting_update_target_confirmed": True,
        "entities": {"start_at_date": "2026-05-10", "start_at_time": "11:00", "duration_minutes": 30},
    }
    db = _FakeDb(
        session={
            "intent": "meeting.update",
            "missing_field": "meeting_update_edit_choice",
            "idempotency_key": "idem-mu-date",
            "payload": dict(payload),
        }
    )
    picked = handler.handle_user_attempt(
        request_id="r-mu-date-1",
        user_id="u-mu-date",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="дата",
        metadata={"callback_query": {"id": "cb-mu-date", "data": "clarify:v1:meeting_update_edit:date"}},
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-mu-date",
        source_message_id="msg-mu-date-1",
    )
    assert picked["rec"]["missing_field"] == "meeting_update_edit_date"
    assert "На какую дату" in str(picked.get("clarifying_question") or "")

    db.session = {
        "intent": "meeting.update",
        "missing_field": "meeting_update_edit_date",
        "idempotency_key": "idem-mu-date",
        "payload": dict(picked["command"]),
    }
    out = handler.handle_user_attempt(
        request_id="r-mu-date-2",
        user_id="u-mu-date",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="11.05.2026",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-mu-date",
        source_message_id="msg-mu-date-2",
    )
    assert out["rec"]["missing_field"] == "awaiting_final_confirm"
    q = str(out.get("clarifying_question") or "")
    assert "Новые параметры:" in q
    assert "Дата: 11.05.2026" in q
    assert "Время: 11:00" in q


def test_meeting_update_comment_keep_preserves_current_comment_in_summary() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    payload = {
        "intent": "meeting.update",
        "calendar_event_id": "evt-u-comment-keep-1",
        "start_at_date": "2026-05-10",
        "start_at_time": "11:00",
        "duration_minutes": 30,
        "comment_text": "старый комментарий",
        "comment": "старый комментарий",
        "meeting_update_source_event_id": "evt-u-comment-keep-1",
        "meeting_update_source_date": "2026-05-10",
        "meeting_update_source_time": "11:00",
        "meeting_update_source_duration": 30,
        "meeting_update_source_title": "Встреча с Алексеем",
        "__meeting_update_target_confirmed": True,
        "entities": {
            "start_at_date": "2026-05-10",
            "start_at_time": "11:00",
            "duration_minutes": 30,
            "comment_text": "старый комментарий",
            "comment": "старый комментарий",
        },
    }
    db = _FakeDb(
        session={
            "intent": "meeting.update",
            "missing_field": "meeting_update_edit_comment",
            "idempotency_key": "idem-mu-comment-keep",
            "payload": dict(payload),
        }
    )
    out = handler.handle_user_attempt(
        request_id="r-mu-comment-keep",
        user_id="u-mu-comment-keep",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="оставить без изменений",
        metadata={"callback_query": {"id": "cb-mu-comment-keep", "data": "clarify:v1:temporal_comment:keep"}},
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-mu-comment-keep",
        source_message_id="msg-mu-comment-keep",
    )
    assert out["rec"]["missing_field"] == "awaiting_final_confirm"
    q = str(out.get("clarifying_question") or "")
    assert "Комментарий: старый комментарий" in q
    cmd = out["command"]
    assert str(cmd.get("comment_text") or "") == "старый комментарий"
    assert str(cmd.get("comment") or "") == "старый комментарий"


def test_meeting_update_comment_clear_resets_comment_and_renders_final_confirmation() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    payload = {
        "intent": "meeting.update",
        "calendar_event_id": "evt-u-comment-clear-1",
        "start_at_date": "2026-05-10",
        "start_at_time": "11:00",
        "duration_minutes": 30,
        "comment_text": "старый комментарий",
        "comment": "старый комментарий",
        "meeting_update_source_event_id": "evt-u-comment-clear-1",
        "meeting_update_source_date": "2026-05-10",
        "meeting_update_source_time": "11:00",
        "meeting_update_source_duration": 30,
        "meeting_update_source_title": "Встреча с Алексеем",
        "__meeting_update_target_confirmed": True,
        "entities": {
            "start_at_date": "2026-05-10",
            "start_at_time": "11:00",
            "duration_minutes": 30,
            "comment_text": "старый комментарий",
            "comment": "старый комментарий",
        },
    }
    db = _FakeDb(
        session={
            "intent": "meeting.update",
            "missing_field": "meeting_update_edit_comment",
            "idempotency_key": "idem-mu-comment-clear",
            "payload": dict(payload),
        }
    )
    out = handler.handle_user_attempt(
        request_id="r-mu-comment-clear",
        user_id="u-mu-comment-clear",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="очистить комментарий",
        metadata={"callback_query": {"id": "cb-mu-comment-clear", "data": "clarify:v1:temporal_comment:clear"}},
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-mu-comment-clear",
        source_message_id="msg-mu-comment-clear",
    )
    assert out["rec"]["missing_field"] == "awaiting_final_confirm"
    q = str(out.get("clarifying_question") or "")
    assert "Комментарий: -" in q
    cmd = out["command"]
    assert str(cmd.get("comment_text") or "") == ""
    assert str(cmd.get("comment") or "") == ""


def test_meeting_update_comment_change_stops_on_summary_without_stale_temporal_prompt() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    payload = {
        "intent": "meeting.update",
        "calendar_event_id": "evt-prod-comment-1",
        "meeting_update_source_event_id": "evt-prod-comment-1",
        "meeting_update_source_date": "2026-05-12",
        "meeting_update_source_time": "11:00",
        "meeting_update_source_duration": 30,
        "meeting_update_source_title": "Встреча по договору",
        "__meeting_update_target_confirmed": True,
        "entities": {
            "text": "измени встречу",
            "calendar_event_id": "evt-prod-comment-1",
            "meeting_update_source_event_id": "evt-prod-comment-1",
            "meeting_update_source_date": "2026-05-12",
            "meeting_update_source_time": "11:00",
            "meeting_update_source_duration": 30,
        },
    }
    db = _FakeDb(
        session={
            "intent": "meeting.update",
            "missing_field": "meeting_update_edit_comment",
            "idempotency_key": "idem-prod-comment-1",
            "payload": dict(payload),
        }
    )

    out = handler.handle_user_attempt(
        request_id="r-prod-comment-1",
        user_id="u-prod-comment-1",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="подписать договор",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-prod-comment-1",
        source_message_id="msg-prod-comment-1",
    )

    assert out["outcome"] == "rec_needs_clarification"
    assert out["rec"]["missing_field"] == "awaiting_final_confirm"
    q = str(out.get("clarifying_question") or "")
    assert "Новые параметры:" in q
    assert "Комментарий: подписать договор" in q
    assert "На какое время перенести/изменить встречу?" not in q
    assert "Во сколько запланировать встречу?" not in q
    assert bool(out["command"].get("meeting_update_has_changes")) is True
    assert db.upserts[-1]["missing_field"] == "awaiting_final_confirm"
    persisted_payload = db.upserts[-1]["payload"]
    assert bool(persisted_payload.get("meeting_update_has_changes")) is True


def test_meeting_update_time_after_comment_preserves_summary_and_no_stale_date_prompt() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    payload = {
        "intent": "meeting.update",
        "calendar_event_id": "evt-prod-time-1",
        "meeting_update_source_event_id": "evt-prod-time-1",
        "meeting_update_source_date": "2026-05-12",
        "meeting_update_source_time": "11:00",
        "meeting_update_source_duration": 30,
        "meeting_update_source_title": "Встреча по договору",
        "comment_text": "подписать договор",
        "comment": "подписать договор",
        "meeting_update_has_changes": True,
        "__meeting_update_target_confirmed": True,
        "entities": {
            "text": "измени встречу",
            "calendar_event_id": "evt-prod-time-1",
            "meeting_update_source_event_id": "evt-prod-time-1",
            "meeting_update_source_date": "2026-05-12",
            "meeting_update_source_time": "11:00",
            "meeting_update_source_duration": 30,
            "comment_text": "подписать договор",
            "comment": "подписать договор",
            "meeting_update_has_changes": True,
        },
    }
    db = _FakeDb(
        session={
            "intent": "meeting.update",
            "missing_field": "meeting_update_edit_time",
            "idempotency_key": "idem-prod-time-1",
            "payload": dict(payload),
        }
    )

    out = handler.handle_user_attempt(
        request_id="r-prod-time-1",
        user_id="u-prod-time-1",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="12 час",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-prod-time-1",
        source_message_id="msg-prod-time-1",
    )

    assert out["outcome"] == "rec_needs_clarification"
    assert out["rec"]["missing_field"] == "awaiting_final_confirm"
    q = str(out.get("clarifying_question") or "")
    assert "Время: 12:00" in q
    assert "Комментарий: подписать договор" in q
    assert "На какую дату перенести/изменить встречу?" not in q
    assert "Во сколько запланировать встречу?" not in q
    persisted_payload = db.upserts[-1]["payload"]
    assert str(persisted_payload.get("meeting_update_new_time") or "") == "12:00"
    assert bool(persisted_payload.get("meeting_update_has_changes")) is True


def test_meeting_update_date_after_time_keeps_comment_and_does_not_fall_into_create_flow() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    payload = {
        "intent": "meeting.update",
        "calendar_event_id": "evt-prod-date-1",
        "meeting_update_source_event_id": "evt-prod-date-1",
        "meeting_update_source_date": "2026-05-12",
        "meeting_update_source_time": "11:00",
        "meeting_update_source_duration": 30,
        "meeting_update_source_title": "Встреча по договору",
        "meeting_update_new_time": "12:00",
        "start_at_time": "12:00",
        "comment_text": "подписать договор",
        "comment": "подписать договор",
        "meeting_update_has_changes": True,
        "__meeting_update_target_confirmed": True,
        "entities": {
            "text": "измени встречу",
            "calendar_event_id": "evt-prod-date-1",
            "meeting_update_source_event_id": "evt-prod-date-1",
            "meeting_update_source_date": "2026-05-12",
            "meeting_update_source_time": "11:00",
            "meeting_update_source_duration": 30,
            "meeting_update_new_time": "12:00",
            "start_at_time": "12:00",
            "comment_text": "подписать договор",
            "comment": "подписать договор",
            "meeting_update_has_changes": True,
        },
    }
    db = _FakeDb(
        session={
            "intent": "meeting.update",
            "missing_field": "meeting_update_edit_date",
            "idempotency_key": "idem-prod-date-1",
            "payload": dict(payload),
        }
    )

    out = handler.handle_user_attempt(
        request_id="r-prod-date-1",
        user_id="u-prod-date-1",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="13.05.2026",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-prod-date-1",
        source_message_id="msg-prod-date-1",
    )

    assert out["outcome"] == "rec_needs_clarification"
    assert out["rec"]["missing_field"] == "awaiting_final_confirm"
    q = str(out.get("clarifying_question") or "")
    assert "Дата: 13.05.2026" in q
    assert "Время: 12:00" in q
    assert "Комментарий: подписать договор" in q
    assert "Во сколько запланировать встречу?" not in q
    assert "На какое время перенести/изменить встречу?" not in q
    persisted_payload = db.upserts[-1]["payload"]
    assert str(persisted_payload.get("meeting_update_new_date") or "") == "2026-05-13"
    assert bool(persisted_payload.get("meeting_update_has_changes")) is True


def test_meeting_create_clarification_still_asks_time_separately() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    future_date = _date_iso(1)
    llm = _StaticLLM(
        {
            "intent": "meeting.create",
            "start_at_date": future_date,
            "entities": {
                "text": "запланируй встречу завтра",
                "start_at_date": future_date,
            },
        }
    )
    db = _FakeDb(session=None)

    out = handler.handle_user_attempt(
        request_id="r-create-time-1",
        user_id="u-create-time-1",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=llm,
        rec_client=rec,
        text="запланируй встречу завтра",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-create-time-1",
        source_message_id="msg-create-time-1",
    )

    assert out["outcome"] == "rec_needs_clarification"
    assert out["rec"]["missing_field"] == "start_at_time"
    assert "Во сколько запланировать встречу?" in str(out.get("clarifying_question") or "")


def test_meeting_create_duration_quick_pick_45_renders_updated_summary() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    future_date = _date_iso(1)
    payload = {
        "intent": "meeting.create",
        "start_at_date": future_date,
        "start_at_time": "11:00",
        "duration_minutes": 30,
        "__source_text": "запланируй встречу",
        "entities": {
            "text": "запланируй встречу",
            "start_at_date": future_date,
            "start_at_time": "11:00",
            "duration_minutes": 30,
        },
    }
    db = _FakeDb(
        session={
            "intent": "meeting.create",
            "missing_field": "temporal_commit_confirm",
            "idempotency_key": "idem-create-duration-45",
            "payload": dict(payload),
        }
    )
    picked = handler.handle_user_attempt(
        request_id="r-create-duration-45-1",
        user_id="u-create-duration-45",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="длительность",
        metadata={"callback_query": {"id": "cb-create-duration-45-open", "data": "clarify:v1:temporal_edit:duration"}},
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-create-duration-45",
        source_message_id="msg-create-duration-45-1",
    )
    assert picked["rec"]["missing_field"] == "temporal_edit_field"
    db.session = {
        "intent": "meeting.create",
        "missing_field": "temporal_edit_field",
        "idempotency_key": "idem-create-duration-45",
        "payload": dict(picked["command"]),
    }
    out = handler.handle_user_attempt(
        request_id="r-create-duration-45-2",
        user_id="u-create-duration-45",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="45 минут",
        metadata={"callback_query": {"id": "cb-create-duration-45-value", "data": "clarify:v1:temporal_duration:45"}},
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-create-duration-45",
        source_message_id="msg-create-duration-45-2",
    )
    assert out["rec"]["missing_field"] == "temporal_commit_confirm"
    q = str(out.get("clarifying_question") or "")
    assert "Длительность: 45 минут" in q
    assert "На сколько минут" not in q


def test_meeting_update_duration_quick_pick_60_renders_update_summary_without_extra_prompt() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    payload = {
        "intent": "meeting.update",
        "calendar_event_id": "evt-duration-60",
        "start_at_date": "2026-05-13",
        "start_at_time": "11:00",
        "duration_minutes": 30,
        "meeting_update_source_event_id": "evt-duration-60",
        "meeting_update_source_date": "2026-05-13",
        "meeting_update_source_time": "11:00",
        "meeting_update_source_duration": 30,
        "meeting_update_source_title": "Встреча по договору",
        "__meeting_update_target_confirmed": True,
        "entities": {
            "start_at_date": "2026-05-13",
            "start_at_time": "11:00",
            "duration_minutes": 30,
        },
    }
    db = _FakeDb(session={"intent": "meeting.update", "missing_field": "meeting_update_edit_duration", "idempotency_key": "idem-mu-duration-60", "payload": dict(payload)})
    out = handler.handle_user_attempt(
        request_id="r-mu-duration-60",
        user_id="u-mu-duration-60",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="60 минут",
        metadata={"callback_query": {"id": "cb-mu-duration-60", "data": "clarify:v1:meeting_update_duration:60"}},
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-mu-duration-60",
        source_message_id="msg-mu-duration-60",
    )
    assert out["rec"]["missing_field"] == "awaiting_final_confirm"
    q = str(out.get("clarifying_question") or "")
    assert "Длительность: 1 час" in q
    assert "На сколько минут" not in q
    assert "Во сколько запланировать встречу?" not in q


def test_timeblock_create_duration_quick_pick_30_renders_updated_summary() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    future_date = _date_iso(1)
    payload = {
        "intent": "timeblock.create",
        "start_at_date": future_date,
        "start_at_time": "11:00",
        "duration_minutes": 45,
        "__source_text": "поставь блок",
        "entities": {
            "text": "поставь блок",
            "start_at_date": future_date,
            "start_at_time": "11:00",
            "duration_minutes": 45,
        },
    }
    db = _FakeDb(
        session={
            "intent": "timeblock.create",
            "missing_field": "temporal_commit_confirm",
            "idempotency_key": "idem-timeblock-create-duration-30",
            "payload": dict(payload),
        }
    )
    picked = handler.handle_user_attempt(
        request_id="r-timeblock-create-duration-30-1",
        user_id="u-timeblock-create-duration-30",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="длительность",
        metadata={"callback_query": {"id": "cb-timeblock-create-duration-open", "data": "clarify:v1:temporal_edit:duration"}},
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-timeblock-create-duration-30",
        source_message_id="msg-timeblock-create-duration-30-1",
    )
    assert picked["rec"]["missing_field"] == "temporal_edit_field"
    db.session = {
        "intent": "timeblock.create",
        "missing_field": "temporal_edit_field",
        "idempotency_key": "idem-timeblock-create-duration-30",
        "payload": dict(picked["command"]),
    }
    out = handler.handle_user_attempt(
        request_id="r-timeblock-create-duration-30-2",
        user_id="u-timeblock-create-duration-30",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="30 минут",
        metadata={"callback_query": {"id": "cb-timeblock-create-duration-30", "data": "clarify:v1:temporal_duration:30"}},
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-timeblock-create-duration-30",
        source_message_id="msg-timeblock-create-duration-30-2",
    )
    assert out["rec"]["missing_field"] == "temporal_commit_confirm"
    assert "Длительность: 30 минут" in str(out.get("clarifying_question") or "")


def test_timeblock_update_duration_quick_pick_15_renders_update_summary() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    payload = {
        "intent": "timeblock.update",
        "time_block_id": 42,
        "start_at_date": "2026-05-13",
        "start_at_time": "11:00",
        "duration_minutes": 30,
        "timeblock_update_source_block_id": 42,
        "timeblock_update_source_date": "2026-05-13",
        "timeblock_update_source_time": "11:00",
        "timeblock_update_source_duration": 30,
        "timeblock_update_source_title": "Фокус по договору",
        "__timeblock_update_target_confirmed": True,
        "entities": {
            "start_at_date": "2026-05-13",
            "start_at_time": "11:00",
            "duration_minutes": 30,
        },
    }
    db = _FakeDb(session={"intent": "timeblock.update", "missing_field": "timeblock_update_edit_duration", "idempotency_key": "idem-tbu-duration-15", "payload": dict(payload)})
    out = handler.handle_user_attempt(
        request_id="r-tbu-duration-15",
        user_id="u-tbu-duration-15",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="15 минут",
        metadata={"callback_query": {"id": "cb-tbu-duration-15", "data": "clarify:v1:timeblock_update_duration:15"}},
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-tbu-duration-15",
        source_message_id="msg-tbu-duration-15",
    )
    assert out["rec"]["missing_field"] == "timeblock_update_final_confirm"
    q = str(out.get("clarifying_question") or "")
    assert "Длительность: 15 минут" in q
    assert "На сколько минут" not in q


def test_meeting_update_duration_other_asks_manual_input_and_manual_reply_still_works() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    payload = {
        "intent": "meeting.update",
        "calendar_event_id": "evt-duration-other",
        "start_at_date": "2026-05-13",
        "start_at_time": "11:00",
        "duration_minutes": 30,
        "meeting_update_source_event_id": "evt-duration-other",
        "meeting_update_source_date": "2026-05-13",
        "meeting_update_source_time": "11:00",
        "meeting_update_source_duration": 30,
        "meeting_update_source_title": "Встреча по договору",
        "__meeting_update_target_confirmed": True,
        "entities": {
            "start_at_date": "2026-05-13",
            "start_at_time": "11:00",
            "duration_minutes": 30,
        },
    }
    db = _FakeDb(session={"intent": "meeting.update", "missing_field": "meeting_update_edit_duration", "idempotency_key": "idem-mu-duration-other", "payload": dict(payload)})
    picked = handler.handle_user_attempt(
        request_id="r-mu-duration-other-1",
        user_id="u-mu-duration-other",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="иное",
        metadata={"callback_query": {"id": "cb-mu-duration-other", "data": "clarify:v1:meeting_update_duration:other"}},
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-mu-duration-other",
        source_message_id="msg-mu-duration-other-1",
    )
    assert picked["rec"]["missing_field"] == "meeting_update_edit_duration"
    assert "На сколько минут" in str(picked.get("clarifying_question") or "")

    db.session = {"intent": "meeting.update", "missing_field": "meeting_update_edit_duration", "idempotency_key": "idem-mu-duration-other", "payload": dict(picked["command"])}
    out = handler.handle_user_attempt(
        request_id="r-mu-duration-other-2",
        user_id="u-mu-duration-other",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="90 минут",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-mu-duration-other",
        source_message_id="msg-mu-duration-other-2",
    )
    assert out["rec"]["missing_field"] == "awaiting_final_confirm"
    assert "Длительность: 90 минут" in str(out.get("clarifying_question") or "")


def test_meeting_update_edit_cancel_callback_cancels_active_session() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    payload = {
        "intent": "meeting.update",
        "calendar_event_id": "evt-u-cancel-1",
        "meeting_update_source_event_id": "evt-u-cancel-1",
        "meeting_update_source_date": "2026-05-10",
        "meeting_update_source_time": "11:00",
        "meeting_update_source_duration": 30,
        "meeting_update_source_title": "Встреча с Алексеем",
        "__meeting_update_target_confirmed": True,
        "entities": {"text": "измени встречу с алексеем"},
    }
    db = _FakeDb(
        session={
            "intent": "meeting.update",
            "missing_field": "meeting_update_edit_choice",
            "idempotency_key": "idem-mu-cancel",
            "payload": dict(payload),
        }
    )
    out = handler.handle_user_attempt(
        request_id="r-mu-cancel",
        user_id="u-mu-cancel",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="отмена",
        metadata={"callback_query": {"id": "cb-mu-cancel", "data": "clarify:v1:meeting_update_edit:cancel"}},
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-mu-cancel",
        source_message_id="msg-mu-cancel",
    )
    assert out["outcome"] == "cancelled"
    assert str(out.get("user_message") or "") == "Ок, остановил текущий сценарий."


def test_meeting_update_date_callback_is_consumed_without_writing_button_label_into_draft() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    payload = {
        "intent": "meeting.update",
        "calendar_event_id": "evt-u-date-callback-1",
        "start_at_date": "2026-05-10",
        "start_at_time": "11:00",
        "duration_minutes": 30,
        "meeting_update_source_event_id": "evt-u-date-callback-1",
        "meeting_update_source_date": "2026-05-10",
        "meeting_update_source_time": "11:00",
        "meeting_update_source_duration": 30,
        "meeting_update_source_title": "Встреча с Алексеем",
        "__meeting_update_target_confirmed": True,
        "__meeting_update_active_edit_field": "date",
        "entities": {"start_at_date": "2026-05-10", "start_at_time": "11:00", "duration_minutes": 30},
    }
    db = _FakeDb(
        session={
            "intent": "meeting.update",
            "missing_field": "meeting_update_edit_date",
            "idempotency_key": "idem-mu-date-callback",
            "payload": dict(payload),
        }
    )
    out = handler.handle_user_attempt(
        request_id="r-mu-date-callback",
        user_id="u-mu-date-callback",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="дата",
        metadata={"callback_query": {"id": "cb-mu-date-repeat", "data": "clarify:v1:meeting_update_edit:date"}},
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-mu-date-callback",
        source_message_id="msg-mu-date-callback",
    )
    assert out["rec"]["missing_field"] == "meeting_update_edit_date"
    assert "На какую дату" in str(out.get("clarifying_question") or "")
    cmd = out["command"]
    assert str(cmd.get("meeting_update_new_date") or "") == ""
    entities = cmd.get("entities") if isinstance(cmd.get("entities"), dict) else {}
    assert str(entities.get("meeting_update_new_date") or "") == ""


def test_meeting_update_date_callback_pressed_twice_does_not_send_second_prompt() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    payload = {
        "intent": "meeting.update",
        "calendar_event_id": "evt-u-date-double-1",
        "start_at_date": "2026-05-10",
        "start_at_time": "11:00",
        "duration_minutes": 30,
        "meeting_update_source_event_id": "evt-u-date-double-1",
        "meeting_update_source_date": "2026-05-10",
        "meeting_update_source_time": "11:00",
        "meeting_update_source_duration": 30,
        "meeting_update_source_title": "Встреча с Алексеем",
        "__meeting_update_target_confirmed": True,
        "__meeting_update_active_edit_field": "date",
        "entities": {"start_at_date": "2026-05-10", "start_at_time": "11:00", "duration_minutes": 30},
    }
    db = _FakeDb(
        session={
            "intent": "meeting.update",
            "missing_field": "meeting_update_edit_date",
            "idempotency_key": "idem-mu-date-double",
            "payload": dict(payload),
        }
    )
    out = handler.handle_user_attempt(
        request_id="r-mu-date-double",
        user_id="u-mu-date-double",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="дата",
        metadata={"callback_query": {"id": "cb-mu-date-double", "data": "clarify:v1:meeting_update_edit:date"}},
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-mu-date-double",
        source_message_id="msg-mu-date-double",
    )
    assert out["outcome"] == "rec_needs_clarification"
    assert out["rec"]["missing_field"] == "meeting_update_edit_date"
    assert "На какую дату" in str(out.get("clarifying_question") or "")


def test_timeblock_update_auto_search_then_target_confirm_then_field_choice() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)

    def _fake_repeat_search(user_id: str, target_hint: str) -> dict:  # noqa: ARG001
        return {
            "ok": True,
            "candidate": {
                "time_block_id": 42,
                "task_id": 7,
                "title": "Фокус по отчёту",
                "comment": "старый комментарий",
                "start_at_date": "2026-05-10",
                "start_at_time": "11:00",
                "duration_minutes": 30,
            },
        }

    handler._timeblock_update_repeat_search = _fake_repeat_search  # type: ignore[attr-defined]
    db = _FakeDb(session=None)
    llm = _StaticLLM(
        {
            "intent": "timeblock.update",
            "entities": {
                "text": "перенеси блок по отчету",
            },
        }
    )

    first = handler.handle_user_attempt(
        request_id="r-tbu-flow-1",
        user_id="u-tbu-flow",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=llm,
        rec_client=rec,
        text="перенеси блок по отчету",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-tbu-flow",
        source_message_id="msg-tbu-flow-1",
    )
    assert first["outcome"] == "rec_needs_clarification"
    assert first["rec"]["missing_field"] == "timeblock_update_target_confirm"
    assert "Вы имеете в виду блок времени" in str(first.get("clarifying_question") or "")

    db.session = {
        "intent": "timeblock.update",
        "missing_field": "timeblock_update_target_confirm",
        "idempotency_key": "idem-tbu-flow",
        "payload": dict(first["command"]),
    }
    second = handler.handle_user_attempt(
        request_id="r-tbu-flow-2",
        user_id="u-tbu-flow",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="да",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-tbu-flow",
        source_message_id="msg-tbu-flow-2",
    )
    assert second["rec"]["missing_field"] == "timeblock_update_edit_choice"
    assert "Что изменить?" in str(second.get("clarifying_question") or "")


def test_timeblock_update_choose_date_and_reply_renders_final_confirmation() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    payload = {
        "intent": "timeblock.update",
        "time_block_id": 42,
        "start_at_date": "2026-05-10",
        "start_at_time": "11:00",
        "duration_minutes": 30,
        "timeblock_update_source_block_id": 42,
        "timeblock_update_source_date": "2026-05-10",
        "timeblock_update_source_time": "11:00",
        "timeblock_update_source_duration": 30,
        "timeblock_update_source_title": "Фокус по отчёту",
        "__timeblock_update_target_confirmed": True,
        "entities": {"start_at_date": "2026-05-10", "start_at_time": "11:00", "duration_minutes": 30},
    }
    db = _FakeDb(session={"intent": "timeblock.update", "missing_field": "timeblock_update_edit_choice", "idempotency_key": "idem-tbu-date", "payload": dict(payload)})
    picked = handler.handle_user_attempt(
        request_id="r-tbu-date-1",
        user_id="u-tbu-date",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="дата",
        metadata={"callback_query": {"id": "cb-tbu-date", "data": "clarify:v1:timeblock_update_edit:date"}},
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-tbu-date",
        source_message_id="msg-tbu-date-1",
    )
    assert picked["rec"]["missing_field"] == "timeblock_update_edit_date"
    db.session = {"intent": "timeblock.update", "missing_field": "timeblock_update_edit_date", "idempotency_key": "idem-tbu-date", "payload": dict(picked["command"])}
    out = handler.handle_user_attempt(
        request_id="r-tbu-date-2",
        user_id="u-tbu-date",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="11.05.2026",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-tbu-date",
        source_message_id="msg-tbu-date-2",
    )
    assert out["rec"]["missing_field"] == "timeblock_update_final_confirm"
    assert "Дата: 11.05.2026" in str(out.get("clarifying_question") or "")


def test_timeblock_update_choose_time_and_reply_renders_final_confirmation() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    payload = {
        "intent": "timeblock.update",
        "time_block_id": 42,
        "start_at_date": "2026-05-10",
        "start_at_time": "11:00",
        "duration_minutes": 30,
        "timeblock_update_source_block_id": 42,
        "timeblock_update_source_date": "2026-05-10",
        "timeblock_update_source_time": "11:00",
        "timeblock_update_source_duration": 30,
        "timeblock_update_source_title": "Фокус по отчёту",
        "__timeblock_update_target_confirmed": True,
        "entities": {"start_at_date": "2026-05-10", "start_at_time": "11:00", "duration_minutes": 30},
    }
    db = _FakeDb(session={"intent": "timeblock.update", "missing_field": "timeblock_update_edit_choice", "idempotency_key": "idem-tbu-time", "payload": dict(payload)})
    picked = handler.handle_user_attempt(
        request_id="r-tbu-time-1",
        user_id="u-tbu-time",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="время",
        metadata={"callback_query": {"id": "cb-tbu-time", "data": "clarify:v1:timeblock_update_edit:time"}},
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-tbu-time",
        source_message_id="msg-tbu-time-1",
    )
    assert picked["rec"]["missing_field"] == "timeblock_update_edit_time"
    db.session = {"intent": "timeblock.update", "missing_field": "timeblock_update_edit_time", "idempotency_key": "idem-tbu-time", "payload": dict(picked["command"])}
    out = handler.handle_user_attempt(
        request_id="r-tbu-time-2",
        user_id="u-tbu-time",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="16",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-tbu-time",
        source_message_id="msg-tbu-time-2",
    )
    assert out["rec"]["missing_field"] == "timeblock_update_final_confirm"
    assert "Время: 16:00" in str(out.get("clarifying_question") or "")


def test_timeblock_update_choose_duration_and_reply_renders_final_confirmation() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    payload = {
        "intent": "timeblock.update",
        "time_block_id": 42,
        "start_at_date": "2026-05-10",
        "start_at_time": "11:00",
        "duration_minutes": 30,
        "timeblock_update_source_block_id": 42,
        "timeblock_update_source_date": "2026-05-10",
        "timeblock_update_source_time": "11:00",
        "timeblock_update_source_duration": 30,
        "timeblock_update_source_title": "Фокус по отчёту",
        "__timeblock_update_target_confirmed": True,
        "entities": {"start_at_date": "2026-05-10", "start_at_time": "11:00", "duration_minutes": 30},
    }
    db = _FakeDb(session={"intent": "timeblock.update", "missing_field": "timeblock_update_edit_choice", "idempotency_key": "idem-tbu-dur", "payload": dict(payload)})
    picked = handler.handle_user_attempt(
        request_id="r-tbu-dur-1",
        user_id="u-tbu-dur",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="длительность",
        metadata={"callback_query": {"id": "cb-tbu-dur", "data": "clarify:v1:timeblock_update_edit:duration"}},
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-tbu-dur",
        source_message_id="msg-tbu-dur-1",
    )
    db.session = {"intent": "timeblock.update", "missing_field": "timeblock_update_edit_duration", "idempotency_key": "idem-tbu-dur", "payload": dict(picked["command"])}
    out = handler.handle_user_attempt(
        request_id="r-tbu-dur-2",
        user_id="u-tbu-dur",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="45 минут",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-tbu-dur",
        source_message_id="msg-tbu-dur-2",
    )
    assert out["rec"]["missing_field"] == "timeblock_update_final_confirm"
    assert "45 минут" in str(out.get("clarifying_question") or "")


def test_timeblock_update_choose_date_time_asks_date_then_time_then_confirmation() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    payload = {
        "intent": "timeblock.update",
        "time_block_id": 42,
        "start_at_date": "2026-05-10",
        "start_at_time": "11:00",
        "duration_minutes": 30,
        "timeblock_update_source_block_id": 42,
        "timeblock_update_source_date": "2026-05-10",
        "timeblock_update_source_time": "11:00",
        "timeblock_update_source_duration": 30,
        "timeblock_update_source_title": "Фокус по отчёту",
        "__timeblock_update_target_confirmed": True,
        "entities": {"start_at_date": "2026-05-10", "start_at_time": "11:00", "duration_minutes": 30},
    }
    db = _FakeDb(session={"intent": "timeblock.update", "missing_field": "timeblock_update_edit_choice", "idempotency_key": "idem-tbu-datetime", "payload": dict(payload)})
    picked = handler.handle_user_attempt(
        request_id="r-tbu-datetime-1",
        user_id="u-tbu-datetime",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="дата и время",
        metadata={"callback_query": {"id": "cb-tbu-datetime", "data": "clarify:v1:timeblock_update_edit:date_time"}},
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-tbu-datetime",
        source_message_id="msg-tbu-datetime-1",
    )
    assert picked["rec"]["missing_field"] == "timeblock_update_edit_date_time_date"
    db.session = {"intent": "timeblock.update", "missing_field": "timeblock_update_edit_date_time_date", "idempotency_key": "idem-tbu-datetime", "payload": dict(picked["command"])}
    date_reply = handler.handle_user_attempt(
        request_id="r-tbu-datetime-2",
        user_id="u-tbu-datetime",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="11.05.2026",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-tbu-datetime",
        source_message_id="msg-tbu-datetime-2",
    )
    assert date_reply["rec"]["missing_field"] == "timeblock_update_edit_date_time_time"
    db.session = {"intent": "timeblock.update", "missing_field": "timeblock_update_edit_date_time_time", "idempotency_key": "idem-tbu-datetime", "payload": dict(date_reply["command"])}
    time_reply = handler.handle_user_attempt(
        request_id="r-tbu-datetime-3",
        user_id="u-tbu-datetime",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="16",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-tbu-datetime",
        source_message_id="msg-tbu-datetime-3",
    )
    assert time_reply["rec"]["missing_field"] == "timeblock_update_final_confirm"
    assert "Время: 16:00" in str(time_reply.get("clarifying_question") or "")


def test_timeblock_update_choose_comment_and_reply_updates_confirmation_summary() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    payload = {
        "intent": "timeblock.update",
        "time_block_id": 42,
        "start_at_date": "2026-05-10",
        "start_at_time": "11:00",
        "duration_minutes": 30,
        "timeblock_update_source_block_id": 42,
        "timeblock_update_source_date": "2026-05-10",
        "timeblock_update_source_time": "11:00",
        "timeblock_update_source_duration": 30,
        "timeblock_update_source_title": "Фокус по отчёту",
        "__timeblock_update_target_confirmed": True,
        "entities": {
            "start_at_date": "2026-05-10",
            "start_at_time": "11:00",
            "duration_minutes": 30,
        },
    }
    db = _FakeDb(session={"intent": "timeblock.update", "missing_field": "timeblock_update_edit_choice", "idempotency_key": "idem-tbu-comment", "payload": dict(payload)})
    picked = handler.handle_user_attempt(
        request_id="r-tbu-comment-1",
        user_id="u-tbu-comment",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="Комментарий",
        metadata={"callback_query": {"id": "cb-tbu-comment", "data": "clarify:v1:timeblock_update_edit:comment"}},
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-tbu-comment",
        source_message_id="msg-tbu-comment-1",
    )
    assert picked["rec"]["missing_field"] == "timeblock_update_edit_comment"
    db.session = {"intent": "timeblock.update", "missing_field": "timeblock_update_edit_comment", "idempotency_key": "idem-tbu-comment", "payload": dict(picked["command"])}
    out = handler.handle_user_attempt(
        request_id="r-tbu-comment-2",
        user_id="u-tbu-comment",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="нужен новый комментарий",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-tbu-comment",
        source_message_id="msg-tbu-comment-2",
    )
    assert out["rec"]["missing_field"] == "timeblock_update_final_confirm"
    assert "Комментарий: нужен новый комментарий" in str(out.get("clarifying_question") or "")


def test_temporal_edit_observability_logs_for_meeting_and_timeblock(caplog) -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)

    meeting_payload = {
        "intent": "meeting.update",
        "calendar_event_id": "evt-log-1",
        "start_at_date": "2026-05-10",
        "start_at_time": "11:00",
        "duration_minutes": 30,
        "meeting_update_source_event_id": "evt-log-1",
        "meeting_update_source_date": "2026-05-10",
        "meeting_update_source_time": "11:00",
        "meeting_update_source_duration": 30,
        "meeting_update_source_title": "Встреча по проекту",
        "__meeting_update_target_confirmed": True,
        "entities": {"start_at_date": "2026-05-10", "start_at_time": "11:00", "duration_minutes": 30},
    }
    meeting_db = _FakeDb(session={"intent": "meeting.update", "missing_field": "meeting_update_edit_choice", "idempotency_key": "idem-log-meeting", "payload": dict(meeting_payload)})

    caplog.set_level("INFO")
    picked = handler.handle_user_attempt(
        request_id="r-log-meeting-1",
        user_id="u-log-meeting",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="Комментарий",
        metadata={"callback_query": {"id": "cb-log-meeting-comment", "data": "clarify:v1:meeting_update_edit:comment"}},
        local_db=meeting_db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-log-meeting",
        source_message_id="msg-log-meeting-1",
    )
    meeting_db.session = {"intent": "meeting.update", "missing_field": "meeting_update_edit_comment", "idempotency_key": "idem-log-meeting", "payload": dict(picked["command"])}
    handler.handle_user_attempt(
        request_id="r-log-meeting-2",
        user_id="u-log-meeting",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="это очень длинный комментарий для проверки безопасного логирования без полного текста",
        local_db=meeting_db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-log-meeting",
        source_message_id="msg-log-meeting-2",
    )

    field_record = next((r for r in caplog.records if r.message == "temporal_edit_field_selected" and getattr(r, "intent", "") == "meeting.update"), None)
    assert field_record is not None
    assert getattr(field_record, "field") == "comment"

    value_record = next((r for r in caplog.records if r.message == "temporal_edit_value_received" and getattr(r, "intent", "") == "meeting.update"), None)
    assert value_record is not None
    assert getattr(value_record, "field") == "comment"
    assert str(getattr(value_record, "normalized_value", "")) != "это очень длинный комментарий для проверки безопасного логирования без полного текста"
    assert str(getattr(value_record, "normalized_value", "")).endswith("...")

    summary_record = next((r for r in caplog.records if r.message == "temporal_edit_summary_rendered" and getattr(r, "intent", "") == "meeting.update"), None)
    assert summary_record is not None
    assert "comment" in list(getattr(summary_record, "changed_fields", []))

    timeblock_payload = {
        "intent": "timeblock.update",
        "time_block_id": 42,
        "start_at_date": "2026-05-10",
        "start_at_time": "11:00",
        "duration_minutes": 30,
        "timeblock_update_source_block_id": 42,
        "timeblock_update_source_date": "2026-05-10",
        "timeblock_update_source_time": "11:00",
        "timeblock_update_source_duration": 30,
        "timeblock_update_source_title": "Фокус",
        "__timeblock_update_target_confirmed": True,
        "__timeblock_update_active_edit_field": "date",
        "entities": {"start_at_date": "2026-05-10", "start_at_time": "11:00", "duration_minutes": 30},
    }
    timeblock_db = _FakeDb(session={"intent": "timeblock.update", "missing_field": "timeblock_update_edit_date", "idempotency_key": "idem-log-timeblock", "payload": dict(timeblock_payload)})
    handler.handle_user_attempt(
        request_id="r-log-timeblock-1",
        user_id="u-log-timeblock",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="дата",
        metadata={"callback_query": {"id": "cb-log-timeblock-date", "data": "clarify:v1:timeblock_update_edit:date"}},
        local_db=timeblock_db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-log-timeblock",
        source_message_id="msg-log-timeblock-1",
    )

    duplicate_record = next((r for r in caplog.records if r.message == "temporal_edit_duplicate_callback_ignored" and getattr(r, "intent", "") == "timeblock.update"), None)
    assert duplicate_record is not None
    assert getattr(duplicate_record, "field") == "date"


def test_timeblock_update_comment_keep_clear_cancel_and_date_callback_guards() -> None:
    handler = _load_handler_module()
    rec = _RecProbe(should_fail_on_call=True)
    payload = {
        "intent": "timeblock.update",
        "time_block_id": 42,
        "start_at_date": "2026-05-10",
        "start_at_time": "11:00",
        "duration_minutes": 30,
        "comment_text": "старый комментарий",
        "comment": "старый комментарий",
        "timeblock_update_source_block_id": 42,
        "timeblock_update_source_date": "2026-05-10",
        "timeblock_update_source_time": "11:00",
        "timeblock_update_source_duration": 30,
        "timeblock_update_source_title": "Фокус по отчёту",
        "timeblock_update_source_comment": "старый комментарий",
        "__timeblock_update_target_confirmed": True,
        "__timeblock_update_active_edit_field": "date",
        "entities": {
            "start_at_date": "2026-05-10",
            "start_at_time": "11:00",
            "duration_minutes": 30,
            "comment_text": "старый комментарий",
            "comment": "старый комментарий",
        },
    }
    keep_db = _FakeDb(session={"intent": "timeblock.update", "missing_field": "timeblock_update_edit_comment", "idempotency_key": "idem-tbu-comment-keep", "payload": dict(payload)})
    keep = handler.handle_user_attempt(
        request_id="r-tbu-comment-keep",
        user_id="u-tbu-comment",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="оставить без изменений",
        metadata={"callback_query": {"id": "cb-tbu-comment-keep", "data": "clarify:v1:temporal_comment:keep"}},
        local_db=keep_db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-tbu-comment",
        source_message_id="msg-tbu-comment-keep",
    )
    assert keep["rec"]["missing_field"] == "timeblock_update_final_confirm"
    assert "Комментарий: старый комментарий" in str(keep.get("clarifying_question") or "")

    clear_db = _FakeDb(session={"intent": "timeblock.update", "missing_field": "timeblock_update_edit_comment", "idempotency_key": "idem-tbu-comment-clear", "payload": dict(payload)})
    clear = handler.handle_user_attempt(
        request_id="r-tbu-comment-clear",
        user_id="u-tbu-comment",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="очистить комментарий",
        metadata={"callback_query": {"id": "cb-tbu-comment-clear", "data": "clarify:v1:temporal_comment:clear"}},
        local_db=clear_db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-tbu-comment",
        source_message_id="msg-tbu-comment-clear",
    )
    assert clear["rec"]["missing_field"] == "timeblock_update_final_confirm"
    assert "Комментарий: -" in str(clear.get("clarifying_question") or "")

    cancel_db = _FakeDb(session={"intent": "timeblock.update", "missing_field": "timeblock_update_edit_choice", "idempotency_key": "idem-tbu-cancel", "payload": dict(payload)})
    cancelled = handler.handle_user_attempt(
        request_id="r-tbu-cancel",
        user_id="u-tbu-comment",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="отмена",
        metadata={"callback_query": {"id": "cb-tbu-cancel", "data": "clarify:v1:timeblock_update_edit:cancel"}},
        local_db=cancel_db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-tbu-comment",
        source_message_id="msg-tbu-cancel",
    )
    assert cancelled["outcome"] == "cancelled"

    date_db = _FakeDb(session={"intent": "timeblock.update", "missing_field": "timeblock_update_edit_date", "idempotency_key": "idem-tbu-date-callback", "payload": dict(payload)})
    date_repeat = handler.handle_user_attempt(
        request_id="r-tbu-date-repeat",
        user_id="u-tbu-comment",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=rec,
        text="дата",
        metadata={"callback_query": {"id": "cb-tbu-date-repeat", "data": "clarify:v1:timeblock_update_edit:date"}},
        local_db=date_db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-tbu-comment",
        source_message_id="msg-tbu-date-repeat",
    )
    assert date_repeat["rec"]["missing_field"] == "timeblock_update_edit_date"
    assert str(date_repeat["command"].get("timeblock_update_new_date") or "") == ""

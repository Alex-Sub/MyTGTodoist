from __future__ import annotations

import importlib.util
import sys
import types
from datetime import date, timedelta
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
APP_SRC = ROOT / "telegram-bot" / "app-integration" / "src"
DECISION_PATH = APP_SRC / "decision" / "decision_engine.py"
HANDLER_PATH = APP_SRC / "app" / "handler.py"


def _date_iso(days_from_today: int) -> str:
    return (date.today() + timedelta(days=days_from_today)).isoformat()


def _date_time_value(days_from_today: int, hour: int = 12) -> str:
    return f"{_date_iso(days_from_today)} {hour:02d}:00"


def _load_module(path: Path, module_name: str):
    preserved: dict[str, Any] = {}
    for key in list(sys.modules.keys()):
        if key == "src" or key.startswith("src."):
            preserved[key] = sys.modules.pop(key)

    src_pkg = types.ModuleType("src")
    src_pkg.__path__ = [str(APP_SRC)]  # type: ignore[attr-defined]
    sys.modules["src"] = src_pkg

    spec = importlib.util.spec_from_file_location(module_name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)

    for key in list(sys.modules.keys()):
        if key == "src" or key.startswith("src."):
            sys.modules.pop(key)
    sys.modules.update(preserved)
    return module


class _NoopAsr:
    def transcribe(self, **kwargs) -> str:  # noqa: ARG002
        raise AssertionError("ASR should not be called in these tests")


class _NeverCalledLLM:
    def parse(self, **kwargs) -> dict:  # noqa: ARG002
        raise AssertionError("LLM should not be called in clarification continuation")


class _NeverCalledREC:
    def query(self, payload: dict) -> dict:  # noqa: ARG002
        raise AssertionError("REC should not be called in clarification continuation")


class _FakeDb:
    def __init__(self, session: dict | None) -> None:
        self.session = session
        self.upserts: list[dict] = []

    def get_active_clarification_session(self, context_key: str, ttl_seconds: int) -> dict | None:  # noqa: ARG002
        return self.session

    def upsert_clarification_session(self, **kwargs) -> None:
        self.upserts.append(kwargs)

    def delete_clarification_session(self, context_key: str) -> None:  # noqa: ARG002
        return None

    def get_command_dedup(self, idempotency_key: str) -> dict | None:  # noqa: ARG002
        return None

    def reserve_command_dedup(self, idempotency_key: str, intent: str) -> bool:  # noqa: ARG002
        return True

    def delete_command_dedup(self, idempotency_key: str) -> None:  # noqa: ARG002
        return None

    def finalize_command_dedup(self, **kwargs) -> None:  # noqa: ARG002
        return None

    def is_command_dedup_stale(self, existing: dict, ttl_seconds: int) -> bool:  # noqa: ARG002
        return False

    def reclaim_command_dedup(self, idempotency_key: str, intent: str) -> bool:  # noqa: ARG002
        return False


def test_temporal_missing_order_is_date_then_time_then_duration() -> None:
    decision = _load_module(DECISION_PATH, "decision_engine_ordering")

    none_filled = decision.has_required_temporal_fields({"intent": "schedule_meeting"}, "")
    assert none_filled["missing_field"] == "start_at_date"

    date_only = decision.has_required_temporal_fields({"intent": "schedule_meeting"}, "завтра")
    assert date_only["missing_field"] == "start_at_time"

    date_and_time = decision.has_required_temporal_fields({"intent": "schedule_meeting"}, "завтра в 12")
    assert date_and_time["missing_field"] == "duration_minutes"


def test_temporal_implicit_and_entities_are_respected() -> None:
    decision = _load_module(DECISION_PATH, "decision_engine_implicit_entities")

    tomorrow_only = decision.has_required_temporal_fields({"intent": "schedule_meeting"}, "завтра")
    assert tomorrow_only["missing_field"] == "start_at_time"

    time_only = decision.has_required_temporal_fields({"intent": "schedule_meeting"}, "в 12")
    assert time_only["missing_field"] == "start_at_date"

    from_entities = decision.has_required_temporal_fields(
        {"intent": "schedule_meeting", "entities": {"start_at_date": "завтра", "start_at_time": "12:00"}},
        "",
    )
    assert from_entities["missing_field"] == "duration_minutes"


def test_strict_field_validation_in_continuation() -> None:
    decision = _load_module(DECISION_PATH, "decision_engine_strict_validation")

    duration_bad = decision.resolve_clarification_continuation(
        "duration_minutes",
        "завтра",
        {"intent": "schedule_meeting", "start_at": _date_time_value(0, 12)},
    )
    assert duration_bad.accepted is False
    assert duration_bad.field_name == "duration_minutes"
    assert duration_bad.reason == "invalid_duration_minutes"

    date_bad = decision.resolve_clarification_continuation(
        "start_at_date",
        "30",
        {"intent": "schedule_meeting"},
    )
    assert date_bad.accepted is False
    assert date_bad.field_name == "start_at_date"
    assert date_bad.reason == "invalid_start_at_date"

    time_bad = decision.resolve_clarification_continuation(
        "start_at_time",
        "25:99",
        {"intent": "schedule_meeting"},
    )
    assert time_bad.accepted is False
    assert time_bad.field_name == "start_at_time"
    assert time_bad.reason == "invalid_start_at_time"

    duration_ok = decision.resolve_clarification_continuation(
        "duration_minutes",
        "30",
        {"intent": "schedule_meeting"},
    )
    assert duration_ok.accepted is True
    assert duration_ok.normalized_value == 30


def test_handler_reasks_same_expected_field_on_invalid_continuation() -> None:
    handler = _load_module(HANDLER_PATH, "handler_strict_reask")
    db = _FakeDb(
        session={
            "intent": "schedule_meeting",
            "missing_field": "duration_minutes",
            "idempotency_key": "idem-strict-1",
            "payload": {"intent": "schedule_meeting", "start_at": _date_time_value(0, 12)},
        }
    )

    result = handler.handle_user_attempt(
        request_id="strict-reask-1",
        user_id="u-strict-1",
        channel="telegram",
        asr_client=_NoopAsr(),
        llm_client=_NeverCalledLLM(),
        rec_client=_NeverCalledREC(),
        text="завтра",
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-strict-1",
        source_message_id="msg-strict-1",
    )

    assert result["outcome"] == "rec_needs_clarification"
    assert result["rec"]["missing_field"] == "duration_minutes"
    assert db.upserts[-1]["missing_field"] == "duration_minutes"

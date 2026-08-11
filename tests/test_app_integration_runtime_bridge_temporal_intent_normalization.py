from __future__ import annotations

import importlib.util
import logging
import sys
import types
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, List


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


def _execution_payload(intent: str, *, date_value: str = "2026-04-12", time_value: str = "14", duration: int = 60) -> Dict[str, Any]:
    return {
        "outcome": "success",
        "needs_clarification": False,
        "rec": {"outcome": "skipped", "reason": "clarification_executable_guard"},
        "command": {
            "intent": intent,
            "start_at_date": date_value,
            "start_at_time": time_value,
            "duration_minutes": duration,
            "task_id": 42,
            "entities": {
                "text": "назначь встречу завтра в 14 на 60 минут",
            },
        },
    }


def _execution_payload_draft_only(intent: str, *, date_value: str = "2026-04-12", time_value: str = "14", duration: int = 60) -> Dict[str, Any]:
    return {
        "outcome": "success",
        "needs_clarification": False,
        "rec": {"outcome": "skipped", "reason": "clarification_executable_guard"},
        "command": {
            "intent": intent,
            "entities": {
                "text": "назначь встречу завтра в 14 на 60 минут",
            },
            "__temporal_draft": {
                "date": date_value,
                "time": time_value,
                "duration_minutes": duration,
            },
        },
    }


def _execution_payload_edited_draft(
    intent: str,
    *,
    old_date: str = "2026-04-12",
    old_time: str = "14:00",
    old_duration: int = 60,
    new_date: str = "2026-04-13",
    new_time: str = "16",
    new_duration: int = 30,
) -> Dict[str, Any]:
    return {
        "outcome": "success",
        "needs_clarification": False,
        "rec": {"outcome": "skipped", "reason": "clarification_executable_guard"},
        "command": {
            "intent": intent,
            "start_at_date": old_date,
            "start_at_time": old_time,
            "duration_minutes": old_duration,
            "__edited_temporal_draft": True,
            "entities": {
                "text": "назначь встречу завтра в 14 на 60 минут",
            },
            "__temporal_draft": {
                "date": new_date,
                "time": new_time,
                "duration_minutes": new_duration,
            },
        },
    }


def _request(runtime_bridge_module):
    return runtime_bridge_module.TelegramRuntimeRequest(
        request_id="req-1",
        app_id="app",
        tenant_id="tenant",
        channel="telegram",
        user_id="u1",
        chat_id="c1",
        source_message_id="m1",
        text="назначь встречу завтра в 14 на 60 минут",
        timezone="Europe/Moscow",
        metadata={},
    )


def _task_request(runtime_bridge_module):
    return runtime_bridge_module.TelegramRuntimeRequest(
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


def _voice_request(runtime_bridge_module):
    return runtime_bridge_module.TelegramRuntimeRequest(
        request_id="tg:voice:1",
        app_id="app",
        tenant_id="tenant",
        channel="telegram",
        user_id="u-voice",
        chat_id="c-voice",
        source_message_id="m-voice",
        text=None,
        audio_bytes=b"voice-bytes",
        audio_mime="audio/ogg",
        audio_filename="voice.ogg",
        timezone="Europe/Moscow",
        metadata={"voice": {"file_id": "file-1", "mime_type": "audio/ogg"}},
    )


def test_temporal_confirm_schedule_meeting_is_normalized_for_worker(caplog) -> None:
    runtime_bridge = _load_module(RUNTIME_BRIDGE_PATH, "app_integration_runtime_bridge_module_a")
    posted: List[Dict[str, Any]] = []

    def _fake_post(url, json, timeout):  # noqa: ANN001
        posted.append({"url": url, "json": dict(json), "timeout": timeout})
        entities = json.get("command", {}).get("entities", {})
        if not str(entities.get("start_at") or "").strip():
            return _FakeResponse({"ok": False, "clarifying_question": "На какое время поставить блок?"})
        return _FakeResponse({"ok": True, "user_message": "Готово.", "calendar_event_id": "evt-1", "debug": {"calendar_commit": "ok"}})

    bridge = runtime_bridge.RuntimeBridge(
        worker_command_url="http://worker/runtime/command",
        direct_handler=lambda _req: _execution_payload("schedule_meeting", time_value="14"),
        http_post=_fake_post,
    )

    with caplog.at_level(logging.INFO):
        result = bridge.process_request(_request(runtime_bridge))

    assert result.outcome == "direct_processed"
    assert posted
    assert posted[0]["json"]["command"]["intent"] == "meeting.create"
    entities = posted[0]["json"]["command"]["entities"]
    assert entities["user_id"] == "u1"
    assert entities["start_at_time"] == "14:00"
    assert entities["start_time"] == "14:00"
    assert "T14:00" in str(entities["start_at"])

    dispatch_records = [r for r in caplog.records if r.getMessage() == "temporal_commit_dispatch"]
    assert dispatch_records
    rec = dispatch_records[-1]
    assert getattr(rec, "raw_intent") == "schedule_meeting"
    assert getattr(rec, "normalized_intent") == "meeting.create"
    assert "start_at" in str(getattr(rec, "payload_keys", []))
    assert result.needs_clarification is False




def test_voice_request_in_direct_mode_is_transcribed_and_routed_without_block_message() -> None:
    runtime_bridge = _load_module(RUNTIME_BRIDGE_PATH, "app_integration_runtime_bridge_module_voice_direct")

    class _AsrProbe:
        def __init__(self) -> None:
            self.calls = 0
            self.request_ids: List[str] = []

        def transcribe(self, audio_bytes: bytes, mime_type: str | None = None, filename: str | None = None, request_id: str | None = None) -> str:  # noqa: ARG002
            assert audio_bytes == b"voice-bytes"
            self.calls += 1
            self.request_ids.append(str(request_id or ""))
            return "запланируй встречу завтра в 12"

    class _LlmProbe:
        def __init__(self) -> None:
            self.calls = 0
            self.last_text = ""

        def parse(self, *, text: str) -> Dict[str, Any]:
            self.calls += 1
            self.last_text = text
            return {
                "intent": "schedule_meeting",
                "entities": {
                    "text": text,
                    "date": "2026-04-29",
                    "time": "12",
                },
            }

    class _RecProbe:
        def __init__(self) -> None:
            self.calls = 0

        def query(self, payload: Dict[str, Any]) -> Dict[str, Any]:  # noqa: ARG002
            self.calls += 1
            return {"outcome": "ok", "hits": []}

    cfg = types.SimpleNamespace(
        app_id="app",
        clarification_ttl_sec=300,
        command_dedup_reservation_ttl_sec=120,
    )
    asr = _AsrProbe()
    llm = _LlmProbe()
    rec = _RecProbe()
    direct_handler = runtime_bridge.build_runtime_core_direct_handler(
        cfg=cfg,
        asr_client=asr,
        llm_client=llm,
        rec_client=rec,
        local_db=None,
    )
    bridge = runtime_bridge.RuntimeBridge(
        worker_command_url="http://worker/runtime/command",
        direct_handler=direct_handler,
        direct_voice_enabled=False,
    )

    result = bridge.process_request(_voice_request(runtime_bridge))

    assert asr.calls == 1
    assert asr.request_ids == ["tg:voice:1"]
    assert llm.calls == 1
    assert llm.last_text == "запланируй встречу завтра в 12"
    assert result.outcome == "direct_processed"
    assert result.needs_clarification is True
    assert "Время: 12:00" in str(result.clarifying_question or "")
    assert "Голосовые сообщения в режиме runtime_core_direct пока не поддерживаются" not in str(result.reply_text or "")
    assert bool(result.telemetry.get("voice_request")) is True


def test_runtime_core_direct_generic_task_phrase_routes_before_memory_and_logs(caplog) -> None:
    runtime_bridge = _load_module(RUNTIME_BRIDGE_PATH, "app_integration_runtime_bridge_module_generic_task_route")

    class _LlmUnknown:
        def __init__(self) -> None:
            self.calls = 0

        def parse(self, *, text: str) -> Dict[str, Any]:
            self.calls += 1
            return {
                "intent": "unknown",
                "entities": {
                    "text": text,
                },
            }

    class _RecProbe:
        def __init__(self) -> None:
            self.calls = 0

        def query(self, payload: Dict[str, Any]) -> Dict[str, Any]:  # noqa: ARG002
            self.calls += 1
            return {"outcome": "empty", "hits": []}

    cfg = types.SimpleNamespace(
        app_id="app",
        clarification_ttl_sec=300,
        command_dedup_reservation_ttl_sec=120,
    )
    llm = _LlmUnknown()
    rec = _RecProbe()
    direct_handler = runtime_bridge.build_runtime_core_direct_handler(
        cfg=cfg,
        asr_client=None,
        llm_client=llm,
        rec_client=rec,
        local_db=None,
    )
    req = runtime_bridge.TelegramRuntimeRequest(
        request_id="tg:generic-task:1",
        app_id="app",
        tenant_id="tenant",
        channel="telegram",
        user_id="u1",
        chat_id="c1",
        source_message_id="m1",
        text="купить хлеб завтра",
        timezone="Europe/Moscow",
        metadata={},
    )

    with caplog.at_level(logging.INFO):
        result = direct_handler(req)

    assert result["outcome"] == "rec_needs_clarification"
    assert result["rec"]["missing_field"] == "task_create_confirm"
    assert str(result["command"].get("intent") or "") == "task.create"
    assert str(result["command"].get("title") or "") == "Купить хлеб"
    expected_planned_at = (date.today() + timedelta(days=1)).isoformat()
    assert str(result["command"].get("planned_at") or "") == expected_planned_at
    assert "В памяти нет данных по этому запросу." not in str(result.get("user_message") or "")
    assert rec.calls == 0

    record = next((r for r in caplog.records if r.getMessage() == "generic_task_fallback_selected"), None)
    assert record is not None
    assert getattr(record, "raw_text") == "купить хлеб завтра"
    assert getattr(record, "title") == "Купить хлеб"
    assert getattr(record, "planned_at") == expected_planned_at
    assert getattr(record, "route") == "task.create"
    route_record = next((r for r in caplog.records if r.getMessage() == "runtime_route_selected"), None)
    assert route_record is not None
    assert getattr(route_record, "raw_text") == "купить хлеб завтра"
    assert getattr(route_record, "route") == "task.create"
    assert getattr(route_record, "reason") in {"explicit_task_marker", "generic_task_with_date", "generic_task_safety_guard_before_memory"}
    entry_record = next((r for r in caplog.records if r.getMessage() == "telegram_text_route_entry"), None)
    assert entry_record is not None
    assert getattr(entry_record, "raw_text") == "купить хлеб завтра"
    assert getattr(entry_record, "route_rules_version") == "stage67-hardguard-v2"


def test_runtime_bridge_normalize_date_ymd_without_year_uses_current_year_for_ddmm() -> None:
    runtime_bridge = _load_module(RUNTIME_BRIDGE_PATH, "app_integration_runtime_bridge_module_date_ddmm")

    class _FakeDate:
        @staticmethod
        def today():
            from datetime import date as _date

            return _date(2026, 4, 20)

    runtime_bridge.date = _FakeDate  # type: ignore[attr-defined]
    parsed = runtime_bridge.RuntimeBridge._normalize_date_ymd("13.04")
    assert parsed == "2026-04-13"


def test_runtime_bridge_normalize_date_ymd_without_year_uses_current_year_for_month_name() -> None:
    runtime_bridge = _load_module(RUNTIME_BRIDGE_PATH, "app_integration_runtime_bridge_module_date_month")

    class _FakeDate:
        @staticmethod
        def today():
            from datetime import date as _date

            return _date(2026, 4, 20)

    runtime_bridge.date = _FakeDate  # type: ignore[attr-defined]
    parsed = runtime_bridge.RuntimeBridge._normalize_date_ymd("21 апреля")
    assert parsed == "2026-04-21"


def test_runtime_bridge_normalize_date_ymd_keeps_explicit_year() -> None:
    runtime_bridge = _load_module(RUNTIME_BRIDGE_PATH, "app_integration_runtime_bridge_module_date_explicit_year")
    parsed = runtime_bridge.RuntimeBridge._normalize_date_ymd("21 апреля 2021")
    assert parsed == "2021-04-21"


def test_runtime_bridge_extract_update_overrides_parses_month_name_without_year() -> None:
    runtime_bridge = _load_module(RUNTIME_BRIDGE_PATH, "app_integration_runtime_bridge_module_update_overrides_month")

    class _FakeDateTime(runtime_bridge.datetime):  # type: ignore[misc]
        @classmethod
        def now(cls, tz=None):  # noqa: ANN001
            base = cls(2026, 4, 20, 10, 0, 0)
            return base.replace(tzinfo=tz) if tz is not None else base

    runtime_bridge.datetime = _FakeDateTime  # type: ignore[attr-defined]
    out = runtime_bridge.RuntimeBridge._extract_update_overrides(
        "перенеси встречу на 21 апреля в 14",
        timezone_name="Europe/Moscow",
    )
    assert out.get("start_at_date") == "2026-04-21"
    assert out.get("start_at_time") == "14:00"


def test_runtime_bridge_temporal_entities_prefer_explicit_text_date_over_stale_year() -> None:
    runtime_bridge = _load_module(RUNTIME_BRIDGE_PATH, "app_integration_runtime_bridge_module_explicit_text_date_priority")
    posted: List[Dict[str, Any]] = []

    class _FakeDate:
        @staticmethod
        def today():
            from datetime import date as _date

            return _date(2026, 4, 20)

    runtime_bridge.date = _FakeDate  # type: ignore[attr-defined]

    def _fake_post(url, json, timeout):  # noqa: ANN001
        posted.append({"url": url, "json": dict(json), "timeout": timeout})
        return _FakeResponse({"ok": True, "user_message": "Готово.", "calendar_event_id": "evt-4", "debug": {"calendar_commit": "ok"}})

    def _direct(_req):  # noqa: ANN001
        return {
            "outcome": "success",
            "needs_clarification": False,
            "rec": {"outcome": "skipped", "reason": "clarification_executable_guard"},
            "command": {
                "intent": "schedule_meeting",
                "entities": {
                    "text": "запланируй встречу 21 апреля в 14",
                    "start_at_date": "2023-04-21",
                    "start_at_time": "14",
                    "duration_minutes": 30,
                },
            },
        }

    bridge = runtime_bridge.RuntimeBridge(
        worker_command_url="http://worker/runtime/command",
        direct_handler=_direct,
        http_post=_fake_post,
    )
    req = runtime_bridge.TelegramRuntimeRequest(
        request_id="req-explicit-date-priority",
        app_id="app",
        tenant_id="tenant",
        channel="telegram",
        user_id="u-date-priority",
        chat_id="c-date-priority",
        source_message_id="m-date-priority",
        text="запланируй встречу 21 апреля в 14",
        timezone="Europe/Moscow",
        metadata={},
    )
    result = bridge.process_request(req)
    assert result.outcome == "direct_processed"
    assert posted
    entities = posted[0]["json"]["command"]["entities"]
    assert str(entities.get("start_at_date") or "") == "2026-04-21"


def test_runtime_bridge_normalize_meeting_title_removes_duplicated_event_words() -> None:
    runtime_bridge = _load_module(RUNTIME_BRIDGE_PATH, "app_integration_runtime_bridge_module_title_clean")
    cleaned = runtime_bridge.RuntimeBridge._normalize_meeting_title("запланируй встречу с Иваном", "встреча")
    assert cleaned == "Встреча с Иваном"
    cleaned_call = runtime_bridge.RuntimeBridge._normalize_meeting_title("созвон с подрядчиком", "созвон")
    assert cleaned_call == "Созвон с подрядчиком"


def test_runtime_bridge_normalize_meeting_title_cleans_arrow_and_command_tail() -> None:
    runtime_bridge = _load_module(RUNTIME_BRIDGE_PATH, "app_integration_runtime_bridge_module_title_clean_arrow")
    cleaned = runtime_bridge.RuntimeBridge._normalize_meeting_title("встреча в 11 → 29.04", "встреча")
    assert cleaned == "Встреча"


def test_runtime_bridge_normalize_meeting_title_collapses_exact_duplicate_kind() -> None:
    runtime_bridge = _load_module(RUNTIME_BRIDGE_PATH, "app_integration_runtime_bridge_module_title_clean_dup_kind")
    cleaned = runtime_bridge.RuntimeBridge._normalize_meeting_title("созвон созвон", "созвон")
    assert cleaned == "Созвон"


def test_runtime_bridge_normalize_meeting_title_keeps_meaningful_subject_after_case_artifact() -> None:
    runtime_bridge = _load_module(RUNTIME_BRIDGE_PATH, "app_integration_runtime_bridge_module_title_case_artifact")
    cleaned = runtime_bridge.RuntimeBridge._normalize_meeting_title("встреча встречу с Иваном", "встреча")
    assert cleaned == "Встреча с Иваном"


def test_temporal_confirm_schedule_call_is_normalized_for_worker() -> None:
    runtime_bridge = _load_module(RUNTIME_BRIDGE_PATH, "app_integration_runtime_bridge_module_schedule_call")
    posted: List[Dict[str, Any]] = []

    def _fake_post(url, json, timeout):  # noqa: ANN001
        posted.append({"url": url, "json": dict(json), "timeout": timeout})
        entities = json.get("command", {}).get("entities", {})
        if not str(entities.get("start_at") or "").strip():
            return _FakeResponse({"ok": False, "clarifying_question": "Во сколько запланировать созвон?"})
        return _FakeResponse({"ok": True, "user_message": "Готово.", "calendar_event_id": "evt-call-1", "debug": {"calendar_commit": "ok"}})

    def _direct(_req):  # noqa: ANN001
        payload = _execution_payload("schedule_call", time_value="15")
        cmd = payload.get("command", {})
        entities = cmd.get("entities", {}) if isinstance(cmd.get("entities"), dict) else {}
        entities["text"] = "запланируй созвон с Иваном завтра в 15 на 60 минут"
        cmd["entities"] = entities
        payload["command"] = cmd
        return payload

    bridge = runtime_bridge.RuntimeBridge(
        worker_command_url="http://worker/runtime/command",
        direct_handler=_direct,
        http_post=_fake_post,
    )

    req = runtime_bridge.TelegramRuntimeRequest(
        request_id="req-schedule-call-1",
        app_id="app",
        tenant_id="tenant",
        channel="telegram",
        user_id="u-call-1",
        chat_id="c-call-1",
        source_message_id="m-call-1",
        text="запланируй созвон с Иваном завтра в 15",
        timezone="Europe/Moscow",
        metadata={},
    )
    result = bridge.process_request(req)

    assert result.outcome == "direct_processed"
    assert posted
    assert posted[0]["json"]["command"]["intent"] == "meeting.create"
    entities = posted[0]["json"]["command"]["entities"]
    assert entities.get("meeting_kind") == "созвон"
    assert str(entities.get("title") or "").startswith("Созвон")


def test_meeting_create_payload_preserves_locked_meeting_kind_for_all_event_types() -> None:
    runtime_bridge = _load_module(RUNTIME_BRIDGE_PATH, "app_integration_runtime_bridge_module_locked_kind_consistency")
    cases = [
        ("встреча", "Встреча"),
        ("собрание", "Собрание"),
        ("созвон", "Созвон"),
        ("мероприятие", "Мероприятие"),
    ]

    for idx, (meeting_kind, title_prefix) in enumerate(cases, start=1):
        posted: List[Dict[str, Any]] = []

        def _fake_post(url, json, timeout):  # noqa: ANN001
            posted.append({"url": url, "json": dict(json), "timeout": timeout})
            return _FakeResponse({"ok": True, "user_message": "ok", "calendar_event_id": f"evt-locked-kind-{idx}", "debug": {"calendar_commit": "ok"}})

        def _direct(_req):  # noqa: ANN001
            payload = _execution_payload("meeting.create", time_value="15")
            cmd = payload.get("command", {})
            cmd["meeting_kind"] = meeting_kind
            entities = cmd.get("entities", {}) if isinstance(cmd.get("entities"), dict) else {}
            entities["meeting_kind"] = meeting_kind
            entities["text"] = "встреча с Иваном завтра в 15 на 60 минут"
            cmd["entities"] = entities
            payload["command"] = cmd
            return payload

        bridge = runtime_bridge.RuntimeBridge(
            worker_command_url="http://worker/runtime/command",
            direct_handler=_direct,
            http_post=_fake_post,
        )
        req = runtime_bridge.TelegramRuntimeRequest(
            request_id=f"req-locked-kind-{idx}",
            app_id="app",
            tenant_id="tenant",
            channel="telegram",
            user_id=f"u-locked-kind-{idx}",
            chat_id=f"c-locked-kind-{idx}",
            source_message_id=f"m-locked-kind-{idx}",
            text=f"запланируй {meeting_kind} завтра в 15",
            timezone="Europe/Moscow",
            metadata={},
        )
        result = bridge.process_request(req)

        assert result.outcome == "direct_processed"
        assert posted
        entities = posted[0]["json"]["command"]["entities"]
        assert entities.get("meeting_kind") == meeting_kind
        assert str(entities.get("title") or "").startswith(title_prefix)


def test_temporal_confirm_schedule_block_is_normalized_for_worker() -> None:
    runtime_bridge = _load_module(RUNTIME_BRIDGE_PATH, "app_integration_runtime_bridge_module_b")
    posted: List[Dict[str, Any]] = []

    def _fake_post(url, json, timeout):  # noqa: ANN001
        posted.append({"url": url, "json": dict(json), "timeout": timeout})
        entities = json.get("command", {}).get("entities", {})
        if not str(entities.get("start_at") or "").strip():
            return _FakeResponse({"ok": False, "clarifying_question": "На какое время поставить блок?"})
        return _FakeResponse({"ok": True, "user_message": "Готово.", "calendar_event_id": "evt-2", "debug": {"calendar_commit": "ok"}})

    bridge = runtime_bridge.RuntimeBridge(
        worker_command_url="http://worker/runtime/command",
        direct_handler=lambda _req: _execution_payload("schedule_block", time_value="16"),
        http_post=_fake_post,
    )

    result = bridge.process_request(_request(runtime_bridge))

    assert result.outcome == "direct_processed"
    assert posted
    assert posted[0]["json"]["command"]["intent"] == "timeblock.create"
    entities = posted[0]["json"]["command"]["entities"]
    assert entities["user_id"] == "u1"
    assert entities["start_at_time"] == "16:00"
    assert entities["start_time"] == "16:00"
    assert "T16:00" in str(entities["start_at"])
    assert result.needs_clarification is False


def test_temporal_confirm_uses_temporal_draft_when_entities_missing() -> None:
    runtime_bridge = _load_module(RUNTIME_BRIDGE_PATH, "app_integration_runtime_bridge_module_c")
    posted: List[Dict[str, Any]] = []

    def _fake_post(url, json, timeout):  # noqa: ANN001
        posted.append({"url": url, "json": dict(json), "timeout": timeout})
        entities = json.get("command", {}).get("entities", {})
        if not str(entities.get("start_at") or "").strip():
            return _FakeResponse({"ok": False, "clarifying_question": "На какое время поставить блок?"})
        return _FakeResponse({"ok": True, "user_message": "Готово.", "calendar_event_id": "evt-3", "debug": {"calendar_commit": "ok"}})

    bridge = runtime_bridge.RuntimeBridge(
        worker_command_url="http://worker/runtime/command",
        direct_handler=lambda _req: _execution_payload_draft_only("schedule_meeting", time_value="14"),
        http_post=_fake_post,
    )

    result = bridge.process_request(_request(runtime_bridge))

    assert result.outcome == "direct_processed"
    assert posted
    assert posted[0]["json"]["command"]["intent"] == "meeting.create"
    entities = posted[0]["json"]["command"]["entities"]
    assert entities["user_id"] == "u1"
    assert entities["start_at_time"] == "14:00"
    assert "T14:00" in str(entities["start_at"])
    assert entities["duration_minutes"] == 60
    assert result.needs_clarification is False


def test_temporal_commit_after_edit_uses_updated_draft_values() -> None:
    runtime_bridge = _load_module(RUNTIME_BRIDGE_PATH, "app_integration_runtime_bridge_module_d")
    posted: List[Dict[str, Any]] = []

    def _fake_post(url, json, timeout):  # noqa: ANN001
        posted.append({"url": url, "json": dict(json), "timeout": timeout})
        return _FakeResponse(
            {"ok": True, "user_message": "Встреча создана.", "calendar_event_id": "evt-1", "debug": {"calendar_commit": "ok"}}
        )

    bridge = runtime_bridge.RuntimeBridge(
        worker_command_url="http://worker/runtime/command",
        direct_handler=lambda _req: _execution_payload_edited_draft("schedule_meeting"),
        http_post=_fake_post,
    )

    result = bridge.process_request(_request(runtime_bridge))

    assert result.outcome == "direct_processed"
    assert posted
    entities = posted[0]["json"]["command"]["entities"]
    assert entities["start_at_date"] == "2026-04-13"
    assert entities["start_at_time"] == "16:00"
    assert entities["start_time"] == "16:00"
    assert entities["duration_minutes"] == 30
    assert "T16:00" in str(entities["start_at"])


def test_meeting_success_message_not_returned_when_calendar_commit_failed() -> None:
    runtime_bridge = _load_module(RUNTIME_BRIDGE_PATH, "app_integration_runtime_bridge_module_e")

    def _fake_post(url, json, timeout):  # noqa: ANN001
        return _FakeResponse({"ok": True, "user_message": "Встреча создана.", "debug": {"calendar_commit": "error"}})

    bridge = runtime_bridge.RuntimeBridge(
        worker_command_url="http://worker/runtime/command",
        direct_handler=lambda _req: _execution_payload("schedule_meeting", time_value="14"),
        http_post=_fake_post,
    )

    result = bridge.process_request(_request(runtime_bridge))

    assert result.outcome == "error"
    assert "не получилось создать встречу" in str(result.reply_text or "").lower()


def test_meeting_success_message_not_returned_when_worker_ok_false() -> None:
    runtime_bridge = _load_module(RUNTIME_BRIDGE_PATH, "app_integration_runtime_bridge_module_e2")

    def _fake_post(url, json, timeout):  # noqa: ANN001
        return _FakeResponse({"ok": False, "user_message": "Встреча создана.", "error": "calendar_event_id_missing"})

    bridge = runtime_bridge.RuntimeBridge(
        worker_command_url="http://worker/runtime/command",
        direct_handler=lambda _req: _execution_payload("schedule_meeting", time_value="14"),
        http_post=_fake_post,
    )

    result = bridge.process_request(_request(runtime_bridge))

    assert result.outcome == "error"
    assert "не получилось создать встречу" in str(result.reply_text or "").lower()


def test_temporal_confirm_reschedule_meeting_is_normalized_to_meeting_update() -> None:
    runtime_bridge = _load_module(RUNTIME_BRIDGE_PATH, "app_integration_runtime_bridge_module_f")
    posted: List[Dict[str, Any]] = []

    def _fake_post(url, json, timeout):  # noqa: ANN001
        posted.append({"url": url, "json": dict(json), "timeout": timeout})
        return _FakeResponse({"ok": True, "user_message": "Встреча перенесена.", "calendar_event_id": "evt-9", "debug": {"calendar_commit": "ok"}})

    bridge = runtime_bridge.RuntimeBridge(
        worker_command_url="http://worker/runtime/command",
        direct_handler=lambda _req: _execution_payload("reschedule_meeting", time_value="16"),
        http_post=_fake_post,
    )

    result = bridge.process_request(_request(runtime_bridge))

    assert result.outcome == "direct_processed"
    assert posted
    assert posted[0]["json"]["command"]["intent"] == "meeting.update"


def test_meeting_update_source_is_loaded_and_merged_before_direct_handler() -> None:
    runtime_bridge = _load_module(RUNTIME_BRIDGE_PATH, "app_integration_runtime_bridge_module_g")
    seen: Dict[str, Any] = {}
    posted: List[Dict[str, Any]] = []

    class _Resp:
        def __init__(self, body: Dict[str, Any], status_code: int = 200) -> None:
            self._body = body
            self.status_code = status_code

        def json(self) -> Dict[str, Any]:
            return dict(self._body)

    def _fake_post(url, json, timeout):  # noqa: ANN001
        posted.append({"url": str(url), "json": dict(json)})
        if str(url).endswith("/runtime/meeting/search"):
            return _Resp(
                {
                    "ok": True,
                    "candidate": {
                        "calendar_event_id": "evt-321",
                        "title": "Созвон с Иваном",
                        "start_at": "2026-04-13T14:00:00+03:00",
                        "start_at_date": "2026-04-13",
                        "start_at_time": "14:00",
                        "duration_minutes": 60,
                    },
                }
            )
        return _Resp({"ok": True, "calendar_event_id": "evt-321", "debug": {"calendar_commit": "ok"}, "user_message": "ok"})

    def _direct(req):  # noqa: ANN001
        cmd = ((req.metadata or {}).get("runtime_command") or {})
        seen["command"] = cmd
        return {
            "outcome": "rec_needs_clarification",
            "needs_clarification": True,
            "clarifying_question": "Проверьте, всё ли верно.",
            "rec": {"outcome": "needs_clarification", "missing_field": "temporal_commit_confirm"},
            "command": cmd,
        }

    bridge = runtime_bridge.RuntimeBridge(
        worker_command_url="http://worker/runtime/command",
        direct_handler=_direct,
        http_post=_fake_post,
    )

    req = runtime_bridge.TelegramRuntimeRequest(
        request_id="req-update-source-1",
        app_id="app",
        tenant_id="tenant",
        channel="telegram",
        user_id="u-source-1",
        chat_id="c-source-1",
        source_message_id="m-source-1",
        text="перенеси встречу на 16",
        timezone="Europe/Moscow",
        metadata={},
    )
    _ = bridge.process_request(req)

    cmd = seen.get("command") or {}
    entities = cmd.get("entities") if isinstance(cmd, dict) else {}
    entities = entities if isinstance(entities, dict) else {}
    assert posted
    assert posted[0]["url"].endswith("/runtime/meeting/search")
    assert posted[0]["json"].get("target_hint") == ""
    assert cmd.get("intent") == "meeting.update"
    assert entities.get("calendar_event_id") == "evt-321"
    assert entities.get("start_at_date") == "2026-04-13"
    assert entities.get("start_at_time") == "16:00"
    assert entities.get("duration_minutes") == 60


def test_meeting_create_confirm_yes_returns_created_message() -> None:
    runtime_bridge = _load_module(RUNTIME_BRIDGE_PATH, "app_integration_runtime_bridge_module_h")

    def _fake_post(url, json, timeout):  # noqa: ANN001
        intent = str(json.get("command", {}).get("intent") or "")
        if intent == "meeting.create":
            return _FakeResponse({"ok": True, "user_message": "Встреча создана", "calendar_event_id": "evt-c1", "debug": {"calendar_commit": "ok"}})
        return _FakeResponse({"ok": True, "user_message": "Встреча перенесена", "calendar_event_id": "evt-u1", "debug": {"calendar_commit": "ok"}})

    bridge = runtime_bridge.RuntimeBridge(
        worker_command_url="http://worker/runtime/command",
        direct_handler=lambda _req: _execution_payload("meeting.create", time_value="14"),
        http_post=_fake_post,
    )
    result = bridge.process_request(_request(runtime_bridge))
    text = str(result.reply_text or "")
    assert "Встреча создана" in text
    assert "Встреча перенесена" not in text


def test_temporal_commit_envelope_preserves_comment_from_command_and_draft() -> None:
    runtime_bridge = _load_module(RUNTIME_BRIDGE_PATH, "app_integration_runtime_bridge_module_comment_env")
    posted: List[Dict[str, Any]] = []

    def _fake_post(url, json, timeout):  # noqa: ANN001
        posted.append({"url": url, "json": dict(json), "timeout": timeout})
        return _FakeResponse({"ok": True, "user_message": "Встреча создана", "calendar_event_id": "evt-comment-env-1", "debug": {"calendar_commit": "ok"}})

    def _direct(_req):  # noqa: ANN001
        payload = _execution_payload_edited_draft("meeting.create", new_date="2026-04-13", new_time="12", new_duration=30)
        cmd = payload["command"]
        cmd["comment_text"] = "обсудить договор"
        entities = cmd.get("entities") if isinstance(cmd.get("entities"), dict) else {}
        entities["comment_text"] = "обсудить договор"
        cmd["entities"] = entities
        draft = cmd.get("__temporal_draft") if isinstance(cmd.get("__temporal_draft"), dict) else {}
        draft["comment"] = "обсудить договор"
        cmd["__temporal_draft"] = draft
        return payload

    bridge = runtime_bridge.RuntimeBridge(
        worker_command_url="http://worker/runtime/command",
        direct_handler=_direct,
        http_post=_fake_post,
    )

    result = bridge.process_request(_request(runtime_bridge))

    assert result.outcome == "direct_processed"
    assert posted
    entities = posted[0]["json"]["command"]["entities"]
    assert entities["comment_text"] == "обсудить договор"


def test_meeting_update_confirm_yes_returns_rescheduled_message() -> None:
    runtime_bridge = _load_module(RUNTIME_BRIDGE_PATH, "app_integration_runtime_bridge_module_i")

    def _fake_post(url, json, timeout):  # noqa: ANN001
        intent = str(json.get("command", {}).get("intent") or "")
        if intent == "meeting.update":
            return _FakeResponse({"ok": True, "user_message": "Встреча перенесена", "calendar_event_id": "evt-u2", "debug": {"calendar_commit": "ok"}})
        return _FakeResponse({"ok": True, "user_message": "Встреча создана", "calendar_event_id": "evt-c2", "debug": {"calendar_commit": "ok"}})

    bridge = runtime_bridge.RuntimeBridge(
        worker_command_url="http://worker/runtime/command",
        direct_handler=lambda _req: _execution_payload("meeting.update", time_value="16"),
        http_post=_fake_post,
    )
    result = bridge.process_request(_request(runtime_bridge))
    text = str(result.reply_text or "")
    assert "Встреча перенесена" in text


def test_meeting_update_without_source_event_returns_not_found_and_skips_handler() -> None:
    runtime_bridge = _load_module(RUNTIME_BRIDGE_PATH, "app_integration_runtime_bridge_module_j")
    called = {"direct": 0}

    class _Resp:
        def __init__(self, body: Dict[str, Any], status_code: int = 200) -> None:
            self._body = body
            self.status_code = status_code

        def json(self) -> Dict[str, Any]:
            return dict(self._body)

    def _fake_post(url, json, timeout):  # noqa: ANN001
        if str(url).endswith("/runtime/meeting/search"):
            return _Resp({"ok": False, "reason": "not_found"})
        return _Resp({"ok": True})

    def _direct(_req):  # noqa: ANN001
        called["direct"] += 1
        return _execution_payload("meeting.update", time_value="16")

    bridge = runtime_bridge.RuntimeBridge(
        worker_command_url="http://worker/runtime/command",
        direct_handler=_direct,
        http_post=_fake_post,
    )
    req = runtime_bridge.TelegramRuntimeRequest(
        request_id="req-update-nosource-1",
        app_id="app",
        tenant_id="tenant",
        channel="telegram",
        user_id="u-nosource-1",
        chat_id="c-nosource-1",
        source_message_id="m-nosource-1",
        text="перенеси встречу на 16",
        timezone="Europe/Moscow",
        metadata={},
    )
    result = bridge.process_request(req)

    assert called["direct"] == 0
    assert result.outcome == "rejected"
    assert "не нашёл встречу для переноса" in str(result.reply_text or "").lower()


def test_meeting_update_empty_hydrated_draft_is_rejected_before_handler() -> None:
    runtime_bridge = _load_module(RUNTIME_BRIDGE_PATH, "app_integration_runtime_bridge_module_k")
    called = {"direct": 0}

    class _Resp:
        def __init__(self, body: Dict[str, Any], status_code: int = 200) -> None:
            self._body = body
            self.status_code = status_code

        def json(self) -> Dict[str, Any]:
            return dict(self._body)

    def _fake_post(url, json, timeout):  # noqa: ANN001
        if str(url).endswith("/runtime/meeting/search"):
            return _Resp(
                {
                    "ok": True,
                    "candidate": {
                        "calendar_event_id": "evt-empty-1",
                        "start_at_date": "",
                        "start_at_time": "",
                        "duration_minutes": None,
                    },
                }
            )
        return _Resp({"ok": True})

    def _direct(_req):  # noqa: ANN001
        called["direct"] += 1
        return _execution_payload("meeting.update", time_value="16")

    bridge = runtime_bridge.RuntimeBridge(
        worker_command_url="http://worker/runtime/command",
        direct_handler=_direct,
        http_post=_fake_post,
    )
    req = runtime_bridge.TelegramRuntimeRequest(
        request_id="req-update-emptydraft-1",
        app_id="app",
        tenant_id="tenant",
        channel="telegram",
        user_id="u-emptydraft-1",
        chat_id="c-emptydraft-1",
        source_message_id="m-emptydraft-1",
        text="перенеси встречу на 16",
        timezone="Europe/Moscow",
        metadata={},
    )
    result = bridge.process_request(req)

    assert called["direct"] == 0
    assert result.outcome == "rejected"
    assert "не нашёл встречу для переноса" in str(result.reply_text or "").lower()


def test_meeting_update_search_uses_target_hint_and_combined_path() -> None:
    runtime_bridge = _load_module(RUNTIME_BRIDGE_PATH, "app_integration_runtime_bridge_module_l")
    seen: Dict[str, Any] = {}

    class _Resp:
        def __init__(self, body: Dict[str, Any], status_code: int = 200) -> None:
            self._body = body
            self.status_code = status_code

        def json(self) -> Dict[str, Any]:
            return dict(self._body)

    def _fake_post(url, json, timeout):  # noqa: ANN001
        if str(url).endswith("/runtime/meeting/search"):
            seen["search_json"] = dict(json)
            return _Resp(
                {
                    "ok": True,
                    "candidate": {
                        "calendar_event_id": "evt-ivan-1",
                        "title": "Встреча с Иваном",
                        "start_at_date": "2026-04-13",
                        "start_at_time": "14:00",
                        "duration_minutes": 60,
                    },
                }
            )
        return _Resp({"ok": True, "calendar_event_id": "evt-ivan-1", "debug": {"calendar_commit": "ok"}, "user_message": "ok"})

    def _direct(req):  # noqa: ANN001
        cmd = ((req.metadata or {}).get("runtime_command") or {})
        seen["command"] = cmd
        return {
            "outcome": "rec_needs_clarification",
            "needs_clarification": True,
            "clarifying_question": "confirm target",
            "rec": {"outcome": "needs_clarification", "missing_field": "awaiting_target_confirm"},
            "command": cmd,
        }

    bridge = runtime_bridge.RuntimeBridge(
        worker_command_url="http://worker/runtime/command",
        direct_handler=_direct,
        http_post=_fake_post,
    )
    req = runtime_bridge.TelegramRuntimeRequest(
        request_id="req-update-hint-1",
        app_id="app",
        tenant_id="tenant",
        channel="telegram",
        user_id="u-hint-1",
        chat_id="c-hint-1",
        source_message_id="m-hint-1",
        text="перенеси собрание с Иваном на 16",
        timezone="Europe/Moscow",
        metadata={},
    )
    _ = bridge.process_request(req)

    search_json = seen.get("search_json") or {}
    assert search_json.get("user_id") == "u-hint-1"
    assert "иваном" in str(search_json.get("target_hint") or "")
    assert str(search_json.get("meeting_kind") or "") == "собрание"
    cmd = seen.get("command") or {}
    entities = cmd.get("entities") if isinstance(cmd, dict) else {}
    entities = entities if isinstance(entities, dict) else {}
    assert entities.get("calendar_event_id") == "evt-ivan-1"
    assert entities.get("start_at_time") == "16:00"
    assert entities.get("meeting_update_new_time") == "16:00"
    assert entities.get("meeting_kind") == "собрание"


def test_meeting_update_multiple_candidates_enriches_request_and_calls_handler() -> None:
    runtime_bridge = _load_module(RUNTIME_BRIDGE_PATH, "app_integration_runtime_bridge_module_m")
    called = {"direct": 0}
    seen: Dict[str, Any] = {}

    class _Resp:
        def __init__(self, body: Dict[str, Any], status_code: int = 200) -> None:
            self._body = body
            self.status_code = status_code

        def json(self) -> Dict[str, Any]:
            return dict(self._body)

    def _fake_post(url, json, timeout):  # noqa: ANN001
        if str(url).endswith("/runtime/meeting/search"):
            return _Resp(
                {
                    "ok": False,
                    "reason": "multiple",
                    "candidates": [
                        {"calendar_event_id": "evt-1", "title": "Встреча с Иваном"},
                        {"calendar_event_id": "evt-2", "title": "Встреча с Иваном (команда)"},
                    ],
                }
            )
        return _Resp({"ok": True})

    def _direct(req):  # noqa: ANN001
        called["direct"] += 1
        cmd = ((req.metadata or {}).get("runtime_command") or {})
        seen["command"] = cmd
        return {
            "outcome": "rec_needs_clarification",
            "needs_clarification": True,
            "clarifying_question": "Нашёл несколько встреч. Уточните номер:",
            "rec": {"outcome": "needs_clarification", "missing_field": "meeting_update_target_select"},
            "command": cmd,
        }

    bridge = runtime_bridge.RuntimeBridge(
        worker_command_url="http://worker/runtime/command",
        direct_handler=_direct,
        http_post=_fake_post,
    )
    req = runtime_bridge.TelegramRuntimeRequest(
        request_id="req-update-multiple-1",
        app_id="app",
        tenant_id="tenant",
        channel="telegram",
        user_id="u-multiple-1",
        chat_id="c-multiple-1",
        source_message_id="m-multiple-1",
        text="перенеси встречу с Иваном на 16",
        timezone="Europe/Moscow",
        metadata={},
    )
    result = bridge.process_request(req)

    assert called["direct"] == 1
    assert result.outcome == "rec_needs_clarification"
    assert "несколько встреч" in str(result.reply_text or "").lower()
    cmd = seen.get("command") or {}
    entities = cmd.get("entities") if isinstance(cmd, dict) else {}
    entities = entities if isinstance(entities, dict) else {}
    assert cmd.get("intent") == "meeting.update"
    assert entities.get("meeting_update_new_time") == "16:00"
    candidates = entities.get("__meeting_update_candidates")
    assert isinstance(candidates, list) and len(candidates) == 2


def test_perenesi_sobranie_is_treated_as_meeting_update_and_runs_target_search() -> None:
    runtime_bridge = _load_module(RUNTIME_BRIDGE_PATH, "app_integration_runtime_bridge_module_n")
    called = {"direct": 0, "search": 0}

    class _Resp:
        def __init__(self, body: Dict[str, Any], status_code: int = 200) -> None:
            self._body = body
            self.status_code = status_code

        def json(self) -> Dict[str, Any]:
            return dict(self._body)

    def _fake_post(url, json, timeout):  # noqa: ANN001
        if str(url).endswith("/runtime/meeting/search"):
            called["search"] += 1
            return _Resp({"ok": False, "reason": "not_found"})
        return _Resp({"ok": True})

    def _direct(_req):  # noqa: ANN001
        called["direct"] += 1
        return _execution_payload("meeting.create", time_value="18")

    bridge = runtime_bridge.RuntimeBridge(
        worker_command_url="http://worker/runtime/command",
        direct_handler=_direct,
        http_post=_fake_post,
    )
    req = runtime_bridge.TelegramRuntimeRequest(
        request_id="req-update-verb-1",
        app_id="app",
        tenant_id="tenant",
        channel="telegram",
        user_id="u-update-verb-1",
        chat_id="c-update-verb-1",
        source_message_id="m-update-verb-1",
        text="перенеси собрание",
        timezone="Europe/Moscow",
        metadata={},
    )
    result = bridge.process_request(req)

    assert called["search"] == 1
    assert called["direct"] == 0
    assert result.outcome == "rejected"
    assert "не наш" in str(result.reply_text or "").lower()

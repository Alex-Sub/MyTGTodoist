from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
APP_SRC = ROOT / "telegram-bot" / "app-integration" / "src"
HANDLER_PATH = APP_SRC / "app" / "handler.py"


def _load_handler_module():
    preserved: dict[str, Any] = {}
    for key in list(sys.modules.keys()):
        if key == "src" or key.startswith("src."):
            preserved[key] = sys.modules.pop(key)

    src_pkg = types.ModuleType("src")
    src_pkg.__path__ = [str(APP_SRC)]  # type: ignore[attr-defined]
    sys.modules["src"] = src_pkg

    spec = importlib.util.spec_from_file_location("app_integration_handler_voice_module", HANDLER_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    for key in list(sys.modules.keys()):
        if key == "src" or key.startswith("src."):
            sys.modules.pop(key)
    sys.modules.update(preserved)
    return module


class _AsrSequence:
    def __init__(self, outputs: list[Any]) -> None:
        self.outputs = list(outputs)
        self.calls = 0

    def transcribe(
        self,
        audio_bytes: bytes,
        mime_type: str | None = None,  # noqa: ARG002
        filename: str | None = None,  # noqa: ARG002
        request_id: str | None = None,  # noqa: ARG002
    ) -> str:
        _ = audio_bytes
        self.calls += 1
        if not self.outputs:
            return ""
        current = self.outputs.pop(0)
        if isinstance(current, BaseException):
            raise current
        return str(current)


class _ProbeLLM:
    def __init__(self, payload: dict | None = None) -> None:
        self.payload = payload or {"intent": "task.create", "entities": {"title": "test"}}
        self.calls = 0

    def parse(self, *, text: str) -> dict:  # noqa: ARG002
        self.calls += 1
        return dict(self.payload)


class _ProbeREC:
    def __init__(self, payload: dict | None = None) -> None:
        self.payload = payload or {"outcome": "ok", "hits": [{"id": "h1", "type": "task"}]}
        self.calls = 0

    def query(self, payload: dict) -> dict:  # noqa: ARG002
        self.calls += 1
        return dict(self.payload)


class _FakeDb:
    def __init__(self, session: dict | None) -> None:
        self.session = session
        self.deleted_contexts: list[str] = []
        self.upserts: list[dict] = []

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

    def finalize_command_dedup(self, **kwargs) -> None:  # noqa: ARG002
        return None

    def is_command_dedup_stale(self, existing: dict, ttl_seconds: int) -> bool:  # noqa: ARG002
        return False

    def reclaim_command_dedup(self, idempotency_key: str, intent: str) -> bool:  # noqa: ARG002
        return False


def test_voice_empty_transcript_retries_once_then_fallback() -> None:
    handler = _load_handler_module()
    asr = _AsrSequence(["", ""])
    llm = _ProbeLLM()
    rec = _ProbeREC()

    result = handler.handle_user_attempt(
        request_id="voice-empty-1",
        user_id="u-voice-empty",
        channel="telegram",
        asr_client=asr,
        llm_client=llm,
        rec_client=rec,
        audio_bytes=b"voice-bytes",
        text=None,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-voice-empty",
        source_message_id="msg-voice-empty",
    )

    assert result["outcome"] == "empty_transcript"
    assert result["user_message"] == "Не удалось разобрать голос. Повтори ещё раз чуть длиннее."
    assert asr.calls == 2
    assert llm.calls == 0
    assert rec.calls == 0


def test_voice_low_quality_initial_command_returns_fallback() -> None:
    handler = _load_handler_module()
    asr = _AsrSequence(["эээ", "мм"])
    llm = _ProbeLLM()
    rec = _ProbeREC()

    result = handler.handle_user_attempt(
        request_id="voice-low-quality-1",
        user_id="u-voice-low-quality",
        channel="telegram",
        asr_client=asr,
        llm_client=llm,
        rec_client=rec,
        audio_bytes=b"voice-bytes",
        text=None,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-voice-low-quality",
        source_message_id="msg-voice-low-quality",
    )

    assert result["outcome"] == "low_quality_transcript"
    assert result["user_message"] == "Распозналась только часть фразы. Повтори команду одной фразой."
    assert asr.calls == 2
    assert llm.calls == 0
    assert rec.calls == 0


def test_short_numeric_clarification_voice_reply_is_accepted() -> None:
    handler = _load_handler_module()
    asr = _AsrSequence(["30"])
    llm = _ProbeLLM()
    rec = _ProbeREC()
    db = _FakeDb(
        session={
            "intent": "schedule_meeting",
            "missing_field": "duration_minutes",
            "idempotency_key": "idem-voice-30",
            "payload": {"intent": "schedule_meeting", "start_at": "2026-04-08T12:00:00+03:00"},
        }
    )

    result = handler.handle_user_attempt(
        request_id="voice-clarification-30",
        user_id="u-voice-clarification",
        channel="telegram",
        asr_client=asr,
        llm_client=llm,
        rec_client=rec,
        audio_bytes=b"voice-bytes",
        text=None,
        local_db=db,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-voice-clarification",
        source_message_id="msg-voice-clarification",
    )

    assert result["outcome"] == "success"
    assert result["rec"]["outcome"] == "skipped"
    assert int(result["command"]["duration_minutes"]) == 30
    assert asr.calls == 1
    assert llm.calls == 0
    assert rec.calls == 0


def test_voice_asr_timeout_retries_once_then_fallback() -> None:
    handler = _load_handler_module()
    asr = _AsrSequence([TimeoutError("t1"), TimeoutError("t2")])
    llm = _ProbeLLM()
    rec = _ProbeREC()

    result = handler.handle_user_attempt(
        request_id="voice-timeout-1",
        user_id="u-voice-timeout",
        channel="telegram",
        asr_client=asr,
        llm_client=llm,
        rec_client=rec,
        audio_bytes=b"voice-bytes",
        text=None,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-voice-timeout",
        source_message_id="msg-voice-timeout",
    )

    assert result["outcome"] == "asr_timeout"
    assert result["user_message"] == "Сейчас не получилось обработать голос. Попробуй ещё раз или напиши текстом."
    assert asr.calls == 2
    assert llm.calls == 0
    assert rec.calls == 0


def test_successful_voice_transcript_is_forwarded_to_runtime_path() -> None:
    handler = _load_handler_module()
    asr = _AsrSequence(["Купить молоко"])
    llm = _ProbeLLM(payload={"intent": "task.create", "entities": {"title": "Купить молоко"}})
    rec = _ProbeREC()

    result = handler.handle_user_attempt(
        request_id="voice-success-1",
        user_id="u-voice-success",
        channel="telegram",
        asr_client=asr,
        llm_client=llm,
        rec_client=rec,
        audio_bytes=b"voice-bytes",
        text=None,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-voice-success",
        source_message_id="msg-voice-success",
    )

    assert result["outcome"] == "success"
    assert result["command"]["intent"] == "task.create"
    assert llm.calls == 1


def test_bad_voice_transcript_does_not_reach_decision_layers() -> None:
    handler = _load_handler_module()
    asr = _AsrSequence(["...", "..."])
    llm = _ProbeLLM()
    rec = _ProbeREC()

    result = handler.handle_user_attempt(
        request_id="voice-no-leak-1",
        user_id="u-voice-no-leak",
        channel="telegram",
        asr_client=asr,
        llm_client=llm,
        rec_client=rec,
        audio_bytes=b"voice-bytes",
        text=None,
        app_id="app",
        tenant_id="tenant",
        source_chat_id="chat-voice-no-leak",
        source_message_id="msg-voice-no-leak",
    )

    assert result["outcome"] == "low_quality_transcript"
    assert llm.calls == 0
    assert rec.calls == 0

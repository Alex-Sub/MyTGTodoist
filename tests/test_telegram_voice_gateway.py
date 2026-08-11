from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path
from typing import Any, Dict, List


ROOT = Path(__file__).resolve().parents[1]
APP_SRC = ROOT / "telegram-bot" / "app-integration" / "src"
HANDLER_PATH = APP_SRC / "app" / "handler.py"
RUNTIME_BRIDGE_PATH = APP_SRC / "integrations" / "telegram" / "runtime_bridge.py"
UPDATE_MAPPER_PATH = APP_SRC / "integrations" / "telegram" / "update_mapper.py"


def _load_module(module_path: Path, module_name: str):
    preserved: dict[str, Any] = {}
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


def test_voice_update_maps_to_runtime_request_with_voice_metadata() -> None:
    mapper = _load_module(UPDATE_MAPPER_PATH, "app_integration_update_mapper_for_voice_gateway")
    update = {
        "update_id": 1001,
        "message": {
            "message_id": 77,
            "chat": {"id": 2002},
            "from": {"id": 1001},
            "voice": {
                "file_id": "voice-file-1",
                "file_unique_id": "voice-uniq-1",
                "duration": 9,
                "mime_type": "audio/ogg",
            },
        },
    }

    req = mapper.map_update_to_request(update, app_id="app", tenant_id="tenant", timezone="Europe/Moscow")

    assert req.text is None
    assert req.audio_filename == "voice-file-1.ogg"
    voice_meta = req.metadata.get("voice")
    assert isinstance(voice_meta, dict)
    assert voice_meta["file_id"] == "voice-file-1"
    assert voice_meta["file_unique_id"] == "voice-uniq-1"
    assert voice_meta["duration"] == 9


def test_voice_request_in_direct_mode_is_transcribed_and_routed_without_block_message() -> None:
    runtime_bridge = _load_module(RUNTIME_BRIDGE_PATH, "app_integration_runtime_bridge_module_voice_gateway")
    handler = _load_module(HANDLER_PATH, "app_integration_handler_voice_gateway_bridge")

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

    asr = _AsrProbe()
    llm = _LlmProbe()
    rec = _RecProbe()

    def direct_handler(req):  # noqa: ANN001
        return handler.handle_user_attempt(
            request_id=req.request_id,
            user_id=req.user_id,
            channel=req.channel,
            asr_client=asr,
            llm_client=llm,
            rec_client=rec,
            audio_bytes=req.audio_bytes,
            audio_mime=req.audio_mime,
            audio_filename=req.audio_filename,
            text=req.text,
            metadata=req.metadata,
            local_db=None,
            app_id=req.app_id,
            tenant_id=req.tenant_id,
            source_chat_id=req.chat_id,
            source_message_id=req.source_message_id,
        )

    bridge = runtime_bridge.RuntimeBridge(
        worker_command_url="http://worker/runtime/command",
        direct_handler=direct_handler,
        direct_voice_enabled=False,
    )
    req = runtime_bridge.TelegramRuntimeRequest(
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

    result = bridge.process_request(req)

    assert asr.calls == 1
    assert asr.request_ids == ["tg:voice:1"]
    assert llm.calls == 1
    assert llm.last_text == "запланируй встречу завтра в 12"
    assert result.outcome == "rec_needs_clarification"
    assert result.needs_clarification is True
    assert "Время: 12:00" in str(result.clarifying_question or "")
    assert "Голосовые сообщения в режиме runtime_core_direct пока не поддерживаются" not in str(result.reply_text or "")
    assert bool(result.telemetry.get("voice_request")) is True


def test_voice_empty_transcript_retries_once_then_fallback() -> None:
    handler = _load_module(HANDLER_PATH, "app_integration_handler_voice_gateway_empty")
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


def test_voice_asr_timeout_retries_once_then_fallback() -> None:
    handler = _load_module(HANDLER_PATH, "app_integration_handler_voice_gateway_timeout")
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


def test_voice_task_transcript_enters_active_task_confirm_flow() -> None:
    handler = _load_module(HANDLER_PATH, "app_integration_handler_voice_gateway_success")
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

    assert result["outcome"] == "rec_needs_clarification"
    assert result["rec"]["missing_field"] == "task_create_confirm"
    assert result["command"]["intent"] == "task.create"
    assert "Создать задачу?" in str(result["user_message"])
    assert llm.calls == 1

import importlib.util
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
BOT_PATH = ROOT / "telegram-bot" / "bot.py"

spec = importlib.util.spec_from_file_location("telegram_bot_module", BOT_PATH)
assert spec is not None and spec.loader is not None
telegram_bot = importlib.util.module_from_spec(spec)
spec.loader.exec_module(telegram_bot)


def test_build_runtime_envelope_from_ml_happy_path() -> None:
    response = {
        "transcript": "создай задачу купить молоко",
        "transcript_norm": "создай задачу купить молоко",
        "command": {
            "intent": "create_task",
            "title": "Купить молоко",
            "details": "2 литра",
            "when": "2026-02-24T10:00:00+01:00",
            "priority": "high",
            "tags": ["home"],
            "assignees": ["alexey"],
        },
    }
    envelope, err = telegram_bot._build_runtime_envelope_from_ml_response(
        chat_id=123,
        update_id=456,
        message_id=789,
        response_json=response,
    )
    assert err is None
    assert envelope is not None
    assert envelope["command"]["intent"] == "task.create"
    entities = envelope["command"]["entities"]
    assert entities["title"] == "Купить молоко"
    assert entities["planned_at"] == "2026-02-24T10:00:00+01:00"
    assert entities["source_msg_id"] == "tg:123:789"


def test_ml_gateway_timeout_returns_asr_timeout(monkeypatch) -> None:
    class FakeRequests:
        def post(self, *args, **kwargs):  # noqa: ANN002, ANN003
            raise TimeoutError("ml timeout")

    monkeypatch.setattr(telegram_bot, "_require_requests", lambda: FakeRequests())
    monkeypatch.setattr(telegram_bot, "ML_CORE_URL", "http://ml.local")
    payload, err = telegram_bot._ml_gateway_voice_command(
        b"abc",
        filename="voice.ogg",
        mime_type="audio/ogg",
    )
    assert payload is None
    assert err == "asr_timeout"


def test_build_runtime_envelope_empty_transcript_returns_asr_empty() -> None:
    response = {
        "transcript": "",
        "transcript_norm": "   ",
        "command": {"intent": "create_task", "title": "Любая задача"},
    }
    envelope, err = telegram_bot._build_runtime_envelope_from_ml_response(
        chat_id=1,
        update_id=2,
        message_id=3,
        response_json=response,
    )
    assert envelope is None
    assert err == "asr_empty"


def test_handle_voice_timeout_returns_retry_message(monkeypatch) -> None:
    sent: list[str] = []
    monkeypatch.setattr(telegram_bot, "_send_message", lambda _chat_id, text: sent.append(text))
    monkeypatch.setattr(telegram_bot, "_tg_get_file_path", lambda _file_id: "voice/file.ogg")
    monkeypatch.setattr(telegram_bot, "_tg_download_file", lambda _path: (b"abc", "voice.ogg", "audio/ogg"))
    monkeypatch.setattr(
        telegram_bot,
        "_ml_gateway_voice_command",
        lambda _audio, **_kwargs: (None, "asr_timeout"),
    )
    message = {"chat": {"id": 42}, "voice": {"file_id": "f1"}, "message_id": 7}
    telegram_bot._handle_voice_message(update_id=1001, message=message)
    assert sent == ["Не получилось разобрать речь. Попробуй сказать ещё раз."]


def test_handle_voice_invalid_json_returns_later_message(monkeypatch) -> None:
    sent: list[str] = []
    monkeypatch.setattr(telegram_bot, "_send_message", lambda _chat_id, text: sent.append(text))
    monkeypatch.setattr(telegram_bot, "_tg_get_file_path", lambda _file_id: "voice/file.ogg")
    monkeypatch.setattr(telegram_bot, "_tg_download_file", lambda _path: (b"abc", "voice.ogg", "audio/ogg"))
    monkeypatch.setattr(
        telegram_bot,
        "_ml_gateway_voice_command",
        lambda _audio, **_kwargs: (None, "llm_invalid_output"),
    )
    message = {"chat": {"id": 42}, "voice": {"file_id": "f1"}, "message_id": 7}
    telegram_bot._handle_voice_message(update_id=1002, message=message)
    assert sent == ["Я сейчас не могу корректно обработать запрос. Давай попробуем позже."]

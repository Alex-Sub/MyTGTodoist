from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
APP_SRC = ROOT / "telegram-bot" / "app-integration" / "src"
BOT_MAIN_PATH = APP_SRC / "integrations" / "telegram" / "bot_main.py"


def _load_bot_main_module():
    preserved: dict[str, Any] = {}
    for key in list(sys.modules.keys()):
        if key == "src" or key.startswith("src.") or key == "requests":
            preserved[key] = sys.modules.pop(key)

    requests_stub = types.ModuleType("requests")

    class _ReadTimeout(Exception):
        pass

    class _HTTPError(Exception):
        def __init__(self, response: Any | None = None) -> None:
            super().__init__("http error")
            self.response = response

    class _Session:
        def post(self, *args, **kwargs):  # noqa: ANN002, ANN003
            raise AssertionError("HTTP session must not be used in this test")

        def get(self, *args, **kwargs):  # noqa: ANN002, ANN003
            raise AssertionError("HTTP session must not be used in this test")

    requests_stub.Session = _Session
    requests_stub.exceptions = types.SimpleNamespace(ReadTimeout=_ReadTimeout, HTTPError=_HTTPError)
    sys.modules["requests"] = requests_stub

    src_pkg = types.ModuleType("src")
    src_pkg.__path__ = [str(APP_SRC)]  # type: ignore[attr-defined]
    sys.modules["src"] = src_pkg

    spec = importlib.util.spec_from_file_location("app_integration_bot_main_module", BOT_MAIN_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)

    for key in list(sys.modules.keys()):
        if key == "src" or key.startswith("src.") or key == "requests":
            sys.modules.pop(key)
    sys.modules.update(preserved)
    return module


class _FakeTransport:
    def __init__(self) -> None:
        self.sent_messages: list[dict[str, Any]] = []
        self.posts: list[dict[str, Any]] = []

    def send_message(self, *, chat_id: str, text: str, reply_to_message_id: str | None = None) -> dict[str, Any]:
        payload = {
            "chat_id": chat_id,
            "text": text,
            "reply_to_message_id": reply_to_message_id,
        }
        self.sent_messages.append(payload)
        return payload

    def _post(self, method: str, payload: dict[str, Any]) -> dict[str, Any]:
        self.posts.append({"method": method, "payload": dict(payload)})
        if method == "sendMessage":
            return {"ok": True, "result": {"message_id": 777}}
        return {"ok": True}


class _FakeResult:
    def __init__(
        self,
        *,
        clarifying_question: str,
        missing_field: str,
        suppress_send: bool = False,
    ) -> None:
        self.outcome = "rec_needs_clarification"
        self.reply_text = clarifying_question
        self.needs_clarification = True
        self.clarifying_question = clarifying_question
        self.duplicate_state = None
        self.telemetry = {"suppress_send": True} if suppress_send else {}
        self.raw_runtime_payload = {"rec": {"missing_field": missing_field}}


class _FakeBridge:
    def __init__(self, results: list[_FakeResult]) -> None:
        self.results = list(results)
        self.requests: list[Any] = []

    def process_request(self, req: Any) -> _FakeResult:
        self.requests.append(req)
        assert self.results, "bridge called more times than expected"
        return self.results.pop(0)


class _FakeLocalDb:
    def __init__(self) -> None:
        self.bound: list[dict[str, str]] = []

    def bind_clarification_prompt_message(self, *, context_key: str, prompt_message_id: str) -> None:
        self.bound.append({"context_key": context_key, "prompt_message_id": prompt_message_id})


def test_process_single_update_suppresses_duplicate_temporal_confirmation_message(tmp_path) -> None:
    bot_main = _load_bot_main_module()
    transport = _FakeTransport()
    bridge = _FakeBridge(
        results=[
            _FakeResult(
                clarifying_question="Проверьте параметры и подтвердите планирование.",
                missing_field="temporal_commit_confirm",
            ),
            _FakeResult(
                clarifying_question="",
                missing_field="temporal_commit_confirm",
                suppress_send=True,
            ),
        ]
    )
    settings = bot_main.AdapterSettings(
        telegram_bot_token="token",
        telegram_adapter_mode="runtime_core_direct",
        worker_command_url="",
        organizer_api_url="http://organizer-api:8000",
        google_sync_review_url="http://google-sync:8010/review",
        ml_gateway_url="http://ml.local",
        app_id="app",
        app_version="test",
        app_env="test",
        tenant_id="tenant",
        local_db_path=str(tmp_path / "local.db"),
        cloud_buffer_dsn="sqlite:///cloud.db",
        app_dict_path="dict.yaml",
        queue_replay_ttl_seconds=600,
        clarification_ttl_sec=300,
        command_dedup_reservation_ttl_sec=120,
        state_path=str(tmp_path / "state.json"),
        health_port=0,
        app_timezone="Europe/Moscow",
        telegram_http_timeout_sec=10.0,
        telegram_http_retries=1,
        telegram_poll_timeout_sec=20,
        telegram_poll_interval_sec=1.0,
        telegram_max_cycles=0,
        direct_voice_enabled=True,
        direct_asr_timeout_sec=45.0,
        direct_asr_retries=1,
    )
    app = bot_main.AdapterApp(settings=settings, transport=transport, bridge=bridge, offset=None)
    update = {
        "update_id": 101,
        "message": {
            "message_id": 303,
            "chat": {"id": 202},
            "from": {"id": 404},
            "text": "запланируй встречу завтра в 11",
        },
    }

    bot_main.process_single_update(app, update)
    bot_main.process_single_update(app, update)

    send_posts = [item for item in transport.posts if item.get("method") == "sendMessage"]
    assert len(send_posts) == 1
    assert send_posts[0]["payload"]["text"] == "Проверьте параметры и подтвердите планирование.\n\nЕсли нужно исправить параметры, выберите поле кнопкой или отправьте уточнение сообщением.\nМожно выбрать поле для правки, подтвердить или отменить сценарий."


def test_process_single_update_binds_clarification_prompt_message_id(tmp_path) -> None:
    bot_main = _load_bot_main_module()
    transport = _FakeTransport()
    bridge = _FakeBridge(
        results=[
            _FakeResult(
                clarifying_question="Какой комментарий добавить?",
                missing_field="temporal_edit_field",
            )
        ]
    )
    fake_db = _FakeLocalDb()
    bridge.direct_local_db = fake_db
    settings = bot_main.AdapterSettings(
        telegram_bot_token="token",
        telegram_adapter_mode="runtime_core_direct",
        worker_command_url="",
        organizer_api_url="http://organizer-api:8000",
        google_sync_review_url="http://google-sync:8010/review",
        ml_gateway_url="http://ml.local",
        app_id="app",
        app_version="test",
        app_env="test",
        tenant_id="tenant",
        local_db_path=str(tmp_path / "local.db"),
        cloud_buffer_dsn="sqlite:///cloud.db",
        app_dict_path="dict.yaml",
        queue_replay_ttl_seconds=600,
        clarification_ttl_sec=300,
        command_dedup_reservation_ttl_sec=120,
        state_path=str(tmp_path / "state.json"),
        health_port=0,
        app_timezone="Europe/Moscow",
        telegram_http_timeout_sec=10.0,
        telegram_http_retries=1,
        telegram_poll_timeout_sec=20,
        telegram_poll_interval_sec=1.0,
        telegram_max_cycles=0,
        direct_voice_enabled=True,
        direct_asr_timeout_sec=45.0,
        direct_asr_retries=1,
    )
    app = bot_main.AdapterApp(settings=settings, transport=transport, bridge=bridge, offset=None)
    update = {
        "update_id": 102,
        "callback_query": {
            "id": "cb-102",
            "data": "clarify:v1:temporal_edit:comment",
            "from": {"id": 404},
            "message": {"message_id": 303, "chat": {"id": 202}},
        },
    }

    bot_main.process_single_update(app, update)

    assert fake_db.bound
    assert fake_db.bound[0]["prompt_message_id"] == "777"
    assert fake_db.bound[0]["context_key"] == "app::tenant::telegram::202::404"


def test_load_settings_rejects_legacy_adapter_mode(monkeypatch) -> None:
    bot_main = _load_bot_main_module()
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "token")
    monkeypatch.setenv("APP_ID", "app")
    monkeypatch.setenv("APP_VERSION", "test")
    monkeypatch.setenv("TELEGRAM_ADAPTER_MODE", "compat_worker_bridge")

    try:
        bot_main.load_settings_from_env()
        raise AssertionError("legacy adapter mode must fail loudly")
    except ValueError as exc:
        assert "TELEGRAM_ADAPTER_MODE" in str(exc)
        assert "runtime_core_direct" in str(exc)

from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
APP_SRC = ROOT / "telegram-bot" / "app-integration" / "src"
BOT_MAIN_PATH = APP_SRC / "integrations" / "telegram" / "bot_main.py"
API_PATH = ROOT / "organizer-api" / "app.py"


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

    spec = importlib.util.spec_from_file_location("app_integration_bot_main_module_for_system_tests", BOT_MAIN_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)

    for key in list(sys.modules.keys()):
        if key == "src" or key.startswith("src.") or key == "requests":
            sys.modules.pop(key)
    sys.modules.update(preserved)
    return module


def _load_api_module():
    spec = importlib.util.spec_from_file_location("organizer_api_app_for_system_tests", API_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
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


class _UnusedBridge:
    def process_request(self, req: Any) -> Any:  # noqa: ARG002
        raise AssertionError("/system must not route through runtime bridge")


def test_active_google_health_contract_ok_shape(monkeypatch) -> None:
    api = _load_api_module()
    monkeypatch.setattr(
        api,
        "_google_calendar_write_health",
        lambda: {
            "status": "ok",
            "calendar_write": "ok",
            "services": {"calendar_write": "ok"},
            "probe_event_id": "probe-active-1",
        },
    )

    data = api.google_health()

    assert data == {
        "status": "ok",
        "calendar_write": "ok",
        "services": {"calendar_write": "ok"},
        "probe_event_id": "probe-active-1",
    }


def test_active_google_health_contract_marks_google_down_on_calendar_write_failure(monkeypatch) -> None:
    api = _load_api_module()
    monkeypatch.setattr(
        api,
        "_google_calendar_write_health",
        lambda: {
            "status": "down",
            "calendar_write": "down",
            "services": {"calendar_write": "down"},
            "error": "RuntimeError:write denied",
        },
    )

    data = api.google_health()

    assert data["status"] == "down"
    assert data["calendar_write"] == "down"
    assert data["services"]["calendar_write"] == "down"
    assert "error" in data


def test_active_adapter_mode_rejects_legacy_compat(monkeypatch) -> None:
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


def test_active_system_command_sends_human_readable_summary(monkeypatch, tmp_path) -> None:
    bot_main = _load_bot_main_module()
    transport = _FakeTransport()
    settings = bot_main.AdapterSettings(
        telegram_bot_token="token",
        telegram_adapter_mode="runtime_core_direct",
        worker_command_url="http://organizer-worker:8002/runtime/command",
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
    app = bot_main.AdapterApp(settings=settings, transport=transport, bridge=_UnusedBridge(), offset=None)

    def _fake_fetch(url: str, *, timeout_sec: float) -> dict[str, Any]:  # noqa: ARG001
        if url == "http://organizer-worker:8002/health":
            return {"ok": True}
        if url == "http://organizer-api:8000/health":
            return {"ok": True}
        if url == "http://organizer-api:8000/google/health":
            return {"status": "ok", "services": {"calendar_write": "ok"}}
        if url == "http://ml.local/health":
            return {"status": "degraded", "services": {"asr": "ok", "llm": "down"}}
        raise AssertionError(f"unexpected url: {url}")

    monkeypatch.setattr(bot_main, "_system_fetch_json", _fake_fetch)
    monkeypatch.setattr(
        bot_main,
        "_load_build_info",
        lambda path=bot_main._BUILD_INFO_PATH: {
            "service": "telegram-bot",
            "git_sha": "abc1234deadbeef",
            "build_timestamp_utc": "2026-05-22T16:06:08Z",
            "route_rules_version": "stage67-hardguard-v2",
            "handler_sha256": "9f12ab34cdef5678",
        },
    )
    update = {
        "update_id": 401,
        "message": {
            "message_id": 303,
            "chat": {"id": 202},
            "from": {"id": 404},
            "text": "/system",
        },
    }

    next_offset = bot_main.process_single_update(app, update)

    assert next_offset == 402
    assert transport.sent_messages
    text = transport.sent_messages[0]["text"]
    assert "Состояние системы" in text
    assert "Bot/runtime: OK" in text
    assert "Adapter mode: runtime_core_direct" in text
    assert "Build:" in text
    assert "route_rules_version: stage67-hardguard-v2" in text
    assert "git_sha: abc1234deadb" in text
    assert "handler_sha256: 9f12ab34cdef" in text
    assert "build_timestamp_utc: 2026-05-22T16:06:08Z" in text
    assert "Worker: OK" in text
    assert "API: OK" in text
    assert "Google: OK" in text
    assert "Voice/ASR: OK" in text


def test_active_system_command_marks_google_down_when_google_health_degraded(monkeypatch, tmp_path) -> None:
    bot_main = _load_bot_main_module()
    transport = _FakeTransport()
    settings = bot_main.AdapterSettings(
        telegram_bot_token="token",
        telegram_adapter_mode="runtime_core_direct",
        worker_command_url="http://organizer-worker:8002/runtime/command",
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
    app = bot_main.AdapterApp(settings=settings, transport=transport, bridge=_UnusedBridge(), offset=None)

    def _fake_fetch(url: str, *, timeout_sec: float) -> dict[str, Any]:  # noqa: ARG001
        if url.endswith("/google/health"):
            return {"status": "down", "services": {"calendar_write": "down"}, "error": "write denied"}
        if url.endswith("/health"):
            return {"ok": True}
        raise AssertionError(f"unexpected url: {url}")

    monkeypatch.setattr(bot_main, "_system_fetch_json", _fake_fetch)
    update = {
        "update_id": 402,
        "message": {
            "message_id": 304,
            "chat": {"id": 203},
            "from": {"id": 405},
            "text": "/system",
        },
    }

    bot_main.process_single_update(app, update)

    assert transport.sent_messages
    assert "Google: DOWN" in transport.sent_messages[0]["text"]


def test_active_system_command_marks_worker_and_api_down_when_unavailable(monkeypatch, tmp_path) -> None:
    bot_main = _load_bot_main_module()
    transport = _FakeTransport()
    settings = bot_main.AdapterSettings(
        telegram_bot_token="token",
        telegram_adapter_mode="runtime_core_direct",
        worker_command_url="http://organizer-worker:8002/runtime/command",
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
    app = bot_main.AdapterApp(settings=settings, transport=transport, bridge=_UnusedBridge(), offset=None)

    def _fake_fetch(url: str, *, timeout_sec: float) -> dict[str, Any]:  # noqa: ARG001
        if url == "http://organizer-api:8000/google/health":
            return {"status": "ok", "services": {"calendar_write": "ok"}}
        if url == "http://ml.local/health":
            return {"status": "down", "services": {"asr": "down"}}
        raise RuntimeError(f"unavailable url: {url}")

    monkeypatch.setattr(bot_main, "_system_fetch_json", _fake_fetch)
    update = {
        "update_id": 403,
        "message": {
            "message_id": 305,
            "chat": {"id": 204},
            "from": {"id": 406},
            "text": "/system",
        },
    }

    bot_main.process_single_update(app, update)

    assert transport.sent_messages
    text = transport.sent_messages[0]["text"]
    assert "Worker: DOWN" in text
    assert "API: DOWN" in text
    assert "Voice/ASR: DOWN" in text


def test_sheets_review_command_returns_no_changes_summary(monkeypatch, tmp_path) -> None:
    bot_main = _load_bot_main_module()
    transport = _FakeTransport()
    settings = bot_main.AdapterSettings(
        telegram_bot_token="token",
        telegram_adapter_mode="runtime_core_direct",
        worker_command_url="http://organizer-worker:8002/runtime/command",
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
    app = bot_main.AdapterApp(settings=settings, transport=transport, bridge=_UnusedBridge(), offset=None)

    def _fake_fetch(url: str, *, timeout_sec: float) -> dict[str, Any]:  # noqa: ARG001
        assert url == "http://google-sync:8010/review?limit=5"
        return {
            "ok": True,
            "summary": {
                "total_changes": 0,
                "proposed": 0,
                "confirm_required": 0,
                "conflict": 0,
                "invalid": 0,
            },
            "changes": [],
        }

    monkeypatch.setattr(bot_main, "_system_fetch_json", _fake_fetch)
    update = {
        "update_id": 404,
        "message": {
            "message_id": 306,
            "chat": {"id": 205},
            "from": {"id": 407},
            "text": "/sheets_review",
        },
    }

    next_offset = bot_main.process_single_update(app, update)

    assert next_offset == 405
    assert transport.sent_messages
    text = transport.sent_messages[0]["text"]
    assert "Проверка Google Sheets" in text
    assert "Всего изменений: 0" in text
    assert "Изменений не найдено." in text


def test_sheets_review_command_returns_pending_changes_summary_without_apply(monkeypatch, tmp_path) -> None:
    bot_main = _load_bot_main_module()
    transport = _FakeTransport()
    settings = bot_main.AdapterSettings(
        telegram_bot_token="token",
        telegram_adapter_mode="runtime_core_direct",
        worker_command_url="http://organizer-worker:8002/runtime/command",
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
    app = bot_main.AdapterApp(settings=settings, transport=transport, bridge=_UnusedBridge(), offset=None)
    fetched: list[str] = []

    def _fake_fetch(url: str, *, timeout_sec: float) -> dict[str, Any]:  # noqa: ARG001
        fetched.append(url)
        assert "apply" not in url
        return {
            "ok": True,
            "summary": {
                "total_changes": 2,
                "proposed": 1,
                "confirm_required": 1,
                "conflict": 0,
                "invalid": 0,
            },
            "changes": [
                {
                    "change_id": "chg-00001",
                    "sheet": "Задачи",
                    "entity_type": "task",
                    "entity_id": "17",
                    "field": "Комментарий",
                    "status": "proposed",
                },
                {
                    "change_id": "chg-00002",
                    "sheet": "Календарь",
                    "entity_type": "timeblock",
                    "entity_id": "3",
                    "field": "Начало",
                    "status": "confirm_required",
                },
            ],
        }

    monkeypatch.setattr(bot_main, "_system_fetch_json", _fake_fetch)
    update = {
        "update_id": 405,
        "message": {
            "message_id": 307,
            "chat": {"id": 206},
            "from": {"id": 408},
            "text": "/sheets_review",
        },
    }

    bot_main.process_single_update(app, update)

    assert fetched == ["http://google-sync:8010/review?limit=5"]
    text = transport.sent_messages[0]["text"]
    assert "Всего изменений: 2" in text
    assert "Предложено: 1" in text
    assert "Нужно подтверждение: 1" in text
    assert "chg-00001 | Задачи | task:17 | Комментарий | proposed" in text
    assert ".\\scripts\\sheets_review_apply.ps1 -Mode apply -ChangeId chg-00001" in text
    assert "Apply из Telegram отключен." in text


def test_sheets_review_command_shows_conflicts_clearly(monkeypatch, tmp_path) -> None:
    bot_main = _load_bot_main_module()
    transport = _FakeTransport()
    settings = bot_main.AdapterSettings(
        telegram_bot_token="token",
        telegram_adapter_mode="runtime_core_direct",
        worker_command_url="http://organizer-worker:8002/runtime/command",
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
    app = bot_main.AdapterApp(settings=settings, transport=transport, bridge=_UnusedBridge(), offset=None)

    monkeypatch.setattr(
        bot_main,
        "_system_fetch_json",
        lambda url, *, timeout_sec: {
            "ok": True,
            "summary": {
                "total_changes": 1,
                "proposed": 0,
                "confirm_required": 0,
                "conflict": 1,
                "invalid": 0,
            },
            "changes": [
                {
                    "change_id": "chg-00003",
                    "sheet": "Задачи",
                    "entity_type": "task",
                    "entity_id": "21",
                    "field": "Задача",
                    "status": "conflict",
                }
            ],
        },
    )
    update = {
        "update_id": 406,
        "message": {
            "message_id": 308,
            "chat": {"id": 207},
            "from": {"id": 409},
            "text": "/sheets_review",
        },
    }

    bot_main.process_single_update(app, update)

    text = transport.sent_messages[0]["text"]
    assert "Конфликт: 1" in text
    assert "chg-00003 | Задачи | task:21 | Задача | conflict" in text

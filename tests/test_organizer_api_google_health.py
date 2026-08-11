from __future__ import annotations

import importlib.util
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
API_PATH = ROOT / "organizer-api" / "app.py"


def _load_api_module():
    spec = importlib.util.spec_from_file_location("organizer_api_app_for_google_health", API_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_google_health_reports_ok_on_successful_calendar_write(monkeypatch) -> None:
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

    assert data["status"] == "ok"
    assert data["calendar_write"] == "ok"
    assert data["services"]["calendar_write"] == "ok"
    assert data["probe_event_id"] == "probe-active-1"


def test_google_health_reports_down_on_write_failure(monkeypatch) -> None:
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

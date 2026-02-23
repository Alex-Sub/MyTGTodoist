import importlib
import os


def test_import_yaml_available() -> None:
    mod = importlib.import_module("yaml")
    assert mod is not None


def test_import_google_calendar_deps_when_calendar_mode_full(monkeypatch) -> None:
    monkeypatch.setenv("CALENDAR_SYNC_MODE", "full")
    if (os.getenv("CALENDAR_SYNC_MODE") or "").strip().lower() == "full":
        importlib.import_module("googleapiclient")
        importlib.import_module("google.oauth2")

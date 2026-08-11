from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[2]
SCHEDULER_PATH = ROOT / "src" / "google" / "scheduler.py"


class _LoggerStub:
    def __init__(self) -> None:
        self.messages: list[tuple[str, str]] = []

    def info(self, message: str, *args) -> None:
        self.messages.append(("info", message.format(*args)))

    def warning(self, message: str, *args) -> None:
        self.messages.append(("warning", message.format(*args)))

    def error(self, message: str, *args) -> None:
        self.messages.append(("error", message.format(*args)))


def _stub_module(name: str, **attrs) -> types.ModuleType:
    mod = types.ModuleType(name)
    for key, value in attrs.items():
        setattr(mod, key, value)
    return mod


def _load_scheduler_with_stubs(monkeypatch: pytest.MonkeyPatch):
    logger = _LoggerStub()
    settings = types.SimpleNamespace(
        google_sheets_sync_enabled=False,
        sync_poll_idle_sec=21600,
        google_sheets_spreadsheet_id="",
        google_sheets_sync_dry_run=False,
        google_vitrina_sheet_name="VITRINA_TASKS",
        google_ops_log_sheet_name="OPS_LOG",
        google_ops_retention_days=14,
        sync_timezone="Europe/Moscow",
        timezone="Europe/Moscow",
        sync_window_days=7,
        google_calendar_id_default="primary",
    )

    stubbed = {
        "httpx": _stub_module("httpx", HTTPStatusError=type("HTTPStatusError", (Exception,), {})),
        "loguru": _stub_module("loguru", logger=logger),
        "sqlalchemy": _stub_module("sqlalchemy", func=object(), select=lambda *a, **k: None, text=lambda *a, **k: None),
        "zoneinfo": _stub_module("zoneinfo", ZoneInfo=lambda key: key),
        "src": _stub_module("src"),
        "src.config": _stub_module("src.config", settings=settings),
        "src.db": _stub_module("src.db"),
        "src.db.models": _stub_module(
            "src.db.models",
            CalendarSyncState=type("CalendarSyncState", (), {}),
            Conflict=type("Conflict", (), {}),
            Item=type("Item", (), {}),
            SyncOutbox=type("SyncOutbox", (), {}),
        ),
        "src.db.session": _stub_module("src.db.session", get_session=lambda: None),
        "src.exports": _stub_module("src.exports"),
        "src.exports.vitrina_tasks": _stub_module("src.exports.vitrina_tasks", build_vitrina=lambda session: ([], [])),
        "src.google": _stub_module("src.google"),
        "src.google.calendar_client": _stub_module("src.google.calendar_client", CalendarClient=type("CalendarClient", (), {})),
        "src.google.google_sync": _stub_module(
            "src.google.google_sync",
            pull_google_tasks_with_conflicts=lambda session: {},
            sync_task_completed=lambda *a, **k: None,
            sync_task_created=lambda *a, **k: None,
            sync_task_updated=lambda *a, **k: None,
        ),
        "src.google.sheets_client": _stub_module("src.google.sheets_client", SheetsClient=type("SheetsClient", (), {})),
        "src.google.sheet_pull": _stub_module("src.google.sheet_pull", pull_google_sheet_apply_rows=lambda *a, **k: ({}, [])),
        "src.google.sync_in": _stub_module("src.google.sync_in", sync_in_calendar_window=lambda *a, **k: {}),
        "src.google.sync_out": _stub_module("src.google.sync_out", sync_out_meeting=lambda *a, **k: None),
        "src.google.runtime_sheets_sync": _stub_module(
            "src.google.runtime_sheets_sync",
            run_runtime_sheets_sync_scheduler=lambda: None,
        ),
    }
    for name, mod in stubbed.items():
        monkeypatch.setitem(sys.modules, name, mod)

    module_name = "tests_google_scheduler_guardrails_runtime"
    sys.modules.pop(module_name, None)
    spec = importlib.util.spec_from_file_location(module_name, SCHEDULER_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module, logger, settings


def test_runtime_sheets_enabled_uses_canonical_runtime_scheduler(monkeypatch: pytest.MonkeyPatch) -> None:
    module, logger, settings = _load_scheduler_with_stubs(monkeypatch)
    settings.google_sheets_sync_enabled = False
    monkeypatch.setenv("GOOGLE_SHEETS_SYNC_ENABLED", "1")
    monkeypatch.setenv("GOOGLE_SYNC_MODE", "full_bidir")

    called = {"runtime": 0, "items_check": 0, "legacy": 0}

    async def _runtime_scheduler():
        called["runtime"] += 1

    async def _legacy_scheduler():
        called["legacy"] += 1

    def _items_check():
        called["items_check"] += 1

    monkeypatch.setattr(module, "run_runtime_sheets_sync_scheduler", _runtime_scheduler)
    monkeypatch.setattr(module, "run_full_bidir_scheduler", _legacy_scheduler)
    monkeypatch.setattr(module, "_ensure_expected_db_has_items", _items_check)

    module.main()

    assert called == {"runtime": 1, "items_check": 0, "legacy": 0}
    assert any("canonical=runtime_sheets" in text for level, text in logger.messages if level == "info")
    assert any("legacy items-based sheets mode skipped" in text for level, text in logger.messages if level == "warning")


def test_runtime_sheets_mode_skips_legacy_items_sanity_check(monkeypatch: pytest.MonkeyPatch) -> None:
    module, logger, settings = _load_scheduler_with_stubs(monkeypatch)
    settings.google_sheets_sync_enabled = False
    monkeypatch.setenv("GOOGLE_SYNC_MODE", "runtime_sheets")
    monkeypatch.delenv("GOOGLE_SHEETS_SYNC_ENABLED", raising=False)

    called = {"runtime": 0}

    async def _runtime_scheduler():
        called["runtime"] += 1

    def _items_check():
        raise AssertionError("legacy items sanity check must not run in runtime sheets mode")

    monkeypatch.setattr(module, "run_runtime_sheets_sync_scheduler", _runtime_scheduler)
    monkeypatch.setattr(module, "_ensure_expected_db_has_items", _items_check)

    module.main()

    assert called["runtime"] == 1
    assert any("canonical=runtime_sheets" in text for level, text in logger.messages if level == "info")


def test_legacy_full_bidir_is_blocked_without_explicit_opt_in(monkeypatch: pytest.MonkeyPatch) -> None:
    module, logger, settings = _load_scheduler_with_stubs(monkeypatch)
    settings.google_sheets_sync_enabled = False
    monkeypatch.setenv("GOOGLE_SYNC_MODE", "full_bidir")
    monkeypatch.delenv("GOOGLE_SHEETS_SYNC_ENABLED", raising=False)
    monkeypatch.delenv("GOOGLE_SYNC_ALLOW_LEGACY_ITEMS_MODE", raising=False)

    def _items_check():
        raise AssertionError("legacy items sanity check must not run when legacy mode is blocked")

    async def _legacy_scheduler():
        raise AssertionError("legacy scheduler must not run without explicit opt-in")

    monkeypatch.setattr(module, "_ensure_expected_db_has_items", _items_check)
    monkeypatch.setattr(module, "run_full_bidir_scheduler", _legacy_scheduler)

    with pytest.raises(SystemExit):
        module.main()

    assert any("legacy items-based sheets mode disabled" in text for level, text in logger.messages if level == "warning")


def test_legacy_full_bidir_runs_only_with_explicit_opt_in(monkeypatch: pytest.MonkeyPatch) -> None:
    module, logger, settings = _load_scheduler_with_stubs(monkeypatch)
    settings.google_sheets_sync_enabled = False
    monkeypatch.setenv("GOOGLE_SYNC_MODE", "full_bidir")
    monkeypatch.setenv("GOOGLE_SYNC_ALLOW_LEGACY_ITEMS_MODE", "1")
    monkeypatch.delenv("GOOGLE_SHEETS_SYNC_ENABLED", raising=False)

    called = {"items_check": 0, "legacy": 0, "validate": 0}

    def _items_check():
        called["items_check"] += 1

    def _validate():
        called["validate"] += 1

    async def _legacy_scheduler():
        called["legacy"] += 1

    monkeypatch.setattr(module, "_ensure_expected_db_has_items", _items_check)
    monkeypatch.setattr(module, "_validate_calendar_pull_mode", _validate)
    monkeypatch.setattr(module, "run_full_bidir_scheduler", _legacy_scheduler)

    module.main()

    assert called == {"items_check": 1, "legacy": 1, "validate": 1}
    assert any("status=non_canonical" in text for level, text in logger.messages if level == "warning")

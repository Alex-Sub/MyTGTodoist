from __future__ import annotations

import importlib.util
import sqlite3
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
API_PATH = ROOT / "organizer-api" / "app.py"


def _load_api_module():
    spec = importlib.util.spec_from_file_location("organizer_api_app_for_sync_conflicts", API_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_sync_conflicts_returns_open_items_only(tmp_path: Path, monkeypatch) -> None:
    api = _load_api_module()
    db_path = tmp_path / "organizer.db"
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            CREATE TABLE sync_conflicts (
                id TEXT PRIMARY KEY,
                item_id TEXT NOT NULL,
                db_payload_json TEXT NULL,
                sheet_payload_json TEXT NULL,
                calendar_payload_json TEXT NULL,
                status TEXT NOT NULL DEFAULT 'open',
                resolution TEXT NULL,
                created_at TEXT NULL,
                resolved_at TEXT NULL
            )
            """
        )
        conn.execute(
            """
            INSERT INTO sync_conflicts (id, item_id, db_payload_json, status, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            ("sc-1", "item-1", '{"title":"A"}', "open", "2026-05-10T10:00:00+00:00"),
        )
        conn.execute(
            """
            INSERT INTO sync_conflicts (id, item_id, sheet_payload_json, status, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            ("sc-2", "item-2", '{"title":"B"}', "resolved", "2026-05-10T11:00:00+00:00"),
        )
        conn.commit()
    finally:
        conn.close()

    api.DB_PATH = str(db_path)

    def _unexpected_resolve(*args, **kwargs):  # noqa: ANN002, ANN003
        raise AssertionError("read-only sync conflicts endpoint must not call resolve/action")

    monkeypatch.setattr(api, "_runtime_sync_conflict_apply_action", _unexpected_resolve, raising=False)
    body = api.get_sync_conflicts(limit=20)

    assert body["ok"] is True
    assert body["count"] == 1
    assert len(body["items"]) == 1
    assert str(body["items"][0]["id"]) == "sc-1"
    assert str(body["items"][0]["item_id"]) == "item-1"


def test_sync_conflicts_returns_empty_list_when_table_missing(tmp_path: Path) -> None:
    api = _load_api_module()
    db_path = tmp_path / "organizer.db"
    conn = sqlite3.connect(db_path)
    conn.close()

    api.DB_PATH = str(db_path)
    body = api.get_sync_conflicts(limit=20)

    assert body == {"ok": True, "items": [], "count": 0}


def test_sync_conflicts_returns_empty_list_when_no_open_rows(tmp_path: Path) -> None:
    api = _load_api_module()
    db_path = tmp_path / "organizer.db"
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            CREATE TABLE sync_conflicts (
                id TEXT PRIMARY KEY,
                item_id TEXT NOT NULL,
                db_payload_json TEXT NULL,
                sheet_payload_json TEXT NULL,
                calendar_payload_json TEXT NULL,
                status TEXT NOT NULL DEFAULT 'open',
                resolution TEXT NULL,
                created_at TEXT NULL,
                resolved_at TEXT NULL
            )
            """
        )
        conn.execute(
            """
            INSERT INTO sync_conflicts (id, item_id, status, created_at)
            VALUES (?, ?, ?, ?)
            """,
            ("sc-closed", "item-closed", "resolved", "2026-05-10T12:00:00+00:00"),
        )
        conn.commit()
    finally:
        conn.close()

    api.DB_PATH = str(db_path)
    body = api.get_sync_conflicts(limit=20)

    assert body == {"ok": True, "items": [], "count": 0}

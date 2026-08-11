from __future__ import annotations

import importlib.util
import sqlite3
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
API_PATH = ROOT / "organizer-api" / "app.py"


def _load_api_module():
    spec = importlib.util.spec_from_file_location("organizer_api_app_for_comment_projection", API_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_p7_day_projection_includes_timeblock_comment(tmp_path: Path) -> None:
    api = _load_api_module()
    db_path = tmp_path / "organizer.db"
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            CREATE TABLE time_blocks (
                id INTEGER PRIMARY KEY,
                task_id INTEGER NOT NULL,
                start_at TEXT NOT NULL,
                end_at TEXT NOT NULL,
                created_at TEXT NOT NULL,
                comment TEXT NULL
            )
            """
        )
        conn.execute(
            """
            INSERT INTO time_blocks (task_id, start_at, end_at, created_at, comment)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                42,
                "2026-04-13T10:00:00+00:00",
                "2026-04-13T11:00:00+00:00",
                "2026-04-13T09:00:00+00:00",
                "глубокая работа",
            ),
        )
        conn.commit()
    finally:
        conn.close()

    api.DB_PATH = str(db_path)
    api.P7_MODE = "on"
    api.LOCAL_TZ_OFFSET_MIN = 0

    body = api.get_p7_day(date="2026-04-13")

    assert body["date"] == "2026-04-13"
    assert body["blocks"]
    assert str(body["blocks"][0].get("comment") or "") == "глубокая работа"

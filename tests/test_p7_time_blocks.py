import importlib
import os
import sqlite3
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS_DIR = ROOT / "migrations"
API_PATH = ROOT / "organizer-api"

sys.path.append(str(API_PATH))


def _apply_runtime_migrations(conn: sqlite3.Connection) -> None:
    migs = sorted(p for p in MIGRATIONS_DIR.iterdir() if p.name.endswith(".sql"))
    for path in migs:
        try:
            num = int(path.name.split("_", 1)[0])
        except Exception:
            continue
        if num < 10 or num > 26:
            continue
        conn.executescript(path.read_text(encoding="utf-8"))
    conn.commit()


def _load_api(db_path: Path):
    os.environ["DB_PATH"] = str(db_path)
    os.environ["P7_MODE"] = "on"
    os.environ["LOCAL_TZ_OFFSET_MIN"] = "0"
    app_mod = importlib.import_module("app")
    app_mod.DB_PATH = str(db_path)
    app_mod.P7_MODE = "on"
    app_mod.LOCAL_TZ_OFFSET_MIN = 0
    return importlib.reload(app_mod)


def test_p7_day_view(tmp_path: Path) -> None:
    db_path = tmp_path / "p7_api.db"
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("PRAGMA foreign_keys=ON")
        _apply_runtime_migrations(conn)
        conn.execute(
            """
            INSERT INTO tasks (id, title, status, created_at, updated_at)
            VALUES (1, 'Task A', 'NEW', '2026-02-07T08:00:00+00:00', '2026-02-07T08:00:00+00:00')
            """
        )
        conn.execute(
            """
            INSERT INTO time_blocks (id, task_id, start_at, end_at, created_at)
            VALUES (10, 1, '2026-02-07T09:00:00+00:00', '2026-02-07T10:00:00+00:00',
                    '2026-02-07T08:30:00+00:00')
            """
        )
        conn.execute(
            """
            INSERT INTO time_blocks (id, task_id, start_at, end_at, created_at)
            VALUES (11, 1, '2026-02-08T09:00:00+00:00', '2026-02-08T10:00:00+00:00',
                    '2026-02-08T08:30:00+00:00')
            """
        )
        conn.commit()

    app_mod = _load_api(db_path)
    resp = app_mod.get_p7_day(date="2026-02-07")
    assert resp["date"] == "2026-02-07"
    assert resp["timezone"] == "UTC+00:00"
    assert len(resp["blocks"]) == 1
    assert resp["blocks"][0]["id"] == 10


def test_p7_requires_flag(tmp_path: Path) -> None:
    db_path = tmp_path / "p7_disabled.db"
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("PRAGMA foreign_keys=ON")
        _apply_runtime_migrations(conn)

    os.environ["DB_PATH"] = str(db_path)
    os.environ["P7_MODE"] = "off"
    app_mod = importlib.import_module("app")
    app_mod.DB_PATH = str(db_path)
    app_mod.P7_MODE = "off"
    app_mod = importlib.reload(app_mod)
    with pytest.raises(Exception):
        _ = app_mod.get_p7_day(date="2026-02-07")

import os
import sqlite3
import importlib
import sys
from pathlib import Path


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
        if num < 10 or num > 23:
            continue
        sql = path.read_text(encoding="utf-8")
        conn.executescript(sql)
    conn.commit()


def test_projects_inbox_filter(tmp_path: Path) -> None:
    db_path = tmp_path / "api.db"
    os.environ["DB_PATH"] = str(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("PRAGMA foreign_keys=ON")
        _apply_runtime_migrations(conn)
        conn.execute(
            "INSERT INTO projects (id, title, status, created_at, updated_at) VALUES (1, 'A', 'ACTIVE', 't', 't')"
        )
        conn.execute(
            "INSERT INTO projects (id, title, status, created_at, updated_at) VALUES (2, 'B', 'ACTIVE', 't', 't')"
        )
        conn.execute(
            "INSERT INTO projects (id, title, status, created_at, updated_at) VALUES (3, 'C', 'ACTIVE', 't', 't')"
        )
        conn.execute(
            "INSERT INTO tasks (id, title, status, state, planned_at, created_at, updated_at, parent_type, parent_id) "
            "VALUES (10, 'done', 'DONE', 'DONE', NULL, 't', 't', 'project', 2)"
        )
        conn.execute(
            "INSERT INTO tasks (id, title, status, state, planned_at, created_at, updated_at, parent_type, parent_id) "
            "VALUES (11, 'open', 'NEW', 'NEW', NULL, 't', 't', 'project', 3)"
        )
        conn.commit()

    app_mod = importlib.import_module("app")
    app_mod.DB_PATH = str(db_path)
    app_mod = importlib.reload(app_mod)
    rows = app_mod.list_projects(inbox=1)
    ids = [r.get("id") for r in rows]
    assert 1 in ids
    assert 2 in ids
    assert 3 not in ids


def test_projects_inbox_open_task_states(tmp_path: Path) -> None:
    db_path = tmp_path / "api_states.db"
    os.environ["DB_PATH"] = str(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("PRAGMA foreign_keys=ON")
        _apply_runtime_migrations(conn)
        conn.executemany(
            "INSERT INTO projects (id, title, status, created_at, updated_at) VALUES (?, ?, 'ACTIVE', 't', 't')",
            [
                (1, "P1"),
                (2, "P2"),
                (3, "P3"),
                (4, "P4"),
                (5, "P5"),
            ],
        )
        conn.executemany(
            "INSERT INTO tasks (id, title, status, state, planned_at, created_at, updated_at, parent_type, parent_id) "
            "VALUES (?, ?, 'NEW', ?, NULL, 't', 't', 'project', ?)",
            [
                (10, "t10", "PLANNED", 1),
                (11, "t11", "SCHEDULED", 2),
                (12, "t12", "IN_PROGRESS", 3),
                (13, "t13", "DONE", 4),
            ],
        )
        conn.commit()

    app_mod = importlib.import_module("app")
    app_mod.DB_PATH = str(db_path)
    app_mod = importlib.reload(app_mod)
    rows = app_mod.list_projects(inbox=1)
    ids = {r.get("id") for r in rows}

    assert 1 not in ids
    assert 2 not in ids
    assert 3 not in ids
    assert 4 in ids
    assert 5 in ids

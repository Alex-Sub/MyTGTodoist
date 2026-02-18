import sqlite3
from datetime import date
from pathlib import Path

import pytest

from organizer_worker import db


ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS_DIR = ROOT / "migrations"


def _apply_runtime_migrations(conn: sqlite3.Connection) -> None:
    migs = sorted(p for p in MIGRATIONS_DIR.iterdir() if p.name.endswith(".sql"))
    for path in migs:
        try:
            num = int(path.name.split("_", 1)[0])
        except Exception:
            continue
        if num < 1 or num > 40:
            continue
        conn.executescript(path.read_text(encoding="utf-8"))
    conn.commit()


@pytest.fixture()
def runtime_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    db_path = tmp_path / "runtime_strategic.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    db.DB_PATH = str(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("PRAGMA foreign_keys=ON")
        _apply_runtime_migrations(conn)
    return db_path


def test_create_goal_reschedule_overdue_and_close_summary(runtime_db: Path) -> None:
    today = date(2026, 2, 20).isoformat()
    tomorrow = date(2026, 2, 21).isoformat()
    with db.connect() as conn:
        cycle_id = db.create_cycle(conn, name="Q1", start_date="2026-01-01", end_date="2026-03-31")
        goal_id = db.create_goal(
            conn,
            cycle_id=cycle_id,
            title="Launch",
            success_criteria="Publish v1",
            planned_end_date="2026-02-10",
        )
        event_id = db.reschedule_goal(conn, goal_id=goal_id, new_end_date="2026-02-15")
        assert int(event_id) > 0

        digest = db.compute_daily_digest(conn, today=today, tomorrow=tomorrow, user_id="u1")
        assert digest["goals_overdue"] >= 1

        summary = db.close_cycle(conn, cycle_id=cycle_id)
        assert summary["goals_total"] == 1
        assert summary["goals_overdue"] >= 1


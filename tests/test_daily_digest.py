import sqlite3
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
    db_path = tmp_path / "runtime_digest.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    db.DB_PATH = str(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("PRAGMA foreign_keys=ON")
        _apply_runtime_migrations(conn)
    return db_path


def test_compute_daily_digest_counters(runtime_db: Path) -> None:
    today = "2026-02-20"
    tomorrow = "2026-02-21"
    with db.connect() as conn:
        cycle_id = db.create_cycle(conn, name="Sprint", start_date="2026-02-01", end_date="2026-02-28")
        g1 = db.create_goal(
            conn,
            cycle_id=cycle_id,
            title="Goal overdue",
            success_criteria="Done",
            planned_end_date="2026-02-10",
        )
        g2 = db.create_goal(
            conn,
            cycle_id=cycle_id,
            title="Goal due soon",
            success_criteria="Done",
            planned_end_date=today,
        )

        t1 = db.create_task(conn, title="Task today", planned_at=f"{today}T10:00:00Z")
        t2 = db.create_task(conn, title="Task tomorrow", planned_at=f"{tomorrow}T09:00:00Z")
        db.link_task_to_goal(conn, task_id=t1, goal_id=g1)
        db.link_task_to_goal(conn, task_id=t2, goal_id=g2)

        # Make goal g1 high-risk by repeated rescheduling.
        db.reschedule_goal(conn, goal_id=g1, new_end_date="2026-02-11")
        db.reschedule_goal(conn, goal_id=g1, new_end_date="2026-02-12")
        db.reschedule_goal(conn, goal_id=g1, new_end_date="2026-02-13")

        # Keep g2 not-at-risk by adding time block in next 2 days.
        db.create_time_block(
            conn,
            task_id=t2,
            start_at="2026-02-21T12:00:00Z",
            end_at="2026-02-21T13:00:00Z",
        )

        digest = db.compute_daily_digest(conn, today=today, tomorrow=tomorrow, user_id="u1")

    assert digest["goals_active"] >= 2
    assert digest["goals_overdue"] >= 1
    assert digest["goals_due_soon"] >= 1
    assert digest["goals_at_risk"] >= 1
    assert digest["tasks_today"] >= 1
    assert digest["tasks_tomorrow"] >= 1
    assert digest["tasks_active_total"] >= 2


import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest

from organizer_worker import db, handlers


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


def _parse_iso(value: str) -> datetime:
    v = value.strip()
    if v.endswith("Z"):
        v = v[:-1] + "+00:00"
    dt = datetime.fromisoformat(v)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


@pytest.fixture()
def runtime_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    db_path = tmp_path / "runtime_nudges.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    db.DB_PATH = str(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("PRAGMA foreign_keys=ON")
        _apply_runtime_migrations(conn)
        cycle_id = db.create_cycle(conn, name="Q1", start_date="2026-02-01", end_date="2026-03-31")
        db.create_goal(
            conn,
            cycle_id=cycle_id,
            title="Просроченная цель",
            success_criteria="done",
            planned_end_date="2026-02-01",
        )
        conn.commit()
    return db_path


def test_nudges_list_and_ack(runtime_db: Path) -> None:
    res_list = handlers.dispatch_intent({"intent": "nudge.list", "entities": {"user_id": "u1", "today": "2026-02-20"}})
    assert res_list["ok"] is True
    nudges = res_list["debug"]["nudges"]
    assert isinstance(nudges, list)
    assert any(n.get("nudge_type") == "goals.overdue" for n in nudges)

    overdue = [n for n in nudges if n.get("nudge_type") == "goals.overdue"][0]
    res_ack = handlers.dispatch_intent(
        {
            "intent": "nudge.ack",
            "entities": {
                "user_id": "u1",
                "nudge_type": overdue["nudge_type"],
                "entity_type": overdue["entity_type"],
                "entity_id": overdue["entity_id"],
            },
        }
    )
    assert res_ack["ok"] is True

    with sqlite3.connect(str(runtime_db)) as conn:
        row = conn.execute(
            "SELECT nudge_type, entity_type, entity_id FROM nudge_ack WHERE user_id = ?",
            ("u1",),
        ).fetchone()
    assert row is not None
    assert row[0] == overdue["nudge_type"]

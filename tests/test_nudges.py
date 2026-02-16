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
        if num < 1 or num > 26:
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
        conn.execute(
            "INSERT INTO user_nudges (user_id, nudge_key, next_at, last_shown_at, created_at, updated_at) "
            "VALUES ('u1', 'stale', '2000-01-01T00:00:00Z', NULL, 't', 't')"
        )
        conn.commit()
    return db_path


def test_nudges_list_and_ack(runtime_db: Path) -> None:
    res_list = handlers.dispatch_intent({"intent": "nudge.list", "entities": {"user_id": "u1"}})
    assert res_list["ok"] is True
    nudges = res_list["debug"]["nudges"]
    assert isinstance(nudges, list)
    assert [n.get("nudge_key") for n in nudges] == ["stale"]

    before = nudges[0]["next_at"]
    res_ack = handlers.dispatch_intent({"intent": "nudge.ack", "entities": {"user_id": "u1", "nudge_id": "stale"}})
    assert res_ack["ok"] is True

    with sqlite3.connect(str(runtime_db)) as conn:
        row = conn.execute("SELECT next_at, last_shown_at FROM user_nudges WHERE user_id = ? AND nudge_key = ?", ("u1", "stale")).fetchone()
    assert row is not None
    assert row[1] is not None
    assert _parse_iso(str(row[0])) > _parse_iso(before)


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
        if num < 10 or num > 26:
            continue
        conn.executescript(path.read_text(encoding="utf-8"))
    conn.commit()


def _load_api(db_path: Path):
    os.environ["DB_PATH"] = str(db_path)
    app_mod = importlib.import_module("app")
    app_mod.DB_PATH = str(db_path)
    return importlib.reload(app_mod)


def test_state_overload_detection(tmp_path: Path) -> None:
    db_path = tmp_path / "state_overload.db"
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("PRAGMA foreign_keys=ON")
        _apply_runtime_migrations(conn)
        conn.execute(
            "INSERT INTO user_settings (user_id, signals_enabled, overload_enabled, drift_enabled, created_at, updated_at) "
            "VALUES ('1', 1, 1, 1, 't', 't')"
        )
        conn.execute(
            "INSERT INTO tasks (id, title, status, state, planned_at, created_at, updated_at) "
            "VALUES (1, 'unplanned', 'NEW', 'NEW', NULL, 't', 't')"
        )
        conn.execute(
            "INSERT INTO tasks (id, title, status, state, planned_at, created_at, updated_at) "
            "VALUES (2, 'planned', 'NEW', 'PLANNED', '2026-02-10T10:00:00+00:00', 't', 't')"
        )
        conn.commit()

    app_mod = _load_api(db_path)
    payload = app_mod.get_p2_state(user_id="1")
    assert isinstance(payload.get("overload"), dict)
    assert payload["overload"].get("active") is True
    assert payload.get("drift") is None


def test_state_drift_priority(tmp_path: Path) -> None:
    db_path = tmp_path / "state_drift.db"
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("PRAGMA foreign_keys=ON")
        _apply_runtime_migrations(conn)
        conn.execute(
            "INSERT INTO user_settings (user_id, signals_enabled, overload_enabled, drift_enabled, created_at, updated_at) "
            "VALUES ('1', 1, 1, 1, 't', 't')"
        )
        conn.execute(
            "INSERT INTO directions (id, title, status, created_at, updated_at) "
            "VALUES (1, 'Dir', 'ACTIVE', 't', 't')"
        )
        conn.execute(
            "INSERT INTO tasks (id, title, status, state, planned_at, created_at, updated_at) "
            "VALUES (1, 'unplanned', 'NEW', 'NEW', NULL, 't', 't')"
        )
        conn.execute(
            "INSERT INTO tasks (id, title, status, state, planned_at, created_at, updated_at) "
            "VALUES (2, 'planned', 'NEW', 'PLANNED', '2026-02-10T10:00:00+00:00', 't', 't')"
        )
        conn.commit()

    app_mod = _load_api(db_path)
    payload = app_mod.get_p2_state(user_id="1")
    assert isinstance(payload.get("overload"), dict)
    assert payload["overload"].get("active") is True
    assert payload.get("drift") is None


def test_state_drift_only(tmp_path: Path) -> None:
    db_path = tmp_path / "state_drift_only.db"
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("PRAGMA foreign_keys=ON")
        _apply_runtime_migrations(conn)
        conn.execute(
            "INSERT INTO user_settings (user_id, signals_enabled, overload_enabled, drift_enabled, created_at, updated_at) "
            "VALUES ('1', 1, 1, 1, 't', 't')"
        )
        conn.execute(
            "INSERT INTO directions (id, title, status, created_at, updated_at) "
            "VALUES (1, 'Dir', 'ACTIVE', 't', 't')"
        )
        conn.execute(
            "INSERT INTO tasks (id, title, status, state, planned_at, created_at, updated_at) "
            "VALUES (1, 'unplanned', 'NEW', 'NEW', NULL, 't', 't')"
        )
        conn.commit()

    app_mod = _load_api(db_path)
    payload = app_mod.get_p2_state(user_id="1")
    assert payload["overload"] is None
    assert isinstance(payload.get("drift"), dict)
    assert payload["drift"].get("active") is True


def test_state_response_shape(tmp_path: Path) -> None:
    db_path = tmp_path / "state_shape.db"
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("PRAGMA foreign_keys=ON")
        _apply_runtime_migrations(conn)
        conn.execute(
            "INSERT INTO user_settings (user_id, signals_enabled, overload_enabled, drift_enabled, created_at, updated_at) "
            "VALUES ('1', 1, 1, 1, 't', 't')"
        )
        conn.commit()

    app_mod = _load_api(db_path)
    payload = app_mod.get_p2_state(user_id="1")
    assert set(payload.keys()) == {"execution", "overload", "drift"}
    assert payload["execution"] == "ok"


def test_state_gating_disabled(tmp_path: Path) -> None:
    db_path = tmp_path / "state_gate.db"
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("PRAGMA foreign_keys=ON")
        _apply_runtime_migrations(conn)
        conn.execute(
            "INSERT INTO user_settings (user_id, signals_enabled, overload_enabled, drift_enabled, created_at, updated_at) "
            "VALUES ('1', 0, 0, 0, 't', 't')"
        )
        conn.execute(
            "INSERT INTO tasks (id, title, status, state, planned_at, created_at, updated_at) "
            "VALUES (1, 'unplanned', 'NEW', 'NEW', NULL, 't', 't')"
        )
        conn.execute(
            "INSERT INTO tasks (id, title, status, state, planned_at, created_at, updated_at) "
            "VALUES (2, 'planned', 'NEW', 'PLANNED', '2026-02-10T10:00:00+00:00', 't', 't')"
        )
        conn.commit()

    app_mod = _load_api(db_path)
    payload = app_mod.get_p2_state(user_id="1")
    assert payload["overload"] is None
    assert payload["drift"] is None


def test_previous_goals_api(tmp_path: Path) -> None:
    db_path = tmp_path / "prev_goals.db"
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("PRAGMA foreign_keys=ON")
        _apply_runtime_migrations(conn)
        conn.execute(
            "INSERT INTO cycles (id, type, period_key, status, created_at, updated_at) "
            "VALUES (1, 'MONTHLY', '2026-01', 'DONE', 't', 't')"
        )
        conn.execute(
            "INSERT INTO cycles (id, type, period_key, status, created_at, updated_at) "
            "VALUES (2, 'MONTHLY', '2026-02', 'OPEN', 't', 't')"
        )
        conn.execute(
            "INSERT INTO cycle_goals (id, cycle_id, text, status, continued_from_goal_id, created_at, updated_at) "
            "VALUES (10, 1, 'G1', 'ACTIVE', NULL, 't', 't')"
        )
        conn.execute(
            "INSERT INTO cycle_goals (id, cycle_id, text, status, continued_from_goal_id, created_at, updated_at) "
            "VALUES (11, 1, 'G2', 'DROPPED', NULL, 't', 't')"
        )
        conn.commit()

    app_mod = _load_api(db_path)
    rows = app_mod.list_previous_cycle_goals(2)
    ids = [r.get("id") for r in rows]
    assert ids == [10]


def test_api_cycle_goals_list(tmp_path: Path) -> None:
    db_path = tmp_path / "goals_list.db"
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("PRAGMA foreign_keys=ON")
        _apply_runtime_migrations(conn)
        conn.execute(
            "INSERT INTO cycles (id, type, period_key, status, created_at, updated_at) "
            "VALUES (1, 'MONTHLY', '2026-02', 'OPEN', 't', 't')"
        )
        conn.execute(
            "INSERT INTO cycle_goals (id, cycle_id, text, status, continued_from_goal_id, created_at, updated_at) "
            "VALUES (1, 1, 'G1', 'ACTIVE', NULL, 't1', 't1')"
        )
        conn.execute(
            "INSERT INTO cycle_goals (id, cycle_id, text, status, continued_from_goal_id, created_at, updated_at) "
            "VALUES (2, 1, 'G2', 'ACHIEVED', NULL, 't2', 't2')"
        )
        conn.commit()

    app_mod = _load_api(db_path)
    rows = app_mod.list_cycle_goals(1)
    assert len(rows) == 2
    assert [r.get("id") for r in rows] == [1, 2]
    for r in rows:
        assert set(r.keys()) == {
            "id",
            "cycle_id",
            "text",
            "status",
            "continued_from_goal_id",
            "created_at",
        }

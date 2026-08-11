import sqlite3
from pathlib import Path

import pytest

from organizer_worker import canon
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


@pytest.fixture()
def runtime_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    db_path = tmp_path / "runtime_task_intents_v2.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    db.DB_PATH = str(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("PRAGMA foreign_keys=ON")
        _apply_runtime_migrations(conn)
    return db_path


def test_set_status_waiting_vs_paused(runtime_db: Path) -> None:
    t1 = handlers.dispatch_intent({"intent": "task.create", "entities": {"title": "Задача A"}})
    t2 = handlers.dispatch_intent({"intent": "task.create", "entities": {"title": "Задача B"}})
    assert t1["ok"] is True and t2["ok"] is True

    id1 = int(t1["debug"]["task_id"])
    id2 = int(t2["debug"]["task_id"])

    r1 = handlers.dispatch_intent({"intent": "task.set_status", "entities": {"task_id": id1, "status": "приостанови"}})
    r2 = handlers.dispatch_intent({"intent": "task.set_status", "entities": {"task_id": id2, "status": "ждем ответа"}})
    assert r1["ok"] is True and r2["ok"] is True
    assert r1["debug"]["status"] == "PAUSED"
    assert r2["debug"]["status"] == "WAITING"

    with sqlite3.connect(str(runtime_db)) as conn:
        row1 = conn.execute("SELECT status, state FROM tasks WHERE id = ?", (id1,)).fetchone()
        row2 = conn.execute("SELECT status, state FROM tasks WHERE id = ?", (id2,)).fetchone()
    assert row1 == ("PAUSED", "PAUSED")
    assert row2 == ("WAITING", "WAITING")


def test_task_reschedule_requires_when(runtime_db: Path) -> None:
    t = handlers.dispatch_intent({"intent": "task.create", "entities": {"title": "Сдвинуть задачу"}})
    assert t["ok"] is True
    task_id = int(t["debug"]["task_id"])

    res = handlers.dispatch_intent({"intent": "task.reschedule", "entities": {"task_id": task_id}})
    assert res["ok"] is False
    assert res.get("clarifying_question") == "На когда перенести?"
    missing = res.get("debug", {}).get("missing")
    assert isinstance(missing, list)
    assert "entities.planned_at|entities.when" in missing


def test_task_reschedule_empty_when_returns_clarification(runtime_db: Path) -> None:
    t = handlers.dispatch_intent({"intent": "task.create", "entities": {"title": "Сдвинуть задачу пустым when"}})
    assert t["ok"] is True
    task_id = int(t["debug"]["task_id"])

    res = handlers.dispatch_intent({"intent": "task.reschedule", "entities": {"task_id": task_id, "when": "   "}})
    assert res["ok"] is False
    assert bool(res.get("clarifying_question"))

    with sqlite3.connect(str(runtime_db)) as conn:
        row = conn.execute("SELECT planned_at FROM tasks WHERE id = ?", (task_id,)).fetchone()
    assert row is not None
    assert row[0] is None


def test_timeblock_create_empty_start_at_returns_clarification(runtime_db: Path) -> None:
    t = handlers.dispatch_intent({"intent": "task.create", "entities": {"title": "ТБ задача пустой старт"}})
    assert t["ok"] is True
    task_id = int(t["debug"]["task_id"])

    res = handlers.dispatch_intent(
        {
            "intent": "timeblock.create",
            "entities": {"task_id": task_id, "start_at": "   ", "duration_min": 30},
        }
    )
    assert res["ok"] is False
    assert bool(res.get("clarifying_question"))

    with sqlite3.connect(str(runtime_db)) as conn:
        cnt = conn.execute("SELECT COUNT(*) FROM time_blocks WHERE task_id = ?", (task_id,)).fetchone()
    assert cnt is not None
    assert int(cnt[0]) == 0


def test_voice_timeblock_create_with_duration_only_asks_time_not_inbox(runtime_db: Path) -> None:
    t = handlers.dispatch_intent({"intent": "task.create", "entities": {"title": "Встреча"}})
    assert t["ok"] is True
    task_id = int(t["debug"]["task_id"])

    # Voice flow:
    # 1) "Создай встречу"
    # 2) "60"
    # Runtime command arrives as legacy alias timeblock_create with duration only.
    res = handlers.dispatch_intent(
        {
            "intent": "timeblock_create",
            "entities": {"task_id": task_id, "duration_min": 60},
        }
    )
    assert res["ok"] is False
    assert res.get("clarifying_question") == "На какое время поставить блок?"
    assert res.get("debug", {}).get("contract") == "ML CONTRACT v2.2"

    with sqlite3.connect(str(runtime_db)) as conn:
        cnt = conn.execute("SELECT COUNT(*) FROM time_blocks WHERE task_id = ?", (task_id,)).fetchone()
    assert cnt is not None
    assert int(cnt[0]) == 0


def test_timeblock_create_with_start_and_duration_executes(runtime_db: Path) -> None:
    created = handlers.dispatch_intent({"intent": "task.create", "entities": {"title": "Контрактный блок"}})
    assert created["ok"] is True
    task_id = int(created["debug"]["task_id"])

    res = handlers.dispatch_intent(
        {
            "intent": "timeblock.create",
            "entities": {
                "task_id": task_id,
                "start_at": "2026-02-18T10:00:00Z",
                "duration_minutes": 45,
            },
        }
    )
    assert res["ok"] is True
    tb_id = int(res["debug"]["time_block_id"])

    with sqlite3.connect(str(runtime_db)) as conn:
        row = conn.execute(
            "SELECT task_id, start_at, end_at FROM time_blocks WHERE id = ?",
            (tb_id,),
        ).fetchone()
    assert row is not None
    assert int(row[0]) == task_id
    assert str(row[1]).startswith("2026-02-18T10:00:00")
    assert str(row[2]).startswith("2026-02-18T10:45:00")


def test_meeting_create_with_start_and_duration_does_not_require_task(runtime_db: Path) -> None:
    res = handlers.dispatch_intent(
        {
            "intent": "meeting.create",
            "entities": {
                "user_id": "u-1001",
                "start_at": "2026-02-18T10:00:00Z",
                "duration_minutes": 45,
            },
        }
    )
    assert res["ok"] is True
    assert str(res.get("user_message") or "").strip() == "Встреча создана."
    assert str(res.get("debug", {}).get("user_id") or "") == "u-1001"


def test_meeting_create_without_duration_uses_default_30(runtime_db: Path) -> None:
    res = handlers.dispatch_intent(
        {
            "intent": "meeting.create",
            "entities": {
                "user_id": "u-1001",
                "start_at": "2026-02-18T10:00:00Z",
            },
        }
    )
    assert res["ok"] is True
    assert int(res.get("debug", {}).get("duration_minutes") or 0) == 30


def test_meeting_update_accepts_partial_temporal_change(runtime_db: Path) -> None:
    res = handlers.dispatch_intent(
        {
            "intent": "meeting.update",
            "entities": {
                "user_id": "u-1001",
                "start_at_time": "16:00",
            },
        }
    )
    assert res["ok"] is True
    message = str(res.get("user_message") or "").strip().lower()
    assert "встреча" in message
    assert ("обнов" in message) or ("перенес" in message)


def test_timeblock_create_allows_creation_without_task_linkage(runtime_db: Path) -> None:
    res = handlers.dispatch_intent(
        {
            "intent": "timeblock.create",
            "entities": {
                "user_id": "u-1002",
                "start_at": "2026-02-18T10:00:00Z",
                "duration_minutes": 30,
            },
        }
    )
    assert res["ok"] is True
    tb_id = int(res["debug"]["time_block_id"])
    task_id = int(res["debug"]["task_id"])
    with sqlite3.connect(str(runtime_db)) as conn:
        row = conn.execute(
            "SELECT task_id, start_at, end_at FROM time_blocks WHERE id = ?",
            (tb_id,),
        ).fetchone()
        task_row = conn.execute("SELECT title FROM tasks WHERE id = ?", (task_id,)).fetchone()
    assert row is not None
    assert int(row[0]) == task_id
    assert str(row[1]).startswith("2026-02-18T10:00:00")
    assert str(row[2]).startswith("2026-02-18T10:30:00")
    assert task_row is not None
    assert str(task_row[0]) == "Блок времени"


def test_timeblock_create_with_optional_unmatched_task_ref_creates_unlinked_block(runtime_db: Path) -> None:
    res = handlers.dispatch_intent(
        {
            "intent": "timeblock.create",
            "entities": {
                "user_id": "u-1003",
                "start_at": "2026-02-18T12:00:00Z",
                "duration_minutes": 30,
                "task_ref": "несуществующая задача",
                "task_ref_optional": True,
                "comment_text": "купить молоко",
            },
        }
    )
    assert res["ok"] is True
    tb_id = int(res["debug"]["time_block_id"])
    task_id = int(res["debug"]["task_id"])
    with sqlite3.connect(str(runtime_db)) as conn:
        row = conn.execute("SELECT task_id, comment FROM time_blocks WHERE id = ?", (tb_id,)).fetchone()
        task_row = conn.execute("SELECT title FROM tasks WHERE id = ?", (task_id,)).fetchone()
    assert row is not None
    assert int(row[0]) == task_id
    assert str(row[1]) == "купить молоко"
    assert task_row is not None
    assert str(task_row[0]) == "купить молоко"


def test_timeblock_create_with_explicit_unmatched_task_ref_still_returns_not_found(runtime_db: Path) -> None:
    res = handlers.dispatch_intent(
        {
            "intent": "timeblock.create",
            "entities": {
                "user_id": "u-1004",
                "start_at": "2026-02-18T13:00:00Z",
                "duration_minutes": 30,
                "task_ref": "несуществующая задача",
            },
        }
    )
    assert res["ok"] is False
    assert str(res.get("user_message") or "").strip() == "Не нашел подходящую задачу."


def test_timeblock_move_empty_datetimes_returns_clarification_no_changes(runtime_db: Path) -> None:
    t = handlers.dispatch_intent({"intent": "task.create", "entities": {"title": "ТБ задача move пустой старт"}})
    assert t["ok"] is True
    task_id = int(t["debug"]["task_id"])
    create_tb = handlers.dispatch_intent(
        {
            "intent": "timeblock.create",
            "entities": {
                "task_id": task_id,
                "start_at": "2026-02-18T10:00:00Z",
                "duration_min": 30,
            },
        }
    )
    assert create_tb["ok"] is True
    tb_id = int(create_tb["debug"]["time_block_id"])

    with sqlite3.connect(str(runtime_db)) as conn:
        before = conn.execute("SELECT start_at, end_at FROM time_blocks WHERE id = ?", (tb_id,)).fetchone()
    assert before is not None

    res = handlers.dispatch_intent(
        {
            "intent": "timeblock.move",
            "entities": {"time_block_id": tb_id, "start_at": " ", "end_at": "   "},
        }
    )
    assert res["ok"] is False
    assert bool(res.get("clarifying_question"))

    with sqlite3.connect(str(runtime_db)) as conn:
        after = conn.execute("SELECT start_at, end_at FROM time_blocks WHERE id = ?", (tb_id,)).fetchone()
    assert after is not None
    assert tuple(after) == tuple(before)


def test_timeblock_update_alias_updates_existing_block(runtime_db: Path) -> None:
    t = handlers.dispatch_intent({"intent": "task.create", "entities": {"title": "ТБ задача alias update"}})
    assert t["ok"] is True
    task_id = int(t["debug"]["task_id"])
    create_tb = handlers.dispatch_intent(
        {
            "intent": "timeblock.create",
            "entities": {
                "task_id": task_id,
                "start_at": "2026-02-18T10:00:00Z",
                "duration_min": 30,
                "comment_text": "старый комментарий",
            },
        }
    )
    assert create_tb["ok"] is True
    tb_id = int(create_tb["debug"]["time_block_id"])

    res = handlers.dispatch_intent(
        {
            "intent": "timeblock.update",
            "entities": {
                "time_block_id": tb_id,
                "start_at": "2026-02-18T11:00:00Z",
                "end_at": "2026-02-18T11:45:00Z",
                "comment_text": "новый комментарий",
            },
        }
    )
    assert res["ok"] is True
    assert int(res["debug"]["time_block_id"]) == tb_id

    with sqlite3.connect(str(runtime_db)) as conn:
        row = conn.execute("SELECT start_at, end_at, comment FROM time_blocks WHERE id = ?", (tb_id,)).fetchone()
    assert row is not None
    assert str(row[0]).startswith("2026-02-18T11:00:00")
    assert str(row[1]).startswith("2026-02-18T11:45:00")
    assert str(row[2] or "") == "новый комментарий"


def test_task_ref_disambiguation_returns_candidates(runtime_db: Path) -> None:
    r1 = handlers.dispatch_intent({"intent": "task.create", "entities": {"title": "Отчет для клиента"}})
    r2 = handlers.dispatch_intent({"intent": "task.create", "entities": {"title": "Отчет для команды"}})
    assert r1["ok"] is True and r2["ok"] is True

    res = handlers.dispatch_intent(
        {
            "intent": "task.set_status",
            "entities": {"task_ref": "Отчет", "status": "пауза"},
        }
    )
    assert res["ok"] is False
    assert bool(res.get("clarifying_question"))
    choices = res.get("choices")
    assert isinstance(choices, list)
    assert 1 <= len(choices) <= canon.get_disambiguation_top_k()
    assert all(isinstance(ch.get("id"), int) and isinstance(ch.get("label"), str) for ch in choices)
    candidates = res.get("debug", {}).get("candidates_top")
    assert isinstance(candidates, list)
    assert len(candidates) >= 2


def test_paused_present_in_canon_status() -> None:
    status = canon.get_canon().get("common", {}).get("status", {}).get("canonical", {})
    assert "PAUSED" in status
    assert status["PAUSED"]["ru"] == "Приостановлено"


def test_timeblock_create_requires_duration_or_end(runtime_db: Path) -> None:
    t = handlers.dispatch_intent({"intent": "task.create", "entities": {"title": "ТБ задача"}})
    assert t["ok"] is True
    task_id = int(t["debug"]["task_id"])

    res = handlers.dispatch_intent(
        {
            "intent": "timeblock.create",
            "entities": {"task_id": task_id, "start_at": "2026-02-18T10:00:00Z"},
        }
    )
    assert res["ok"] is False
    assert res.get("clarifying_question") == "На сколько минут поставить блок?"


def test_confidence_074_returns_clarification_not_execute(runtime_db: Path) -> None:
    res = handlers.dispatch_intent(
        {
            "intent": "task.create",
            "confidence": 0.74,
            "entities": {"title": "Не должна создаться"},
        }
    )
    assert res["ok"] is False
    assert bool(res.get("clarifying_question"))
    with sqlite3.connect(str(runtime_db)) as conn:
        row = conn.execute("SELECT COUNT(*) FROM tasks").fetchone()
    assert int(row[0]) == 0


def test_confidence_076_with_missing_required_returns_clarification(runtime_db: Path) -> None:
    t = handlers.dispatch_intent({"intent": "task.create", "entities": {"title": "Сдвинуть задачу"}})
    assert t["ok"] is True
    task_id = int(t["debug"]["task_id"])

    res = handlers.dispatch_intent(
        {
            "intent": "task.reschedule",
            "confidence": 0.76,
            "entities": {"task_id": task_id},
        }
    )
    assert res["ok"] is False
    assert res.get("clarifying_question") == "На когда перенести?"


def test_confidence_095_candidates_without_choice_returns_choices(runtime_db: Path) -> None:
    res = handlers.dispatch_intent(
        {
            "intent": "task.set_status",
            "confidence": 0.95,
            "entities": {
                "status": "пауза",
                "task_ref": {
                    "candidates": [
                        {"id": 101, "title": "Отчет для клиента"},
                        {"id": 202, "title": "Отчет для команды"},
                    ]
                },
            },
        }
    )
    assert res["ok"] is False
    assert bool(res.get("clarifying_question"))
    choices = res.get("choices")
    assert isinstance(choices, list)
    assert len(choices) >= 1
    assert all(isinstance(ch.get("id"), int) and isinstance(ch.get("label"), str) for ch in choices)


def test_confidence_02_returns_safe_fail(runtime_db: Path) -> None:
    res = handlers.dispatch_intent(
        {
            "intent": "task.create",
            "confidence": 0.2,
            "entities": {"title": "Не должна создаться"},
        }
    )
    assert res["ok"] is False
    assert res.get("debug", {}).get("reason") == "safe_fail"


def test_unknown_intent_returns_clarify_and_never_writes_db(runtime_db: Path) -> None:
    with sqlite3.connect(str(runtime_db)) as conn:
        before_tasks = int(conn.execute("SELECT COUNT(*) FROM tasks").fetchone()[0])
        before_blocks = int(conn.execute("SELECT COUNT(*) FROM time_blocks").fetchone()[0])

    res = handlers.dispatch_intent(
        {
            "intent": "meeting_create_unknown",
            "entities": {"title": "Встреча", "start_at": "2026-02-18T10:00:00Z", "duration_minutes": 30},
        }
    )
    assert res["ok"] is False
    assert bool(res.get("clarifying_question"))
    choices = res.get("choices")
    assert isinstance(choices, list)
    assert len(choices) >= 1

    with sqlite3.connect(str(runtime_db)) as conn:
        after_tasks = int(conn.execute("SELECT COUNT(*) FROM tasks").fetchone()[0])
        after_blocks = int(conn.execute("SELECT COUNT(*) FROM time_blocks").fetchone()[0])

    assert after_tasks == before_tasks
    assert after_blocks == before_blocks


def _recreate_time_blocks_with_user_id_not_null(db_path: Path) -> None:
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("PRAGMA foreign_keys=OFF")
        conn.execute("ALTER TABLE time_blocks RENAME TO time_blocks_old")
        conn.execute(
            """
            CREATE TABLE time_blocks (
                id INTEGER PRIMARY KEY,
                task_id INTEGER NOT NULL,
                user_id TEXT NOT NULL,
                start_at TEXT NOT NULL,
                end_at TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY(task_id) REFERENCES tasks(id)
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS ix_time_blocks_task_id ON time_blocks(task_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS ix_time_blocks_start_at ON time_blocks(start_at)")
        conn.execute("DROP TABLE time_blocks_old")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.commit()


def test_timeblock_create_with_user_id_column_uses_user_id(runtime_db: Path) -> None:
    _recreate_time_blocks_with_user_id_not_null(runtime_db)

    task = handlers.dispatch_intent({"intent": "task.create", "entities": {"title": "ТБ c user_id"}})
    assert task["ok"] is True
    task_id = int(task["debug"]["task_id"])

    res = handlers.dispatch_intent(
        {
            "intent": "timeblock.create",
            "entities": {
                "task_id": task_id,
                "user_id": "u-777",
                "start_at": "2026-02-18T10:00:00Z",
                "duration_minutes": 30,
            },
        }
    )
    assert res["ok"] is True
    tb_id = int(res["debug"]["time_block_id"])

    with sqlite3.connect(str(runtime_db)) as conn:
        row = conn.execute(
            "SELECT task_id, user_id, start_at, end_at FROM time_blocks WHERE id = ?",
            (tb_id,),
        ).fetchone()
    assert row is not None
    assert int(row[0]) == task_id
    assert str(row[1]) == "u-777"
    assert str(row[2]).startswith("2026-02-18T10:00:00")
    assert str(row[3]).startswith("2026-02-18T10:30:00")


def test_timeblock_create_with_user_id_column_missing_user_id_returns_meaningful_error(runtime_db: Path) -> None:
    _recreate_time_blocks_with_user_id_not_null(runtime_db)

    task = handlers.dispatch_intent({"intent": "task.create", "entities": {"title": "ТБ без user_id"}})
    assert task["ok"] is True
    task_id = int(task["debug"]["task_id"])

    res = handlers.dispatch_intent(
        {
            "intent": "timeblock.create",
            "entities": {
                "task_id": task_id,
                "start_at": "2026-02-18T10:00:00Z",
                "duration_minutes": 30,
            },
        }
    )
    assert res["ok"] is False
    assert "не найден пользователь" in str(res.get("user_message") or "").lower()
    assert str(res.get("debug", {}).get("reason") or "") == "missing_user_id"

    with sqlite3.connect(str(runtime_db)) as conn:
        cnt = conn.execute("SELECT COUNT(*) FROM time_blocks").fetchone()
    assert cnt is not None
    assert int(cnt[0]) == 0

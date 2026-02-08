from datetime import date

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WORKER_ROOT = ROOT / "organizer-worker"
sys.path.append(str(WORKER_ROOT))

import worker  # noqa: E402


def test_reg_nudge_mode_off() -> None:
    today = date(2026, 2, 7)
    due = date(2026, 2, 7)
    assert worker._p4_reg_nudge_should_emit("off", today, due) is False


def test_reg_nudge_mode_daily() -> None:
    today = date(2026, 2, 7)
    due = date(2026, 2, 20)
    assert worker._p4_reg_nudge_should_emit("daily", today, due) is True


def test_reg_nudge_mode_due_day() -> None:
    today = date(2026, 2, 7)
    due = date(2026, 2, 7)
    due_other = date(2026, 2, 8)
    assert worker._p4_reg_nudge_should_emit("due_day", today, due) is True
    assert worker._p4_reg_nudge_should_emit("due_day", today, due_other) is False


def test_p5_drift_calendar_type() -> None:
    assert (
        worker._p5_drift_calendar_type(
            "SCHEDULED", 404, "2026-02-01T10:00:00+00:00", None, False
        )
        == "missing_event"
    )
    assert (
        worker._p5_drift_calendar_type(
            "DONE", None, "2026-02-01T10:00:00+00:00", "2026-02-01T10:00:00+00:00", True
        )
        == "unexpected_event"
    )
    assert (
        worker._p5_drift_calendar_type(
            "SCHEDULED", None, "2026-02-01T10:00:00+00:00", "2026-02-01T11:00:00+00:00", True
        )
        == "time_mismatch"
    )


def test_p5_overload_signals() -> None:
    day = "2026-02-07"
    signals = worker._p5_overload_signals(day, tasks_today=10, regs_due=6, backlog=60)
    sigs = {s[0] for s in signals}
    assert "capacity_items" in sigs
    assert "capacity_minutes" in sigs
    assert "due_today" in sigs
    assert "backlog" in sigs


def test_p5_should_run() -> None:
    assert worker._p5_should_run("off") is False
    assert worker._p5_should_run("log") is True


def test_p5_nudge_daily_once_and_rollover() -> None:
    worker._P5_NUDGE_DAY = None
    worker._P5_DRIFT_COUNT_TODAY = 0
    worker._P5_OVERLOAD_COUNT_TODAY = 0
    worker._P5_NUDGE_EMITTED = False
    day = "2026-02-07"
    worker._p5_nudge_reset_if_new_day(day)
    worker._P5_DRIFT_COUNT_TODAY = 1
    assert worker._p5_nudge_emit_if_needed(day, mode="daily") is True
    assert worker._p5_nudge_emit_if_needed(day, mode="daily") is False
    next_day = "2026-02-08"
    worker._p5_nudge_reset_if_new_day(next_day)
    worker._P5_OVERLOAD_COUNT_TODAY = 2
    assert worker._p5_nudge_emit_if_needed(next_day, mode="daily") is True


def test_p5_nudge_off_no_emit() -> None:
    worker._P5_NUDGE_DAY = None
    worker._P5_DRIFT_COUNT_TODAY = 3
    worker._P5_OVERLOAD_COUNT_TODAY = 4
    worker._P5_NUDGE_EMITTED = False
    day = "2026-02-07"
    worker._p5_nudge_reset_if_new_day(day)
    assert worker._p5_nudge_emit_if_needed(day, mode="off") is False


def test_p5_reg_status_due_alias() -> None:
    rows = [
        {"status": "OPEN"},
        {"status": "DUE"},
        {"status": "DONE"},
    ]
    total, counts = worker._p5_regs_due_counts(rows)
    assert total == 2
    assert counts.get("OPEN") == 1
    assert counts.get("DUE") == 1

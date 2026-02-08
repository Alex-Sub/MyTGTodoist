import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BOT_PATH = ROOT / "telegram-bot"
sys.path.append(str(BOT_PATH))

import bot  # noqa: E402


def test_build_inbox_items_filters_and_sorting() -> None:
    cycles = [
        {"id": 2, "status": "OPEN", "type": "MONTHLY_REVIEW", "period_key": "2026-02"},
        {"id": 3, "status": "DONE", "type": "MONTHLY_REVIEW", "period_key": "2026-01"},
    ]
    directions = [
        {"id": 5, "status": "ACTIVE", "title": "Health"},
        {"id": 6, "status": "ARCHIVED", "title": "Old"},
    ]
    projects = [
        {"id": 9, "status": "ACTIVE", "title": "Run"},
        {"id": 10, "status": "DONE", "title": "Done"},
    ]
    tasks = [
        {"id": 20, "status": "NEW", "state": "NEW", "planned_at": None, "title": "A"},
        {"id": 21, "status": "NEW", "state": "PLANNED", "planned_at": None, "title": "B"},
        {"id": 22, "status": "NEW", "state": "NEW", "planned_at": "2026-02-10T10:00:00+00:00", "title": "C"},
    ]

    items = bot._build_inbox_items(cycles, directions, projects, tasks)
    kinds = [i.get("kind") for i in items]
    ids = [i.get("id") for i in items]

    assert kinds == ["cycle", "direction", "project", "task"]
    assert ids == [2, 5, 9, 20]


def test_task_inbox_filter_branches() -> None:
    task_unplanned = {"id": 1, "status": "NEW", "state": "NEW", "planned_at": None}
    task_planned = {"id": 2, "status": "NEW", "state": "NEW", "planned_at": "2026-02-10T10:00:00+00:00"}
    task_not_new = {"id": 3, "status": "NEW", "state": "PLANNED", "planned_at": None}

    assert bot._task_is_in_inbox(task_unplanned) is True
    assert bot._task_is_in_inbox(task_planned) is False
    assert bot._task_is_in_inbox(task_not_new) is False

    # planning branches imply planned_at is set (removed from inbox)
    planned_iso_1 = bot._compute_plan_iso(1)
    planned_iso_3 = bot._compute_plan_iso(3)
    planned_iso_7 = bot._compute_plan_iso(7)
    assert bot._task_is_in_inbox({"status": "NEW", "state": "NEW", "planned_at": planned_iso_1}) is False
    assert bot._task_is_in_inbox({"status": "NEW", "state": "NEW", "planned_at": planned_iso_3}) is False
    assert bot._task_is_in_inbox({"status": "NEW", "state": "NEW", "planned_at": planned_iso_7}) is False


def test_plan_menu_keyboard_callbacks() -> None:
    kb = bot._plan_menu_keyboard(42)
    buttons = [b for row in kb.get("inline_keyboard", []) for b in row]
    callbacks = {b.get("callback_data") for b in buttons}
    assert f"p2:task:plan_in:42:1" in callbacks
    assert f"p2:task:plan_in:42:3" in callbacks
    assert f"p2:task:plan_in:42:7" not in callbacks
    assert f"p2:task:plan_choose:42" in callbacks
    assert f"p2:task:plan_none:42" in callbacks
    assert f"p2:task:plan_back:42" in callbacks


def test_compute_plan_iso_local_to_utc(monkeypatch) -> None:
    from datetime import timezone, timedelta
    monkeypatch.setattr(bot, "_tz_local", lambda: timezone(timedelta(hours=3)))
    planned_iso = bot._compute_plan_iso(1)
    assert planned_iso.endswith("+00:00") or planned_iso.endswith("Z")


def test_pending_plan_input_calls_worker(monkeypatch) -> None:
    calls = {}
    monkeypatch.setattr(bot, "_save_p2_pending_state", lambda _s: None)
    monkeypatch.setattr(bot, "_prune_p2_pending_state", lambda _s, _t: None)
    monkeypatch.setattr(bot, "_send_message", lambda _c, _t: None)

    def _fake_worker_post_ex(path, payload):
        calls["path"] = path
        calls["payload"] = payload
        return True, {"id": payload.get("task_id")}, 200, None

    monkeypatch.setattr(bot, "_worker_post_ex", _fake_worker_post_ex)
    bot._p2_pending_state.clear()
    bot._p2_pending_state[1] = {"mode": "task_plan", "task_id": 7, "expires_at": 999999}
    bot._p2_handle_text(1, 2, "2026-02-10 09:30")
    assert calls.get("path") == "/p2/commands/plan_task"
    assert calls.get("payload", {}).get("task_id") == 7


def test_state_line_from_payload() -> None:
    assert bot._state_line_from_payload({"overload": {"active": True}}) == "Состояние: • Перегрузка"
    assert bot._state_line_from_payload({"drift": {"active": True}}) == "Состояние: • Связность"
    assert bot._state_line_from_payload({}) is None


def test_nudge_due_logic() -> None:
    from datetime import timezone, timedelta
    now = datetime.now(timezone.utc)
    future = (now + timedelta(days=1)).isoformat()
    past = (now - timedelta(days=1)).isoformat()
    assert bot._nudge_is_due(past, now) is True
    assert bot._nudge_is_due(future, now) is False


def test_signals_config_keyboard_labels() -> None:
    kb = bot._signals_config_keyboard(1, 0, "7")
    buttons = [b for row in kb.get("inline_keyboard", []) for b in row]
    texts = [b.get("text") for b in buttons]
    assert "Перегрузка: ВКЛ" in texts
    assert "Связность: ВЫКЛ" in texts


def test_default_signals_selection() -> None:
    assert bot._default_signals_selection(0, 0) == (1, 0)
    assert bot._default_signals_selection(1, 0) == (1, 0)
    assert bot._default_signals_selection(0, 1) == (0, 1)


def test_signals_shortcut_keyboard_callbacks() -> None:
    kb = bot._signals_shortcut_keyboard("9")
    buttons = [b for row in kb.get("inline_keyboard", []) for b in row]
    callbacks = {b.get("callback_data") for b in buttons}
    assert "p2:signals:bulk_on:9" in callbacks
    assert "p2:signals:bulk_off:9" in callbacks

import sys
from pathlib import Path
from datetime import datetime, timezone, timedelta

ROOT = Path(__file__).resolve().parents[1]
BOT_PATH = ROOT / "telegram-bot"
sys.path.append(str(BOT_PATH))

import bot  # noqa: E402


class _FixedDateTime(datetime):
    @classmethod
    def now(cls, tz=None):
        dt = datetime(2026, 2, 7, 10, 0, 0, tzinfo=timezone.utc)
        if tz is None:
            return dt.replace(tzinfo=None)
        return dt.astimezone(tz)


def test_today_render_end_to_end(monkeypatch) -> None:
    monkeypatch.setattr(bot, "datetime", _FixedDateTime)
    monkeypatch.setattr(bot, "_tz_local", lambda: timezone.utc)

    def fake_api_get_ex(path, params=None):
        if path == "/p2/tasks":
            return True, [
                {"id": 1, "title": "A", "state": "SCHEDULED", "planned_at": "2026-02-07T09:00:00+00:00"},
                {"id": 2, "title": "B", "state": "PLANNED", "planned_at": "2026-02-07T12:00:00+00:00"},
                {"id": 3, "title": "C", "state": "PLANNED", "planned_at": "2026-02-08T09:00:00+00:00"},
                {"id": 10, "title": "Back1", "state": "NEW", "planned_at": None},
                {"id": 11, "title": "Back2", "state": "IN_PROGRESS", "planned_at": None},
                {"id": 12, "title": "Back3", "state": "NEW", "planned_at": None},
                {"id": 13, "title": "Back4", "state": "IN_PROGRESS", "planned_at": None},
                {"id": 14, "title": "Back5", "state": "NEW", "planned_at": None},
                {"id": 15, "title": "Back6", "state": "NEW", "planned_at": None},
            ], 200, None
        if path == "/p4/regulations":
            return True, [{"id": 100, "title": "Отчет", "status": "ACTIVE", "day_of_month": 7}], 200, None
        if path.startswith("/p4/regulations/") and path.endswith("/runs"):
            return True, [
                {"id": 200, "status": "OPEN", "due_date": "2026-02-07"}
            ], 200, None
        return False, None, 404, "not found"

    monkeypatch.setattr(bot, "_api_get_ex", fake_api_get_ex)

    captured = {}

    def fake_send(chat_id, text, reply_markup):
        captured["text"] = text
        captured["reply_markup"] = reply_markup

    monkeypatch.setattr(bot, "_send_message_with_keyboard", fake_send)

    bot._today_render(1, None)
    text = captured.get("text") or ""
    assert "🧭 Сегодня — 07.02.2026" in text
    lines = [l for l in text.splitlines() if l.startswith("• #")]
    assert lines[0].startswith("• #1")
    assert "📅 Регламенты на сегодня" in text
    assert "• Отчет (до 7)" in text
    assert "📥 Backlog" in text
    assert "Back6" in text
    assert "Back1" not in text  # top-5 newest only


def test_today_callback_task_done(monkeypatch) -> None:
    calls = {}

    def fake_worker_post_ex(path, payload):
        calls["path"] = path
        calls["payload"] = payload
        return True, {"id": 1}, 200, None

    monkeypatch.setattr(bot, "_worker_post_ex", fake_worker_post_ex)
    monkeypatch.setattr(bot, "_today_render", lambda chat_id, message_id, page=1: calls.setdefault("render", True))

    res = bot._handle_today_callback("today:task:done:7", 11, 22)
    assert res == "Готово"
    assert calls["path"] == "/p2/commands/complete_task"
    assert calls["payload"]["task_id"] == 7
    assert calls["payload"]["source_msg_id"] == "tg:11:22:today:task:done"
    assert calls.get("render") is True


def test_today_callback_reg_complete_and_skip(monkeypatch) -> None:
    calls = []

    def fake_worker_post_ex(path, payload):
        calls.append((path, payload))
        return True, {"id": 1}, 200, None

    monkeypatch.setattr(bot, "_worker_post_ex", fake_worker_post_ex)
    monkeypatch.setattr(bot, "_today_render", lambda chat_id, message_id, page=1: None)

    res1 = bot._handle_today_callback("today:reg:complete:5", 11, 22)
    res2 = bot._handle_today_callback("today:reg:skip:6", 11, 22)
    assert res1 == "Готово"
    assert res2 == "Пропущено"
    assert calls[0][0] == "/p4/commands/complete_reg_run"
    assert calls[0][1]["run_id"] == 5
    assert calls[0][1]["source_msg_id"] == "tg:11:22:today:reg:complete"
    assert calls[1][0] == "/p4/commands/skip_reg_run"
    assert calls[1][1]["run_id"] == 6
    assert calls[1][1]["source_msg_id"] == "tg:11:22:today:reg:skip"

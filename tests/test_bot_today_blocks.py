import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BOT_PATH = ROOT / "telegram-bot"
sys.path.append(str(BOT_PATH))

import bot  # noqa: E402


def test_today_includes_blocks_section(monkeypatch) -> None:
    bot.P7_MODE = "on"

    def fake_api_get_ex(path: str, params: dict | None = None):
        if path == "/p2/tasks":
            return True, [], 200, None
        if path == "/p4/regulations":
            return True, [], 200, None
        if path == "/p7/day":
            return True, {
                "blocks": [
                    {
                        "id": 1,
                        "task_id": 10,
                        "start_at": "2026-02-07T09:00:00+00:00",
                        "end_at": "2026-02-07T10:00:00+00:00",
                    }
                ]
            }, 200, None
        if path == "/p2/tasks/10":
            return True, {"title": "Task X"}, 200, None
        return False, None, 404, "not found"

    captured = {}

    def fake_send(chat_id: int, text: str, reply_markup: dict) -> None:
        captured["text"] = text
        captured["reply_markup"] = reply_markup

    monkeypatch.setattr(bot, "_api_get_ex", fake_api_get_ex)
    monkeypatch.setattr(bot, "_send_message_with_keyboard", fake_send)

    bot._today_render(1, None, page=1)
    text = captured.get("text") or ""
    assert "⏱️ Блоки дня" in text
    assert "#10 Task X" in text


def test_today_block_move_callback(monkeypatch) -> None:
    bot.P7_MODE = "on"
    sent = {}

    def fake_worker_post_ex(path: str, payload: dict):
        sent["path"] = path
        sent["payload"] = payload
        return True, {"id": 1}, 200, None

    def fake_today_render(chat_id: int, message_id: int | None, page: int = 1) -> None:
        sent["render"] = (chat_id, message_id, page)

    monkeypatch.setattr(bot, "_worker_post_ex", fake_worker_post_ex)
    monkeypatch.setattr(bot, "_today_render", fake_today_render)

    resp = bot._handle_today_callback("today:block:move:12:10", 7, 9)
    assert resp == "Сдвинуто"
    assert sent["path"] == "/p7/commands/move_block"
    assert sent["payload"]["block_id"] == 12
    assert sent["payload"]["delta_minutes"] == 10
    assert sent["payload"]["source_msg_id"] == "tg:7:9:today:block:move"
    assert sent["render"] == (7, 9, 1)


def test_today_block_delete_callback(monkeypatch) -> None:
    bot.P7_MODE = "on"
    sent = {}

    def fake_worker_post_ex(path: str, payload: dict):
        sent["path"] = path
        sent["payload"] = payload
        return True, {"deleted": True}, 200, None

    def fake_today_render(chat_id: int, message_id: int | None, page: int = 1) -> None:
        sent["render"] = (chat_id, message_id, page)

    monkeypatch.setattr(bot, "_worker_post_ex", fake_worker_post_ex)
    monkeypatch.setattr(bot, "_today_render", fake_today_render)

    resp = bot._handle_today_callback("today:block:del:22", 5, 3)
    assert resp == "Удалено"
    assert sent["path"] == "/p7/commands/delete_block"
    assert sent["payload"]["block_id"] == 22
    assert sent["payload"]["source_msg_id"] == "tg:5:3:today:block:del"
    assert sent["render"] == (5, 3, 1)


def test_today_block_invalid_delta() -> None:
    bot.P7_MODE = "on"
    assert bot._handle_today_callback("today:block:move:1:5", 1, 1) == "Некорректная команда"

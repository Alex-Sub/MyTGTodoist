import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BOT_PATH = ROOT / "telegram-bot"
sys.path.append(str(BOT_PATH))

import bot  # noqa: E402


def test_today_formatting_and_sorting() -> None:
    day = "2026-02-07"
    tasks = [
        {"id": 2, "title": "B", "time": "12:00", "state": "PLANNED"},
        {"id": 1, "title": "A", "time": "09:00", "state": "SCHEDULED"},
    ]
    blocks = [
        {
            "id": 10,
            "task_id": 1,
            "title": "Block A",
            "start_utc": bot._parse_iso_utc("2026-02-07T09:00:00+00:00"),
            "end_utc": bot._parse_iso_utc("2026-02-07T10:00:00+00:00"),
        }
    ]
    regs = [
        {"run_id": 10, "title": "Отчет", "day_of_month": 7},
    ]
    backlog = [
        {"id": 5, "title": "Backlog"},
    ]
    text, keyboard = bot._today_build_message(day, tasks, blocks, 1, 1, regs, backlog)
    lines = [l for l in text.splitlines() if l.startswith("• #")]
    assert lines[0].startswith("• #1")
    assert "🧭 Сегодня — 07.02.2026" in text
    assert "📌 Задачи на сегодня" in text
    assert "⏱️ Блоки дня" in text
    assert "#1 Block A" in text
    assert "📅 Регламенты на сегодня" in text
    assert "📥 Backlog" in text
    assert "⚠️ Сигналы" in text
    assert "inline_keyboard" in keyboard


def test_today_idempotency_key() -> None:
    key = bot._today_source_msg_id(1, 2, "task:done")
    assert key == "tg:1:2:today:task:done"


def test_today_callback_invalid() -> None:
    assert bot._handle_today_callback("today:bad", 1, 1) == "Некорректная команда"
    assert bot._handle_today_callback("today:" + ("x" * 100), 1, 1) == "Некорректная команда"

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BOT_PATH = ROOT / "telegram-bot"
sys.path.append(str(BOT_PATH))

import bot  # noqa: E402


def test_regs_sorting_and_formatting() -> None:
    items = [
        {"reg_id": 1, "run_id": 10, "title": "Б", "day_of_month": 5, "status_label": "DONE"},
        {"reg_id": 2, "run_id": 11, "title": "А", "day_of_month": 3, "status_label": "DUE"},
        {"reg_id": 3, "run_id": 12, "title": "В", "day_of_month": 7, "status_label": "MISSED"},
    ]
    text, keyboard, _ = bot._regs_build_message("2026-02", 1, items)
    lines = [line for line in text.splitlines() if line.startswith(("🔴", "🟢", "⚠️"))]
    assert lines[0].startswith("🔴")
    assert lines[1].startswith("⚠️")
    assert lines[2].startswith("🟢")
    assert "📅 Регламенты — Февраль 2026" in text
    assert "Легенда:" in text
    assert "inline_keyboard" in keyboard


def test_regs_period_helpers() -> None:
    assert bot._regs_parse_period_key("2026-02") == (2026, 2)
    assert bot._regs_parse_period_key("2026-13") is None
    assert bot._regs_shift_period("2026-01", -1) == "2025-12"
    assert bot._regs_shift_period("2026-12", 1) == "2027-01"


def test_regs_idempotency_key() -> None:
    key = bot._regs_source_msg_id(123, 456, "complete")
    assert key == "tg:123:456:regs:complete"


def test_regs_no_run_only_disable() -> None:
    items = [
        {"reg_id": 1, "run_id": None, "title": "Регламент", "day_of_month": 5, "status_label": "DUE"},
    ]
    _text, keyboard, _ = bot._regs_build_message("2026-02", 1, items)
    row = keyboard["inline_keyboard"][0]
    labels = [b["text"] for b in row]
    assert labels == ["⛔ Отключить"]


def test_regs_callback_invalid() -> None:
    assert bot._handle_regs_callback("regs:bad", 1, 1) == "Некорректная команда"
    assert bot._handle_regs_callback("regs:" + ("x" * 100), 1, 1) == "Некорректная команда"

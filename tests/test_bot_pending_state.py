import json
import time
from pathlib import Path

import importlib
import sys


ROOT = Path(__file__).resolve().parents[1]
BOT_PATH = ROOT / "telegram-bot"
sys.path.append(str(BOT_PATH))

import bot  # noqa: E402


def test_pending_state_roundtrip(tmp_path: Path) -> None:
    path = tmp_path / "pending.json"
    bot.P2_PENDING_PATH = str(path)
    state = {
        123: {"mode": "task", "parent_type": "project", "parent_id": 1, "expires_at": time.time() + 60}
    }
    bot._save_p2_pending_state(state)
    loaded = bot._load_p2_pending_state()
    assert 123 in loaded
    assert loaded[123]["parent_type"] == "project"


def test_pending_state_prune_and_corruption(tmp_path: Path) -> None:
    path = tmp_path / "pending.json"
    bot.P2_PENDING_PATH = str(path)
    expired = {1: {"mode": "task", "expires_at": time.time() - 1}}
    bot._save_p2_pending_state(expired)
    state = bot._load_p2_pending_state()
    bot._prune_p2_pending_state(state, time.time())
    assert 1 not in state

    # corruption guard
    path.write_text("{broken", encoding="utf-8")
    loaded = bot._load_p2_pending_state()
    assert loaded == {}

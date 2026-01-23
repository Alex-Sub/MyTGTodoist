from __future__ import annotations

from datetime import datetime
import shlex
from zoneinfo import ZoneInfo

from src.config import settings


def parse_meet_args(text: str) -> tuple[str, datetime, int]:
    parts = shlex.split(text)
    if not parts or parts[0] != "/meet":
        raise ValueError("Not a /meet command")

    tokens = parts[1:]
    if len(tokens) < 3:
        raise ValueError("Usage: /meet <title> YYYY-MM-DD HH:MM [duration]")

    duration = 60
    if len(tokens) >= 4 and tokens[-1].isdigit():
        duration = int(tokens[-1])
        time_token = tokens[-2]
        date_token = tokens[-3]
        title_tokens = tokens[:-3]
    else:
        time_token = tokens[-1]
        date_token = tokens[-2]
        title_tokens = tokens[:-2]

    if not title_tokens:
        raise ValueError("Title is required")

    title = " ".join(title_tokens).strip()
    tz = ZoneInfo(settings.timezone)
    scheduled_at = datetime.strptime(f"{date_token} {time_token}", "%Y-%m-%d %H:%M")
    scheduled_at = scheduled_at.replace(tzinfo=tz)

    return title, scheduled_at, duration

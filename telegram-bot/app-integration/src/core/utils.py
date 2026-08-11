from __future__ import annotations

from typing import Any


def norm_text(value: Any) -> str:
    return " ".join(str(value or "").strip().split())


def as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    raw = str(value or "").strip().lower()
    return raw in {"1", "true", "yes", "on", "y"}

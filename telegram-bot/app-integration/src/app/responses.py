from __future__ import annotations

from typing import Any, Dict, Optional


def runtime_response(
    *,
    outcome: str,
    user_message: str,
    command: Optional[Dict[str, Any]] = None,
    rec: Optional[Dict[str, Any]] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "outcome": str(outcome or "").strip(),
        "user_message": str(user_message or "").strip(),
    }
    if command is not None:
        out["command"] = command
    if rec is not None:
        out["rec"] = rec
    if isinstance(extra, dict):
        out.update(extra)
    return out

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class DeliveryResult:
    inserted: bool
    reason: str = ""


def build_idempotency_key(*, source_channel: str, chat_id: str, source_message_id: str, intent: str) -> str:
    ch = str(source_channel or "").strip()
    cid = str(chat_id or "").strip()
    mid = str(source_message_id or "").strip()
    it = str(intent or "").strip().lower()
    if not ch or not cid or not mid or not it:
        return ""
    return f"{ch}:{cid}:{mid}:{it}"


def resolve_idempotency_key(
    *,
    command: Dict[str, Any],
    channel: str,
    chat_id: str,
    source_message_id: Optional[str],
    active_session_key: str = "",
) -> str:
    if str(active_session_key or "").strip():
        return str(active_session_key).strip()
    cmd = command if isinstance(command, dict) else {}
    src_channel = str(cmd.get("__source_channel") or channel or "").strip()
    src_chat = str(cmd.get("__source_chat_id") or chat_id or "").strip()
    src_message = str(cmd.get("__source_message_id") or source_message_id or "").strip()
    intent = str(cmd.get("intent") or "").strip()
    return build_idempotency_key(
        source_channel=src_channel,
        chat_id=src_chat,
        source_message_id=src_message,
        intent=intent,
    )

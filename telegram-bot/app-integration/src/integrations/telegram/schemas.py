from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class TelegramRuntimeRequest:
    request_id: str
    app_id: str
    tenant_id: str
    channel: str
    user_id: str
    chat_id: str
    source_message_id: str
    text: Optional[str] = None
    audio_bytes: Optional[bytes] = None
    audio_mime: Optional[str] = None
    audio_filename: Optional[str] = None
    timezone: str = "UTC"
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class TelegramRuntimeResult:
    outcome: str
    reply_text: Optional[str] = None
    needs_clarification: bool = False
    clarifying_question: Optional[str] = None
    duplicate_state: Optional[str] = None
    telemetry: Dict[str, Any] = field(default_factory=dict)
    raw_runtime_payload: Optional[Dict[str, Any]] = None

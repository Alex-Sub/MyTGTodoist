from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class RuntimeRequest:
    request_id: str
    user_id: str
    channel: str
    text: Optional[str] = None
    source_chat_id: Optional[str] = None
    source_message_id: Optional[str] = None


@dataclass(frozen=True)
class RuntimeResult:
    outcome: str
    user_message: str
    command: Optional[Dict[str, Any]] = None
    rec: Optional[Dict[str, Any]] = None
    extra: Dict[str, Any] = field(default_factory=dict)

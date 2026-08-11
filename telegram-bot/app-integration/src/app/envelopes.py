from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class RuntimeEnvelope:
    request_id: str
    user_id: str
    channel: str
    app_id: str = ""
    tenant_id: str = ""
    source_chat_id: Optional[str] = None
    source_message_id: Optional[str] = None


@dataclass(frozen=True)
class RuntimePayload:
    text: Optional[str] = None
    audio_bytes: Optional[bytes] = None


def envelope_to_dict(env: RuntimeEnvelope) -> Dict[str, Any]:
    return {
        "request_id": env.request_id,
        "user_id": env.user_id,
        "channel": env.channel,
        "app_id": env.app_id,
        "tenant_id": env.tenant_id,
        "source_chat_id": env.source_chat_id,
        "source_message_id": env.source_message_id,
    }

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Protocol


@dataclass(frozen=True)
class BufferEvent:
    id: str
    created_at: str
    user_id: str
    channel: str
    payload_type: str
    payload_ref: str
    transcript: Optional[str]
    normalized_text: Optional[str]
    status: str
    fail_reason: Optional[str]
    delivered_at: Optional[str]
    request_id: str


class CloudBufferAdapter(Protocol):
    def ensure_schema(self) -> None: ...
    def insert_event(self, row: Dict[str, Any]) -> str: ...
    def fetch_pending(self, limit: int) -> List[BufferEvent]: ...
    def mark_delivered(self, event_id: str) -> None: ...
    def mark_needs_review(self, event_id: str, reason: str) -> None: ...
    def mark_failed(self, event_id: str, reason: str) -> None: ...
    def cleanup(self, *, delivered_days: int, keep_last_per_user: int) -> int: ...

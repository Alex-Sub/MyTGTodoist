from __future__ import annotations

from typing import Any, Dict, Optional

from src.app.handler import handle_user_attempt
from src.core.config import AppConfig
from src.reliability.local_db import LocalMainDb


def handle_user_attempt_runtime(
    *,
    cfg: AppConfig,
    tenant_id: str,
    request_id: str,
    user_id: str,
    channel: str,
    asr_client: Any,
    llm_client: Any,
    rec_client: Any,
    audio_bytes: Optional[bytes] = None,
    audio_mime: Optional[str] = None,
    audio_filename: Optional[str] = None,
    text: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
    local_db: Optional[LocalMainDb] = None,
    source_chat_id: Optional[str] = None,
    source_message_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Runtime wrapper that wires production identity/context explicitly.
    """
    tid = str(tenant_id or "").strip()
    if not tid:
        raise ValueError("tenant_id is required")

    ttl = int(cfg.clarification_ttl_sec)
    if ttl <= 0:
        raise ValueError("clarification_ttl_sec must be > 0")
    dedup_ttl = int(cfg.command_dedup_reservation_ttl_sec)
    if dedup_ttl <= 0:
        raise ValueError("command_dedup_reservation_ttl_sec must be > 0")

    return handle_user_attempt(
        request_id=request_id,
        user_id=user_id,
        channel=channel,
        asr_client=asr_client,
        llm_client=llm_client,
        rec_client=rec_client,
        audio_bytes=audio_bytes,
        audio_mime=audio_mime,
        audio_filename=audio_filename,
        text=text,
        metadata=metadata,
        local_db=local_db,
        app_id=cfg.app_id,
        tenant_id=tid,
        source_chat_id=source_chat_id,
        source_message_id=source_message_id,
        clarification_ttl_sec=ttl,
        command_dedup_reservation_ttl_sec=dedup_ttl,
    )

from __future__ import annotations

import json
import uuid
from typing import Any, Dict, Optional

from src.integrations.telegram.schemas import TelegramRuntimeRequest

_BINARY_CONFIRM_CALLBACK_TEXT: Dict[str, Dict[str, str]] = {
    "past": {"yes": "yes", "no": "no"},
    "temporal": {"yes": "yes", "no": "no"},
    "task_create": {"create": "создать", "edit": "исправить", "comment": "Комментарий", "cancel": "не создавать"},
    "task_create_duplicate": {"yes": "создать ещё одну", "no": "не создавать"},
    "meeting_target": {"yes": "yes", "no": "no"},
    "meeting_update_confirm": {"yes": "yes", "no": "no"},
    "timeblock_target": {"yes": "yes", "no": "no"},
    "timeblock_update_confirm": {"yes": "yes", "no": "no"},
    "meeting_comment_target": {"yes": "yes", "no": "no"},
    "meeting_comment_final": {"yes": "yes", "no": "no"},
    "task_comment_target": {"yes": "yes", "no": "no"},
    "task_comment_final": {"yes": "yes", "no": "no"},
    "ambiguous_create": {
        "meeting": "Событие в календаре",
        "timeblock": "Блок времени",
        "task": "Задачу",
    },
}
_TEMPORAL_EDIT_CALLBACK_TEXT: Dict[str, str] = {
    "date": "дата",
    "time": "время",
    "duration": "длительность",
    "comment": "Комментарий",
    "cancel": "отмена",
}
_TEMPORAL_COMMENT_CALLBACK_TEXT: Dict[str, str] = {
    "keep": "оставить без изменений",
    "clear": "очистить комментарий",
}
_MEETING_UPDATE_EDIT_CALLBACK_TEXT: Dict[str, str] = {
    "date": "дата",
    "time": "время",
    "duration": "длительность",
    "comment": "Комментарий",
    "date_time": "дата и время",
    "cancel": "отмена",
}
_TIMEBLOCK_UPDATE_EDIT_CALLBACK_TEXT: Dict[str, str] = {
    "date": "дата",
    "time": "время",
    "duration": "длительность",
    "comment": "Комментарий",
    "date_time": "дата и время",
    "cancel": "отмена",
}
_SYNC_CONFLICT_CALLBACK_TEXT: Dict[str, str] = {
    "delete_db": "удалить из базы",
    "restore_calendar": "восстановить в календаре",
    "edit": "изменить",
    "skip": "пропустить",
}


def _safe_raw_update(update: Dict[str, Any], *, max_bytes: int = 4096) -> Optional[Dict[str, Any]]:
    try:
        raw = json.dumps(update, ensure_ascii=False)
    except Exception:
        return None
    if len(raw.encode("utf-8")) > max_bytes:
        return None
    return update


def _request_id(update: Dict[str, Any], message: Dict[str, Any], chat_id: str, message_id: str) -> str:
    update_id = str(update.get("update_id") or "").strip()
    if update_id and chat_id and message_id:
        return f"tg:{update_id}:{chat_id}:{message_id}"
    return str(uuid.uuid4())


def _decode_callback_data_to_text(callback_data: str) -> Optional[str]:
    raw = str(callback_data or "").strip()
    if not raw:
        return None
    if raw.startswith("sync_conflict:"):
        parts = raw.split(":")
        if len(parts) == 3:
            mapped = _SYNC_CONFLICT_CALLBACK_TEXT.get(str(parts[1] or "").strip().lower())
            return mapped if isinstance(mapped, str) else raw
        return raw
    parts = raw.split(":")
    if len(parts) != 4 or parts[0] != "clarify" or parts[1] != "v1":
        return raw
    family = parts[2]
    value = parts[3]
    binary_family = _BINARY_CONFIRM_CALLBACK_TEXT.get(family)
    if isinstance(binary_family, dict):
        mapped = binary_family.get(value)
        return mapped if isinstance(mapped, str) else raw
    if family == "temporal_edit":
        mapped = _TEMPORAL_EDIT_CALLBACK_TEXT.get(value)
        return mapped if isinstance(mapped, str) else raw
    if family == "meeting_update_edit":
        mapped = _MEETING_UPDATE_EDIT_CALLBACK_TEXT.get(value)
        return mapped if isinstance(mapped, str) else raw
    if family == "timeblock_update_edit":
        mapped = _TIMEBLOCK_UPDATE_EDIT_CALLBACK_TEXT.get(value)
        return mapped if isinstance(mapped, str) else raw
    if family == "temporal_comment":
        mapped = _TEMPORAL_COMMENT_CALLBACK_TEXT.get(value)
        return mapped if isinstance(mapped, str) else raw
    if family == "duration" and value in {"15", "30", "45", "60"}:
        return value
    return raw


def _pick_message(update: Dict[str, Any]) -> Dict[str, Any]:
    msg = update.get("message")
    if isinstance(msg, dict):
        return msg
    edited = update.get("edited_message")
    if isinstance(edited, dict):
        return edited
    callback = update.get("callback_query")
    if isinstance(callback, dict):
        callback_message = callback.get("message")
        if isinstance(callback_message, dict):
            return callback_message
    return {}


def map_update_to_request(
    update: Dict[str, Any],
    *,
    app_id: str,
    tenant_id: str,
    timezone: str = "UTC",
    channel: str = "telegram",
) -> TelegramRuntimeRequest:
    if not isinstance(update, dict):
        raise TypeError("telegram update must be a dict")

    callback_query = update.get("callback_query")
    callback_query = callback_query if isinstance(callback_query, dict) else None
    message = _pick_message(update)
    chat = message.get("chat") if isinstance(message.get("chat"), dict) else {}
    sender = message.get("from") if isinstance(message.get("from"), dict) else {}
    if callback_query and isinstance(callback_query.get("from"), dict):
        sender = callback_query.get("from")  # type: ignore[assignment]
    voice = message.get("voice") if isinstance(message.get("voice"), dict) else None

    chat_id = str(chat.get("id") or "")
    user_id = str(sender.get("id") or chat_id or "")
    message_id = str(message.get("message_id") or "")
    request_id = _request_id(update, message, chat_id, message_id)

    text_raw = message.get("text")
    if callback_query is not None:
        text_raw = _decode_callback_data_to_text(str(callback_query.get("data") or ""))
    text = text_raw.strip() if isinstance(text_raw, str) else None
    if text == "":
        text = None

    audio_mime = None
    audio_filename = None
    metadata: Dict[str, Any] = {"update_id": str(update.get("update_id") or "")}
    metadata["flow_id"] = request_id
    if callback_query is not None:
        metadata["callback_query"] = {
            "id": str(callback_query.get("id") or ""),
            "data": str(callback_query.get("data") or ""),
        }
    if voice:
        audio_mime = str(voice.get("mime_type") or "audio/ogg")
        voice_file_id = str(voice.get("file_id") or "").strip()
        audio_filename = f"{voice_file_id or 'voice'}.ogg"
        metadata["voice"] = {
            "file_id": voice_file_id,
            "file_unique_id": str(voice.get("file_unique_id") or ""),
            "duration": int(voice.get("duration") or 0),
            "mime_type": audio_mime,
        }

    raw_update = _safe_raw_update(update)
    if raw_update is not None:
        metadata["raw_update"] = raw_update

    return TelegramRuntimeRequest(
        request_id=request_id,
        app_id=app_id,
        tenant_id=tenant_id,
        channel=channel,
        user_id=user_id,
        chat_id=chat_id,
        source_message_id=message_id,
        text=text,
        audio_mime=audio_mime,
        audio_filename=audio_filename,
        timezone=timezone,
        metadata=metadata,
    )

from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass
from typing import Callable, Optional

LOG = logging.getLogger(__name__)

_SHORT_CLARIFICATION_WORDS = {
    "да",
    "нет",
    "сегодня",
    "завтра",
}

_NOISE_TOKENS = {
    "а",
    "э",
    "ээ",
    "эээ",
    "эм",
    "мм",
    "ммм",
    "угу",
    "ага",
    "...",
}


@dataclass
class VoiceProcessingResult:
    ok: bool
    text: str
    status: str
    debug_reason: str | None = None
    user_message: str | None = None


def _fallback_message_for_status(status: str) -> str:
    st = str(status or "").strip().lower()
    if st == "empty_transcript":
        return "Не удалось разобрать голос. Повтори ещё раз чуть длиннее."
    if st == "low_quality_transcript":
        return "Распозналась только часть фразы. Повтори команду одной фразой."
    if st in {"asr_timeout", "asr_failed"}:
        return "Сейчас не получилось обработать голос. Попробуй ещё раз или напиши текстом."
    if st in {"download_failed", "convert_failed"}:
        return "Не удалось обработать аудио. Попробуй отправить голос ещё раз."
    return "Сейчас не получилось обработать голос. Попробуй ещё раз или напиши текстом."


def _is_time_or_date_fragment(text: str) -> bool:
    t = str(text or "").strip().lower()
    if not t:
        return False
    patterns = (
        r"^\d{1,2}:\d{2}$",
        r"^\d{1,2}[./-]\d{1,2}([./-]\d{2,4})?$",
        r"^\d{4}-\d{2}-\d{2}$",
    )
    return any(re.fullmatch(p, t) for p in patterns)


def _looks_like_noise(text: str) -> bool:
    t = str(text or "").strip().lower()
    if not t:
        return True
    if t in _NOISE_TOKENS:
        return True
    if re.fullmatch(r"[^\w\d]+", t):
        return True
    if re.fullmatch(r"(.)\1{3,}", t):
        return True
    return False


def _is_short_numeric(text: str) -> bool:
    return bool(re.fullmatch(r"\d{1,4}", str(text or "").strip()))


def _is_valid_temporal_or_command_fragment(text: str) -> bool:
    t = str(text or "").strip().lower()
    if _is_short_numeric(t):
        return True
    if _is_time_or_date_fragment(t):
        return True
    return False


def _preview(text: str, *, max_len: int = 32) -> str:
    s = str(text or "").replace("\n", " ").strip()
    if len(s) <= max_len:
        return s
    return f"{s[:max_len]}..."


def assess_transcript_quality(
    text: str,
    *,
    is_clarification_reply: bool,
) -> str:
    normalized = str(text or "").strip()
    lowered = normalized.lower()
    if not normalized:
        return "retryable_failure"

    if is_clarification_reply:
        if lowered in _SHORT_CLARIFICATION_WORDS:
            return "ok"
        if _is_valid_temporal_or_command_fragment(normalized):
            return "ok"
        if len(normalized) < 2:
            return "retryable_failure"
        if _looks_like_noise(normalized):
            return "retryable_failure"
        return "ok"

    if len(normalized) < 3:
        return "retryable_failure"
    if _looks_like_noise(normalized):
        return "retryable_failure"
    if not re.search(r"[A-Za-zА-Яа-яЁё]", normalized) and not _is_valid_temporal_or_command_fragment(normalized):
        return "retryable_failure"

    parts = [part for part in re.split(r"\s+", lowered) if part]
    if len(parts) == 1 and len(parts[0]) <= 3:
        return "needs_user_repeat"
    return "ok"


def _result(
    *,
    status: str,
    text: str = "",
    debug_reason: str | None = None,
) -> VoiceProcessingResult:
    st = str(status or "").strip().lower() or "unexpected_error"
    return VoiceProcessingResult(
        ok=(st == "ok"),
        text=str(text or "").strip(),
        status=st,
        debug_reason=debug_reason,
        user_message=None if st == "ok" else _fallback_message_for_status(st),
    )


def process_voice_pipeline(
    *,
    request_id: str,
    user_id: str,
    is_clarification_reply: bool,
    asr_transcribe: Callable[[bytes], str],
    audio_bytes: bytes | None = None,
    download_audio: Optional[Callable[[], bytes]] = None,
    prepare_audio: Optional[Callable[[bytes], bytes]] = None,
) -> VoiceProcessingResult:
    started = time.perf_counter()
    rid = str(request_id or "").strip()
    uid = str(user_id or "").strip()
    LOG.info(
        "voice_received",
        extra={
            "request_id": rid,
            "user_id": uid,
            "is_clarification_reply": bool(is_clarification_reply),
        },
    )

    try:
        LOG.info("voice_download_started", extra={"request_id": rid, "user_id": uid})
        if download_audio is not None:
            raw_audio = download_audio()
        else:
            raw_audio = audio_bytes
        if not raw_audio:
            out = _result(status="download_failed", debug_reason="audio_bytes_empty")
            LOG.warning(
                "voice_pipeline_completed",
                extra={
                    "request_id": rid,
                    "user_id": uid,
                    "duration_ms": round((time.perf_counter() - started) * 1000, 2),
                    "transcript_len": 0,
                    "final_status": out.status,
                    "reason_code": out.debug_reason,
                },
            )
            return out
        LOG.info(
            "voice_download_ok",
            extra={"request_id": rid, "user_id": uid, "audio_bytes_len": len(raw_audio)},
        )
    except Exception as exc:
        out = _result(status="download_failed", debug_reason=type(exc).__name__)
        LOG.warning(
            "voice_pipeline_completed",
            extra={
                "request_id": rid,
                "user_id": uid,
                "duration_ms": round((time.perf_counter() - started) * 1000, 2),
                "transcript_len": 0,
                "final_status": out.status,
                "reason_code": out.debug_reason,
            },
        )
        return out

    try:
        prepared_audio = prepare_audio(raw_audio) if prepare_audio is not None else raw_audio
        if not prepared_audio:
            out = _result(status="convert_failed", debug_reason="prepared_audio_empty")
            LOG.warning(
                "voice_pipeline_completed",
                extra={
                    "request_id": rid,
                    "user_id": uid,
                    "duration_ms": round((time.perf_counter() - started) * 1000, 2),
                    "transcript_len": 0,
                    "final_status": out.status,
                    "reason_code": out.debug_reason,
                },
            )
            return out
        LOG.info(
            "voice_convert_ok",
            extra={"request_id": rid, "user_id": uid, "audio_bytes_len": len(prepared_audio)},
        )
    except Exception as exc:
        out = _result(status="convert_failed", debug_reason=type(exc).__name__)
        LOG.warning(
            "voice_pipeline_completed",
            extra={
                "request_id": rid,
                "user_id": uid,
                "duration_ms": round((time.perf_counter() - started) * 1000, 2),
                "transcript_len": 0,
                "final_status": out.status,
                "reason_code": out.debug_reason,
            },
        )
        return out

    final_result: VoiceProcessingResult | None = None
    for attempt in (1, 2):
        LOG.info("asr_attempt_%s", attempt, extra={"request_id": rid, "user_id": uid})
        try:
            transcript = str(asr_transcribe(prepared_audio) or "").strip()
        except TimeoutError:
            if attempt == 1:
                continue
            final_result = _result(status="asr_timeout", debug_reason="timeout")
            break
        except Exception as exc:
            final_result = _result(status="asr_failed", debug_reason=type(exc).__name__)
            break

        LOG.info(
            "asr_transcript_received",
            extra={
                "request_id": rid,
                "user_id": uid,
                "attempt": attempt,
                "transcript_len": len(transcript),
                "transcript_preview": _preview(transcript),
            },
        )
        if not transcript:
            if attempt == 1:
                continue
            final_result = _result(status="empty_transcript", debug_reason="empty_after_retry")
            break

        quality = assess_transcript_quality(transcript, is_clarification_reply=is_clarification_reply)
        if quality == "ok":
            final_result = _result(status="ok", text=transcript)
            break

        LOG.info(
            "voice_quality_gate_failed",
            extra={
                "request_id": rid,
                "user_id": uid,
                "attempt": attempt,
                "quality_result": quality,
                "transcript_len": len(transcript),
                "transcript_preview": _preview(transcript),
            },
        )
        if quality == "retryable_failure" and attempt == 1:
            continue
        final_result = _result(status="low_quality_transcript", debug_reason=quality)
        break

    if final_result is None:
        final_result = _result(status="unexpected_error", debug_reason="final_result_missing")

    log_level = logging.INFO if final_result.ok else logging.WARNING
    LOG.log(
        log_level,
        "voice_pipeline_completed",
        extra={
            "request_id": rid,
            "user_id": uid,
            "duration_ms": round((time.perf_counter() - started) * 1000, 2),
            "transcript_len": len(final_result.text or ""),
            "final_status": final_result.status,
            "reason_code": final_result.debug_reason,
        },
    )
    return final_result

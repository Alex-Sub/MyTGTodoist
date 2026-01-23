from __future__ import annotations

import httpx
from loguru import logger

from src.core.asr_config import load_asr_config


async def asr_transcribe(
    ogg_bytes: bytes,
    filename: str = "voice.ogg",
    language: str = "ru",
) -> str:
    config = load_asr_config()
    asr_url = config.url
    asr_api_key = config.api_key
    timeout_raw = config.timeout_seconds

    if not asr_url or not asr_api_key:
        logger.error("ASR client is not configured")
        return ""

    try:
        timeout = float(timeout_raw) if timeout_raw else 180.0
    except ValueError:
        timeout = 180.0

    request_url = f"{asr_url.rstrip('/')}/v1/asr/transcribe?language={language}"
    logger.info("ASR client config url={} key_set={}", asr_url, bool(asr_api_key))
    logger.info(
        "ASR request: url={} timeout={} key_len={}",
        request_url,
        timeout,
        len(asr_api_key),
    )
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(timeout), trust_env=False) as client:
            response = await client.post(
                request_url,
                headers={"X-API-KEY": asr_api_key},
                files={"file": (filename, ogg_bytes, "audio/ogg")},
            )
    except httpx.RequestError as exc:
        logger.warning("ASR request failed: {}", exc)
        return ""

    if response.status_code != 200:
        logger.warning(
            "ASR request failed status={} headers={} body_head={!r}",
            response.status_code,
            dict(response.headers),
            (response.text or "")[:200],
        )
        return ""

    try:
        payload = response.json()
    except ValueError:
        logger.warning("ASR response JSON parse error")
        return ""
    text = (payload.get("text") or "").strip()
    logger.info("ASR request ok status=200 text_len={}", len(text))
    return text

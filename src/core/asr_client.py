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
    ml_core_url = config.ml_core_url
    asr_api_key = config.api_key
    timeout_raw = config.timeout_seconds

    if not ml_core_url:
        logger.error("ML gateway client is not configured (ML_CORE_URL is empty)")
        return ""

    try:
        timeout = float(timeout_raw) if timeout_raw else 180.0
    except ValueError:
        timeout = 180.0

    request_url = f"{ml_core_url.rstrip('/')}/voice-command"
    logger.info("ML gateway config url={} key_set={}", ml_core_url, bool(asr_api_key))
    logger.info(
        "ML voice request: url={} timeout={} key_len={}",
        request_url,
        timeout,
        len(asr_api_key),
    )
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(timeout), trust_env=False) as client:
            response = await client.post(
                request_url,
                headers={"X-API-KEY": asr_api_key} if asr_api_key else {},
                files={"file": (filename, ogg_bytes, "audio/ogg")},
            )
    except httpx.RequestError as exc:
        logger.warning("ML voice request failed: {}", exc)
        return ""

    if response.status_code != 200:
        logger.warning("ML voice request failed status={} body_head={!r}", response.status_code, (response.text or "")[:200])
        return ""

    try:
        payload = response.json()
    except ValueError:
        logger.warning("ML voice response JSON parse error")
        return ""
    text = (payload.get("text") or "").strip()
    if not text:
        command = payload.get("command")
        if isinstance(command, dict):
            text = str(
                command.get("text")
                or command.get("text_normalized")
                or command.get("utterance")
                or ""
            ).strip()
    logger.info("ML voice request ok status=200 text_len={}", len(text))
    return text

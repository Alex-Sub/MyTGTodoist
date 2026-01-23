from __future__ import annotations

import os
from pathlib import Path
from uuid import uuid4

from fastapi import APIRouter, UploadFile
from loguru import logger

from src.core.asr import convert_to_wav16k_mono, transcribe_wav

router = APIRouter()


@router.post("/asr/telegram")
async def asr_telegram(file: UploadFile) -> dict:
    data = await file.read()
    size = len(data)
    logger.info("ASR file_bytes={}", size)

    ext = Path(file.filename or "").suffix or ".ogg"
    tmp_dir = Path("data/cache/voice/tmp")
    tmp_dir.mkdir(parents=True, exist_ok=True)
    uid = uuid4().hex
    ogg_path = tmp_dir / f"{uid}{ext}"
    wav_path = tmp_dir / f"{uid}.wav"

    try:
        ogg_path.write_bytes(data)
        convert_to_wav16k_mono(str(ogg_path), str(wav_path))
        text = transcribe_wav(str(wav_path))
        logger.info("ASR text_head={}", text[:50])
    except Exception as exc:
        logger.error("ASR failed: {}", exc)
        text = ""
    finally:
        for path in (ogg_path, wav_path):
            try:
                path.unlink(missing_ok=True)
            except OSError:
                pass

    return {"text": text, "lang": "ru"}

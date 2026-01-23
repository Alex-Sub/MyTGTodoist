from __future__ import annotations

import os
import subprocess
from pathlib import Path


def convert_to_wav16k_mono(input_path: str, wav_path: str) -> None:
    ffmpeg_bin = os.getenv("FFMPEG_BIN", "ffmpeg")
    command = [
        ffmpeg_bin,
        "-y",
        "-i",
        input_path,
        "-ac",
        "1",
        "-ar",
        "16000",
        "-c:a",
        "pcm_s16le",
        wav_path,
    ]
    subprocess.run(command, check=True, capture_output=True)


def transcribe_wav(wav_path: str) -> str:
    whisper_bin = os.getenv("ASR_WHISPER_BIN")
    model_path = os.getenv("ASR_MODEL_PATH")
    if not whisper_bin or not model_path:
        return ""

    wav = Path(wav_path)
    out_base = wav.with_suffix("")
    command = [
        whisper_bin,
        "-m",
        model_path,
        "-f",
        str(wav),
        "-l",
        "ru",
        "-otxt",
        "-of",
        str(out_base),
    ]
    try:
        subprocess.run(command, check=True, capture_output=True)
    except subprocess.SubprocessError:
        return ""

    txt_path = out_base.with_suffix(".txt")
    try:
        text = txt_path.read_text(encoding="utf-8").strip()
    except OSError:
        return ""
    finally:
        try:
            txt_path.unlink(missing_ok=True)
        except OSError:
            pass

    return text

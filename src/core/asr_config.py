from __future__ import annotations

import os
from functools import lru_cache
from dataclasses import dataclass


@dataclass(frozen=True)
class ASRConfig:
    url: str
    api_key: str
    timeout_seconds: float


@lru_cache(maxsize=1)
def load_asr_config() -> ASRConfig:
    url = os.getenv("ASR_URL", "").strip() or os.getenv("ASR_SERVICE_URL", "").strip()
    api_key = os.getenv("ASR_API_KEY", "").strip()
    raw = os.getenv("ASR_TIMEOUT_SECONDS", "180").strip()
    try:
        timeout = float(raw) if raw else 180.0
    except ValueError:
        timeout = 180.0
    return ASRConfig(url=url, api_key=api_key, timeout_seconds=timeout)

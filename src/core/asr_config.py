from __future__ import annotations

import os
from functools import lru_cache
from dataclasses import dataclass


@dataclass(frozen=True)
class ASRConfig:
    ml_gateway_url: str
    api_key: str
    timeout_seconds: float
    legacy_asr_url: str


@lru_cache(maxsize=1)
def load_asr_config() -> ASRConfig:
    ml_gateway_url = os.getenv("ML_GATEWAY_URL", "").strip()
    # ASR_SERVICE_URL retired: keep only ASR_URL as legacy ASR endpoint input.
    legacy_asr_url = os.getenv("ASR_URL", "").strip()
    api_key = os.getenv("ASR_API_KEY", "").strip()
    raw = os.getenv("ASR_TIMEOUT_SECONDS", "180").strip()
    try:
        timeout = float(raw) if raw else 180.0
    except ValueError:
        timeout = 180.0
    return ASRConfig(
        ml_gateway_url=ml_gateway_url,
        api_key=api_key,
        timeout_seconds=timeout,
        legacy_asr_url=legacy_asr_url,
    )

from __future__ import annotations

import hashlib
import json
import os
import time
import urllib.request
from dataclasses import dataclass
from typing import Any, Dict, Optional

import yaml


def _sha256_bytes(b: bytes) -> str:
    h = hashlib.sha256()
    h.update(b)
    return h.hexdigest()


def _read_file(path: str) -> bytes:
    with open(path, "rb") as f:
        return f.read()


@dataclass
class DictLoadResult:
    data: Dict[str, Any]
    sha256: str
    source: str


def load_yaml_or_json_bytes(raw: bytes) -> Dict[str, Any]:
    try:
        obj = yaml.safe_load(raw.decode("utf-8"))
    except Exception:
        obj = None
    if isinstance(obj, dict):
        return obj
    try:
        obj2 = json.loads(raw.decode("utf-8"))
    except Exception:
        obj2 = None
    if isinstance(obj2, dict):
        return obj2
    return {}


def load_global_dict(*, url: str, path: str, cache_path: str) -> DictLoadResult:
    """
    Loads global dictionary from URL or file.
    Caches last successful load by sha256.
    """
    os.makedirs(os.path.dirname(os.path.abspath(cache_path)), exist_ok=True)

    if path:
        raw = _read_file(path)
        sha = _sha256_bytes(raw)
        return DictLoadResult(data=load_yaml_or_json_bytes(raw), sha256=sha, source=f"file:{path}")

    if url:
        try:
            with urllib.request.urlopen(url, timeout=10) as resp:
                raw = resp.read()
        except Exception:
            # fallback to cache
            if os.path.exists(cache_path):
                cached = json.loads(_read_file(cache_path).decode("utf-8"))
                return DictLoadResult(data=cached.get("data") or {}, sha256=str(cached.get("sha256") or ""), source="cache")
            raise

        sha = _sha256_bytes(raw)
        data = load_yaml_or_json_bytes(raw)
        with open(cache_path, "w", encoding="utf-8") as f:
            json.dump({"sha256": sha, "fetched_at": int(time.time()), "data": data}, f, ensure_ascii=False)
        return DictLoadResult(data=data, sha256=sha, source=f"url:{url}")

    return DictLoadResult(data={}, sha256="", source="none")

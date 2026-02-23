from __future__ import annotations

from pathlib import Path

CANON_MOUNT_HINT = "canon not mounted: add ./canon:/canon:ro to compose"


def ensure_canon_mounted(canon_path: str | Path = "/canon/intents_v2.yml") -> Path:
    path = Path(canon_path)
    if not path.exists():
        raise RuntimeError(f"{CANON_MOUNT_HINT}; missing file: {path}")
    if not path.is_file():
        raise RuntimeError(f"{CANON_MOUNT_HINT}; path is not a file: {path}")
    return path

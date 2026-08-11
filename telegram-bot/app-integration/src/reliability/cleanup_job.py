from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from .queue_store import CloudBufferAdapter


@dataclass
class CleanupCfg:
    delivered_days: int = 7
    keep_last_per_user: int = 200


def run_cleanup(cloud: CloudBufferAdapter, cfg: Optional[CleanupCfg] = None) -> int:
    c = cfg or CleanupCfg()
    return cloud.cleanup(delivered_days=int(c.delivered_days), keep_last_per_user=int(c.keep_last_per_user))


def cleanup_once(cloud: CloudBufferAdapter, *, cleanup_days: int = 7, keep_per_user: Optional[int] = 200):
    """
    Small wrapper for tests and one-shot runs (without CLI).
    """
    deleted = cloud.cleanup(
        delivered_days=int(cleanup_days),
        keep_last_per_user=int(keep_per_user or 0),
    )
    return {"deleted": int(deleted)}

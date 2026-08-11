from __future__ import annotations

from .buffer_adapter import CloudBufferAdapter
from .buffer_postgres import PostgresCloudBuffer
from .buffer_sqlite import SqliteCloudBuffer


def create_cloud_buffer(dsn: str) -> CloudBufferAdapter:
    s = (dsn or "").strip()
    if s.startswith("sqlite:///"):
        return SqliteCloudBuffer(s)
    if s.startswith("postgres://") or s.startswith("postgresql://"):
        return PostgresCloudBuffer(s)
    raise ValueError("unsupported CLOUD_BUFFER_DSN (use sqlite:///... or postgres://...)")

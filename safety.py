from __future__ import annotations

from pathlib import Path

from config import ALLOWED_ROOTS


class FileServiceError(ValueError):
    """User-facing error for invalid filesystem operations."""


def _resolved_allowed_roots() -> list[Path]:
    return [root.resolve() for root in ALLOWED_ROOTS]


def ensure_within_allowed_roots(path: Path) -> Path:
    """Resolve path and ensure it is inside one of the configured roots."""
    resolved = path.resolve()

    for root in _resolved_allowed_roots():
        if resolved == root or root in resolved.parents:
            return resolved

    raise FileServiceError(
        f"Access denied: '{resolved}' is outside allowed roots."
    )


def validate_requested_path(raw_path: str | Path) -> Path:
    if not raw_path:
        raise FileServiceError("Path is required.")
    return ensure_within_allowed_roots(Path(raw_path))


def validate_root_or_all(raw_root: str | Path | None) -> list[Path]:
    if raw_root is None:
        return _resolved_allowed_roots()

    resolved = validate_requested_path(raw_root)
    if not resolved.exists() or not resolved.is_dir():
        raise FileServiceError(f"Root path is not an existing directory: '{resolved}'.")
    return [resolved]

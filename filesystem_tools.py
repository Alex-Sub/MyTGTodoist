from __future__ import annotations

import os
from pathlib import Path

from config import (
    DEFAULT_MAX_CHARS,
    DEFAULT_MAX_RESULTS,
    DEFAULT_TREE_DEPTH,
    EXCLUDED_DIRS,
    TEXT_FILE_EXTENSIONS,
    TEXT_FILE_NAMES,
)
from safety import (
    FileServiceError,
    ensure_within_allowed_roots,
    validate_requested_path,
    validate_root_or_all,
)


def _sort_entries(entries: list[Path]) -> list[Path]:
    return sorted(entries, key=lambda item: (not item.is_dir(), item.name.lower()))


def _is_text_file(path: Path) -> bool:
    name_lower = path.name.lower()
    if name_lower in TEXT_FILE_NAMES:
        return True
    return path.suffix.lower() in TEXT_FILE_EXTENSIONS


def _iter_paths_for_search(roots: list[Path]):
    for root in roots:
        for current_root, dir_names, file_names in os.walk(root, topdown=True):
            dir_names[:] = [d for d in dir_names if d not in EXCLUDED_DIRS]
            current = Path(current_root)
            yield current
            for file_name in file_names:
                yield current / file_name


def list_dir(path: str) -> dict:
    target = validate_requested_path(path)
    if not target.exists():
        raise FileServiceError(f"Directory not found: '{target}'.")
    if not target.is_dir():
        raise FileServiceError(f"Path is not a directory: '{target}'.")

    children = _sort_entries(list(target.iterdir()))
    entries = [
        {
            "name": child.name,
            "path": str(child),
            "type": "dir" if child.is_dir() else "file",
        }
        for child in children
    ]

    return {
        "path": str(target),
        "items": entries,
    }


def find_files(query: str, root: str | None = None, max_results: int = DEFAULT_MAX_RESULTS) -> dict:
    if not query:
        raise FileServiceError("Query is required.")
    if max_results <= 0:
        raise FileServiceError("max_results must be > 0.")

    roots = validate_root_or_all(root)
    query_lower = query.lower()
    matches: list[dict[str, str]] = []

    for item in _iter_paths_for_search(roots):
        if query_lower in item.name.lower():
            matches.append(
                {
                    "path": str(item),
                    "type": "dir" if item.is_dir() else "file",
                }
            )
            if len(matches) >= max_results:
                break

    return {
        "query": query,
        "root": str(root) if root else None,
        "results": matches,
        "count": len(matches),
        "max_results": max_results,
    }


def read_text_file(path: str, max_chars: int = DEFAULT_MAX_CHARS) -> dict:
    if max_chars <= 0:
        raise FileServiceError("max_chars must be > 0.")

    target = validate_requested_path(path)
    if not target.exists() or not target.is_file():
        raise FileServiceError(f"File not found: '{target}'.")

    if not _is_text_file(target):
        raise FileServiceError(
            "File type is not allowed for read_text_file. "
            "Allowed: .py, .md, .txt, .json, .yaml, .yml, .toml, .ini, .env, .env.example, .cfg, .csv, .log"
        )

    content = target.read_text(encoding="utf-8", errors="replace")
    truncated = len(content) > max_chars
    if truncated:
        content = content[:max_chars]

    return {
        "path": str(target),
        "content": content,
        "truncated": truncated,
    }


def _build_tree(path: Path, depth: int) -> dict:
    node = {
        "name": path.name,
        "path": str(path),
        "type": "dir" if path.is_dir() else "file",
    }

    if depth <= 0 or not path.is_dir():
        return node

    children = []
    for child in _sort_entries(list(path.iterdir())):
        try:
            safe_child = ensure_within_allowed_roots(child)
        except FileServiceError:
            continue

        if child.is_dir() and child.name in EXCLUDED_DIRS:
            continue
        children.append(_build_tree(safe_child, depth - 1))

    node["children"] = children
    return node


def project_tree(path: str, depth: int = DEFAULT_TREE_DEPTH) -> dict:
    if depth < 0:
        raise FileServiceError("depth must be >= 0.")

    target = validate_requested_path(path)
    if not target.exists():
        raise FileServiceError(f"Path not found: '{target}'.")

    return _build_tree(target, depth)


def grep_text(query: str, root: str | None = None, max_results: int = DEFAULT_MAX_RESULTS) -> dict:
    if not query:
        raise FileServiceError("Query is required.")
    if max_results <= 0:
        raise FileServiceError("max_results must be > 0.")

    roots = validate_root_or_all(root)
    query_lower = query.lower()
    matches: list[dict[str, str | int]] = []

    for item in _iter_paths_for_search(roots):
        if item.is_dir() or not _is_text_file(item):
            continue

        try:
            safe_item = ensure_within_allowed_roots(item)
        except FileServiceError:
            continue

        try:
            with safe_item.open("r", encoding="utf-8", errors="replace") as handle:
                for line_number, line in enumerate(handle, start=1):
                    if query_lower in line.lower():
                        matches.append(
                            {
                                "path": str(safe_item),
                                "line_number": line_number,
                                "matched_line": line.rstrip("\n\r"),
                            }
                        )
                        if len(matches) >= max_results:
                            return {
                                "query": query,
                                "root": str(root) if root else None,
                                "results": matches,
                                "count": len(matches),
                                "max_results": max_results,
                            }
        except OSError as exc:
            raise FileServiceError(f"Cannot read file '{safe_item}': {exc}") from exc

    return {
        "query": query,
        "root": str(root) if root else None,
        "results": matches,
        "count": len(matches),
        "max_results": max_results,
    }

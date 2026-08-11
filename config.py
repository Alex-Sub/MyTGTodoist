from __future__ import annotations

from pathlib import Path

# Default allowed roots for v1. Extend this list if more roots are needed.
ALLOWED_ROOTS: list[Path] = [
    Path(r"D:\My_AI_Prodgekt\MyTGTodoist"),
    Path(r"D:\VMShare"),
]

# Directories excluded from recursive traversal.
EXCLUDED_DIRS: set[str] = {
    ".git",
    ".venv",
    "__pycache__",
    "node_modules",
    "dist",
    "build",
    ".mypy_cache",
    ".pytest_cache",
}

# Text file extensions allowed for reading and grep.
TEXT_FILE_EXTENSIONS: set[str] = {
    ".py",
    ".md",
    ".txt",
    ".json",
    ".yaml",
    ".yml",
    ".toml",
    ".ini",
    ".cfg",
    ".csv",
    ".log",
}

# Extra text file names allowed regardless of extension.
TEXT_FILE_NAMES: set[str] = {
    ".env",
    ".env.example",
}

DEFAULT_MAX_RESULTS = 100
DEFAULT_MAX_CHARS = 200_000
DEFAULT_TREE_DEPTH = 3

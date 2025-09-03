# utils/file_ops.py
# Lightweight filesystem helpers used across the project.

from __future__ import annotations

import re
from pathlib import Path
from typing import Union

PathLike = Union[str, Path]

__all__ = ["safe_name", "write_text"]


def safe_name(s: str) -> str:
    """
    Convert an arbitrary string into a filesystem-safe name.
    - Replaces non [a-zA-Z0-9_-] with underscores.
    - Trims to 80 characters.
    - Never returns an empty string.

    Examples:
        safe_name("GetFile (v1)/beta!") -> "GetFile_v1_beta"
    """
    s = (s or "unnamed").strip()
    s = re.sub(r"[^a-zA-Z0-9_\-]+", "_", s)
    s = s[:80].strip("_")
    return s or "unnamed"


def write_text(path: PathLike, text: str, *, encoding: str = "utf-8") -> None:
    """
    Write text to a file, creating parent directories if needed.
    """
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(text, encoding=encoding)


# Removed read_text function - no longer used after migration guide consolidation

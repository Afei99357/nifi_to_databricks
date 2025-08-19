# utils/file_ops.py
# Lightweight filesystem helpers used across the project.

from __future__ import annotations

import re
from pathlib import Path
from typing import Union
import os, requests, base64
import codecs

PathLike = Union[str, Path]

__all__ = ["safe_name", "write_text", "read_text"]


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


def read_text(path: str) -> str:
    path = str(path)  # <-- normalize PosixPath to str

    # Handle Workspace API path
    if path.startswith("/Workspace/"):
        host = os.environ.get("DATABRICKS_HOST")
        token = os.environ.get("DATABRICKS_TOKEN")
        if not (host and token):
            raise ValueError("Need DATABRICKS_HOST and DATABRICKS_TOKEN for /Workspace paths")

        url = host if host.startswith("http") else f"https://{host}"
        resp = requests.get(
            f"{url}/api/2.0/workspace/export",
            headers={"Authorization": f"Bearer {token}"},
            params={"path": path, "format": "SOURCE"},
            timeout=60,
        )
        resp.raise_for_status()
        raw = base64.b64decode(resp.json()["content"])
    else:
        raw = Path(path).read_bytes()

    # Try UTF-8 first, fall back to UTF-16, then Latin-1
    for enc in ("utf-8-sig", "utf-16", "utf-8", "latin-1"):
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            continue

    # Last resort: replace bad chars
    return raw.decode("utf-8", errors="replace")


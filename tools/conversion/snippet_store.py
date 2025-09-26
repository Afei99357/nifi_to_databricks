"""Helpers for persisting per-processor Databricks snippets."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable

SNIPPET_STORE_DIR = Path("derived_processor_snippets")
SNIPPET_STORE_FILE = SNIPPET_STORE_DIR / "snippets.json"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_snippet_store() -> Dict[str, Any]:
    if SNIPPET_STORE_FILE.exists():
        with SNIPPET_STORE_FILE.open("r", encoding="utf-8") as fh:
            try:
                payload = json.load(fh)
                if isinstance(payload, dict):
                    payload.setdefault("snippets", {})
                    return payload
            except json.JSONDecodeError:
                pass
    return {"snippets": {}, "metadata": {"created": _now_iso(), "version": 1}}


def save_snippet_store(store: Dict[str, Any]) -> None:
    SNIPPET_STORE_DIR.mkdir(parents=True, exist_ok=True)
    with SNIPPET_STORE_FILE.open("w", encoding="utf-8") as fh:
        json.dump(store, fh, ensure_ascii=False, indent=2)


def update_snippet_store(
    store: Dict[str, Any],
    records: Iterable[Dict[str, Any]],
    *,
    endpoint: str,
    max_tokens: int,
    batch_index: int,
) -> None:
    snippets = store.setdefault("snippets", {})
    timestamp = _now_iso()
    for record in records:
        processor_id = str(record.get("processor_id") or "")
        if not processor_id:
            continue
        code = record.get("databricks_code")
        if not code or not str(code).strip():
            continue
        snippets[processor_id] = {
            "processor_id": processor_id,
            "template": record.get("template"),
            "name": record.get("name"),
            "short_type": record.get("short_type"),
            "parent_group": record.get("parent_group"),
            "parent_group_path": record.get("parent_group_path"),
            "migration_category": record.get("migration_category"),
            "databricks_target": record.get("databricks_target"),
            "recommended_target": record.get("recommended_target"),
            "migration_needed": record.get("migration_needed"),
            "implementation_hint": record.get("implementation_hint"),
            "blockers": record.get("blockers"),
            "next_step": record.get("next_step"),
            "code_language": record.get("code_language", "unknown"),
            "databricks_code": str(code),
            "confidence": record.get("confidence"),
            "classification_source": record.get("classification_source"),
            "rule": record.get("rule"),
            "batch_index": batch_index,
            "endpoint": endpoint,
            "max_tokens": max_tokens,
            "cached_at": timestamp,
        }


__all__ = [
    "load_snippet_store",
    "save_snippet_store",
    "update_snippet_store",
]

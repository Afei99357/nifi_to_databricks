"""Utilities to slice NiFi processors into LLM-friendly batches."""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Sequence

DEFAULT_MAX_PROCESSORS = 10
DEFAULT_MAX_CHARS = 30000


@dataclass(frozen=True)
class ProcessorSnapshot:
    processor_id: str
    template: str
    name: str
    short_type: str
    migration_category: str
    parent_group_path: str
    feature_evidence: Dict[str, Any]
    classification_source: str
    rule: str
    databricks_target: str
    confidence: float
    scripts_detail: Optional[Dict[str, Any]] = None

    def to_prompt_dict(self) -> Dict[str, Any]:
        payload = {
            "processor_id": self.processor_id,
            "template": self.template,
            "name": self.name,
            "short_type": self.short_type,
            "migration_category": self.migration_category,
            "parent_group_path": self.parent_group_path,
            "classification_source": self.classification_source,
            "rule": self.rule,
            "databricks_target": self.databricks_target,
            "confidence": round(self.confidence, 3),
            "feature_evidence": self.feature_evidence,
        }
        if self.scripts_detail is not None:
            payload["scripts"] = self.scripts_detail
        return payload


def _safe_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _normalise_feature_evidence(feature: Dict[str, Any] | None) -> Dict[str, Any]:
    feature = feature or {}
    sql = feature.get("sql", {}) or {}
    scripts = feature.get("scripts", {}) or {}
    tables = feature.get("tables", {}) or {}
    variables = feature.get("variables", {}) or {}
    connections = feature.get("connections", {}) or {}
    controller_services = feature.get("controller_services", []) or []

    return {
        "sql": {
            "has_sql": bool(sql.get("has_sql")),
            "has_dml": bool(sql.get("has_dml")),
            "has_metadata_only": bool(sql.get("has_metadata_only")),
        },
        "scripts": {
            "inline_count": int(scripts.get("inline_count") or 0),
            "external_count": int(scripts.get("external_count") or 0),
            "external_hosts": scripts.get("external_hosts", []),
        },
        "tables": {
            "tables": tables.get("tables", []),
            "properties": tables.get("properties", []),
        },
        "variables": {
            "defines": variables.get("defines", []),
            "uses": variables.get("uses", []),
        },
        "connections": {
            "incoming": connections.get("incoming", []),
            "outgoing": connections.get("outgoing", []),
        },
        "controller_services": controller_services,
        "uses_variables": bool(feature.get("uses_variables")),
    }


def snapshot_from_record(
    record: Dict[str, Any],
    *,
    scripts_detail: Optional[Dict[str, Any]] = None,
) -> ProcessorSnapshot | None:
    processor_id = _safe_str(record.get("processor_id") or record.get("id"))
    if not processor_id:
        return None

    feature_evidence = _normalise_feature_evidence(record.get("feature_evidence"))

    return ProcessorSnapshot(
        processor_id=processor_id,
        template=_safe_str(record.get("template") or ""),
        name=_safe_str(record.get("name") or ""),
        short_type=_safe_str(
            record.get("short_type") or record.get("processor_type") or ""
        ),
        migration_category=_safe_str(record.get("migration_category") or ""),
        parent_group_path=_safe_str(
            record.get("parent_group_path")
            or record.get("parent_group")
            or record.get("parentGroupPath")
            or ""
        ),
        feature_evidence=feature_evidence,
        classification_source=_safe_str(
            record.get("classification_source") or record.get("source") or ""
        ),
        rule=_safe_str(record.get("rule") or ""),
        databricks_target=_safe_str(record.get("databricks_target") or ""),
        confidence=float(record.get("confidence") or 0.0),
        scripts_detail=scripts_detail,
    )


def estimate_prompt_chars(snapshot: ProcessorSnapshot) -> int:
    """Rough estimate of prompt size by serialising to JSON."""

    payload = snapshot.to_prompt_dict()
    serialised = json.dumps(payload, ensure_ascii=False)
    return len(serialised)


def batch_snapshots(
    snapshots: Sequence[ProcessorSnapshot],
    *,
    max_processors: int = DEFAULT_MAX_PROCESSORS,
    max_chars: int = DEFAULT_MAX_CHARS,
) -> List[List[ProcessorSnapshot]]:
    batches: List[List[ProcessorSnapshot]] = []
    current: List[ProcessorSnapshot] = []
    current_chars = 0

    for snapshot in snapshots:
        snap_chars = estimate_prompt_chars(snapshot)
        if not current:
            current.append(snapshot)
            current_chars = snap_chars
            continue

        enlarged = len(current) + 1
        if enlarged > max_processors or current_chars + snap_chars > max_chars:
            batches.append(current)
            current = [snapshot]
            current_chars = snap_chars
        else:
            current.append(snapshot)
            current_chars += snap_chars

    if current:
        batches.append(current)

    return batches


def build_batch_payload(
    batch: Sequence[ProcessorSnapshot],
    *,
    batch_index: int,
) -> Dict[str, Any]:
    processor_dicts = [snapshot.to_prompt_dict() for snapshot in batch]
    processor_ids = [snapshot.processor_id for snapshot in batch]
    templates = sorted({snapshot.template for snapshot in batch if snapshot.template})
    prompt_char_count = sum(estimate_prompt_chars(snapshot) for snapshot in batch)

    return {
        "batch_index": batch_index,
        "processor_ids": processor_ids,
        "templates": templates,
        "processor_count": len(batch),
        "processors": processor_dicts,
        "prompt_char_count": prompt_char_count,
    }


def build_batches_from_records(
    records: Iterable[Dict[str, Any]],
    *,
    max_processors: int = DEFAULT_MAX_PROCESSORS,
    max_chars: int = DEFAULT_MAX_CHARS,
    script_lookup: Optional[Dict[str, Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    snapshots: List[ProcessorSnapshot] = []
    for record in records:
        scripts_detail = None
        if script_lookup:
            scripts_detail = script_lookup.get(
                str(record.get("processor_id") or record.get("id") or "")
            )
        snapshot = snapshot_from_record(record, scripts_detail=scripts_detail)
        if snapshot is None:
            continue
        snapshots.append(snapshot)

    # Sort for determinism: template, parent group path, processor id
    snapshots.sort(
        key=lambda snap: (
            snap.template,
            snap.parent_group_path,
            snap.processor_id,
        )
    )

    batches = batch_snapshots(
        snapshots,
        max_processors=max_processors,
        max_chars=max_chars,
    )

    return [
        build_batch_payload(batch, batch_index=index)
        for index, batch in enumerate(batches, start=1)
    ]


__all__ = [
    "ProcessorSnapshot",
    "snapshot_from_record",
    "estimate_prompt_chars",
    "batch_snapshots",
    "build_batch_payload",
    "build_batches_from_records",
    "build_script_lookup",
]


def build_script_lookup(
    script_results: Iterable[Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    lookup: Dict[str, Dict[str, Any]] = {}
    for entry in script_results:
        processor_id = str(entry.get("processor_id") or "")
        if not processor_id:
            continue
        inline_scripts = entry.get("inline_scripts", []) or []
        external_scripts = entry.get("external_scripts", []) or []
        external_with_notes: List[Dict[str, Any]] = []
        for script in external_scripts:
            if isinstance(script, dict):
                noted = dict(script)
                noted.setdefault(
                    "manual_review_note",
                    "External file – review content separately before migration.",
                )
                external_with_notes.append(noted)
            else:
                external_with_notes.append(script)

        lookup[processor_id] = {
            "inline_scripts": inline_scripts,
            "external_scripts": external_with_notes,
        }

    return lookup

"""Helpers to build processor payloads for LLM triage."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Sequence


@dataclass(frozen=True)
class ProcessorPayload:
    processor_id: str
    template: str
    name: str
    short_type: str
    group: str
    migration_category: str
    databricks_target: str
    classification_source: str
    rule: str
    confidence: float
    feature_summary: Dict[str, object]
    lineage_summary: Dict[str, List[str]]
    variables: Dict[str, List[str]]
    controller_services: List[str]
    notes: str

    def to_payload(self) -> Dict[str, object]:
        return {
            "processor_id": self.processor_id,
            "template": self.template,
            "name": self.name,
            "short_type": self.short_type,
            "group": self.group,
            "current_category": self.migration_category,
            "databricks_target": self.databricks_target,
            "classification_source": self.classification_source,
            "rule": self.rule,
            "confidence": round(self.confidence, 3),
            "feature_summary": self.feature_summary,
            "lineage_summary": self.lineage_summary,
            "variables": self.variables,
            "controller_services": self.controller_services,
            "notes": self.notes,
        }


def load_classification_records(paths: Sequence[Path]) -> List[Dict[str, object]]:
    records: List[Dict[str, object]] = []
    for path in paths:
        if path.suffix.lower() != ".json":
            continue
        with path.open("r", encoding="utf-8") as fh:
            payload = json.load(fh)
        template = payload.get("workflow", {}).get("filename") or path.name
        for record in payload.get("classifications", []) or []:
            cloned = dict(record)
            cloned["template"] = template
            records.append(cloned)
    return records


def _summarise_variables(raw: Dict[str, object] | None) -> Dict[str, List[str]]:
    if not raw:
        return {"defines": [], "uses": []}
    defines = [
        str(item.get("variable"))
        for item in raw.get("defines", []) or []
        if isinstance(item, dict) and item.get("variable")
    ]
    uses = [
        str(item.get("variable"))
        for item in raw.get("uses", []) or []
        if isinstance(item, dict) and item.get("variable")
    ]
    return {"defines": sorted(set(defines)), "uses": sorted(set(uses))}


def _summarise_controller_services(raw: Iterable) -> List[str]:
    services: List[str] = []
    for item in raw or []:
        if isinstance(item, dict):
            name = item.get("name") or item.get("type")
            if name:
                services.append(str(name))
        elif isinstance(item, str):
            services.append(item)
    return sorted(set(services))


def _summarise_connections(
    ids: Iterable[str],
    id_to_short_type: Dict[str, str],
) -> List[str]:
    labels: List[str] = []
    for identifier in ids or []:
        short_type = id_to_short_type.get(identifier)
        if short_type:
            labels.append(short_type)
    return sorted(set(labels))


def build_payloads(records: Sequence[Dict[str, object]]) -> List[ProcessorPayload]:
    id_to_short_type = {
        str(rec.get("processor_id")): str(
            rec.get("short_type") or rec.get("processor_type") or ""
        )
        for rec in records
        if rec.get("processor_id")
    }

    payloads: List[ProcessorPayload] = []
    for rec in records:
        processor_id = str(rec.get("processor_id") or rec.get("id") or "")
        if not processor_id:
            continue

        feature_evidence = rec.get("feature_evidence", {}) or {}
        sql = feature_evidence.get("sql", {}) or {}
        scripts = feature_evidence.get("scripts", {}) or {}
        tables = feature_evidence.get("tables", {}) or {}
        connections = feature_evidence.get("connections", {}) or {}

        feature_summary: Dict[str, object] = {
            "has_sql": bool(sql.get("has_sql")),
            "has_dml": bool(sql.get("has_dml")),
            "sql_metadata_only": bool(sql.get("has_metadata_only")),
            "inline_script_count": int(scripts.get("inline_count") or 0),
            "external_script_count": int(scripts.get("external_count") or 0),
            "external_hosts": scripts.get("external_hosts", []),
            "tables": tables.get("tables", []),
            "table_properties": tables.get("properties", []),
            "uses_variables": bool(feature_evidence.get("uses_variables")),
        }

        incoming_ids = connections.get("incoming", []) or []
        outgoing_ids = connections.get("outgoing", []) or []
        lineage_summary = {
            "incoming_types": _summarise_connections(incoming_ids, id_to_short_type),
            "outgoing_types": _summarise_connections(outgoing_ids, id_to_short_type),
        }

        variables = _summarise_variables(feature_evidence.get("variables"))
        controller_services = _summarise_controller_services(
            feature_evidence.get("controller_services", [])
        )

        payload = ProcessorPayload(
            processor_id=processor_id,
            template=str(rec.get("template") or ""),
            name=str(rec.get("name") or ""),
            short_type=str(rec.get("short_type") or rec.get("processor_type") or ""),
            group=str(rec.get("parent_group") or rec.get("parentGroupId") or ""),
            migration_category=str(rec.get("migration_category") or "Ambiguous"),
            databricks_target=str(rec.get("databricks_target") or ""),
            classification_source=str(rec.get("classification_source") or ""),
            rule=str(rec.get("rule") or ""),
            confidence=float(rec.get("confidence") or 0.0),
            feature_summary=feature_summary,
            lineage_summary=lineage_summary,
            variables=variables,
            controller_services=controller_services,
            notes=str(rec.get("notes") or ""),
        )
        payloads.append(payload)
    return payloads


def format_payloads_for_prompt(payloads: Sequence[ProcessorPayload]) -> str:
    payload = {
        "processor_count": len(payloads),
        "processors": [payload.to_payload() for payload in payloads],
    }
    return json.dumps(payload, ensure_ascii=False, indent=2)


__all__ = [
    "ProcessorPayload",
    "load_classification_records",
    "build_payloads",
    "format_payloads_for_prompt",
]

"""Helpers to build processor dossiers for LLM triage."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Sequence

from tools.catalog import load_catalog

CATALOG = load_catalog()


@dataclass(frozen=True)
class ProcessorDossier:
    processor_id: str
    template: str
    name: str
    short_type: str
    migration_category: str
    databricks_target: str
    classification_source: str
    rule: str
    confidence: float
    catalog_category: str
    catalog_default: str
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
            "current_category": self.migration_category,
            "databricks_target": self.databricks_target,
            "classification_source": self.classification_source,
            "rule": self.rule,
            "confidence": round(self.confidence, 3),
            "catalog_category": self.catalog_category,
            "catalog_default": self.catalog_default,
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


def _catalog_metadata(short_type: str) -> Dict[str, str]:
    if not short_type:
        return {"category": "", "default": ""}
    category = CATALOG.category_for(short_type) or ""
    metadata = CATALOG.metadata_for(short_type)
    default_category = metadata.get("default_migration_category", "")
    return {"category": category, "default": default_category}


def build_dossiers(records: Sequence[Dict[str, object]]) -> List[ProcessorDossier]:
    id_to_short_type = {
        str(rec.get("processor_id")): str(
            rec.get("short_type") or rec.get("processor_type") or ""
        )
        for rec in records
        if rec.get("processor_id")
    }

    dossiers: List[ProcessorDossier] = []
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

        catalog_info = _catalog_metadata(str(rec.get("short_type") or ""))

        dossier = ProcessorDossier(
            processor_id=processor_id,
            template=str(rec.get("template") or ""),
            name=str(rec.get("name") or ""),
            short_type=str(rec.get("short_type") or rec.get("processor_type") or ""),
            migration_category=str(rec.get("migration_category") or "Ambiguous"),
            databricks_target=str(rec.get("databricks_target") or ""),
            classification_source=str(rec.get("classification_source") or ""),
            rule=str(rec.get("rule") or ""),
            confidence=float(rec.get("confidence") or 0.0),
            catalog_category=catalog_info["category"],
            catalog_default=catalog_info["default"],
            feature_summary=feature_summary,
            lineage_summary=lineage_summary,
            variables=variables,
            controller_services=controller_services,
            notes=str(rec.get("notes") or ""),
        )
        dossiers.append(dossier)
    return dossiers


def format_dossiers_for_prompt(dossiers: Sequence[ProcessorDossier]) -> str:
    payload = {
        "processor_count": len(dossiers),
        "processors": [dossier.to_payload() for dossier in dossiers],
    }
    return json.dumps(payload, ensure_ascii=False, indent=2)


__all__ = [
    "ProcessorDossier",
    "load_classification_records",
    "build_dossiers",
    "format_dossiers_for_prompt",
]

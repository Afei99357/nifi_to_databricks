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
    group_path: str
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
    scripts: Dict[str, object]
    scheduling_strategy: str
    scheduling_period: str
    sql_context: Dict[str, object] | None = None  # NEW: Phase 1 SQL extraction results
    flow_context: Dict[str, object] | None = None  # NEW: Phase 2 parallel flow context

    def to_payload(self) -> Dict[str, object]:
        payload = {
            "processor_id": self.processor_id,
            "template": self.template,
            "name": self.name,
            "short_type": self.short_type,
            "group": self.group,
            "group_path": self.group_path,
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
            "scripts": self.scripts,
            "scheduling_strategy": self.scheduling_strategy,
            "scheduling_period": self.scheduling_period,
        }
        # Include sql_context if present (Phase 1 integration)
        if self.sql_context:
            payload["sql_context"] = self.sql_context
        # Include flow_context if present (Phase 2 integration)
        if self.flow_context:
            payload["flow_context"] = self.flow_context
        return payload


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


def _summarise_scripts(raw: Dict[str, object] | None) -> Dict[str, object]:
    if not raw:
        return {"inline": [], "external": [], "external_hosts": []}

    inline_scripts: List[Dict[str, object]] = []
    for script in raw.get("inline_scripts", []) or []:
        if not isinstance(script, dict):
            inline_scripts.append({"content": str(script)})
            continue
        inline_scripts.append(
            {
                "property": str(script.get("property_name") or ""),
                "language": str(script.get("script_type") or "unknown"),
                "line_count": int(script.get("line_count") or 0),
                "content": script.get("content", ""),
                "preview": script.get("content_preview", ""),
                "referenced_queries": script.get("referenced_queries", []),
                "resolved_queries": script.get("resolved_queries", []),
            }
        )

    external_scripts: List[Dict[str, object]] = []
    for script in raw.get("external_scripts", []) or []:
        if not isinstance(script, dict):
            external_scripts.append({"path": str(script)})
            continue
        external_scripts.append(
            {
                "path": str(script.get("path") or ""),
                "type": str(script.get("type") or "unknown"),
                "property_source": str(script.get("property_source") or ""),
                "manual_review_note": script.get("manual_review_note", ""),
            }
        )

    return {
        "inline": inline_scripts,
        "external": external_scripts,
        "external_hosts": raw.get("external_hosts", []),
    }


def _find_sql_context_for_processor(
    processor_id: str,
    record: Dict[str, object],
    sql_extraction: Dict[str, object] | None,
) -> Dict[str, object] | None:
    """Find relevant SQL schemas/transformations for a processor (Phase 1 integration).

    Matches processors to SQL extraction results by processor_id for precise linkage.
    - CREATE TABLE processor gets schema only
    - INSERT OVERWRITE processor gets transformation only
    - If a processor has both, returns both

    Args:
        processor_id: Processor ID
        record: Classification record with processor details
        sql_extraction: SQL extraction results from extract_sql_from_nifi_workflow()

    Returns:
        SQL context dict with matched schema and/or transformation, or None
    """
    if not sql_extraction:
        return None

    schemas = sql_extraction.get("schemas", {}) or {}
    transformations = sql_extraction.get("transformations", {}) or {}

    if not schemas and not transformations:
        return None

    matched_schema = None
    matched_transformation = None
    matched_table = None

    # Check if this processor has a CREATE TABLE (schema)
    for table_name, schema in schemas.items():
        if schema.get("processor_id") == processor_id:
            matched_schema = schema
            matched_table = table_name
            break

    # Check if this processor has an INSERT OVERWRITE (transformation)
    for table_name, transform in transformations.items():
        if transform.get("processor_id") == processor_id:
            matched_transformation = transform
            if not matched_table:
                matched_table = table_name
            break

    # Return context if we found anything
    if matched_schema or matched_transformation:
        return {
            "table": matched_table,
            "schema": matched_schema,
            "transformation": matched_transformation,
        }

    return None


def build_payloads(
    records: Sequence[Dict[str, object]],
    sql_extraction: Dict[str, object] | None = None,
    parallel_flows: Dict[str, object] | None = None,
) -> List[ProcessorPayload]:
    """Build processor payloads for LLM triage.

    Args:
        records: Classification records from workflow analysis
        sql_extraction: Optional SQL extraction results from Phase 1 (schemas, transformations)
        parallel_flows: Optional parallel flow detection from Phase 2 (flow groups, parameters)

    Returns:
        List of processor payloads enriched with SQL context and flow context where applicable
    """
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
        scripts = _summarise_scripts(rec.get("scripts_detail"))

        # NEW: Find SQL context for this processor (Phase 1 integration)
        sql_context = _find_sql_context_for_processor(processor_id, rec, sql_extraction)

        # NEW: Find flow context for this processor (Phase 2 integration)
        flow_context = None
        if parallel_flows:
            processor_to_flow = parallel_flows.get("processor_to_flow", {})
            flow_context = processor_to_flow.get(processor_id)

        group_name = (
            str(
                rec.get("parent_group")
                or rec.get("parentGroupName")
                or rec.get("parentGroupId")
                or "Root"
            )
            or "Root"
        )
        group_path = (
            str(
                rec.get("parent_group_path") or rec.get("parentGroupPath") or group_name
            )
            or group_name
        )

        payload = ProcessorPayload(
            processor_id=processor_id,
            template=str(rec.get("template") or ""),
            name=str(rec.get("name") or ""),
            short_type=str(rec.get("short_type") or rec.get("processor_type") or ""),
            group=group_name,
            group_path=group_path,
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
            scripts=scripts,
            scheduling_strategy=str(rec.get("schedulingStrategy") or ""),
            scheduling_period=str(rec.get("schedulingPeriod") or ""),
            sql_context=sql_context,  # NEW: Phase 1 SQL extraction context
            flow_context=flow_context,  # NEW: Phase 2 parallel flow context
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

"""Feature extraction helpers for NiFi processors.

Turns raw NiFi template content into structured feature dictionaries used by the
new declarative classifier.
"""

from __future__ import annotations

import json
import re
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List

from .script_extraction import extract_all_scripts_from_nifi_xml
from .table_extraction import extract_all_tables_from_nifi_xml
from .variable_extraction import extract_variable_dependencies
from .xml_tools import parse_nifi_template_impl

SQL_DML_PATTERN = re.compile(r"\b(INSERT|UPDATE|DELETE|MERGE|UPSERT)\b", re.IGNORECASE)
SQL_METADATA_PATTERN = re.compile(
    r"\b(REFRESH|MSCK|ANALYZE|DESCRIBE|SHOW\s+TABLES|SHOW\s+PARTITIONS)\b",
    re.IGNORECASE,
)


def _safe_str(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (str, bytes)):
        return (
            value.decode("utf-8", errors="ignore")
            if isinstance(value, bytes)
            else value
        )
    return str(value)


def _collect_controller_services(properties: Dict[str, Any]) -> List[str]:
    services = []
    for key, value in properties.items():
        if not isinstance(key, str):
            continue
        if "controller service" in key.lower() and isinstance(value, str) and value:
            services.append(value)
    return services


def _property_blob(properties: Dict[str, Any]) -> str:
    parts: List[str] = []
    for key, value in properties.items():
        if value is None:
            continue
        parts.append(_safe_str(value))
    return "\n".join(parts)


def _build_connection_maps(
    connections: Iterable[Dict[str, Any]],
) -> tuple[Dict[str, List[str]], Dict[str, List[str]]]:
    incoming: Dict[str, List[str]] = defaultdict(list)
    outgoing: Dict[str, List[str]] = defaultdict(list)
    for connection in connections:
        source = connection.get("source")
        dest = connection.get("destination")
        if source and dest:
            outgoing[source].append(dest)
            incoming[dest].append(source)
    return incoming, outgoing


def _index_table_results(
    table_results: List[Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    tables_by_proc: Dict[str, Dict[str, Any]] = defaultdict(
        lambda: {"tables": set(), "properties": set()}
    )
    for entry in table_results:
        proc_id = entry.get("processor_id")
        if not proc_id:
            continue
        table_name = entry.get("table_name")
        if table_name:
            tables_by_proc[proc_id]["tables"].add(table_name)
        property_name = entry.get("property_name")
        if property_name:
            tables_by_proc[proc_id]["properties"].add(property_name)
    # Convert sets to sorted lists for deterministic outputs
    normalised: Dict[str, Dict[str, Any]] = {}
    for proc_id, payload in tables_by_proc.items():
        normalised[proc_id] = {
            "tables": sorted(payload["tables"]),
            "properties": sorted(payload["properties"]),
        }
    return normalised


def _index_script_results(
    script_results: List[Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    idx: Dict[str, Dict[str, Any]] = {}
    for result in script_results:
        proc_id = result.get("processor_id")
        if not proc_id:
            continue
        inline_scripts = result.get("inline_scripts", [])
        external_scripts = result.get("external_scripts", [])
        idx[proc_id] = {
            "inline_count": len(inline_scripts),
            "external_count": len(external_scripts),
            "inline_scripts": inline_scripts,
            "external_scripts": external_scripts,
            "external_hosts": result.get("external_hosts", []),
        }
    return idx


def _index_variable_dependencies(
    variable_payload: Dict[str, Any],
) -> Dict[str, Dict[str, Any]]:
    processors = variable_payload.get("processors", {})
    variables = variable_payload.get("variables", {})
    usage_index: Dict[str, Dict[str, Any]] = {
        proc_id: {"defines": [], "uses": []} for proc_id in processors.keys()
    }

    for var_name, details in variables.items():
        for definition in details.get("definitions", []):
            proc_id = definition.get("processor_id")
            if proc_id:
                usage_index.setdefault(proc_id, {"defines": [], "uses": []})
                usage_index[proc_id]["defines"].append(
                    {
                        "variable": var_name,
                        "property": definition.get("property_name"),
                        "value": definition.get("property_value"),
                    }
                )
        for usage in details.get("usages", []):
            proc_id = usage.get("processor_id")
            if proc_id:
                usage_index.setdefault(proc_id, {"defines": [], "uses": []})
                usage_index[proc_id]["uses"].append(
                    {
                        "variable": var_name,
                        "property": usage.get("property_name"),
                        "expression": usage.get("variable_expression"),
                    }
                )
    return usage_index


def _detect_sql_flags(property_blob: str) -> Dict[str, bool]:
    if not property_blob:
        return {"has_sql": False, "has_dml": False, "has_metadata_only": False}
    has_sql = "select" in property_blob.lower() or "insert" in property_blob.lower()
    has_dml = bool(SQL_DML_PATTERN.search(property_blob))
    has_metadata = bool(SQL_METADATA_PATTERN.search(property_blob))
    return {
        "has_sql": has_sql,
        "has_dml": has_dml,
        "has_metadata_only": has_metadata and not has_dml,
    }


def extract_processor_features(xml_path: str) -> Dict[str, Any]:
    """Extract structured processor features for a NiFi template.

    Args:
        xml_path: Path to the NiFi template XML file.

    Returns:
        Dictionary containing workflow metadata and per-processor feature dictionaries.
    """
    xml_path_obj = Path(xml_path)
    if not xml_path_obj.exists():
        raise FileNotFoundError(f"NiFi template not found: {xml_path}")

    with xml_path_obj.open("r", encoding="utf-8") as fh:
        xml_content = fh.read()

    template_data = parse_nifi_template_impl(xml_content)
    processors: List[Dict[str, Any]] = template_data.get("processors", [])
    connections = template_data.get("connections", [])

    incoming_map, outgoing_map = _build_connection_maps(connections)

    table_results = extract_all_tables_from_nifi_xml(xml_path)
    table_index = _index_table_results(table_results)

    script_results = extract_all_scripts_from_nifi_xml(xml_path)
    script_index = _index_script_results(script_results)

    variable_payload = extract_variable_dependencies(xml_path)
    variable_index = _index_variable_dependencies(variable_payload)

    processor_features: List[Dict[str, Any]] = []

    for proc in processors:
        proc_id = proc.get("id") or proc.get("processor_id")
        proc_name = proc.get("name", "Unknown")
        proc_type = proc.get("type") or proc.get("processor_type") or "Unknown"
        properties = proc.get("properties", {}) or {}

        property_blob = _property_blob(properties)
        sql_flags = _detect_sql_flags(property_blob)

        scripts = script_index.get(
            proc_id,
            {
                "inline_count": 0,
                "external_count": 0,
                "inline_scripts": [],
                "external_scripts": [],
                "external_hosts": [],
            },
        )
        tables = table_index.get(proc_id, {"tables": [], "properties": []})
        variables = variable_index.get(proc_id, {"defines": [], "uses": []})

        features = {
            "id": proc_id,
            "name": proc_name,
            "processor_type": proc_type,
            "short_type": proc_type.split(".")[-1] if proc_type else "Unknown",
            "parent_group": proc.get("parentGroupName", "Root"),
            "properties": properties,
            "controller_services": _collect_controller_services(properties),
            "connections": {
                "incoming": incoming_map.get(proc_id, []),
                "outgoing": outgoing_map.get(proc_id, []),
            },
            "scripts": scripts,
            "sql": sql_flags,
            "tables": tables,
            "variables": variables,
            "uses_variables": bool(re.search(r"\$\{[^}]+\}", property_blob)),
            "has_comments": bool(proc.get("comments")),
        }

        processor_features.append(features)

    return {
        "workflow": {
            "filename": xml_path_obj.name,
            "path": str(xml_path_obj),
            "processor_count": len(processors),
            "connection_count": len(connections),
        },
        "processors": processor_features,
    }


def to_json(features: Dict[str, Any]) -> str:
    """Serialize extracted features to JSON for debugging/export."""
    return json.dumps(features, indent=2, ensure_ascii=False)


__all__ = ["extract_processor_features", "to_json"]

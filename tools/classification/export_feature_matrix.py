"""Export flattened processor features for downstream analytics (e.g., t-SNE)."""

from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter
from pathlib import Path
from typing import Dict, Iterable, List, Sequence

CURATED_NEIGHBOR_TYPES = [
    "ExecuteSQL",
    "PutSQL",
    "PutFile",
    "PutHDFS",
    "ListFile",
    "ListSFTP",
    "FetchFile",
    "FetchSFTP",
    "ExecuteScript",
    "InvokeScriptedProcessor",
    "ExecuteStreamCommand",
    "RouteOnAttribute",
    "UpdateAttribute",
    "LogMessage",
    "ControlRate",
    "ListenHTTP",
    "Wait",
    "GenerateFlowFile",
    "ReplaceText",
    "ExtractText",
    "EvaluateJsonPath",
    "GetFile",
    "PutKudu",
    "PutHiveStreaming",
    "PutKafka",
]

NAME_KEYWORDS = {
    "name_has_log": ["log", "logging"],
    "name_has_retry": ["retry", "requeue"],
    "name_has_sftp": ["sftp"],
}

GROUP_KEYWORDS = {
    "group_has_alert": ["alert", "monitor"],
}

CONTROLLER_KEYWORDS = {
    "uses_dbcp": ["dbcp"],
    "uses_kafka": ["kafka"],
    "uses_jms": ["jms"],
    "uses_hive": ["hive"],
    "uses_impala": ["impala"],
    "uses_kudu": ["kudu"],
}


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input",
        default="derived_classification_results",
        help="Directory, file, or glob pattern pointing to classification JSON outputs",
    )
    parser.add_argument(
        "--output",
        default="derived_classification_features.csv",
        help="CSV file to write flattened feature matrix",
    )
    parser.add_argument(
        "--include-ambiguous",
        action="store_true",
        help="Include processors labelled Ambiguous or with confidence < 0.5",
    )
    parser.add_argument(
        "--min-confidence",
        type=float,
        default=0.0,
        help="Discard rows with confidence below this threshold (ignored if include-ambiguous)",
    )
    return parser.parse_args(argv)


def gather_files(input_arg: str) -> List[Path]:
    candidate = Path(input_arg)
    if candidate.exists():
        if candidate.is_file():
            return [candidate]
        if candidate.is_dir():
            return sorted(p for p in candidate.glob("*.json"))
    return sorted(Path().glob(input_arg))


def slugify(name: str) -> str:
    name = re.sub(r"[^0-9A-Za-z]+", "_", name).strip("_")
    return name.lower()


def contains_keyword(text: str | None, keywords: Iterable[str]) -> bool:
    if not text:
        return False
    text_lower = text.lower()
    return any(keyword in text_lower for keyword in keywords)


def extract_controller_text(controller_services: Iterable) -> str:
    fragments: List[str] = []
    for svc in controller_services or []:
        if isinstance(svc, dict):
            for value in svc.values():
                if isinstance(value, str):
                    fragments.append(value.lower())
        elif isinstance(svc, str):
            fragments.append(svc.lower())
    return " ".join(fragments)


def extract_property_text(properties: Dict[str, object]) -> str:
    fragments: List[str] = []
    for value in (properties or {}).values():
        if isinstance(value, str):
            fragments.append(value.lower())
    return " ".join(fragments)


def uses_expression_language(properties: Dict[str, object]) -> bool:
    for value in (properties or {}).values():
        if isinstance(value, str) and "${" in value:
            return True
    return False


def neighbor_type_flags(
    neighbor_ids: Iterable[str],
    id_to_short_type: Dict[str, str],
    prefix: str,
) -> Dict[str, int]:
    flags: Dict[str, int] = {}
    for proc_type in CURATED_NEIGHBOR_TYPES:
        flags[f"{prefix}_has_{slugify(proc_type)}"] = 0
    counts = Counter()
    for neighbor_id in neighbor_ids:
        short_type = id_to_short_type.get(neighbor_id)
        if not short_type:
            continue
        counts[short_type] += 1
        if short_type in CURATED_NEIGHBOR_TYPES:
            flags[f"{prefix}_has_{slugify(short_type)}"] = 1
    return flags


def flatten_record(
    template_name: str,
    record: Dict[str, object],
    id_to_short_type: Dict[str, str],
    include_ambiguous: bool,
    min_confidence: float,
) -> Dict[str, object] | None:
    category = record.get("migration_category") or "Ambiguous"
    confidence = float(record.get("confidence") or 0.0)
    if not include_ambiguous and (
        category == "Ambiguous" or confidence < min_confidence
    ):
        return None

    evidence = record.get("feature_evidence", {}) or {}
    sql = evidence.get("sql", {}) or {}
    scripts = evidence.get("scripts", {}) or {}
    tables = evidence.get("tables", {}) or {}
    connections = evidence.get("connections", {}) or {}
    controller_services = evidence.get("controller_services", []) or []
    variables = evidence.get("variables", {}) or {}
    properties = record.get("properties", {}) or {}

    incoming = connections.get("incoming", []) or []
    outgoing = connections.get("outgoing", []) or []

    row: Dict[str, object] = {
        "template": template_name,
        "processor_id": record.get("processor_id"),
        "processor_name": record.get("name"),
        "processor_type": record.get("processor_type"),
        "short_type": record.get("short_type"),
        "parent_group": record.get("parent_group"),
        "migration_category": category,
        "confidence": confidence,
        "classification_source": record.get("classification_source"),
        "databricks_target": record.get("databricks_target"),
        "rule": record.get("rule"),
        "promotion_applied": 1 if record.get("classification_source") == "promotion" else 0,
        "uses_expression_language": int(uses_expression_language(properties)),
        "uses_variables": 1 if evidence.get("uses_variables") else 0,
        "sql_has_sql": 1 if sql.get("has_sql") else 0,
        "sql_has_dml": 1 if sql.get("has_dml") else 0,
        "sql_has_metadata_only": 1 if sql.get("has_metadata_only") else 0,
        "scripts_inline_count": int(scripts.get("inline_count", 0) or 0),
        "scripts_external_count": int(scripts.get("external_count", 0) or 0),
        "scripts_has_inline": 1 if (scripts.get("inline_count", 0) or 0) > 0 else 0,
        "scripts_has_external": 1 if (scripts.get("external_count", 0) or 0) > 0 else 0,
        "scripts_external_hosts_count": len(set(scripts.get("external_hosts", []) or [])),
        "table_count": len(tables.get("tables", []) or []),
        "table_property_reference_count": len(tables.get("properties", []) or []),
        "variables_define_count": len(variables.get("defines", []) or []),
        "variables_use_count": len(variables.get("uses", []) or []),
        "controller_service_count": len(controller_services),
        "incoming_count": len(incoming),
        "outgoing_count": len(outgoing),
        "is_fanin": 1 if len(incoming) > 1 else 0,
        "is_fanout": 1 if len(outgoing) > 1 else 0,
        "is_terminal": 1 if len(outgoing) == 0 else 0,
    }

    controller_text = " ".join(
        filter(
            None,
            [extract_controller_text(controller_services), extract_property_text(properties)],
        )
    )

    for feature_name, keywords in CONTROLLER_KEYWORDS.items():
        row[feature_name] = 1 if contains_keyword(controller_text, keywords) else 0

    processor_name = record.get("name") or ""
    for feature_name, keywords in NAME_KEYWORDS.items():
        row[feature_name] = 1 if contains_keyword(processor_name, keywords) else 0

    parent_group = record.get("parent_group") or ""
    for feature_name, keywords in GROUP_KEYWORDS.items():
        row[feature_name] = 1 if contains_keyword(parent_group, keywords) else 0

    neighbor_flags_in = neighbor_type_flags(incoming, id_to_short_type, "incoming")
    neighbor_flags_out = neighbor_type_flags(outgoing, id_to_short_type, "outgoing")
    row.update(neighbor_flags_in)
    row.update(neighbor_flags_out)

    return row


def write_csv(output_path: Path, rows: List[Dict[str, object]]) -> None:
    if not rows:
        raise ValueError("No rows were generated; ensure the input contains classification JSON.")
    fieldnames = list(rows[0].keys())
    with output_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    files = gather_files(args.input)
    if not files:
        raise SystemExit(f"No classification JSON files found for input {args.input}")

    rows: List[Dict[str, object]] = []
    for file_path in files:
        if file_path.suffix.lower() != ".json":
            continue
        with file_path.open("r", encoding="utf-8") as fh:
            payload = json.load(fh)
        records = payload.get("classifications", []) or []
        id_to_short_type = {
            rec.get("processor_id"): rec.get("short_type") or rec.get("processor_type")
            for rec in records
            if rec.get("processor_id")
        }
        template_name = payload.get("workflow", {}).get("filename") or file_path.name
        for record in records:
            flat = flatten_record(
                template_name,
                record,
                id_to_short_type,
                include_ambiguous=args.include_ambiguous,
                min_confidence=args.min_confidence,
            )
            if flat is not None:
                rows.append(flat)

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    write_csv(output_path, rows)
    print(f"Wrote {len(rows)} rows to {output_path}")
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    raise SystemExit(main())

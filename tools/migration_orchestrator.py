"""Migration orchestration helpers built on the declarative classifier."""

from __future__ import annotations

import json
import os
from typing import Any, Dict, Iterable, Optional

from tools.classification import classify_workflow
from tools.improved_pruning import detect_data_flow_chains

ESSENTIAL_CATEGORIES: set[str] = {
    "Business Logic",
    "Source Adapter",
    "Sink Adapter",
}

SUPPORT_CATEGORIES: set[str] = {
    "Orchestration / Monitoring",
}


def _categorise_processors(classifications: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    essentials: list[Dict[str, Any]] = []
    support: list[Dict[str, Any]] = []
    infrastructure: list[Dict[str, Any]] = []
    ambiguous: list[Dict[str, Any]] = []

    for record in classifications:
        category = record.get("migration_category", "Infrastructure Only")
        if category in ESSENTIAL_CATEGORIES:
            essentials.append(record)
        elif category in SUPPORT_CATEGORIES:
            support.append(record)
        elif category == "Ambiguous":
            ambiguous.append(record)
        else:
            infrastructure.append(record)

    return {
        "essentials": essentials,
        "support": support,
        "infrastructure": infrastructure,
        "ambiguous": ambiguous,
    }


def _build_summary_markdown(summary: Dict[str, int]) -> str:
    lines = ["# Processor Classification Overview", ""]
    total = sum(summary.values())
    lines.append(f"- **Total Processors**: {total}")
    for category, count in sorted(summary.items(), key=lambda item: item[0]):
        lines.append(f"- **{category}**: {count}")
    lines.append("")
    return "\n".join(lines)


def _build_pruned_payload(groups: Dict[str, Any]) -> Dict[str, Any]:
    pruned_processors = [dict(proc, removal_reason="") for proc in groups["essentials"]]
    removed_support = [
        dict(proc, removal_reason="Support orchestration") for proc in groups["support"]
    ]
    removed_infra = [
        dict(proc, removal_reason="Infrastructure") for proc in groups["infrastructure"]
    ]
    unknown = [
        dict(proc, removal_reason="Needs review") for proc in groups["ambiguous"]
    ]

    return {
        "pruned_processors": pruned_processors,
        "removed_processors": removed_support + removed_infra,
        "unknown_processors": unknown,
        "summary": {
            "essential_count": len(pruned_processors),
            "support_count": len(removed_support),
            "infrastructure_count": len(removed_infra),
            "ambiguous_count": len(unknown),
        },
    }


def extract_nifi_assets_only(
    xml_path: str,
    progress_callback: Optional[callable] = None,
) -> Dict[str, Any]:
    """Compatibility shim that delegates to asset extraction helpers."""

    def _log(msg: str) -> None:
        print(msg)
        if progress_callback:
            progress_callback(msg)

    _log("📦 Starting asset extraction...")
    try:
        from tools.asset_extraction import extract_assets_from_properties
        from tools.xml_tools import parse_nifi_template_impl

        with open(xml_path, "r", encoding="utf-8") as fh:
            xml_content = fh.read()

        template_data = parse_nifi_template_impl(xml_content)
        processors = template_data.get("processors", [])

        script_files: set[str] = set()
        hdfs_paths: set[str] = set()
        database_hosts: set[str] = set()
        external_hosts: set[str] = set()
        table_references: set[str] = set()

        for processor in processors:
            extract_assets_from_properties(
                processor.get("properties", {}),
                script_files,
                hdfs_paths,
                database_hosts,
                external_hosts,
                table_references,
            )

        _log("✅ Asset extraction completed")
        return {
            "assets": {
                "script_files": sorted(script_files),
                "hdfs_paths": sorted(hdfs_paths),
                "database_hosts": sorted(database_hosts),
                "external_hosts": sorted(external_hosts),
                "table_references": sorted(table_references),
            },
            "summary": {
                "total_processors": len(processors),
                "script_files_count": len(script_files),
                "hdfs_paths_count": len(hdfs_paths),
                "database_hosts_count": len(database_hosts),
                "external_hosts_count": len(external_hosts),
                "table_references_count": len(table_references),
            },
        }

    except Exception as exc:  # pragma: no cover - defensive
        return {"error": f"Failed to extract assets: {exc}"}


def migrate_nifi_to_databricks_simplified(
    xml_path: str,
    out_dir: str,
    project: str,
    notebook_path: Optional[str] = None,
    max_processors_per_chunk: int = 25,
    progress_callback: Optional[callable] = None,
) -> Dict[str, Any]:
    """
    Orchestrate declarative classification + lineage helpers for Streamlit UI.

    Args:
        xml_path: Path to NiFi XML template file
        out_dir: Output directory for migration artifacts
        project: Project name for generated assets
        notebook_path: Optional notebook path (for reference in guide)
        max_processors_per_chunk: Max processors per chunk for large workflows

    Returns:
        Dictionary containing migration guide and analysis results
    """

    def _log(message: str) -> None:
        print(message)
        if progress_callback:
            progress_callback(message)

    _log("🚀 Starting declarative NiFi to Databricks analysis")

    os.makedirs(f"{out_dir}/{project}", exist_ok=True)

    with open(xml_path, "r", encoding="utf-8") as fh:
        xml_content = fh.read()

    classification_result = classify_workflow(xml_path)
    _log("📊 Classification complete")

    groups = _categorise_processors(classification_result["classifications"])
    pruned_payload = _build_pruned_payload(groups)

    chains_result = detect_data_flow_chains(
        xml_content,
        json.dumps(pruned_payload, default=str),
    )
    _log("🔗 Data flow chain detection finished")

    migration_summary = {
        "essential_processors": len(groups["essentials"]),
        "support_processors": len(groups["support"]),
        "infrastructure_processors": len(groups["infrastructure"]),
        "ambiguous_processors": len(groups["ambiguous"]),
    }

    result = {
        "workflow": classification_result.get("workflow", {}),
        "summary": classification_result.get("summary", {}),
        "migration_summary": migration_summary,
        "classifications": classification_result.get("classifications", []),
        "ambiguous": classification_result.get("ambiguous", []),
        "reports": {
            "classification_markdown": _build_summary_markdown(
                classification_result.get("summary", {})
            ),
            "pruned_summary": pruned_payload.get("summary", {}),
        },
        "analysis": {
            "pruned_processors": pruned_payload,
            "data_flow_chains": chains_result,
        },
        "configuration": {
            "xml_path": xml_path,
            "out_dir": out_dir,
            "project": project,
            "notebook_path": notebook_path,
            "approach": "declarative_pipeline",
            "max_processors_per_chunk": max_processors_per_chunk,
        },
    }

    _log("✅ Declarative migration analysis ready")
    return result


def analyze_nifi_workflow_only(xml_path: str) -> Dict[str, Any]:
    """
    Perform only the analysis phase without migration.
    Useful for understanding workflow before committing to migration.

    Args:
        xml_path: Path to NiFi XML template file

    Returns:
        Dictionary containing analysis results
    """

    print("🔍 Analyzing NiFi workflow (analysis only)...")

    # Read XML content
    with open(xml_path, "r", encoding="utf-8") as f:
        xml_content = f.read()

    classification_result = classify_workflow(xml_path)
    groups = _categorise_processors(classification_result["classifications"])
    pruned_payload = _build_pruned_payload(groups)
    chains_result = detect_data_flow_chains(
        xml_content,
        json.dumps(pruned_payload, default=str),
    )

    analysis_result = {
        "workflow": classification_result.get("workflow", {}),
        "summary": classification_result.get("summary", {}),
        "classifications": classification_result.get("classifications", []),
        "pruned_processors": pruned_payload,
        "data_flow_chains": chains_result,
        "xml_path": xml_path,
    }

    print("✅ Analysis completed!")
    return analysis_result

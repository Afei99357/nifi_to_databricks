# tools/improved_pruning.py
# Updated processor pruning and data flow chain detection that works with improved classifier

import json
import xml.etree.ElementTree as ET
from collections import Counter
from typing import Any, Dict, List

from .xml_tools import parse_nifi_template_impl


def _get_cls(proc: Dict[str, Any]) -> str:
    """Normalize old vs new field names for classification."""
    return proc.get("data_manipulation_type") or proc.get("classification") or "unknown"


def _get_ptype(proc: Dict[str, Any]) -> str:
    """Get processor type from either processor_type or type field."""
    return proc.get("processor_type") or proc.get("type") or "Unknown"


def prune_infrastructure_processors(classification_results_json: str) -> str:
    """
    Prune infrastructure-only processors, QUARANTINE unknowns, and keep only
    data-bearing processors (movement/transformation/external).
    Returns JSON with separate 'unknown_processors'.
    """
    KEPT = {"data_transformation", "data_movement", "external_processing"}

    try:
        # ---------- Parse/unwrap possible shapes ----------
        if isinstance(classification_results_json, str):
            data = json.loads(classification_results_json)
            if isinstance(data, list):
                classifications = data
            elif isinstance(data, dict):
                classifications = (
                    data.get("classification_results")
                    or data.get("classification_breakdown")
                    or data.get("processor_classifications")
                    or data.get("pruned_processors")  # allow piping back in
                    or []
                )
            else:
                classifications = []
        else:
            classifications = classification_results_json

        if not classifications:
            return json.dumps(
                {
                    "error": "No processor classifications found in input",
                    "input_sample": str(classification_results_json)[:200],
                }
            )

        # ---------- Buckets ----------
        essential_processors: List[Dict[str, Any]] = []
        removed_processors: List[Dict[str, Any]] = []  # infra only
        unknown_processors: List[Dict[str, Any]] = []  # quarantine bucket

        kept_counts = Counter()
        impact_counts = Counter()
        removed_cls_counts = Counter()
        unknown_cls_counts = Counter()

        for proc in classifications:
            cls = _get_cls(proc)
            # Ensure processor_type is present for downstream code
            if "processor_type" not in proc:
                proc["processor_type"] = _get_ptype(proc)

            if cls in KEPT:
                essential_processors.append(proc)
                kept_counts[cls] += 1
                impact_counts[proc.get("data_impact_level", "unknown")] += 1
            elif cls == "unknown":
                quarantined = dict(proc)
                quarantined["removal_reason"] = (
                    "Classified as 'unknown' - requires manual review"
                )
                unknown_processors.append(quarantined)
                unknown_cls_counts[cls] += 1
            else:
                removed = dict(proc)
                removed["removal_reason"] = (
                    f"Classified as '{cls}' - infrastructure only"
                )
                removed_processors.append(removed)
                removed_cls_counts[cls] += 1

        # ---------- Stats ----------
        original_count = len(classifications)
        kept_count = len(essential_processors)
        pruned_count = len(removed_processors) + len(
            unknown_processors
        )  # both are out of essential set
        reduction_percentage = (
            round((pruned_count / original_count * 100), 1) if original_count else 0.0
        )

        return json.dumps(
            {
                "pruned_processors": essential_processors,  # kept (essential)
                "removed_processors": removed_processors,  # infra-only
                "unknown_processors": unknown_processors,  # quarantine (manual review)
                "summary": {
                    "original_count": original_count,
                    "kept_count": kept_count,
                    "pruned_count": pruned_count,
                    "unknown_count": len(unknown_processors),
                    "reduction_percentage": reduction_percentage,
                    "migration_efficiency": f"Reduced from {original_count} to {kept_count} processors ({reduction_percentage}% reduction)",
                },
                "kept_by_classification": dict(kept_counts),
                "kept_by_impact": dict(impact_counts),
                "removed_classifications": dict(removed_cls_counts),
                "unknown_classifications": dict(
                    unknown_cls_counts
                ),  # usually {"unknown": N}
            }
        )

    except Exception as e:
        return json.dumps(
            {
                "error": f"Failed to prune processors: {str(e)}",
                "input_type": type(classification_results_json).__name__,
            }
        )


def detect_data_flow_chains(xml_content: str, pruned_processors_json: str) -> str:
    """
    Detect source → transformation → sink chains using existing topological sorting
    and connection parsing functionality.

    Leverages existing tools:
    - parse_nifi_template_impl: for connection parsing
    - build_migration_plan: for topological DAG construction

    Args:
        xml_content: NiFi XML template content or file path
        pruned_processors_json: Output from prune_infrastructure_processors

    Returns:
        JSON with detected data flow chains
    """
    try:
        # Parse pruned processors
        pruned_data = json.loads(pruned_processors_json)
        essential_processors = pruned_data.get("pruned_processors", [])

        if not essential_processors:
            return json.dumps(
                {
                    "error": "No essential processors found after pruning",
                    "data_flow_chains": [],
                    "chain_summary": {
                        "total_chains": 0,
                        "avg_chain_length": 0,
                        "processors_in_chains": 0,
                        "isolated_processors": 0,
                    },
                }
            )

        # Extract connections from XML
        all_edges = extract_processor_connections(xml_content)

        # Create lookup for essential processors
        essential_ids = {p.get("id", ""): p for p in essential_processors}
        essential_id_set = set(essential_ids.keys())

        # Create filtered tasks from essential processors (for chain detection)
        filtered_tasks = [
            {"id": p.get("id", ""), "name": p.get("name", ""), "type": _get_ptype(p)}
            for p in essential_processors
        ]

        # Filter edges to only include essential processors
        filtered_edges = [
            e
            for e in all_edges
            if len(e) >= 2 and e[0] in essential_id_set and e[1] in essential_id_set
        ]

        # Detect chains from filtered DAG
        chains = _detect_chains_from_filtered_dag(
            filtered_tasks, filtered_edges, essential_ids
        )

        # Calculate summary statistics
        total_chains = len(chains)
        total_processors_in_chains = sum(
            len(chain.get("processors", [])) for chain in chains
        )
        isolated_processors = len(essential_processors) - total_processors_in_chains
        avg_chain_length = (
            round(total_processors_in_chains / total_chains, 1)
            if total_chains > 0
            else 0
        )

        return json.dumps(
            {
                "data_flow_chains": chains,
                "chain_summary": {
                    "total_chains": total_chains,
                    "avg_chain_length": avg_chain_length,
                    "processors_in_chains": total_processors_in_chains,
                    "isolated_processors": isolated_processors,
                },
                "filtered_dag": {
                    "tasks": filtered_tasks,
                    "edges": filtered_edges,
                },
            }
        )

    except Exception as e:
        return json.dumps(
            {
                "error": f"Failed to detect data flow chains: {str(e)}",
                "xml_content_type": type(xml_content).__name__,
            }
        )


def _detect_chains_from_filtered_dag(
    filtered_tasks: List[Dict[str, Any]],
    filtered_edges: List[List[str]],
    essential_ids: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Detect linear chains from the filtered DAG of essential processors.
    Updated to use _get_ptype helper for processor type access.
    """
    # Build adjacency lists
    outgoing = {}  # node -> [children]
    incoming = {}  # node -> [parents]

    for task in filtered_tasks:
        task_id = task.get("id", "")
        outgoing[task_id] = []
        incoming[task_id] = []

    for edge in filtered_edges:
        if len(edge) >= 2:
            source, target = edge[0], edge[1]
            if source in outgoing and target in incoming:
                outgoing[source].append(target)
                incoming[target].append(source)

    # Find chain sources (nodes with no incoming edges or multiple outgoing)
    sources = [
        node
        for node in outgoing.keys()
        if len(incoming[node]) == 0 or len(outgoing[node]) > 1
    ]

    chains = []
    visited = set()

    for source in sources:
        if source in visited:
            continue

        # Follow the chain from this source
        chain_processors = []
        current = source

        while current and current not in visited:
            visited.add(current)
            if current in essential_ids:
                processor_data = essential_ids[current]
                chain_processors.append(processor_data)

            # Move to next in chain (if single outgoing edge)
            next_nodes = outgoing.get(current, [])
            if len(next_nodes) == 1 and incoming.get(next_nodes[0], []) == [current]:
                current = next_nodes[0]
            else:
                current = None  # End of chain

        # Only create chain if we have processors
        if len(chain_processors) >= 1:
            source_proc = chain_processors[0]
            sink_proc = (
                chain_processors[-1] if len(chain_processors) > 1 else source_proc
            )

            chain = {
                "chain_id": f"flow_{len(chains) + 1}",
                "source": {
                    "name": source_proc.get("name", "Unknown"),
                    "type": _get_ptype(
                        source_proc
                    ),  # Use helper for consistent type access
                    "classification": _get_cls(source_proc),
                    "id": source_proc.get("id", ""),
                },
                "processors": chain_processors,
                "sink": {
                    "name": sink_proc.get("name", "Unknown"),
                    "type": _get_ptype(
                        sink_proc
                    ),  # Use helper for consistent type access
                    "classification": _get_cls(sink_proc),
                    "id": sink_proc.get("id", ""),
                },
                "length": len(chain_processors),
                "business_pattern": _identify_business_pattern(chain_processors),
            }

            chains.append(chain)

    return chains


def _identify_business_pattern(processors: List[Dict[str, Any]]) -> str:
    """
    Identify the business pattern of a chain based on source and sink types.
    Updated to use _get_ptype helper for processor type access.
    """
    if not processors:
        return "unknown"

    source_type = _get_ptype(processors[0]).lower()  # Use helper
    sink_type = _get_ptype(processors[-1]).lower() if len(processors) > 1 else ""

    # File-based patterns
    if any(t in source_type for t in ["getfile", "listfile"]):
        if any(t in sink_type for t in ["puthdfs", "putfile"]):
            return "file_ingestion"
        elif any(t in sink_type for t in ["putsql", "putdatabase"]):
            return "file_to_database"
        else:
            return "file_processing"

    # Stream-based patterns
    elif any(t in source_type for t in ["consumekafka", "listenkafka"]):
        if any(t in sink_type for t in ["publishkafka", "putkafka"]):
            return "stream_processing"
        elif any(t in sink_type for t in ["puthdfs", "putfile"]):
            return "stream_to_file"
        else:
            return "stream_ingestion"

    # Database patterns
    elif any(t in source_type for t in ["executesql", "queryrecord"]):
        if any(t in sink_type for t in ["putsql", "putdatabase"]):
            return "database_transformation"
        elif any(t in sink_type for t in ["puthdfs", "putfile"]):
            return "database_export"
        else:
            return "database_processing"

    # HTTP/API patterns
    elif any(t in source_type for t in ["listenhttp", "invokehttp"]):
        return "api_processing"

    else:
        return "data_transformation"


def extract_processor_connections(xml_content: str) -> List[List[str]]:
    """
    Extract processor connections from NiFi XML.

    Args:
        xml_content: NiFi XML template content

    Returns:
        List of [source_id, destination_id] pairs
    """
    try:
        root = ET.fromstring(xml_content)
        edges = []
        for conn in root.findall(".//connections"):
            src = (conn.findtext(".//source/id") or "").strip()
            dst = (conn.findtext(".//destination/id") or "").strip()
            if src and dst:
                edges.append([src, dst])
        return edges
    except Exception as e:
        print(f"Warning: Failed to extract connections: {e}")
        return []

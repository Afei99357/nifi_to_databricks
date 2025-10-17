"""Parallel Flow Detection for NiFi to Databricks Migration.

Detects independent parallel data flows in NiFi workflows and identifies
parameterized patterns for multi-task notebook generation.
"""

from collections import defaultdict
from typing import Any, Dict, List, Set, Tuple

import networkx as nx


def build_processor_graph(template_data: Dict[str, Any]) -> nx.DiGraph:
    """Build directed graph from NiFi processors and connections.

    Includes processors, input/output ports, and funnels to create complete flow graph.
    """
    G = nx.DiGraph()

    # Add processor nodes
    for proc in template_data.get("processors", []):
        G.add_node(proc["id"], **proc)

    # Add input port nodes
    for port in template_data.get("input_ports", []):
        G.add_node(port["id"], **port, type="INPUT_PORT")

    # Add output port nodes
    for port in template_data.get("output_ports", []):
        G.add_node(port["id"], **port, type="OUTPUT_PORT")

    # Add edges from connections (includes FUNNEL, INPUT_PORT, OUTPUT_PORT connections)
    for conn in template_data.get("connections", []):
        source_id = conn.get("source")
        dest_id = conn.get("destination")
        if source_id and dest_id:
            G.add_edge(source_id, dest_id)

    return G


def find_parallel_subgraphs(G: nx.DiGraph) -> List[Set[str]]:
    """Find independent parallel subgraphs (no shared processors)."""
    # Get weakly connected components (treat as undirected for grouping)
    undirected = G.to_undirected()
    components = list(nx.connected_components(undirected))

    return components


def calculate_flow_similarity(flow1: Set[str], flow2: Set[str], G: nx.DiGraph) -> float:
    """Calculate structural similarity between two flows based on processor types."""
    types1 = [G.nodes[node_id].get("type", "") for node_id in flow1]
    types2 = [G.nodes[node_id].get("type", "") for node_id in flow2]

    # Simple type sequence matching
    if len(types1) != len(types2):
        return 0.0

    matches = sum(1 for t1, t2 in zip(sorted(types1), sorted(types2)) if t1 == t2)
    return matches / len(types1) if types1 else 0.0


def cluster_similar_flows(
    components: List[Set[str]], G: nx.DiGraph, threshold: float = 0.9
) -> List[List[Set[str]]]:
    """Cluster flows with structural similarity above threshold."""
    clusters = []
    used = set()

    for i, flow1 in enumerate(components):
        if i in used:
            continue

        cluster = [flow1]
        used.add(i)

        for j, flow2 in enumerate(components):
            if j <= i or j in used:
                continue

            similarity = calculate_flow_similarity(flow1, flow2, G)
            if similarity >= threshold:
                cluster.append(flow2)
                used.add(j)

        clusters.append(cluster)

    return clusters


def extract_flow_parameters(flow: Set[str], G: nx.DiGraph) -> Dict[str, Any]:
    """Extract differentiating parameters from a flow."""
    params = {}

    # Look for common parameter patterns in processor properties
    for node_id in flow:
        props = G.nodes[node_id].get("properties", {})

        # Extract site/facility
        for key, value in props.items():
            if "site" in key.lower() or "fab" in key.lower():
                params["site"] = value
            elif "table" in key.lower() and "name" in key.lower():
                params["table"] = value
            elif "path" in key.lower() and (
                "s3" in str(value).lower() or "landing" in str(value).lower()
            ):
                params["path"] = value

    return params


def detect_parallel_process_groups(
    template_data: Dict[str, Any], G: nx.DiGraph, threshold: float = 0.9
) -> List[Dict[str, Any]]:
    """Detect parallel flows organized as process groups within connected components.

    This finds parallel patterns where flows are connected via shared infrastructure
    but run in parallel (no direct edges between group processors).
    """
    processors = template_data.get("processors", [])

    # Group processors by parentGroupName
    groups_map = defaultdict(list)
    for proc in processors:
        group_name = proc.get("parentGroupName") or proc.get("parentGroupId") or "Root"
        groups_map[group_name].append(proc)

    # Find groups with similar processor signatures
    parallel_groups = []
    group_names = list(groups_map.keys())

    for i, group1_name in enumerate(group_names):
        group1_procs = groups_map[group1_name]
        group1_types = sorted([p.get("type", "") for p in group1_procs])

        for j, group2_name in enumerate(group_names):
            if j <= i:
                continue

            group2_procs = groups_map[group2_name]
            group2_types = sorted([p.get("type", "") for p in group2_procs])

            # Calculate similarity
            if len(group1_types) != len(group2_types):
                continue

            matches = sum(1 for t1, t2 in zip(group1_types, group2_types) if t1 == t2)
            similarity = matches / len(group1_types) if group1_types else 0.0

            if similarity >= threshold:
                # Check if groups have direct edges between them
                group1_ids = {p["id"] for p in group1_procs}
                group2_ids = {p["id"] for p in group2_procs}

                has_direct_edge = any(
                    G.has_edge(id1, id2) or G.has_edge(id2, id1)
                    for id1 in group1_ids
                    for id2 in group2_ids
                )

                if not has_direct_edge:
                    # Extract parameters from each group
                    params1 = extract_flow_parameters(group1_ids, G)
                    params2 = extract_flow_parameters(group2_ids, G)

                    parallel_groups.append(
                        {
                            "pattern_type": "process_group_parallel",
                            "groups": [
                                {
                                    "group_name": group1_name,
                                    "processor_count": len(group1_procs),
                                    "processor_ids": list(group1_ids),
                                    "parameters": params1,
                                    "signature": group1_types,
                                },
                                {
                                    "group_name": group2_name,
                                    "processor_count": len(group2_procs),
                                    "processor_ids": list(group2_ids),
                                    "parameters": params2,
                                    "signature": group2_types,
                                },
                            ],
                            "similarity": similarity,
                        }
                    )

    return parallel_groups


def detect_parallel_flows(template_data: Dict[str, Any]) -> Dict[str, Any]:
    """Main function to detect parallel flows and extract flow context.

    Detects both:
    1. Disconnected parallel subgraphs (independent flows)
    2. Process-group-based parallel flows (connected but parallel)

    Returns:
        Dictionary with flow groups and metadata for LLM integration
    """
    # Build graph
    G = build_processor_graph(template_data)

    # Layer 1: Find disconnected parallel subgraphs
    components = find_parallel_subgraphs(G)
    clusters = cluster_similar_flows(components, G, threshold=0.9)

    # Layer 2: Find process-group-based parallel flows
    process_group_patterns = detect_parallel_process_groups(template_data, G)

    # Build flow context
    flow_groups = []
    processor_to_flow = {}
    cluster_idx = 0

    # Add disconnected flow clusters
    for cluster in clusters:
        if len(cluster) <= 1:
            continue

        flows_in_cluster = []
        for flow_idx, flow in enumerate(cluster):
            flow_id = f"disconnected_flow_{cluster_idx}_{flow_idx}"
            params = extract_flow_parameters(flow, G)

            for proc_id in flow:
                processor_to_flow[proc_id] = {
                    "flow_id": flow_id,
                    "cluster_id": cluster_idx,
                    "is_parallel": True,
                    "pattern_type": "disconnected",
                    "parameters": params,
                }

            flows_in_cluster.append(
                {"flow_id": flow_id, "processor_ids": list(flow), "parameters": params}
            )

        flow_groups.append(
            {
                "cluster_id": cluster_idx,
                "pattern_type": "disconnected",
                "flows": flows_in_cluster,
                "is_parallel": True,
            }
        )
        cluster_idx += 1

    # Add process-group-based parallel flows
    for pg_idx, pattern in enumerate(process_group_patterns):
        flows_in_cluster = []

        for group_idx, group in enumerate(pattern["groups"]):
            flow_id = f"process_group_{cluster_idx}_{group_idx}"

            for proc_id in group["processor_ids"]:
                processor_to_flow[proc_id] = {
                    "flow_id": flow_id,
                    "cluster_id": cluster_idx,
                    "is_parallel": True,
                    "pattern_type": "process_group",
                    "group_name": group["group_name"],
                    "parameters": group["parameters"],
                }

            flows_in_cluster.append(
                {
                    "flow_id": flow_id,
                    "group_name": group["group_name"],
                    "processor_ids": group["processor_ids"],
                    "parameters": group["parameters"],
                }
            )

        flow_groups.append(
            {
                "cluster_id": cluster_idx,
                "pattern_type": "process_group",
                "flows": flows_in_cluster,
                "is_parallel": True,
                "similarity": pattern["similarity"],
            }
        )
        cluster_idx += 1

    return {
        "flow_groups": flow_groups,
        "processor_to_flow": processor_to_flow,
        "total_parallel_flows": sum(len(group["flows"]) for group in flow_groups),
    }

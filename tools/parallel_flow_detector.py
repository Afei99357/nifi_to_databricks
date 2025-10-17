"""Parallel Flow Detection for NiFi to Databricks Migration.

Detects independent parallel data flows in NiFi workflows and identifies
parameterized patterns for multi-task notebook generation.
"""

from collections import defaultdict
from typing import Any, Dict, List, Set, Tuple

import networkx as nx


def build_processor_graph(template_data: Dict[str, Any]) -> nx.DiGraph:
    """Build directed graph from NiFi processors and connections."""
    G = nx.DiGraph()

    # Add processor nodes
    for proc in template_data.get("processors", []):
        G.add_node(proc["id"], **proc)

    # Add edges from connections
    for conn in template_data.get("connections", []):
        source_id = conn.get("sourceId")
        dest_id = conn.get("destinationId")
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


def detect_parallel_flows(template_data: Dict[str, Any]) -> Dict[str, Any]:
    """Main function to detect parallel flows and extract flow context.

    Returns:
        Dictionary with flow groups and metadata for LLM integration
    """
    # Build graph
    G = build_processor_graph(template_data)

    # Find parallel subgraphs
    components = find_parallel_subgraphs(G)

    # Cluster similar flows
    clusters = cluster_similar_flows(components, G, threshold=0.9)

    # Build flow context
    flow_groups = []
    processor_to_flow = {}

    for cluster_idx, cluster in enumerate(clusters):
        flows_in_cluster = []

        for flow_idx, flow in enumerate(cluster):
            flow_id = f"flow_{cluster_idx}_{flow_idx}"
            params = extract_flow_parameters(flow, G)

            # Map processors to flow
            for proc_id in flow:
                processor_to_flow[proc_id] = {
                    "flow_id": flow_id,
                    "cluster_id": cluster_idx,
                    "is_parallel": len(cluster) > 1,
                    "parameters": params,
                }

            flows_in_cluster.append(
                {"flow_id": flow_id, "processor_ids": list(flow), "parameters": params}
            )

        if len(cluster) > 1:  # Only include parallel clusters
            flow_groups.append(
                {
                    "cluster_id": cluster_idx,
                    "flows": flows_in_cluster,
                    "is_parallel": True,
                }
            )

    return {
        "flow_groups": flow_groups,
        "processor_to_flow": processor_to_flow,
        "total_parallel_flows": sum(len(group["flows"]) for group in flow_groups),
    }

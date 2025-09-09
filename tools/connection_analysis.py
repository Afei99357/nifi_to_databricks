# tools/connection_analysis.py
# Fan-in / Fan-out analysis utilities for NiFi XML workflows
# Provides architectural insights for NiFi → Databricks migration planning

import json
import xml.etree.ElementTree as ET
from collections import defaultdict, deque
from typing import Dict, List, Optional, Set, Tuple


def build_nifi_graph(xml_text: str, allowed_ids: Optional[Set[str]] = None) -> dict:
    """
    Build a directed graph of the NiFi flow.

    Args:
        xml_text: Raw NiFi XML content
        allowed_ids: if provided, keep only edges where both src/dst are in this set

    Returns:
        dict: {
            "nodes": {id -> {id, name, type, x, y}},
            "edges": [(source_id, dest_id), ...],
            "in_adj": {node_id -> [upstream_ids]},
            "out_adj": {node_id -> [downstream_ids]}
        }
    """
    root = ET.fromstring(xml_text)

    nodes: Dict[str, dict] = {}
    edges: List[Tuple[str, str]] = []

    def _add_node(_id, name, ntype, x=None, y=None):
        if _id and _id not in nodes:
            nodes[_id] = {
                "id": _id,
                "name": name or _id,
                "type": ntype or "Unknown",
                "x": x,
                "y": y,
            }

    # Extract processors
    for p in root.findall(".//processors/processor"):
        _id = (p.findtext("id") or "").strip()
        name = (p.findtext("name") or "").strip() or _id
        ptype = (p.findtext("type") or "").strip()
        pos = p.find("position")
        x = (
            float(pos.findtext("x"))
            if pos is not None and pos.find("x") is not None
            else None
        )
        y = (
            float(pos.findtext("y"))
            if pos is not None and pos.find("y") is not None
            else None
        )
        _add_node(_id, name, ptype, x, y)

    # Extract ports (inside process groups)
    for port_tag, label in [
        (".//inputPorts/port", "[InPort]"),
        (".//outputPorts/port", "[OutPort]"),
    ]:
        for pt in root.findall(port_tag):
            _id = (pt.findtext("id") or "").strip()
            name = (pt.findtext("name") or "").strip()
            _add_node(_id, f"{label} {name}", label)

    # Extract connections
    for c in root.findall(".//connections/connection"):
        src = c.find("source")
        dst = c.find("destination")
        if src is None or dst is None:
            continue
        s_id = (src.findtext("id") or "").strip()
        d_id = (dst.findtext("id") or "").strip()
        if not s_id or not d_id:
            continue
        if allowed_ids and (s_id not in allowed_ids or d_id not in allowed_ids):
            continue
        edges.append((s_id, d_id))

    # Build adjacency lists
    out_adj = defaultdict(list)
    in_adj = defaultdict(list)
    present = set()
    for s, d in edges:
        out_adj[s].append(d)
        in_adj[d].append(s)
        present.add(s)
        present.add(d)

    # Keep only nodes present in this (possibly filtered) graph
    nodes = {nid: nodes[nid] for nid in nodes if nid in present}

    return {
        "nodes": nodes,
        "edges": edges,
        "in_adj": dict(in_adj),
        "out_adj": dict(out_adj),
    }


def top_fanin_fanout(graph: dict, k: int = 10) -> dict:
    """
    Return top-k fan-in and fan-out hotspots with metadata.

    Args:
        graph: Output from build_nifi_graph()
        k: Number of top hotspots to return

    Returns:
        dict: {"top_in": [...], "top_out": [...]} with enriched node data
    """
    in_deg = {n: len(graph["in_adj"].get(n, [])) for n in graph["nodes"]}
    out_deg = {n: len(graph["out_adj"].get(n, [])) for n in graph["nodes"]}

    top_in_ids = sorted(in_deg.items(), key=lambda x: x[1], reverse=True)[:k]
    top_out_ids = sorted(out_deg.items(), key=lambda x: x[1], reverse=True)[:k]

    def _enrich(items):
        enriched = []
        for nid, deg in items:
            if deg == 0:  # Skip nodes with no connections
                continue
            meta = graph["nodes"][nid]
            enriched.append(
                {
                    "id": nid,
                    "name": meta["name"],
                    "type": meta["type"],
                    "degree": deg,
                    "x": meta.get("x"),
                    "y": meta.get("y"),
                }
            )
        return enriched

    return {"top_in": _enrich(top_in_ids), "top_out": _enrich(top_out_ids)}


def neighborhood(
    graph: dict,
    center_id: str,
    up_depth: int = 2,
    down_depth: int = 2,
    max_nodes: int = 150,
) -> dict:
    """
    Extract a subgraph around a hotspot node.

    Args:
        graph: Output from build_nifi_graph()
        center_id: ID of the hotspot node to analyze
        up_depth: How many hops upstream to include
        down_depth: How many hops downstream to include
        max_nodes: Maximum nodes to prevent huge subgraphs

    Returns:
        dict: {"center": node_meta, "nodes": {id->meta}, "edges": [(s,d)]}
    """
    nodes = graph["nodes"]
    in_adj = graph["in_adj"]
    out_adj = graph["out_adj"]

    if center_id not in nodes:
        return {"center": None, "nodes": {}, "edges": []}

    # Upstream BFS
    up_seen = {center_id}
    up_edges = []
    q = deque([(center_id, 0)])
    while q:
        nid, d = q.popleft()
        if d >= up_depth:
            continue
        for src in in_adj.get(nid, []):
            if src not in up_seen:
                up_seen.add(src)
                up_edges.append((src, nid))
                if len(up_seen) < max_nodes:
                    q.append((src, d + 1))

    # Downstream BFS
    down_seen = {center_id}
    down_edges = []
    q = deque([(center_id, 0)])
    while q:
        nid, d = q.popleft()
        if d >= down_depth:
            continue
        for dst in out_adj.get(nid, []):
            if dst not in down_seen:
                down_seen.add(dst)
                down_edges.append((nid, dst))
                if len(down_seen) < max_nodes:
                    q.append((dst, d + 1))

    sub_ids = (up_seen | down_seen) & set(nodes.keys())
    sub_nodes = {nid: nodes[nid] for nid in sub_ids}
    sub_edges = list({*up_edges, *down_edges})  # de-dup

    return {
        "center": nodes.get(center_id),
        "nodes": sub_nodes,
        "edges": sub_edges,
    }


def render_hotspots_md(graph: dict, hotspots: dict, up_depth=2, down_depth=2) -> str:
    """
    Generate human-friendly Markdown report showing hotspot details.

    Args:
        graph: Output from build_nifi_graph()
        hotspots: Output from top_fanin_fanout()
        up_depth: Depth for context (not used in current implementation)
        down_depth: Depth for context (not used in current implementation)

    Returns:
        str: Markdown formatted report
    """
    lines = ["# NiFi Connection Analysis - Fan-in / Fan-out Hotspots", ""]

    # Fan-in section
    lines.append("## 📥 Top Fan-in Hotspots (Merge/Join Points)")
    lines.append("*These processors receive data from multiple upstream sources*")
    lines.append("")

    if not hotspots["top_in"]:
        lines.append("- *(No significant fan-in nodes found)*")
    else:
        for h in hotspots["top_in"]:
            nid = h["id"]
            meta = graph["nodes"][nid]
            ups = [graph["nodes"][u]["name"] for u in graph["in_adj"].get(nid, [])][:5]
            dns = [graph["nodes"][d]["name"] for d in graph["out_adj"].get(nid, [])][:5]

            lines.append(f"### **{meta['name']}** (`{nid}`)")
            lines.append(
                f"- **Fan-in**: {len(graph['in_adj'].get(nid, []))} inputs, **Fan-out**: {len(graph['out_adj'].get(nid, []))} outputs"
            )
            lines.append(f"- **Type**: `{meta['type'].split('.')[-1]}`")
            if meta.get("x") and meta.get("y"):
                lines.append(f"- **Position**: ({meta.get('x')}, {meta.get('y')})")

            if ups:
                lines.append(f"- **Upstream**: {', '.join(ups)}")
            if dns:
                lines.append(f"- **Downstream**: {', '.join(dns)}")
            lines.append("")

    # Fan-out section
    lines.append("## 📤 Top Fan-out Hotspots (Split/Route Points)")
    lines.append("*These processors send data to multiple downstream destinations*")
    lines.append("")

    if not hotspots["top_out"]:
        lines.append("- *(No significant fan-out nodes found)*")
    else:
        for h in hotspots["top_out"]:
            nid = h["id"]
            meta = graph["nodes"][nid]
            ups = [graph["nodes"][u]["name"] for u in graph["in_adj"].get(nid, [])][:5]
            dns = [graph["nodes"][d]["name"] for d in graph["out_adj"].get(nid, [])][:5]

            lines.append(f"### **{meta['name']}** (`{nid}`)")
            lines.append(
                f"- **Fan-out**: {len(graph['out_adj'].get(nid, []))} outputs, **Fan-in**: {len(graph['in_adj'].get(nid, []))} inputs"
            )
            lines.append(f"- **Type**: `{meta['type'].split('.')[-1]}`")
            if meta.get("x") and meta.get("y"):
                lines.append(f"- **Position**: ({meta.get('x')}, {meta.get('y')})")

            if ups:
                lines.append(f"- **Upstream**: {', '.join(ups)}")
            if dns:
                lines.append(f"- **Downstream**: {', '.join(dns)}")
            lines.append("")

    return "\n".join(lines)


def to_dot(subgraph: dict) -> str:
    """
    Export a neighborhood subgraph to Graphviz DOT format for visualization.
    Usage: dot -Tpng hotspot.dot -o hotspot.png

    Args:
        subgraph: Output from neighborhood()

    Returns:
        str: DOT format content
    """
    lines = [
        "digraph nifi {",
        "  rankdir=LR;",
        '  node [shape=box, fontname="Helvetica"];',
    ]

    center_id = subgraph["center"]["id"] if subgraph["center"] else None

    for nid, meta in subgraph["nodes"].items():
        label = (meta["name"] or nid).replace('"', r"\"")
        ntype = meta["type"].split(".")[-1]
        style = (
            'style="filled,bold", fillcolor="lightyellow"' if nid == center_id else ""
        )
        lines.append(f'  "{nid}" [label="{label}\\n({ntype})" {style}];')

    for s, d in subgraph["edges"]:
        lines.append(f'  "{s}" -> "{d}";')

    lines.append("}")
    return "\n".join(lines)


def summarize_fanin_fanout_report(
    xml_text: str,
    pruned_ids: Optional[Set[str]] = None,
    k: int = 10,
    up_depth: int = 2,
    down_depth: int = 2,
) -> dict:
    """
    One-call convenience function for complete fan-in/fan-out analysis.

    Args:
        xml_text: Raw NiFi XML content
        pruned_ids: Optional set of processor IDs to filter to (for pruned analysis)
        k: Number of top hotspots to return
        up_depth: Upstream depth for neighborhood analysis
        down_depth: Downstream depth for neighborhood analysis

    Returns:
        dict: {"graph": graph_data, "hotspots": hotspot_data, "markdown": report_md}
    """
    try:
        graph = build_nifi_graph(xml_text, allowed_ids=pruned_ids)
        hotspots = top_fanin_fanout(graph, k=k)
        md = render_hotspots_md(
            graph, hotspots, up_depth=up_depth, down_depth=down_depth
        )

        return {
            "graph": graph,
            "hotspots": hotspots,
            "markdown": md,
        }
    except Exception as e:
        return {
            "graph": {"nodes": {}, "edges": [], "in_adj": {}, "out_adj": {}},
            "hotspots": {"top_in": [], "top_out": []},
            "markdown": f"# Connection Analysis Failed\n\nError: {str(e)}",
        }


def generate_connection_analysis_reports(xml_content: str, pruned_result: dict) -> dict:
    """
    Generate both full and pruned connection analysis for migration planning.

    Args:
        xml_content: Raw NiFi XML content
        pruned_result: Output from prune_infrastructure_processors()

    Returns:
        dict: Complete connection analysis with full and pruned views
    """
    try:
        # Parse pruned result to get essential processor IDs
        if isinstance(pruned_result, str):
            pruned_data = json.loads(pruned_result)
        else:
            pruned_data = pruned_result

        essential_ids = {
            p.get("id") for p in pruned_data.get("pruned_processors", []) if p.get("id")
        }

        # Full workflow analysis (all processors)
        print("🔍 [CONNECTION] Analyzing full workflow connections...")
        full_analysis = summarize_fanin_fanout_report(xml_content, k=10)

        # Essential processors analysis (pruned view)
        print("🎯 [CONNECTION] Analyzing essential processor connections...")
        pruned_analysis = summarize_fanin_fanout_report(
            xml_content, pruned_ids=essential_ids, k=10
        )

        # Calculate reduction metrics
        total_nodes = len(full_analysis["graph"]["nodes"])
        essential_nodes = len(pruned_analysis["graph"]["nodes"])
        total_edges = len(full_analysis["graph"]["edges"])
        essential_edges = len(pruned_analysis["graph"]["edges"])

        complexity_reduction = (
            ((total_nodes - essential_nodes) / total_nodes * 100)
            if total_nodes > 0
            else 0
        )

        return {
            "full_workflow_connections": full_analysis["markdown"],
            "essential_connections": pruned_analysis["markdown"],
            "connection_summary": {
                "total_processors": total_nodes,
                "essential_processors": essential_nodes,
                "total_connections": total_edges,
                "essential_connections": essential_edges,
                "complexity_reduction": f"{complexity_reduction:.1f}%",
                "pruning_effectiveness": (
                    "High"
                    if complexity_reduction > 60
                    else "Medium" if complexity_reduction > 30 else "Low"
                ),
            },
            "full_analysis_data": full_analysis,
            "pruned_analysis_data": pruned_analysis,
        }
    except Exception as e:
        return {
            "full_workflow_connections": f"# Connection Analysis Failed\n\nError analyzing full workflow: {str(e)}",
            "essential_connections": f"# Connection Analysis Failed\n\nError analyzing essential connections: {str(e)}",
            "connection_summary": {
                "total_processors": 0,
                "essential_processors": 0,
                "total_connections": 0,
                "essential_connections": 0,
                "complexity_reduction": "0.0%",
                "pruning_effectiveness": "Unknown",
            },
            "error": str(e),
        }

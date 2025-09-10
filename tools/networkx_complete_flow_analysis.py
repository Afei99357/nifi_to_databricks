# tools/networkx_complete_flow_analysis.py
# Complete NiFi workflow analysis using NetworkX for comprehensive migration planning
# Multi-tier connection categorization with migration-focused insights

import json
import xml.etree.ElementTree as ET
from typing import Any, Dict, Optional, Set

import networkx as nx


def _txt(elem, *paths):
    """Helper for safe text extraction with component/* fallbacks"""
    for p in paths:
        n = elem.find(p)
        if n is not None:
            t = (n.text or "").strip()
            if t:
                return t
    return ""


def build_complete_nifi_graph(xml_text: str, allowed_ids: Optional[Set[str]] = None):
    """
    Build a comprehensive NetworkX graph of the entire NiFi workflow.

    Args:
        xml_text: Raw XML content from NiFi template
        allowed_ids: Optional set of processor IDs to include (for pruned analysis)

    Returns:
        NetworkX MultiDiGraph with rich node and edge attributes (supports parallel edges)
    """
    root = ET.fromstring(xml_text)
    G = (
        nx.MultiDiGraph()
    )  # Support multiple parallel edges (success/failure relationships)

    # Component type mapping for migration analysis
    COMPONENT_TYPES = {
        "processor": {
            "priority": "critical",
            "databricks_equivalent": "PySpark operations",
        },
        "input_port": {
            "priority": "critical",
            "databricks_equivalent": "Pipeline inputs",
        },
        "output_port": {
            "priority": "critical",
            "databricks_equivalent": "Pipeline outputs",
        },
        "funnel": {"priority": "medium", "databricks_equivalent": "DataFrame unions"},
        "remote_input_port": {
            "priority": "high",
            "databricks_equivalent": "External data sources",
        },
        "remote_output_port": {
            "priority": "high",
            "databricks_equivalent": "External data sinks",
        },
    }

    # Extract processors with rich attributes - handle both regular and versioned flows
    valid_processor_ids = set()
    for proc in root.findall(".//processors"):
        proc_id = _txt(proc, "id", "component/id")
        if not proc_id:
            continue

        valid_processor_ids.add(proc_id)
        proc_name = _txt(proc, "name", "component/name") or f"Processor-{proc_id[:8]}"
        proc_type = _txt(proc, "type", "component/type")

        G.add_node(
            proc_id,
            component_type="processor",
            name=proc_name,
            processor_type=proc_type,
            short_type=proc_type.split(".")[-1] if proc_type else "Unknown",
            migration_priority=COMPONENT_TYPES["processor"]["priority"],
            databricks_equivalent=COMPONENT_TYPES["processor"]["databricks_equivalent"],
        )

    # Extract input ports - correct XPath
    for port in root.findall(".//inputPorts"):
        port_id = _txt(port, "id", "component/id")
        if not port_id:
            continue

        port_name = _txt(port, "name", "component/name") or f"InputPort-{port_id[:8]}"

        G.add_node(
            port_id,
            component_type="input_port",
            name=port_name,
            processor_type="InputPort",
            short_type="InputPort",
            migration_priority=COMPONENT_TYPES["input_port"]["priority"],
            databricks_equivalent=COMPONENT_TYPES["input_port"][
                "databricks_equivalent"
            ],
        )

    # Extract output ports - correct XPath
    for port in root.findall(".//outputPorts"):
        port_id = _txt(port, "id", "component/id")
        if not port_id:
            continue

        port_name = _txt(port, "name", "component/name") or f"OutputPort-{port_id[:8]}"

        G.add_node(
            port_id,
            component_type="output_port",
            name=port_name,
            processor_type="OutputPort",
            short_type="OutputPort",
            migration_priority=COMPONENT_TYPES["output_port"]["priority"],
            databricks_equivalent=COMPONENT_TYPES["output_port"][
                "databricks_equivalent"
            ],
        )

    # Extract funnels
    for funnel in root.findall(".//funnels/funnel"):
        funnel_id = _txt(funnel, "id", "component/id")
        if not funnel_id:
            continue

        name = _txt(funnel, "name", "component/name") or f"Funnel-{funnel_id[:8]}"

        G.add_node(
            funnel_id,
            component_type="funnel",
            name=name,
            processor_type="Funnel",
            short_type="Funnel",
            migration_priority=COMPONENT_TYPES["funnel"]["priority"],
            databricks_equivalent=COMPONENT_TYPES["funnel"]["databricks_equivalent"],
        )

    # Extract remote process group ports - correct XPath
    for rpg in root.findall(".//remoteProcessGroups/remoteProcessGroup"):
        target_uri = (
            _txt(
                rpg,
                "targetUri",
                "targetUris",
                "component/targetUri",
                "component/targetUris",
            )
            or "Unknown"
        )

        # Remote input ports
        for port in rpg.findall(".//inputPorts/remoteProcessGroupPort"):
            port_id = _txt(port, "id", "component/id")
            if not port_id:
                continue

            port_name = (
                _txt(port, "name", "component/name") or f"RemoteInput-{port_id[:8]}"
            )

            G.add_node(
                port_id,
                component_type="remote_input_port",
                name=port_name,
                processor_type="RemoteInputPort",
                short_type="RemoteInputPort",
                target_uri=target_uri,
                migration_priority=COMPONENT_TYPES["remote_input_port"]["priority"],
                databricks_equivalent=COMPONENT_TYPES["remote_input_port"][
                    "databricks_equivalent"
                ],
            )

        # Remote output ports
        for port in rpg.findall(".//outputPorts/remoteProcessGroupPort"):
            port_id = _txt(port, "id", "component/id")
            if not port_id:
                continue

            port_name = (
                _txt(port, "name", "component/name") or f"RemoteOutput-{port_id[:8]}"
            )

            G.add_node(
                port_id,
                component_type="remote_output_port",
                name=port_name,
                processor_type="RemoteOutputPort",
                short_type="RemoteOutputPort",
                target_uri=target_uri,
                migration_priority=COMPONENT_TYPES["remote_output_port"]["priority"],
                databricks_equivalent=COMPONENT_TYPES["remote_output_port"][
                    "databricks_equivalent"
                ],
            )

    # Extract connections with categorization - one edge per relationship
    for conn in root.findall(".//connections"):
        cid = _txt(conn, "id", "component/id") or "conn"
        src = _txt(conn, "source/id", "component/source/id")
        dst = _txt(conn, "destination/id", "component/destination/id")
        if not src or not dst or (src not in G) or (dst not in G):
            continue
        if allowed_ids and (src not in allowed_ids or dst not in allowed_ids):
            continue

        src_type = G.nodes[src]["component_type"]
        dst_type = G.nodes[dst]["component_type"]
        connection_type = f"{src_type}_to_{dst_type}"
        migration_impact = _get_connection_migration_impact(src_type, dst_type)

        # relationships (with component/* fallback)
        rels = [
            r.text or ""
            for r in (
                conn.findall("selectedRelationships/relationship")
                or conn.findall("component/selectedRelationships/relationship")
            )
        ] or ["unlabeled"]

        # queue / backpressure (with component/* fallback)
        bpo = (
            _txt(
                conn,
                "flowFileQueue/backPressureObjectThreshold",
                "component/flowFileQueue/backPressureObjectThreshold",
            )
            or None
        )
        bps = (
            _txt(
                conn,
                "flowFileQueue/backPressureDataSizeThreshold",
                "component/flowFileQueue/backPressureDataSizeThreshold",
            )
            or None
        )
        prios = [
            p.text or ""
            for p in (
                conn.findall("flowFileQueue/prioritizers/child")
                or conn.findall("component/flowFileQueue/prioritizers/child")
            )
        ]

        # edge category (with remote precedence)
        if ("remote" in src_type) or ("remote" in dst_type):
            edge_category = "external_connection"
        elif src_type == "processor" and dst_type == "processor":
            edge_category = "processor_flow"
        elif "port" in src_type or "port" in dst_type:
            edge_category = "port_connection"
        elif src_type == "funnel" or dst_type == "funnel":
            edge_category = "routing_flow"
        else:
            edge_category = "other"

        for rel in rels:
            G.add_edge(
                src,
                dst,
                key=f"{cid}:{rel}",
                cid=cid,
                relationship=rel,
                connection_type=connection_type,
                src_component=src_type,
                dst_component=dst_type,
                migration_impact=migration_impact,
                edge_category=edge_category,
                backpressure_object_threshold=bpo,
                backpressure_size_threshold=bps,
                prioritizers=prios,
                connection_name=_txt(conn, "name", "component/name"),
            )

    return G


def _get_connection_migration_impact(src_type: str, dst_type: str) -> str:
    """Determine migration impact of a connection type"""
    if src_type == "processor" and dst_type == "processor":
        return "high"  # Core business logic flow
    elif "port" in src_type or "port" in dst_type:
        return "critical"  # Pipeline boundaries
    elif "remote" in src_type or "remote" in dst_type:
        return "critical"  # External system integration
    elif src_type == "funnel" or dst_type == "funnel":
        return "medium"  # Data routing
    else:
        return "low"


def analyze_complete_workflow(G, k: int = 10) -> Dict[str, Any]:
    """
    Perform comprehensive analysis of the NiFi workflow using NetworkX algorithms.

    Args:
        G: NetworkX MultiDiGraph from build_complete_nifi_graph()
        k: Number of top hotspots to return

    Returns:
        Complete analysis with multiple perspectives
    """
    analysis = {}

    # 1. Component Overview
    component_counts = {}
    for node_id in G.nodes():
        comp_type = G.nodes[node_id]["component_type"]
        component_counts[comp_type] = component_counts.get(comp_type, 0) + 1

    analysis["component_overview"] = {
        "total_components": G.number_of_nodes(),
        "total_connections": G.number_of_edges(),
        "component_breakdown": component_counts,
    }

    # 2. Processing Hotspots (Processor-only analysis with edge filtering)
    # Build processor-only multigraph edge-subgraph first
    processor_nodes = [
        n for n, d in G.nodes(data=True) if d.get("component_type") == "processor"
    ]
    pp_edges = [
        (u, v, k)
        for u, v, k, d in G.edges(keys=True, data=True)
        if (
            u in processor_nodes
            and v in processor_nodes
            and d.get("edge_category") == "processor_flow"
        )
    ]
    pp = G.edge_subgraph(pp_edges).copy()

    if pp.number_of_nodes() > 0:
        # Collapse parallel edges for centrality
        pp_simple = nx.DiGraph()
        pp_simple.add_nodes_from((n, G.nodes[n]) for n in pp.nodes())
        pp_simple.add_edges_from((u, v) for u, v in pp.edges())

        n = pp_simple.number_of_nodes()
        k_sample = min(100, n) if n > 100 else None
        betweenness = nx.betweenness_centrality(pp_simple, k=k_sample)
        # Separate in/out degrees (on simple graph)
        in_deg = dict(pp_simple.in_degree())
        out_deg = dict(pp_simple.out_degree())

        def enrich(dct):
            return [
                {
                    "node_id": nid,
                    "score": score,
                    "name": G.nodes[nid]["name"],
                    "processor_type": G.nodes[nid]["short_type"],
                    "full_type": G.nodes[nid]["processor_type"],
                }
                for nid, score in sorted(dct.items(), key=lambda x: x[1], reverse=True)[
                    :k
                ]
            ]

        analysis["processing_hotspots"] = {
            "critical_path_bottlenecks": enrich(betweenness),
            "high_connectivity_in": enrich(in_deg),
            "high_connectivity_out": enrich(out_deg),
        }
    else:
        analysis["processing_hotspots"] = {
            "critical_path_bottlenecks": [],
            "high_connectivity_in": [],
            "high_connectivity_out": [],
        }

    # 3. Integration Points Analysis
    integration_components = [
        n
        for n in G.nodes()
        if G.nodes[n]["component_type"]
        in ["input_port", "output_port", "remote_input_port", "remote_output_port"]
    ]

    integration_analysis = []
    for comp_id in integration_components:
        node_data = G.nodes[comp_id]
        integration_analysis.append(
            {
                "id": comp_id,
                "name": node_data["name"],
                "component_type": node_data["component_type"],
                "in_degree": G.in_degree(comp_id),
                "out_degree": G.out_degree(comp_id),
                "migration_priority": node_data["migration_priority"],
                "databricks_equivalent": node_data["databricks_equivalent"],
            }
        )

    analysis["integration_points"] = sorted(
        integration_analysis,
        key=lambda x: x["in_degree"] + x["out_degree"],
        reverse=True,
    )[:k]

    # 4. Routing Infrastructure
    routing_components = [
        n for n in G.nodes() if G.nodes[n]["component_type"] == "funnel"
    ]
    routing_analysis = []
    for funnel_id in routing_components:
        node_data = G.nodes[funnel_id]
        routing_analysis.append(
            {
                "id": funnel_id,
                "name": node_data["name"],
                "in_degree": G.in_degree(funnel_id),
                "out_degree": G.out_degree(funnel_id),
                "consolidation_factor": G.in_degree(
                    funnel_id
                ),  # How many streams it merges
            }
        )

    analysis["routing_infrastructure"] = sorted(
        routing_analysis, key=lambda x: x["consolidation_factor"], reverse=True
    )

    # 5. External Interfaces
    external_components = [
        n for n in G.nodes() if "remote" in G.nodes[n]["component_type"]
    ]

    external_analysis = []
    for ext_id in external_components:
        node_data = G.nodes[ext_id]
        external_analysis.append(
            {
                "id": ext_id,
                "name": node_data["name"],
                "component_type": node_data["component_type"],
                "target_uri": node_data.get("target_uri", "Unknown"),
                "connection_count": G.in_degree(ext_id) + G.out_degree(ext_id),
            }
        )

    analysis["external_interfaces"] = external_analysis

    # 6. Connection Type Analysis (MultiDiGraph aware)
    connection_types = {}
    edge_categories = {}
    relationship_patterns = {}
    migration_impacts = {"critical": 0, "high": 0, "medium": 0, "low": 0}

    for src, dst, key, edge_data in G.edges(keys=True, data=True):
        # Connection type breakdown
        conn_type = edge_data["connection_type"]
        connection_types[conn_type] = connection_types.get(conn_type, 0) + 1

        # Edge category breakdown
        edge_cat = edge_data.get("edge_category", "unknown")
        edge_categories[edge_cat] = edge_categories.get(edge_cat, 0) + 1

        # Migration impact breakdown
        impact = edge_data.get("migration_impact", "low")
        migration_impacts[impact] = migration_impacts.get(impact, 0) + 1

        # Relationship pattern analysis (now per edge, not per list)
        relationship = edge_data.get("relationship", "")
        if relationship:
            relationship_patterns[relationship] = (
                relationship_patterns.get(relationship, 0) + 1
            )

    analysis["connection_analysis"] = {
        "connection_type_breakdown": connection_types,
        "edge_category_breakdown": edge_categories,
        "migration_impact_breakdown": migration_impacts,
        "relationship_patterns": relationship_patterns,
        "total_parallel_edges": G.number_of_edges(),
        "unique_node_pairs": len(set((u, v) for u, v, _ in G.edges(keys=True))),
    }

    # 7. Path Analysis (data flow tracing)
    try:
        # Find all source nodes (no incoming edges)
        sources = [n for n in G.nodes() if G.in_degree(n) == 0]
        # Find all sink nodes (no outgoing edges)
        sinks = [n for n in G.nodes() if G.out_degree(n) == 0]

        critical_paths = []
        for source in sources[:3]:  # Limit to avoid explosion
            for sink in sinks[:3]:
                try:
                    path = nx.shortest_path(G, source, sink)
                    if len(path) > 2:  # Only meaningful paths
                        critical_paths.append(
                            {
                                "source": G.nodes[source]["name"],
                                "sink": G.nodes[sink]["name"],
                                "path_length": len(path),
                                "processor_count": sum(
                                    1
                                    for n in path
                                    if G.nodes[n]["component_type"] == "processor"
                                ),
                            }
                        )
                except nx.NetworkXNoPath:
                    continue

        analysis["data_flow_paths"] = sorted(
            critical_paths, key=lambda x: x["processor_count"], reverse=True
        )[:k]
    except:
        analysis["data_flow_paths"] = []

    return analysis


def generate_complete_flow_markdown_report(analysis: Dict[str, Any]) -> str:
    """Generate comprehensive markdown report from complete flow analysis"""
    lines = [
        "# 🕸️ Complete NiFi Flow Analysis",
        "",
        "## 📊 Migration Impact Summary",
        f"- **Critical Components**: {analysis['component_overview']['total_components']} total components",
        f"- **Total Connections**: {analysis['component_overview']['total_connections']} connections",
        "",
        "### Component Breakdown:",
    ]

    # Component breakdown with migration priorities
    component_icons = {
        "processor": "🔧 **CRITICAL**",
        "input_port": "📥 **CRITICAL**",
        "output_port": "📤 **CRITICAL**",
        "funnel": "🔀 **MEDIUM**",
        "remote_input_port": "🌐 **HIGH**",
        "remote_output_port": "🌍 **HIGH**",
    }

    for comp_type, count in analysis["component_overview"][
        "component_breakdown"
    ].items():
        icon = component_icons.get(comp_type, "❓ **UNKNOWN**")
        lines.append(f"- {icon}: {count} {comp_type.replace('_', ' ').title()}(s)")

    lines.extend(["", "---", ""])

    # Processing Hotspots (Business Logic)
    lines.extend(
        [
            "## 🔧 Processing Hotspots (Business Logic)",
            "*CRITICAL - Core processors that handle actual data transformation*",
            "",
            "### Migration Priority: CRITICAL - Core business logic",
            "",
        ]
    )

    if analysis["processing_hotspots"]["critical_path_bottlenecks"]:
        lines.append("#### Critical Path Bottlenecks")
        lines.append(
            "*Processors that lie on many data flow paths - potential performance bottlenecks*"
        )
        lines.append("")

        for hotspot in analysis["processing_hotspots"]["critical_path_bottlenecks"][:5]:
            lines.append(
                f"- **{hotspot['name']}** (`{hotspot['processor_type']}`): Centrality Score {hotspot['score']:.3f}"
            )
            lines.append(
                f"  - **Databricks Design**: Replace with optimized PySpark operations"
            )
        lines.append("")

    if analysis["processing_hotspots"]["high_connectivity_in"]:
        lines.append("#### High Input Connectivity (Merge Points)")
        lines.append(
            "*Processors receiving data from many sources - complex join/merge logic*"
        )
        lines.append("")

        for hotspot in analysis["processing_hotspots"]["high_connectivity_in"][:5]:
            lines.append(
                f"- **{hotspot['name']}** (`{hotspot['processor_type']}`): {hotspot['score']} inputs"
            )
            lines.append(
                f"  - **Databricks Design**: Use DataFrame.union() or complex join operations"
            )
        lines.append("")

    if analysis["processing_hotspots"]["high_connectivity_out"]:
        lines.append("#### High Output Connectivity (Split Points)")
        lines.append(
            "*Processors sending data to many destinations - complex routing logic*"
        )
        lines.append("")

        for hotspot in analysis["processing_hotspots"]["high_connectivity_out"][:5]:
            lines.append(
                f"- **{hotspot['name']}** (`{hotspot['processor_type']}`): {hotspot['score']} outputs"
            )
            lines.append(
                f"  - **Databricks Design**: Use conditional logic and multiple output streams"
            )
        lines.append("")

    # Integration Points (Pipeline Interfaces)
    lines.extend(
        [
            "## 🚪 Integration Points (Pipeline Interfaces)",
            "*CRITICAL - Components that handle data entry/exit from the workflow*",
            "",
            "### Migration Priority: CRITICAL - Design pipeline boundaries",
            "",
        ]
    )

    for integration in analysis["integration_points"][:5]:
        icon_map = {
            "input_port": "📥",
            "output_port": "📤",
            "remote_input_port": "🌐",
            "remote_output_port": "🌍",
        }
        icon = icon_map.get(integration["component_type"], "❓")

        lines.extend(
            [
                f"### {icon} **{integration['name']}**",
                f"- **Type**: {integration['component_type'].replace('_', ' ').title()}",
                f"- **Connections**: {integration['in_degree']} in, {integration['out_degree']} out",
                f"- **Migration Priority**: {integration['migration_priority'].upper()}",
                f"- **Databricks Design**: {integration['databricks_equivalent']}",
                "",
            ]
        )

    # Routing Infrastructure (Flow Control)
    if analysis["routing_infrastructure"]:
        lines.extend(
            [
                "## 🔀 Routing Infrastructure (Flow Control)",
                "*MEDIUM - Funnels and routing components that consolidate data streams*",
                "",
                "### Migration Priority: MEDIUM - DataFrame union/join operations",
                "",
            ]
        )

        for funnel in analysis["routing_infrastructure"]:
            lines.extend(
                [
                    f"### **{funnel['name']}**",
                    f"- **Consolidates**: {funnel['consolidation_factor']} input streams",
                    f"- **Outputs to**: {funnel['out_degree']} destinations",
                    f"- **Databricks Design**: Replace with DataFrame union operations",
                    f"- **Implementation**: `df1.union(df2).union(df3)...`",
                    "",
                ]
            )

    # External Interfaces (Cross-System)
    if analysis["external_interfaces"]:
        lines.extend(
            [
                "## 🌐 External Interfaces (Cross-System)",
                "*HIGH - Remote connections requiring external system integration*",
                "",
                "### Migration Priority: HIGH - Requires connectivity planning",
                "",
            ]
        )

        for external in analysis["external_interfaces"]:
            icon = "🌐" if "input" in external["component_type"] else "🌍"
            lines.extend(
                [
                    f"### {icon} **{external['name']}**",
                    f"- **Type**: {external['component_type'].replace('_', ' ').title()}",
                    f"- **Target**: {external['target_uri']}",
                    f"- **Connections**: {external['connection_count']}",
                    f"- **Migration**: Requires external connectivity planning",
                    f"- **Databricks Design**: Replace with cloud messaging or API connections",
                    "",
                ]
            )

    # Connection Analysis with Migration Impact
    lines.extend(
        [
            "## 🔗 Connection Analysis",
            "",
            f"**Total Parallel Edges**: {analysis['connection_analysis']['total_parallel_edges']}",
            f"**Unique Node Pairs**: {analysis['connection_analysis']['unique_node_pairs']}",
            "",
            "### Migration Impact Breakdown:",
        ]
    )

    migration_impacts = analysis["connection_analysis"]["migration_impact_breakdown"]
    for impact, count in migration_impacts.items():
        icon = {"critical": "🔴", "high": "🟡", "medium": "🔵", "low": "⚪"}.get(
            impact, "❓"
        )
        priority_text = {
            "critical": "CRITICAL - Pipeline boundaries & external systems",
            "high": "HIGH - Core business logic flows",
            "medium": "MEDIUM - Data routing and consolidation",
            "low": "LOW - Simple connections",
        }.get(impact, "Unknown priority")
        lines.append(
            f"- {icon} **{impact.title()}**: {count} connections - {priority_text}"
        )

    lines.extend(["", "### Connection Types:"])
    for conn_type, count in analysis["connection_analysis"][
        "connection_type_breakdown"
    ].items():
        lines.append(f"- **{conn_type.replace('_', ' → ')}**: {count} connections")

    lines.extend(["", "### Edge Categories:"])
    for edge_cat, count in analysis["connection_analysis"][
        "edge_category_breakdown"
    ].items():
        lines.append(f"- **{edge_cat.replace('_', ' ')}**: {count} connections")

    # Show relationship patterns if available
    if analysis["connection_analysis"]["relationship_patterns"]:
        lines.extend(["", "### NiFi Relationship Patterns:"])
        for rel, count in sorted(
            analysis["connection_analysis"]["relationship_patterns"].items(),
            key=lambda x: x[1],
            reverse=True,
        )[:10]:
            lines.append(f"- **{rel}**: {count} uses")

    # Data Flow Paths
    if analysis["data_flow_paths"]:
        lines.extend(
            [
                "",
                "## 🛤️ Critical Data Flow Paths",
                "*End-to-end data processing pathways*",
                "",
            ]
        )

        for i, path in enumerate(analysis["data_flow_paths"][:3], 1):
            complexity_level = (
                "High"
                if path["processor_count"] > 5
                else "Medium" if path["processor_count"] > 2 else "Low"
            )
            lines.extend(
                [
                    f"### Path {i}: {path['source']} → {path['sink']}",
                    f"- **Total Steps**: {path['path_length']}",
                    f"- **Processing Steps**: {path['processor_count']} processors",
                    f"- **Migration Complexity**: {complexity_level}",
                    f"- **Databricks Design**: {'Multi-stage pipeline with complex dependencies' if complexity_level == 'High' else 'Standard ETL pipeline' if complexity_level == 'Medium' else 'Simple data flow'}",
                    "",
                ]
            )

    # Migration Recommendations
    lines.extend(
        [
            "",
            "## 🎯 Migration Recommendations",
            "",
            "### Immediate Actions (CRITICAL Priority)",
            "1. **Processing Hotspots** → Design PySpark operations for core business logic",
            "2. **Integration Points** → Plan pipeline input/output architecture",
            "3. **External Interfaces** → Design connectivity for external systems",
            "",
            "### Secondary Actions (HIGH/MEDIUM Priority)",
            "4. **Routing Infrastructure** → Replace funnels with DataFrame operations",
            "5. **Connection Patterns** → Implement relationship logic in Databricks",
            "",
            "### Architecture Considerations",
            "- **High fan-in processors** → Consider Delta Live Tables for complex joins",
            "- **High fan-out processors** → Use multiple output streams or conditional logic",
            "- **External connections** → Plan for cloud messaging or REST APIs",
            "- **Complex paths** → Design multi-stage pipelines with proper dependencies",
        ]
    )

    return "\n".join(lines)


def generate_connection_analysis_reports(xml_content: str, pruned_result: dict) -> dict:
    """
    Generate comprehensive NetworkX-based connection analysis for migration planning.

    Args:
        xml_content: Raw NiFi XML content
        pruned_result: Output from prune_infrastructure_processors()

    Returns:
        dict: Complete connection analysis with NetworkX insights
    """
    try:
        # Parse pruned result to get essential processor IDs
        if isinstance(pruned_result, str):
            pruned_data = json.loads(pruned_result)
        else:
            pruned_data = pruned_result

        # Full workflow analysis using NetworkX
        print("🔍 [CONNECTION] Building complete NiFi workflow graph...")
        try:
            G = build_complete_nifi_graph(xml_content)
            print(
                f"🔍 [CONNECTION] Built graph with {G.number_of_nodes()} nodes and {G.number_of_edges()} edges"
            )
            networkx_analysis = analyze_complete_workflow(G, k=10)
            full_workflow_markdown = generate_complete_flow_markdown_report(
                networkx_analysis
            )
        except Exception as e:
            import traceback

            print(f"⚠️ [CONNECTION] NetworkX analysis failed: {e}")
            print(f"⚠️ [CONNECTION] Full traceback: {traceback.format_exc()}")
            # Fallback to basic analysis
            networkx_analysis = {
                "component_overview": {"total_components": 0, "total_connections": 0},
                "processing_hotspots": {
                    "critical_path_bottlenecks": [],
                    "high_connectivity_in": [],
                    "high_connectivity_out": [],
                },
                "integration_points": [],
                "routing_infrastructure": [],
                "external_interfaces": [],
                "connection_analysis": {
                    "migration_impact_breakdown": {
                        "critical": 0,
                        "high": 0,
                        "medium": 0,
                        "low": 0,
                    }
                },
                "data_flow_paths": [],
            }
            full_workflow_markdown = f"# NetworkX Analysis Failed\n\nError: {str(e)}"

        # Calculate metrics
        processor_count = networkx_analysis["component_overview"][
            "component_breakdown"
        ].get("processor", 0)
        total_processors = processor_count
        actual_essential_count = len(pruned_data.get("pruned_processors", []))
        total_connections = networkx_analysis["component_overview"]["total_connections"]

        complexity_reduction = (
            ((total_processors - actual_essential_count) / total_processors * 100)
            if total_processors > 0
            else 0
        )

        return {
            "full_workflow_connections": full_workflow_markdown,
            "connection_summary": {
                "total_processors": total_processors,
                "essential_processors": actual_essential_count,
                "total_connections": total_connections,
                "complexity_reduction": f"{complexity_reduction:.1f}%",
                "pruning_effectiveness": (
                    "High"
                    if complexity_reduction > 60
                    else "Medium" if complexity_reduction > 30 else "Low"
                ),
            },
            "networkx_analysis": networkx_analysis,
        }
    except Exception as e:
        return {
            "full_workflow_connections": f"# Complete Flow Analysis Failed\n\nError analyzing workflow: {str(e)}",
            "connection_summary": {
                "total_processors": 0,
                "essential_processors": 0,
                "total_connections": 0,
                "complexity_reduction": "0.0%",
                "pruning_effectiveness": "Unknown",
            },
            "error": str(e),
        }

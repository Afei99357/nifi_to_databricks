"""
NiFi Variable Dependencies Analysis
Analyzes variable definitions, modifications, and usage across processors.
"""

import re
from collections import defaultdict
from typing import Any, Dict, List, Set, Tuple

from .xml_tools import parse_nifi_template_impl

__all__ = [
    "extract_variable_dependencies",
    "find_variable_definitions",
    "find_variable_usage",
    "trace_variable_flows",
    "analyze_variable_transformations",
]


def extract_variable_dependencies(xml_path: str) -> Dict[str, Any]:
    """
    Extract comprehensive variable dependency mapping from NiFi workflow.

    Args:
        xml_path: Path to NiFi XML template file

    Returns:
        Dictionary with variable analysis results
    """
    # Parse XML to get processors and connections
    with open(xml_path, "r", encoding="utf-8") as f:
        xml_content = f.read()

    template_data = parse_nifi_template_impl(xml_content)
    processors = template_data["processors"]
    connections = template_data["connections"]

    # Extract variable patterns
    variable_definitions = find_variable_definitions(processors)
    variable_usage = find_variable_usage(processors)
    variable_flows = trace_variable_flows(processors, connections)
    variable_transformations = analyze_variable_transformations(processors)

    # Build comprehensive mapping
    all_variables = set(variable_definitions.keys()) | set(variable_usage.keys())
    variable_analysis = {}

    for var_name in all_variables:
        definitions = variable_definitions.get(var_name, [])
        usages = variable_usage.get(var_name, [])
        flows = variable_flows.get(var_name, [])
        transformations = variable_transformations.get(var_name, [])

        variable_analysis[var_name] = {
            "variable_name": var_name,
            "definitions": definitions,
            "usages": usages,
            "flows": flows,
            "transformations": transformations,
            "definition_count": len(definitions),
            "usage_count": len(usages),
            "processor_count": len(
                set(
                    [d["processor_id"] for d in definitions]
                    + [u["processor_id"] for u in usages]
                )
            ),
            "is_defined": len(definitions) > 0,
            "is_external": len(definitions) == 0,
        }

    return {
        "total_variables": len(all_variables),
        "defined_variables": len(
            [v for v in variable_analysis.values() if v["is_defined"]]
        ),
        "external_variables": len(
            [v for v in variable_analysis.values() if v["is_external"]]
        ),
        "total_processors": len(processors),
        "variables": variable_analysis,
        "processors": {p["id"]: p for p in processors},
        "connections": connections,
    }


def find_variable_definitions(processors: List[Dict[str, Any]]) -> Dict[str, List]:
    """
    Find processors that DEFINE variables (typically UpdateAttribute processors).

    Args:
        processors: List of all processors from XML

    Returns:
        Dictionary mapping variable names to their definitions
    """
    variable_definitions = defaultdict(list)

    for proc in processors:
        proc_id = proc.get("id", "unknown")
        proc_name = proc.get("name", "unknown")
        proc_type = proc.get("type", "unknown")
        properties = proc.get("properties", {})

        # Focus on processors that typically define variables
        if "UpdateAttribute" in proc_type or "EvaluateJsonPath" in proc_type:
            for prop_name, prop_value in properties.items():
                if isinstance(prop_value, str) and prop_value:
                    # Property names without ${} are likely variable definitions
                    # Property values can contain expressions that define variables
                    if not re.search(r"\$\{[^}]+\}", prop_name):
                        # This property defines a variable with prop_name as the variable name
                        variable_definitions[prop_name].append(
                            {
                                "processor_id": proc_id,
                                "processor_name": proc_name,
                                "processor_type": proc_type,
                                "property_name": prop_name,
                                "property_value": prop_value,
                                "definition_type": (
                                    "static" if "${" not in prop_value else "dynamic"
                                ),
                            }
                        )

    return dict(variable_definitions)


def find_variable_usage(processors: List[Dict[str, Any]]) -> Dict[str, List]:
    """
    Find processors that USE variables (${variable} patterns).

    Args:
        processors: List of all processors from XML

    Returns:
        Dictionary mapping variable names to their usages
    """
    variable_usage = defaultdict(list)

    # Pattern to match ${variable.name} expressions
    variable_pattern = re.compile(r"\$\{([^}]+)\}")

    for proc in processors:
        proc_id = proc.get("id", "unknown")
        proc_name = proc.get("name", "unknown")
        proc_type = proc.get("type", "unknown")
        properties = proc.get("properties", {})

        # Scan all properties for ${variable} usage
        for prop_name, prop_value in properties.items():
            if isinstance(prop_value, str):
                # Find all ${variable} patterns
                variable_matches = variable_pattern.findall(prop_value)

                for var_name in variable_matches:
                    # Clean up variable name (remove function calls, etc.)
                    clean_var_name = (
                        var_name.split(":")[0] if ":" in var_name else var_name
                    )

                    variable_usage[clean_var_name].append(
                        {
                            "processor_id": proc_id,
                            "processor_name": proc_name,
                            "processor_type": proc_type,
                            "property_name": prop_name,
                            "property_value": prop_value,
                            "variable_expression": f"${{{var_name}}}",
                            "usage_context": f"Used in {prop_name}",
                            "full_expression": var_name,
                            "has_functions": ":" in var_name,
                        }
                    )

    return dict(variable_usage)


def trace_variable_flows(
    processors: List[Dict[str, Any]], connections: List[Dict[str, Any]]
) -> Dict[str, List]:
    """
    Trace how variables flow through processor connections.

    Args:
        processors: List of all processors
        connections: List of all connections

    Returns:
        Dictionary mapping variable names to their flow chains
    """
    # Build processor lookup and connection graph
    processor_lookup = {p["id"]: p for p in processors}
    connection_graph = defaultdict(list)

    for conn in connections:
        source_id = conn["source"]
        dest_id = conn["destination"]
        relationships = conn.get("relationships", [])

        if source_id in processor_lookup and dest_id in processor_lookup:
            connection_graph[source_id].append(
                {
                    "target_id": dest_id,
                    "relationships": relationships,
                    "connection_id": conn.get("id", "unknown"),
                }
            )

    # Get variable definitions and usage
    variable_definitions = find_variable_definitions(processors)
    variable_usage = find_variable_usage(processors)

    variable_flows = defaultdict(list)

    # For each variable, trace its flow from definition to usage
    for var_name in set(variable_definitions.keys()) | set(variable_usage.keys()):
        defining_processors = [
            d["processor_id"] for d in variable_definitions.get(var_name, [])
        ]
        using_processors = [u["processor_id"] for u in variable_usage.get(var_name, [])]

        # Build flow chains from each defining processor
        for def_proc_id in defining_processors:
            flow_chain = trace_processor_flow(
                def_proc_id,
                using_processors,
                connection_graph,
                processor_lookup,
                var_name,
            )
            if flow_chain:
                variable_flows[var_name].extend(flow_chain)

    return dict(variable_flows)


def trace_processor_flow(
    start_proc_id: str,
    target_proc_ids: List[str],
    connection_graph: Dict,
    processor_lookup: Dict,
    var_name: str,
) -> List[Dict]:
    """
    Trace flow from a starting processor to target processors.
    """
    flow_chains = []
    visited = set()

    def dfs(current_id: str, path: List[str], relationships_path: List[str]):
        if current_id in visited and len(path) > 1:
            return

        visited.add(current_id)
        path.append(current_id)

        # Check if we reached a target processor
        if current_id in target_proc_ids:
            flow_chains.append(
                {
                    "variable_name": var_name,
                    "flow_chain": path.copy(),
                    "relationships": relationships_path.copy(),
                    "processors": [
                        {
                            "processor_id": pid,
                            "processor_name": processor_lookup[pid].get(
                                "name", "unknown"
                            ),
                            "processor_type": processor_lookup[pid].get(
                                "type", "unknown"
                            ),
                        }
                        for pid in path
                        if pid in processor_lookup
                    ],
                    "chain_length": len(path),
                }
            )

        # Continue tracing through connections (limit depth to avoid infinite loops)
        if len(path) < 20:
            for connection in connection_graph.get(current_id, []):
                target_id = connection["target_id"]
                relationships = connection["relationships"]
                if target_id not in path:  # Avoid cycles
                    dfs(target_id, path.copy(), relationships_path + relationships)

        path.pop()
        visited.discard(current_id)

    dfs(start_proc_id, [], [])
    return flow_chains


def analyze_variable_transformations(
    processors: List[Dict[str, Any]],
) -> Dict[str, List]:
    """
    Analyze how variables are transformed/modified by processors.

    Args:
        processors: List of all processors

    Returns:
        Dictionary mapping variable names to their transformations
    """
    transformations = defaultdict(list)

    for proc in processors:
        proc_id = proc.get("id", "unknown")
        proc_name = proc.get("name", "unknown")
        proc_type = proc.get("type", "unknown")
        properties = proc.get("properties", {})

        # Look for UpdateAttribute processors that modify variables
        if "UpdateAttribute" in proc_type:
            for prop_name, prop_value in properties.items():
                if isinstance(prop_value, str) and "${" in prop_value:
                    # This processor transforms variables
                    # Extract input variables and output variable
                    input_vars = re.findall(r"\$\{([^}]+)\}", prop_value)
                    output_var = prop_name

                    for input_var in input_vars:
                        clean_input_var = (
                            input_var.split(":")[0] if ":" in input_var else input_var
                        )

                        transformations[clean_input_var].append(
                            {
                                "processor_id": proc_id,
                                "processor_name": proc_name,
                                "processor_type": proc_type,
                                "input_variable": clean_input_var,
                                "output_variable": output_var,
                                "transformation_expression": prop_value,
                                "transformation_type": (
                                    "modification"
                                    if clean_input_var == output_var
                                    else "derivation"
                                ),
                                "has_functions": ":" in input_var,
                            }
                        )

    return dict(transformations)

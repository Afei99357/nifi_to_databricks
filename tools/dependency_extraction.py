"""
Comprehensive dependency analysis for NiFi processors.
Analyzes ALL processors in workflow to extract complete dependency mapping.
"""

import re
from collections import defaultdict, deque
from typing import Any, Dict, List, Set, Tuple

from .xml_tools import parse_nifi_template_impl

__all__ = [
    "extract_all_processor_dependencies",
    "find_variable_dependencies",
    "map_processor_connections",
    "find_property_dependencies",
    "find_configuration_dependencies",
    "generate_dependency_report",
]


def extract_all_processor_dependencies(xml_path: str) -> Dict[str, Any]:
    """
    Extract comprehensive dependency mapping across ALL processors in the workflow.

    Args:
        xml_path: Path to NiFi XML template file

    Returns:
        Dictionary with complete dependency analysis results
    """
    # Parse XML to get all processors and connections
    with open(xml_path, "r", encoding="utf-8") as f:
        xml_content = f.read()

    template_data = parse_nifi_template_impl(xml_content)
    processors = template_data["processors"]
    connections = template_data["connections"]

    # Extract different types of dependencies
    variable_deps = find_variable_dependencies(processors)
    connection_deps = map_processor_connections(processors, connections)
    property_deps = find_property_dependencies(processors)
    config_deps = find_configuration_dependencies(processors)

    # Build comprehensive dependency mapping
    all_dependencies = build_comprehensive_dependency_map(
        processors, variable_deps, connection_deps, property_deps, config_deps
    )

    # Generate impact analysis
    impact_analysis = generate_impact_analysis(processors, all_dependencies)

    return {
        "total_processors": len(processors),
        "processors": processors,
        "dependencies": {
            "variable_dependencies": variable_deps,
            "connection_dependencies": connection_deps,
            "property_dependencies": property_deps,
            "configuration_dependencies": config_deps,
            "comprehensive_mapping": all_dependencies,
        },
        "impact_analysis": impact_analysis,
        "dependency_statistics": calculate_dependency_statistics(all_dependencies),
    }


def find_variable_dependencies(processors: List[Dict[str, Any]]) -> Dict[str, Dict]:
    """
    Track ALL ${variable} definitions and usages across processors.

    Args:
        processors: List of all processors from XML

    Returns:
        Dictionary mapping variables to their definitions and usages
    """
    variable_definitions = defaultdict(
        list
    )  # variable_name -> list of processors that define it
    variable_usages = defaultdict(
        list
    )  # variable_name -> list of processors that use it

    for proc in processors:
        proc_id = proc.get("id", "unknown")
        proc_name = proc.get("name", "unknown")
        proc_type = proc.get("type", "unknown")
        properties = proc.get("properties", {})

        # Find variable definitions (processors that SET variables)
        for prop_name, prop_value in properties.items():
            if isinstance(prop_value, str):
                # Check if this processor defines variables
                # Common patterns: UpdateAttribute processors, EvaluateJsonPath with destination attributes
                if (
                    "UpdateAttribute" in proc_type
                    or "EvaluateJsonPath" in proc_type
                    or "RouteOnAttribute" in proc_type
                ):

                    # Property names that don't contain ${} are likely variable definitions
                    if not re.search(r"\$\{[^}]+\}", prop_name):
                        variable_definitions[prop_name].append(
                            {
                                "processor_id": proc_id,
                                "processor_name": proc_name,
                                "processor_type": proc_type,
                                "property_name": prop_name,
                                "property_value": prop_value,
                                "definition_context": "property_definition",
                            }
                        )

        # Find variable usages (processors that USE ${variable})
        for prop_name, prop_value in properties.items():
            if isinstance(prop_value, str):
                # Find all ${variable} patterns
                variable_matches = re.findall(r"\$\{([^}]+)\}", prop_value)
                for var_name in variable_matches:
                    variable_usages[var_name].append(
                        {
                            "processor_id": proc_id,
                            "processor_name": proc_name,
                            "processor_type": proc_type,
                            "property_name": prop_name,
                            "property_value": prop_value,
                            "usage_context": f"Used in {prop_name}",
                        }
                    )

    # Build variable dependency mapping
    variable_deps = {}

    # Add all defined variables
    for var_name, definitions in variable_definitions.items():
        variable_deps[var_name] = {
            "variable_name": var_name,
            "definitions": definitions,
            "usages": variable_usages.get(var_name, []),
            "is_defined": len(definitions) > 0,
            "is_used": len(variable_usages.get(var_name, [])) > 0,
            "dependency_type": "variable_dependency",
        }

    # Add variables that are used but not defined (external dependencies)
    for var_name, usages in variable_usages.items():
        if var_name not in variable_deps:
            variable_deps[var_name] = {
                "variable_name": var_name,
                "definitions": [],
                "usages": usages,
                "is_defined": False,
                "is_used": True,
                "dependency_type": "external_variable_dependency",
            }

    return variable_deps


def map_processor_connections(
    processors: List[Dict[str, Any]], connections: List[Dict[str, Any]]
) -> Dict[str, Dict]:
    """
    Map processor-to-processor flow connections comprehensively.

    Args:
        processors: List of all processors
        connections: List of all connections

    Returns:
        Dictionary mapping processor dependencies via connections
    """
    # Build processor lookup
    processor_lookup = {proc["id"]: proc for proc in processors}

    # Build adjacency lists
    outgoing_connections = defaultdict(
        list
    )  # proc_id -> list of destination processors
    incoming_connections = defaultdict(list)  # proc_id -> list of source processors

    for conn in connections:
        source_id = conn["source"]
        dest_id = conn["destination"]
        relationships = conn.get("relationships", [])

        if source_id in processor_lookup and dest_id in processor_lookup:
            source_proc = processor_lookup[source_id]
            dest_proc = processor_lookup[dest_id]

            connection_info = {
                "connection_id": f"{source_id}->{dest_id}",
                "source_processor": {
                    "id": source_id,
                    "name": source_proc.get("name", "unknown"),
                    "type": source_proc.get("type", "unknown"),
                },
                "destination_processor": {
                    "id": dest_id,
                    "name": dest_proc.get("name", "unknown"),
                    "type": dest_proc.get("type", "unknown"),
                },
                "relationships": relationships,
            }

            outgoing_connections[source_id].append(connection_info)
            incoming_connections[dest_id].append(connection_info)

    # Build connection dependency mapping
    connection_deps = {}

    for proc in processors:
        proc_id = proc["id"]
        proc_name = proc.get("name", "unknown")
        proc_type = proc.get("type", "unknown")

        connection_deps[proc_id] = {
            "processor_id": proc_id,
            "processor_name": proc_name,
            "processor_type": proc_type,
            "dependencies_outgoing": outgoing_connections.get(proc_id, []),
            "dependencies_incoming": incoming_connections.get(proc_id, []),
            "total_outgoing": len(outgoing_connections.get(proc_id, [])),
            "total_incoming": len(incoming_connections.get(proc_id, [])),
            "dependency_type": "connection_dependency",
        }

    return connection_deps


def find_property_dependencies(processors: List[Dict[str, Any]]) -> Dict[str, List]:
    """
    Identify processors that reference other processors' outputs or configurations.

    Args:
        processors: List of all processors

    Returns:
        Dictionary mapping property-based dependencies
    """
    property_deps = defaultdict(list)

    # Build processor name/id lookup for reference detection
    processor_references = {}
    for proc in processors:
        proc_id = proc.get("id", "")
        proc_name = proc.get("name", "")
        processor_references[proc_id] = proc_name
        processor_references[proc_name] = proc_id

    for proc in processors:
        proc_id = proc.get("id", "unknown")
        proc_name = proc.get("name", "unknown")
        proc_type = proc.get("type", "unknown")
        properties = proc.get("properties", {})

        # Look for references to other processors in property values
        for prop_name, prop_value in properties.items():
            if isinstance(prop_value, str):
                # Common patterns that might reference other processors:
                # 1. Controller service references
                # 2. Processor group references
                # 3. Named references in configurations

                potential_refs = []

                # Check for processor ID patterns (UUIDs)
                uuid_pattern = (
                    r"[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}"
                )
                uuid_matches = re.findall(uuid_pattern, prop_value.lower())

                for uuid_match in uuid_matches:
                    if uuid_match in processor_references:
                        potential_refs.append(
                            {
                                "reference_type": "processor_id_reference",
                                "referenced_processor_id": uuid_match,
                                "referenced_processor_name": processor_references[
                                    uuid_match
                                ],
                                "property_name": prop_name,
                                "property_value": prop_value,
                            }
                        )

                # Check for processor name references
                for ref_name in processor_references:
                    if (
                        ref_name != proc_name and len(ref_name) > 3
                    ):  # Avoid short false positives
                        if ref_name.lower() in prop_value.lower():
                            potential_refs.append(
                                {
                                    "reference_type": "processor_name_reference",
                                    "referenced_processor_name": ref_name,
                                    "referenced_processor_id": processor_references.get(
                                        ref_name, "unknown"
                                    ),
                                    "property_name": prop_name,
                                    "property_value": prop_value,
                                }
                            )

                if potential_refs:
                    property_deps[proc_id].extend(
                        [
                            {
                                "processor_id": proc_id,
                                "processor_name": proc_name,
                                "processor_type": proc_type,
                                "dependency_type": "property_dependency",
                                **ref,
                            }
                            for ref in potential_refs
                        ]
                    )

    return dict(property_deps)


def find_configuration_dependencies(
    processors: List[Dict[str, Any]],
) -> Dict[str, List]:
    """
    Find shared configuration values and references across processors.

    Args:
        processors: List of all processors

    Returns:
        Dictionary mapping configuration-based dependencies
    """
    config_deps = defaultdict(list)

    # Group processors by shared configuration values
    config_value_groups = defaultdict(list)

    for proc in processors:
        proc_id = proc.get("id", "unknown")
        proc_name = proc.get("name", "unknown")
        proc_type = proc.get("type", "unknown")
        properties = proc.get("properties", {})

        for prop_name, prop_value in properties.items():
            if isinstance(prop_value, str) and len(prop_value.strip()) > 0:
                # Skip very common values that aren't meaningful dependencies
                if prop_value.lower() not in [
                    "true",
                    "false",
                    "success",
                    "failure",
                    "1",
                    "0",
                    "yes",
                    "no",
                ]:
                    config_key = f"{prop_name}:{prop_value}"
                    config_value_groups[config_key].append(
                        {
                            "processor_id": proc_id,
                            "processor_name": proc_name,
                            "processor_type": proc_type,
                            "property_name": prop_name,
                            "property_value": prop_value,
                        }
                    )

    # Identify shared configurations (used by multiple processors)
    for config_key, processors_using_config in config_value_groups.items():
        if len(processors_using_config) > 1:
            prop_name, prop_value = config_key.split(":", 1)

            for proc_info in processors_using_config:
                config_deps[proc_info["processor_id"]].append(
                    {
                        "processor_id": proc_info["processor_id"],
                        "processor_name": proc_info["processor_name"],
                        "processor_type": proc_info["processor_type"],
                        "dependency_type": "configuration_dependency",
                        "shared_config_key": config_key,
                        "shared_property_name": prop_name,
                        "shared_property_value": prop_value,
                        "shared_with_processors": [
                            p
                            for p in processors_using_config
                            if p["processor_id"] != proc_info["processor_id"]
                        ],
                    }
                )

    return dict(config_deps)


def build_comprehensive_dependency_map(
    processors: List[Dict[str, Any]],
    variable_deps: Dict,
    connection_deps: Dict,
    property_deps: Dict,
    config_deps: Dict,
) -> Dict[str, Dict]:
    """
    Build comprehensive dependency mapping combining all dependency types.

    Returns:
        Dictionary with complete dependency relationships for each processor
    """
    comprehensive_deps = {}

    for proc in processors:
        proc_id = proc["id"]
        proc_name = proc.get("name", "unknown")
        proc_type = proc.get("type", "unknown")

        # Collect dependencies from all sources
        depends_on = []
        dependents = []

        # Variable dependencies
        for var_name, var_info in variable_deps.items():
            # If this processor defines a variable
            for definition in var_info.get("definitions", []):
                if definition["processor_id"] == proc_id:
                    # Find processors that use this variable
                    for usage in var_info.get("usages", []):
                        if usage["processor_id"] != proc_id:
                            dependents.append(
                                {
                                    "dependent_processor_id": usage["processor_id"],
                                    "dependent_processor_name": usage["processor_name"],
                                    "dependency_type": "variable_provider",
                                    "variable_name": var_name,
                                    "details": f"Provides variable '{var_name}' used in {usage['property_name']}",
                                }
                            )

            # If this processor uses a variable
            for usage in var_info.get("usages", []):
                if usage["processor_id"] == proc_id:
                    # Find processors that define this variable
                    for definition in var_info.get("definitions", []):
                        if definition["processor_id"] != proc_id:
                            depends_on.append(
                                {
                                    "dependency_processor_id": definition[
                                        "processor_id"
                                    ],
                                    "dependency_processor_name": definition[
                                        "processor_name"
                                    ],
                                    "dependency_type": "variable_consumer",
                                    "variable_name": var_name,
                                    "details": f"Uses variable '{var_name}' from {definition['property_name']}",
                                }
                            )

        # Connection dependencies
        conn_info = connection_deps.get(proc_id, {})
        for incoming in conn_info.get("dependencies_incoming", []):
            depends_on.append(
                {
                    "dependency_processor_id": incoming["source_processor"]["id"],
                    "dependency_processor_name": incoming["source_processor"]["name"],
                    "dependency_type": "flow_dependency",
                    "details": f"Receives flow from {incoming['source_processor']['name']}",
                }
            )

        for outgoing in conn_info.get("dependencies_outgoing", []):
            dependents.append(
                {
                    "dependent_processor_id": outgoing["destination_processor"]["id"],
                    "dependent_processor_name": outgoing["destination_processor"][
                        "name"
                    ],
                    "dependency_type": "flow_provider",
                    "details": f"Sends flow to {outgoing['destination_processor']['name']}",
                }
            )

        # Property dependencies
        for prop_dep in property_deps.get(proc_id, []):
            depends_on.append(
                {
                    "dependency_processor_id": prop_dep.get(
                        "referenced_processor_id", "unknown"
                    ),
                    "dependency_processor_name": prop_dep.get(
                        "referenced_processor_name", "unknown"
                    ),
                    "dependency_type": "property_reference",
                    "details": f"References {prop_dep.get('referenced_processor_name', 'processor')} in {prop_dep['property_name']}",
                }
            )

        # Configuration dependencies
        for config_dep in config_deps.get(proc_id, []):
            for shared_proc in config_dep.get("shared_with_processors", []):
                depends_on.append(
                    {
                        "dependency_processor_id": shared_proc["processor_id"],
                        "dependency_processor_name": shared_proc["processor_name"],
                        "dependency_type": "configuration_shared",
                        "details": f"Shares configuration '{config_dep['shared_property_name']}' with {shared_proc['processor_name']}",
                    }
                )

        comprehensive_deps[proc_id] = {
            "processor_id": proc_id,
            "processor_name": proc_name,
            "processor_type": proc_type,
            "depends_on": depends_on,
            "dependents": dependents,
            "dependency_count": len(depends_on),
            "dependent_count": len(dependents),
            "total_relationships": len(depends_on) + len(dependents),
        }

    return comprehensive_deps


def generate_impact_analysis(
    processors: List[Dict[str, Any]], comprehensive_deps: Dict[str, Dict]
) -> Dict[str, Any]:
    """
    Generate impact analysis for dependency changes.

    Returns:
        Dictionary with impact analysis results
    """
    impact_analysis = {
        "high_impact_processors": [],
        "isolated_processors": [],
        "dependency_chains": [],
        "circular_dependencies": [],
    }

    # Identify high-impact processors (many dependents)
    for proc_id, dep_info in comprehensive_deps.items():
        if dep_info["dependent_count"] > 3:  # Threshold for high impact
            impact_analysis["high_impact_processors"].append(
                {
                    "processor_id": proc_id,
                    "processor_name": dep_info["processor_name"],
                    "processor_type": dep_info["processor_type"],
                    "dependent_count": dep_info["dependent_count"],
                    "impact_level": (
                        "high" if dep_info["dependent_count"] > 5 else "medium"
                    ),
                }
            )

    # Identify isolated processors (no dependencies)
    for proc_id, dep_info in comprehensive_deps.items():
        if dep_info["total_relationships"] == 0:
            impact_analysis["isolated_processors"].append(
                {
                    "processor_id": proc_id,
                    "processor_name": dep_info["processor_name"],
                    "processor_type": dep_info["processor_type"],
                }
            )

    # Find dependency chains
    impact_analysis["dependency_chains"] = find_dependency_chains(comprehensive_deps)

    # Detect circular dependencies
    impact_analysis["circular_dependencies"] = detect_circular_dependencies(
        comprehensive_deps
    )

    return impact_analysis


def find_dependency_chains(comprehensive_deps: Dict[str, Dict]) -> List[Dict]:
    """Find longest dependency chains in the workflow."""
    chains = []
    visited = set()

    def dfs_chain(proc_id, current_chain):
        if proc_id in visited or proc_id in current_chain:
            return [current_chain]

        current_chain.append(proc_id)
        dep_info = comprehensive_deps.get(proc_id, {})

        longest_chains = []
        has_dependencies = False

        for dep in dep_info.get("depends_on", []):
            dep_proc_id = dep["dependency_processor_id"]
            has_dependencies = True
            sub_chains = dfs_chain(dep_proc_id, current_chain.copy())
            longest_chains.extend(sub_chains)

        if not has_dependencies:
            longest_chains = [current_chain]

        return longest_chains

    # Find chains starting from processors with no incoming dependencies
    for proc_id, dep_info in comprehensive_deps.items():
        if len(dep_info.get("depends_on", [])) == 0 and proc_id not in visited:
            proc_chains = dfs_chain(proc_id, [])
            for chain in proc_chains:
                if len(chain) > 2:  # Only include meaningful chains
                    chain_info = {"chain_length": len(chain), "processors": []}
                    for p_id in chain:
                        p_info = comprehensive_deps.get(p_id, {})
                        chain_info["processors"].append(
                            {
                                "processor_id": p_id,
                                "processor_name": p_info.get(
                                    "processor_name", "unknown"
                                ),
                                "processor_type": p_info.get(
                                    "processor_type", "unknown"
                                ),
                            }
                        )
                    chains.append(chain_info)
                visited.update(chain)

    return sorted(chains, key=lambda x: x["chain_length"], reverse=True)


def detect_circular_dependencies(comprehensive_deps: Dict[str, Dict]) -> List[Dict]:
    """Detect circular dependency patterns."""
    circular_deps = []
    visited = set()
    recursion_stack = set()

    def dfs_circular(proc_id, path):
        if proc_id in recursion_stack:
            # Found circular dependency
            cycle_start = path.index(proc_id)
            cycle = path[cycle_start:] + [proc_id]
            return [cycle]

        if proc_id in visited:
            return []

        visited.add(proc_id)
        recursion_stack.add(proc_id)
        path.append(proc_id)

        cycles = []
        dep_info = comprehensive_deps.get(proc_id, {})

        for dep in dep_info.get("depends_on", []):
            dep_proc_id = dep["dependency_processor_id"]
            cycles.extend(dfs_circular(dep_proc_id, path.copy()))

        recursion_stack.remove(proc_id)
        return cycles

    all_cycles = []
    for proc_id in comprehensive_deps.keys():
        if proc_id not in visited:
            cycles = dfs_circular(proc_id, [])
            all_cycles.extend(cycles)

    # Convert cycles to readable format
    for cycle in all_cycles:
        cycle_info = {
            "cycle_length": len(cycle) - 1,  # -1 because last element repeats first
            "processors": [],
        }
        for p_id in cycle[:-1]:  # Exclude repeated last element
            p_info = comprehensive_deps.get(p_id, {})
            cycle_info["processors"].append(
                {
                    "processor_id": p_id,
                    "processor_name": p_info.get("processor_name", "unknown"),
                    "processor_type": p_info.get("processor_type", "unknown"),
                }
            )
        circular_deps.append(cycle_info)

    return circular_deps


def calculate_dependency_statistics(
    comprehensive_deps: Dict[str, Dict],
) -> Dict[str, Any]:
    """Calculate statistics about dependencies in the workflow."""
    total_processors = len(comprehensive_deps)
    total_dependencies = sum(
        dep["dependency_count"] for dep in comprehensive_deps.values()
    )
    total_dependents = sum(
        dep["dependent_count"] for dep in comprehensive_deps.values()
    )

    dependency_counts = [dep["dependency_count"] for dep in comprehensive_deps.values()]
    dependent_counts = [dep["dependent_count"] for dep in comprehensive_deps.values()]

    return {
        "total_processors": total_processors,
        "total_dependencies": total_dependencies,
        "total_dependents": total_dependents,
        "average_dependencies_per_processor": (
            total_dependencies / total_processors if total_processors > 0 else 0
        ),
        "average_dependents_per_processor": (
            total_dependents / total_processors if total_processors > 0 else 0
        ),
        "max_dependencies": max(dependency_counts) if dependency_counts else 0,
        "max_dependents": max(dependent_counts) if dependent_counts else 0,
        "processors_with_no_dependencies": sum(
            1 for count in dependency_counts if count == 0
        ),
        "processors_with_no_dependents": sum(
            1 for count in dependent_counts if count == 0
        ),
        "highly_connected_processors": sum(
            1 for dep in comprehensive_deps.values() if dep["total_relationships"] > 5
        ),
    }


def generate_dependency_report(dependencies: Dict[str, Any]) -> str:
    """
    Generate comprehensive dependency analysis report.

    Args:
        dependencies: Complete dependency analysis results

    Returns:
        Markdown formatted report
    """
    report_lines = []

    # Header
    report_lines.extend(
        [
            "# Comprehensive Processor Dependency Analysis",
            "",
            f"**Total Processors Analyzed:** {dependencies['total_processors']}",
            "",
        ]
    )

    # Statistics
    stats = dependencies["dependency_statistics"]
    report_lines.extend(
        [
            "## Dependency Statistics",
            "",
            f"- **Total Dependencies:** {stats['total_dependencies']}",
            f"- **Total Dependents:** {stats['total_dependents']}",
            f"- **Average Dependencies per Processor:** {stats['average_dependencies_per_processor']:.1f}",
            f"- **Average Dependents per Processor:** {stats['average_dependents_per_processor']:.1f}",
            f"- **Max Dependencies on Single Processor:** {stats['max_dependencies']}",
            f"- **Max Dependents of Single Processor:** {stats['max_dependents']}",
            f"- **Processors with No Dependencies:** {stats['processors_with_no_dependencies']}",
            f"- **Processors with No Dependents:** {stats['processors_with_no_dependents']}",
            f"- **Highly Connected Processors (>5 relationships):** {stats['highly_connected_processors']}",
            "",
        ]
    )

    # Impact Analysis
    impact = dependencies["impact_analysis"]

    if impact["high_impact_processors"]:
        report_lines.extend(
            [
                "## High Impact Processors",
                "",
                "*These processors have many dependents. Changes to them will affect multiple other processors.*",
                "",
            ]
        )

        for proc in impact["high_impact_processors"]:
            report_lines.append(
                f"- **{proc['processor_name']}** ({proc['processor_type']}) - "
                f"{proc['dependent_count']} dependents - *{proc['impact_level']} impact*"
            )
        report_lines.append("")

    if impact["isolated_processors"]:
        report_lines.extend(
            [
                "## Isolated Processors",
                "",
                "*These processors have no dependencies and no dependents.*",
                "",
            ]
        )

        for proc in impact["isolated_processors"]:
            report_lines.append(
                f"- **{proc['processor_name']}** ({proc['processor_type']})"
            )
        report_lines.append("")

    if impact["dependency_chains"]:
        report_lines.extend(
            [
                "## Dependency Chains",
                "",
                "*Longest sequential dependency chains in the workflow.*",
                "",
            ]
        )

        for i, chain in enumerate(impact["dependency_chains"][:5]):  # Top 5 chains
            report_lines.append(f"### Chain {i+1} (Length: {chain['chain_length']})")
            report_lines.append("")

            chain_names = []
            for proc in chain["processors"]:
                chain_names.append(
                    f"**{proc['processor_name']}** ({proc['processor_type']})"
                )

            report_lines.append(" → ".join(chain_names))
            report_lines.append("")

    if impact["circular_dependencies"]:
        report_lines.extend(
            [
                "## ⚠️ Circular Dependencies",
                "",
                "*These processors have circular dependency relationships that may cause issues.*",
                "",
            ]
        )

        for i, cycle in enumerate(impact["circular_dependencies"]):
            report_lines.append(
                f"### Circular Dependency {i+1} (Length: {cycle['cycle_length']})"
            )
            report_lines.append("")

            cycle_names = []
            for proc in cycle["processors"]:
                cycle_names.append(f"**{proc['processor_name']}**")

            report_lines.append(
                " → ".join(cycle_names)
                + f" → **{cycle['processors'][0]['processor_name']}**"
            )
            report_lines.append("")

    # Variable Dependencies Summary
    var_deps = dependencies["dependencies"]["variable_dependencies"]
    if var_deps:
        report_lines.extend(
            [
                "## Variable Dependencies Summary",
                "",
                f"**Total Variables Tracked:** {len(var_deps)}",
                "",
            ]
        )

        defined_vars = [v for v in var_deps.values() if v["is_defined"]]
        undefined_vars = [v for v in var_deps.values() if not v["is_defined"]]

        report_lines.extend(
            [
                f"- **Defined Variables:** {len(defined_vars)}",
                f"- **External/Undefined Variables:** {len(undefined_vars)}",
                "",
            ]
        )

        if undefined_vars:
            report_lines.extend(
                [
                    "### ⚠️ External Variable Dependencies",
                    "",
                    "*These variables are used but not defined within the workflow:*",
                    "",
                ]
            )

            for var in undefined_vars[:10]:  # Top 10
                usage_count = len(var["usages"])
                report_lines.append(
                    f"- **${{{var['variable_name']}}}** - Used by {usage_count} processor(s)"
                )

    return "\n".join(report_lines)

# tools/pruning_tools.py
# Processor pruning and data flow chain detection tools that leverage existing functionality

import json
from typing import Any, Dict, List

from langchain_core.tools import tool

from .migration_tools import build_migration_plan
from .xml_tools import parse_nifi_template


@tool
def prune_infrastructure_processors(classification_results_json: str) -> str:
    """
    Remove infrastructure-only processors using existing sophisticated classifications.

    This tool uses the processor classifications from analyze_processors_batch or
    analyze_nifi_workflow_detailed to filter out infrastructure processors, keeping
    only processors that handle actual data transformation or data movement.

    Args:
        classification_results_json: JSON results from processor classification tools

    Returns:
        JSON with pruned processors and statistics:
        {
            "pruned_processors": [...],  # Essential processors only
            "removed_processors": [...], # Infrastructure processors removed
            "summary": {
                "original_count": N,
                "kept_count": M,
                "pruned_count": P,
                "reduction_percentage": X%
            },
            "kept_by_classification": {
                "data_transformation": N,
                "data_movement": M
            }
        }
    """
    try:
        # Parse classification results
        if isinstance(classification_results_json, str):
            # Handle both direct classification results and wrapped analysis results
            classification_data = json.loads(classification_results_json)

            # Extract classification_results if wrapped in analysis results
            if "classification_results" in classification_data:
                classifications = classification_data["classification_results"]
            elif "classification_breakdown" in classification_data:
                # Handle analyze_nifi_workflow_detailed format
                classifications = classification_data.get(
                    "classification_breakdown", []
                )
            else:
                # Direct classification results
                classifications = (
                    classification_data if isinstance(classification_data, list) else []
                )
        else:
            classifications = classification_results_json

        if not classifications:
            return json.dumps(
                {
                    "error": "No processor classifications found in input",
                    "input_sample": str(classification_results_json)[:200],
                }
            )

        # Separate processors by classification
        essential_processors = []
        removed_processors = []

        kept_counts = {
            "data_transformation": 0,
            "data_movement": 0,
            "external_processing": 0,  # Also keep external processing as it affects data
        }

        for processor in classifications:
            classification = processor.get("data_manipulation_type", "unknown")

            # Keep processors that actually handle data content
            if classification in [
                "data_transformation",
                "data_movement",
                "external_processing",
            ]:
                essential_processors.append(processor)
                if classification in kept_counts:
                    kept_counts[classification] += 1
            else:
                # Remove infrastructure-only processors
                removed_processors.append(
                    {
                        **processor,
                        "removal_reason": f"Classified as '{classification}' - infrastructure only",
                    }
                )

        # Calculate statistics
        original_count = len(classifications)
        kept_count = len(essential_processors)
        pruned_count = len(removed_processors)
        reduction_percentage = (
            round((pruned_count / original_count * 100), 1) if original_count > 0 else 0
        )

        return json.dumps(
            {
                "pruned_processors": essential_processors,
                "removed_processors": removed_processors,
                "summary": {
                    "original_count": original_count,
                    "kept_count": kept_count,
                    "pruned_count": pruned_count,
                    "reduction_percentage": reduction_percentage,
                    "migration_efficiency": f"Reduced from {original_count} to {kept_count} processors ({reduction_percentage}% reduction)",
                },
                "kept_by_classification": kept_counts,
                "removed_classifications": {
                    proc.get("data_manipulation_type", "unknown"): len(
                        [
                            p
                            for p in removed_processors
                            if p.get("data_manipulation_type")
                            == proc.get("data_manipulation_type")
                        ]
                    )
                    for proc in removed_processors
                },
            }
        )

    except Exception as e:
        return json.dumps(
            {
                "error": f"Failed to prune processors: {str(e)}",
                "input_type": type(classification_results_json).__name__,
            }
        )


@tool
def detect_data_flow_chains(xml_content: str, pruned_processors_json: str) -> str:
    """
    Detect source → transformation → sink chains using existing topological sorting
    and connection parsing functionality.

    Leverages existing tools:
    - parse_nifi_template: for connection parsing
    - build_migration_plan: for topological DAG construction

    Args:
        xml_content: NiFi XML template content or file path
        pruned_processors_json: Output from prune_infrastructure_processors

    Returns:
        JSON with detected data flow chains:
        {
            "data_flow_chains": [
                {
                    "chain_id": "flow_1",
                    "source": {"name": "...", "type": "...", "classification": "..."},
                    "processors": [...],  # All processors in this chain
                    "sink": {"name": "...", "type": "...", "classification": "..."},
                    "length": N,
                    "business_pattern": "file_ingestion|stream_processing|data_export|..."
                }
            ],
            "chain_summary": {
                "total_chains": N,
                "avg_chain_length": X.X,
                "processors_in_chains": M,
                "isolated_processors": P  # Processors not part of any chain
            }
        }
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

        # Use existing tools for parsing and DAG construction
        template_data = json.loads(parse_nifi_template.func(xml_content))
        connections = template_data.get("connections", [])

        # Build DAG using existing topological sorting
        dag_result = json.loads(build_migration_plan.func(xml_content))
        all_tasks = dag_result.get("tasks", [])
        all_edges = dag_result.get("edges", [])

        # Create lookup for essential processors
        essential_ids = {p.get("id", ""): p for p in essential_processors}
        essential_id_set = set(essential_ids.keys())

        # Filter DAG to essential processors only
        filtered_tasks = [t for t in all_tasks if t.get("id") in essential_id_set]
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
                    "reduction_achieved": f"Simplified {len(template_data.get('processors', []))} processors into {total_chains} logical data flows",
                },
                "migration_approach": (
                    "semantic_data_flows"
                    if total_chains <= 5
                    else "complex_workflow_chunking"
                ),
            }
        )

    except Exception as e:
        return json.dumps(
            {
                "error": f"Failed to detect data flow chains: {str(e)}",
                "data_flow_chains": [],
                "chain_summary": {
                    "total_chains": 0,
                    "avg_chain_length": 0,
                    "processors_in_chains": 0,
                    "isolated_processors": 0,
                },
            }
        )


@tool
def create_semantic_data_flows(chains_json: str) -> str:
    """
    Convert processor chains into semantic business data flows for migration planning.

    Args:
        chains_json: Output from detect_data_flow_chains

    Returns:
        JSON with semantic data flow descriptions:
        {
            "semantic_data_flows": [
                {
                    "flow_name": "customer_data_pipeline",
                    "business_purpose": "Process customer data from files to analytics store",
                    "source_description": "CSV files from customer system",
                    "transformation_steps": ["Parse customer records", "Validate data", "Enrich with metadata"],
                    "sink_description": "Delta Lake customer analytics table",
                    "databricks_approach": "Jobs with Auto Loader",
                    "complexity": "simple|moderate|complex",
                    "processors_involved": N
                }
            ],
            "migration_blueprint": {
                "recommended_architecture": "databricks_jobs|dlt_pipeline|hybrid",
                "implementation_approach": "semantic_flow_based|processor_based_fallback",
                "complexity_assessment": "simple|moderate|complex"
            }
        }
    """
    try:
        chains_data = json.loads(chains_json)
        data_flow_chains = chains_data.get("data_flow_chains", [])

        if not data_flow_chains:
            return json.dumps(
                {
                    "error": "No data flow chains found",
                    "semantic_data_flows": [],
                    "migration_blueprint": {
                        "recommended_architecture": "unknown",
                        "implementation_approach": "processor_based_fallback",
                        "complexity_assessment": "unknown",
                    },
                }
            )

        semantic_flows = []
        total_processors = 0

        for i, chain in enumerate(data_flow_chains):
            processors = chain.get("processors", [])
            source_proc = processors[0] if processors else None
            sink_proc = processors[-1] if processors else None

            # Analyze chain semantics
            flow_analysis = _analyze_chain_semantics(
                chain, processors, source_proc, sink_proc
            )

            semantic_flow = {
                "flow_name": flow_analysis.get("flow_name", f"data_pipeline_{i+1}"),
                "business_purpose": flow_analysis.get(
                    "business_purpose", "Data processing pipeline"
                ),
                "source_description": flow_analysis.get(
                    "source_description", "Data input"
                ),
                "transformation_steps": flow_analysis.get("transformation_steps", []),
                "sink_description": flow_analysis.get(
                    "sink_description", "Data output"
                ),
                "databricks_approach": flow_analysis.get(
                    "databricks_approach", "Jobs with Delta Lake"
                ),
                "complexity": flow_analysis.get("complexity", "moderate"),
                "processors_involved": len(processors),
                "original_chain_id": chain.get("chain_id", f"chain_{i}"),
            }

            semantic_flows.append(semantic_flow)
            total_processors += len(processors)

        # Determine overall migration approach
        num_flows = len(semantic_flows)
        avg_complexity = _calculate_average_complexity(semantic_flows)

        if num_flows <= 3 and avg_complexity in ["simple", "moderate"]:
            recommended_arch = "databricks_jobs"
            implementation = "semantic_flow_based"
            complexity = "simple"
        elif num_flows <= 5:
            recommended_arch = "dlt_pipeline"
            implementation = "semantic_flow_based"
            complexity = "moderate"
        else:
            recommended_arch = "hybrid"
            implementation = "processor_based_fallback"
            complexity = "complex"

        return json.dumps(
            {
                "semantic_data_flows": semantic_flows,
                "migration_blueprint": {
                    "recommended_architecture": recommended_arch,
                    "implementation_approach": implementation,
                    "complexity_assessment": complexity,
                    "flow_count": num_flows,
                    "total_processors_migrated": total_processors,
                    "migration_strategy": f"Convert {num_flows} data flows using {recommended_arch}",
                },
            }
        )

    except Exception as e:
        return json.dumps(
            {
                "error": f"Failed to create semantic data flows: {str(e)}",
                "semantic_data_flows": [],
                "migration_blueprint": {
                    "recommended_architecture": "unknown",
                    "implementation_approach": "processor_based_fallback",
                    "complexity_assessment": "unknown",
                },
            }
        )


def _detect_chains_from_filtered_dag(
    tasks: List[Dict[str, Any]],
    edges: List[List[str]],
    processor_lookup: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Detect linear chains from filtered DAG structure."""

    if not tasks or not edges:
        return []

    # Build adjacency lists
    outgoing = {}
    incoming = {}

    for task in tasks:
        task_id = task.get("id", "")
        outgoing[task_id] = []
        incoming[task_id] = []

    for edge in edges:
        if len(edge) >= 2:
            src, dst = edge[0], edge[1]
            if src in outgoing and dst in incoming:
                outgoing[src].append(dst)
                incoming[dst].append(src)

    # Find sources (no incoming edges)
    sources = [task_id for task_id in incoming if len(incoming[task_id]) == 0]

    chains = []
    visited = set()

    for source_id in sources:
        if source_id in visited:
            continue

        # Trace chain from this source
        chain_processors = []
        current = source_id

        while current and current not in visited:
            visited.add(current)

            # Add processor to chain
            if current in processor_lookup:
                chain_processors.append(processor_lookup[current])

            # Find next processor (prefer single path)
            next_procs = outgoing.get(current, [])
            if len(next_procs) == 1:
                current = next_procs[0]
            else:
                # Multiple outputs or no outputs - end chain
                break

        if len(chain_processors) >= 1:
            source_proc = chain_processors[0] if chain_processors else None
            sink_proc = chain_processors[-1] if chain_processors else None

            chain = {
                "chain_id": f"flow_{source_id}",
                "source": {
                    "name": (
                        source_proc.get("name", "Unknown") if source_proc else "Unknown"
                    ),
                    "type": (
                        source_proc.get("type", "Unknown") if source_proc else "Unknown"
                    ),
                    "classification": (
                        source_proc.get("data_manipulation_type", "unknown")
                        if source_proc
                        else "unknown"
                    ),
                },
                "processors": chain_processors,
                "sink": {
                    "name": (
                        sink_proc.get("name", "Unknown") if sink_proc else "Unknown"
                    ),
                    "type": (
                        sink_proc.get("type", "Unknown") if sink_proc else "Unknown"
                    ),
                    "classification": (
                        sink_proc.get("data_manipulation_type", "unknown")
                        if sink_proc
                        else "unknown"
                    ),
                },
                "length": len(chain_processors),
                "business_pattern": _identify_business_pattern(chain_processors),
            }
            chains.append(chain)

    return chains


def _identify_business_pattern(processors: List[Dict[str, Any]]) -> str:
    """Identify the business pattern of a processor chain."""

    if not processors:
        return "unknown"

    source_type = processors[0].get("type", "").lower()
    sink_type = processors[-1].get("type", "").lower() if len(processors) > 1 else ""

    # File ingestion patterns
    if "getfile" in source_type or "listfile" in source_type:
        if "puthdfs" in sink_type or "delta" in sink_type:
            return "file_to_lake_ingestion"
        elif "publishkafka" in sink_type:
            return "file_to_stream_publishing"
        else:
            return "file_processing"

    # Stream processing patterns
    elif "consumekafka" in source_type or "listenhttp" in source_type:
        if "puthdfs" in sink_type or "delta" in sink_type:
            return "stream_to_lake_processing"
        elif "publishkafka" in sink_type:
            return "stream_transformation"
        else:
            return "stream_processing"

    # Data export patterns
    elif "delta" in source_type or "gethdfs" in source_type:
        return "data_export"

    else:
        return "data_transformation"


def _analyze_chain_semantics(
    chain: Dict[str, Any],
    processors: List[Dict[str, Any]],
    source_proc: Dict[str, Any],
    sink_proc: Dict[str, Any],
) -> Dict[str, Any]:
    """Analyze chain to extract semantic meaning."""

    business_pattern = chain.get("business_pattern", "data_transformation")

    # Generate flow name based on processors
    flow_name = _generate_flow_name(source_proc, sink_proc, business_pattern)

    # Extract transformation steps
    transformation_steps = []
    for proc in processors[1:-1]:  # Skip source and sink
        if proc.get("data_manipulation_type") == "data_transformation":
            step = _describe_transformation_step(proc)
            if step:
                transformation_steps.append(step)

    # Describe source and sink
    source_desc = _describe_data_source(source_proc) if source_proc else "Data input"
    sink_desc = _describe_data_sink(sink_proc) if sink_proc else "Data output"

    # Determine Databricks approach
    databricks_approach = _recommend_databricks_approach(
        business_pattern, len(processors)
    )

    # Assess complexity
    complexity = _assess_chain_complexity(processors, transformation_steps)

    return {
        "flow_name": flow_name,
        "business_purpose": _generate_business_purpose(
            business_pattern, source_desc, sink_desc
        ),
        "source_description": source_desc,
        "transformation_steps": transformation_steps,
        "sink_description": sink_desc,
        "databricks_approach": databricks_approach,
        "complexity": complexity,
    }


def _generate_flow_name(
    source_proc: Dict[str, Any], sink_proc: Dict[str, Any], pattern: str
) -> str:
    """Generate a meaningful flow name."""

    if not source_proc:
        return "unknown_pipeline"

    source_name = source_proc.get("name", "").lower().replace(" ", "_")
    pattern_name = pattern.replace("_", "_")

    if source_name:
        return f"{source_name}_{pattern_name}"
    else:
        return f"{pattern_name}_pipeline"


def _describe_transformation_step(processor: Dict[str, Any]) -> str:
    """Describe what a transformation processor does."""

    proc_type = processor.get("type", "")
    proc_name = processor.get("name", "")

    type_descriptions = {
        "EvaluateJsonPath": "Extract fields from JSON data",
        "ConvertRecord": "Convert data format",
        "SplitJson": "Split JSON arrays into records",
        "ReplaceText": "Transform text content",
        "ExecuteSQL": "Execute SQL transformation",
        "RouteOnAttribute": "Route data based on conditions",
    }

    return type_descriptions.get(proc_type, f"Process data using {proc_name}")


def _describe_data_source(source_proc: Dict[str, Any]) -> str:
    """Describe the data source."""

    if not source_proc:
        return "Data input"

    proc_type = source_proc.get("type", "")

    source_descriptions = {
        "GetFile": "Files from filesystem",
        "ListFile": "File listings",
        "ConsumeKafka": "Kafka message stream",
        "ListenHTTP": "HTTP endpoint data",
        "GenerateFlowFile": "Generated test data",
    }

    return source_descriptions.get(proc_type, f"Data from {proc_type}")


def _describe_data_sink(sink_proc: Dict[str, Any]) -> str:
    """Describe the data sink."""

    if not sink_proc:
        return "Data output"

    proc_type = sink_proc.get("type", "")

    sink_descriptions = {
        "PutFile": "Files to filesystem",
        "PutHDFS": "HDFS/Delta Lake storage",
        "PublishKafka": "Kafka message stream",
        "PutSQL": "SQL database table",
    }

    return sink_descriptions.get(proc_type, f"Data to {proc_type}")


def _recommend_databricks_approach(pattern: str, processor_count: int) -> str:
    """Recommend Databricks implementation approach."""

    if "stream" in pattern:
        if processor_count <= 3:
            return "Structured Streaming with Delta Lake"
        else:
            return "Delta Live Tables pipeline"
    elif "file" in pattern:
        return "Jobs with Auto Loader and Delta Lake"
    else:
        return "Jobs with Delta Lake"


def _assess_chain_complexity(
    processors: List[Dict[str, Any]], transformations: List[str]
) -> str:
    """Assess the complexity of a processor chain."""

    if len(processors) <= 2 and len(transformations) == 0:
        return "simple"
    elif len(processors) <= 4 and len(transformations) <= 2:
        return "moderate"
    else:
        return "complex"


def _calculate_average_complexity(flows: List[Dict[str, Any]]) -> str:
    """Calculate average complexity across flows."""

    if not flows:
        return "unknown"

    complexity_scores = {"simple": 1, "moderate": 2, "complex": 3}
    total_score = sum(
        complexity_scores.get(flow.get("complexity", "moderate"), 2) for flow in flows
    )
    avg_score = total_score / len(flows)

    if avg_score <= 1.5:
        return "simple"
    elif avg_score <= 2.5:
        return "moderate"
    else:
        return "complex"


def _generate_business_purpose(pattern: str, source_desc: str, sink_desc: str) -> str:
    """Generate a business purpose description."""

    purpose_templates = {
        "file_to_lake_ingestion": f"Ingest and process {source_desc} into {sink_desc}",
        "stream_to_lake_processing": f"Process streaming {source_desc} and store in {sink_desc}",
        "stream_transformation": f"Transform {source_desc} and publish to {sink_desc}",
        "file_processing": f"Process {source_desc} and output to {sink_desc}",
        "data_export": f"Export data from {source_desc} to {sink_desc}",
        "data_transformation": f"Transform data from {source_desc} to {sink_desc}",
    }

    return purpose_templates.get(
        pattern, f"Process data from {source_desc} to {sink_desc}"
    )

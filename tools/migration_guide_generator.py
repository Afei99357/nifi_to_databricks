# tools/migration_guide_generator.py
# Generate LLM-powered migration guides based on actual NiFi workflow analysis

import os
from datetime import datetime
from typing import Any, Dict, List

from databricks_langchain import ChatDatabricks


def analyze_processor_relationships(
    processors: List[Dict[str, Any]], semantic_flows: Dict[str, Any]
) -> Dict[str, Any]:
    """Analyze relationships and dependencies between processors."""

    # Build processor ID to name mapping
    proc_map = {}
    for proc in processors:
        proc_id = proc.get("id")
        if proc_id:
            proc_map[proc_id] = {
                "name": proc.get("name", "Unknown"),
                "type": proc.get("type", "Unknown").split(".")[-1],
                "classification": proc.get("classification", "unknown"),
            }

    # Extract connections from semantic flows
    connections = []
    dependencies = {}
    data_flow_chains = []

    # Get semantic flows data
    semantic_flows_data = semantic_flows.get("semantic_flows", [])

    for flow in semantic_flows_data:
        if isinstance(flow, dict):
            # Extract processor chain from flow
            processors_in_flow = flow.get("processors", [])
            flow_name = flow.get("name", "Unknown Flow")

            if len(processors_in_flow) > 1:
                chain = []
                for proc_info in processors_in_flow:
                    if isinstance(proc_info, dict):
                        proc_id = proc_info.get("id")
                        proc_name = proc_info.get("name", "Unknown")
                        proc_type = proc_info.get("type", "Unknown").split(".")[-1]
                        chain.append(f"{proc_name} ({proc_type})")

                if chain:
                    data_flow_chains.append(
                        {
                            "flow_name": flow_name,
                            "chain": " → ".join(chain),
                            "length": len(chain),
                        }
                    )

                # Build dependencies
                for i in range(len(processors_in_flow) - 1):
                    current = processors_in_flow[i]
                    next_proc = processors_in_flow[i + 1]

                    if isinstance(current, dict) and isinstance(next_proc, dict):
                        current_name = current.get("name", "Unknown")
                        next_name = next_proc.get("name", "Unknown")

                        if next_name not in dependencies:
                            dependencies[next_name] = []
                        dependencies[next_name].append(current_name)

                        connections.append(
                            {
                                "source": current_name,
                                "target": next_name,
                                "source_type": current.get("type", "Unknown").split(
                                    "."
                                )[-1],
                                "target_type": next_proc.get("type", "Unknown").split(
                                    "."
                                )[-1],
                            }
                        )

    # Find entry points (no dependencies) and exit points (no dependents)
    all_processors = set(proc_map.values() for proc_map in proc_map.values())
    processor_names = [proc["name"] for proc in processors]

    entry_points = []
    exit_points = []

    for proc_name in processor_names:
        if proc_name not in dependencies:
            entry_points.append(proc_name)

        # Check if processor is an exit point (no one depends on it)
        is_exit = True
        for conn in connections:
            if conn["source"] == proc_name:
                is_exit = False
                break
        if is_exit and proc_name not in entry_points:
            exit_points.append(proc_name)

    return {
        "total_connections": len(connections),
        "dependencies": dependencies,
        "data_flow_chains": data_flow_chains,
        "entry_points": entry_points,
        "exit_points": exit_points,
        "connection_details": connections[:10],  # Limit for LLM context
    }


def generate_migration_guide(
    processors: List[Dict[str, Any]],
    semantic_flows: Dict[str, Any],
    project_name: str,
    analysis: Dict[str, Any],
) -> str:
    """
    Generate a comprehensive migration guide using LLM analysis of the actual NiFi workflow.

    Returns:
        Migration guide content as markdown string
    """
    print(f"🧠 [GUIDE GENERATION] Using LLM to analyze {len(processors)} processors...")

    # Prepare processor summary for LLM
    processor_summary = []
    for proc in processors:
        processor_info = {
            "name": proc.get("name", "Unknown"),
            "type": proc.get("type", "Unknown").split(".")[-1],
            "classification": proc.get("classification", "unknown"),
            "properties": proc.get("properties", {}),
            "parent_group": proc.get("parentGroupName", "Root"),
        }
        processor_summary.append(processor_info)

    # Analyze processor relationships and dependencies
    processor_relationships = analyze_processor_relationships(
        processors, semantic_flows
    )

    # Create semantic flows summary with relationships
    flows_summary = {
        "total_flows": semantic_flows.get("total_flows", 0),
        "flow_patterns": semantic_flows.get("flow_summaries", [])[:3],  # Top 3 flows
        "complexity": semantic_flows.get("complexity_analysis", {}),
        "processor_relationships": processor_relationships,
    }

    # Create analysis summary
    analysis_summary = {
        "processor_breakdown": analysis.get("breakdown", {}),
        "migration_approach": analysis.get(
            "migration_approach", "focused_essential_only"
        ),
        "total_processors": len(processors),
    }

    # Generate migration guide using LLM
    guide_prompt = f"""
You are a NiFi to Databricks migration expert. Analyze this NiFi workflow and create a comprehensive migration guide.

PROJECT: {project_name}

PROCESSORS ANALYSIS:
{processor_summary}

SEMANTIC FLOWS:
{flows_summary}

MIGRATION CONTEXT:
{analysis_summary}

Please generate a comprehensive migration guide with these sections:

1. **Executive Summary** - High-level migration overview and recommendations
2. **Workflow Analysis** - Understanding the current NiFi workflow structure and data flows
3. **Architecture Recommendations** - Recommended Databricks architecture (Jobs, DLT, Structured Streaming)
4. **Migration Strategy** - Step-by-step approach to migrate each processor type
5. **Code Templates** - Specific PySpark/SQL code patterns for key processors
6. **Data Flow Mapping** - How NiFi connections translate to Databricks data flows
7. **Processor Dependencies** - Analysis of data flow chains and execution order
8. **Testing Strategy** - How to validate the migration
9. **Deployment Checklist** - Steps to deploy and operationalize

Focus on:
- Practical, actionable recommendations
- Real Databricks best practices (Unity Catalog, Delta Lake, Auto Loader)
- Specific code examples for the actual processor types found
- **Processor relationship analysis**: How data flows between processors
- **Execution dependencies**: Which processors must run before others
- **Data flow chains**: Complete end-to-end data processing paths
- **Entry/exit points**: Starting and ending processors in workflows
- Migration complexity assessment
- Performance optimization suggestions
- Cost optimization strategies

Format as detailed markdown with code blocks and checklists.
"""

    try:
        # Generate migration guide using LLM directly
        model_endpoint = os.getenv(
            "MODEL_ENDPOINT", "databricks-meta-llama-3-3-70b-instruct"
        )
        llm = ChatDatabricks(
            endpoint=model_endpoint,
            temperature=0.1,
            max_tokens=4000,
        )

        migration_guide = llm.invoke(guide_prompt).content

        # Add generation metadata
        header = f"""# {project_name} - NiFi to Databricks Migration Guide

*Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*
*Analysis: {len(processors)} essential processors from NiFi workflow*
*Semantic Flows: {flows_summary['total_flows']} data flows identified*

---

"""

        return header + migration_guide

    except Exception as e:
        print(f"❌ [GUIDE GENERATION] LLM generation failed: {e}")
        # Fallback to basic structured guide
        return generate_basic_migration_guide(
            processors, semantic_flows, project_name, analysis
        )


def generate_basic_migration_guide(
    processors: List[Dict[str, Any]],
    semantic_flows: Dict[str, Any],
    project_name: str,
    analysis: Dict[str, Any],
) -> str:
    """Fallback basic migration guide when LLM fails."""

    processor_types = {}
    for proc in processors:
        proc_type = proc.get("type", "Unknown").split(".")[-1]
        if proc_type not in processor_types:
            processor_types[proc_type] = []
        processor_types[proc_type].append(proc.get("name", "Unknown"))

    guide = f"""# {project_name} - NiFi to Databricks Migration Guide

*Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*
*Analysis: {len(processors)} essential processors from NiFi workflow*

## Executive Summary

This migration guide provides recommendations for migrating your NiFi workflow to Databricks.

**Processors to Migrate:**
"""

    for proc_type, names in processor_types.items():
        guide += f"\n- **{proc_type}**: {len(names)} instances\n"
        for name in names[:3]:  # Show first 3
            guide += f"  - {name}\n"
        if len(names) > 3:
            guide += f"  - ... and {len(names) - 3} more\n"

    guide += f"""

## Recommended Architecture

Based on your workflow complexity, we recommend:

- **Primary Pattern**: Databricks Jobs for orchestration
- **Data Storage**: Delta Lake with Unity Catalog
- **File Processing**: Auto Loader for incremental processing
- **Streaming**: Structured Streaming for real-time data

## Next Steps

1. Review processor-specific migration patterns
2. Set up Unity Catalog and Delta Lake schemas
3. Implement Auto Loader for file-based sources
4. Create Databricks Jobs for orchestration
5. Test data flow end-to-end
6. Deploy with monitoring and alerting

## Migration Complexity: {analysis.get('migration_approach', 'Standard')}

Total processors analyzed: {len(processors)}
Semantic flows: {semantic_flows.get('total_flows', 0)}
"""

    return guide

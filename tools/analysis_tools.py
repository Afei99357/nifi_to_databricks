# tools/analysis_tools.py
# Agent tools for comprehensive NiFi workflow analysis and processor classification

import json
from pathlib import Path
from typing import Any, Dict

from utils.workflow_summary import print_workflow_summary_from_data

# Import the sophisticated analysis functions
from .nifi_processor_classifier_tool import analyze_workflow_patterns

# Removed langchain_core.tools import - no longer using # Removed @tool decorator - direct function call approach decorator


# Removed @tool decorator - direct function call approach
def analyze_nifi_workflow_detailed(
    xml_path: str,
    save_markdown: bool = True,
    output_dir: str = None,
    _reuse_analysis: str = None,
) -> str:
    """
    Perform comprehensive NiFi workflow analysis with detailed processor classification.

    This tool provides exactly what's needed for migration planning:
    - Total processor count and workflow complexity
    - Breakdown by DATA_TRANSFORMATION_PROCESSORS, DATA_MOVEMENT_PROCESSORS, INFRASTRUCTURE_PROCESSORS
    - Detailed classification with percentages
    - Migration complexity assessment

    Args:
        xml_path: Path to the NiFi XML template file
        save_markdown: Whether to save detailed markdown report (default: True)

    Returns:
        JSON string with comprehensive analysis including:
        - processor_counts: {"total": N, "data_transformation": X, "data_movement": Y, "infrastructure": Z}
        - complexity: "LOW"/"MEDIUM"/"HIGH"
        - classification_breakdown: detailed analysis results
        - migration_insights: strategic recommendations
    """
    try:
        # Use the sophisticated analysis system (reuse existing analysis if provided)
        if _reuse_analysis:
            analysis_result = (
                json.loads(_reuse_analysis)
                if isinstance(_reuse_analysis, str)
                else _reuse_analysis
            )
        else:
            analysis_result = analyze_workflow_patterns(
                xml_path=xml_path, save_markdown=save_markdown, output_dir=output_dir
            )
            analysis_result = (
                json.loads(analysis_result)
                if isinstance(analysis_result, str)
                else analysis_result
            )

        # Extract key metrics for agent decision making
        total_processors = analysis_result.get("total_processors", 0)

        # Get detailed classification breakdown
        classification_results = analysis_result.get("classification_results", [])

        # Count by classification type
        data_transformation_count = len(
            [
                p
                for p in classification_results
                if p.get("classification") == "data_transformation"
            ]
        )
        data_movement_count = len(
            [
                p
                for p in classification_results
                if p.get("classification") == "data_movement"
            ]
        )
        infrastructure_count = len(
            [
                p
                for p in classification_results
                if p.get("classification") == "infrastructure"
            ]
        )

        # Calculate percentages
        transformation_pct = (
            (data_transformation_count / total_processors * 100)
            if total_processors > 0
            else 0
        )
        movement_pct = (
            (data_movement_count / total_processors * 100)
            if total_processors > 0
            else 0
        )
        infrastructure_pct = (
            (infrastructure_count / total_processors * 100)
            if total_processors > 0
            else 0
        )

        # Determine workflow complexity based on processors and patterns
        complexity = "LOW"
        if total_processors > 50:
            complexity = "HIGH"
        elif total_processors > 20:
            complexity = "MEDIUM"

        # Create agent-friendly summary
        summary = {
            "analysis_type": "comprehensive_workflow_analysis",
            "processor_counts": {
                "total": total_processors,
                "data_transformation": data_transformation_count,
                "data_movement": data_movement_count,
                "infrastructure": infrastructure_count,
            },
            "percentages": {
                "data_transformation": round(transformation_pct, 1),
                "data_movement": round(movement_pct, 1),
                "infrastructure": round(infrastructure_pct, 1),
            },
            "complexity": complexity,
            "classification_breakdown": classification_results,
            "migration_insights": {
                "processors_to_migrate": data_transformation_count
                + data_movement_count,
                "processors_to_prune": infrastructure_count,
                "migration_complexity": complexity,
                "recommended_approach": (
                    "semantic_data_flow"
                    if transformation_pct > 30
                    else "simplified_migration"
                ),
            },
            "workflow_characteristics": analysis_result.get(
                "workflow_characteristics", {}
            ),
            "source_processors": [
                p
                for p in classification_results
                if p.get("type")
                in ["GetFile", "ListFile", "ConsumeKafka", "ListenHTTP"]
            ],
            "sink_processors": [
                p
                for p in classification_results
                if p.get("type") in ["PutFile", "PutHDFS", "PublishKafka", "PutSQL"]
            ],
        }

        return json.dumps(summary, indent=2)

    except Exception as e:
        return json.dumps(
            {"error": f"Failed to analyze workflow: {str(e)}", "xml_path": xml_path}
        )


# Removed @tool decorator - direct function call approach
def classify_processor_types(xml_path: str, _reuse_analysis: str = None) -> str:
    """
    Classify all processors in a NiFi workflow using hybrid rule-based + LLM analysis.

    Provides detailed processor-by-processor classification for migration planning.

    Args:
        xml_path: Path to the NiFi XML template file

    Returns:
        JSON string with processor classifications:
        - processor_name, type, classification (data_transformation/data_movement/infrastructure)
        - reasoning for each classification
        - migration recommendations per processor
    """
    try:
        # Reuse the comprehensive analysis but focus on processor details
        if _reuse_analysis:
            analysis_result = (
                json.loads(_reuse_analysis)
                if isinstance(_reuse_analysis, str)
                else _reuse_analysis
            )
        else:
            analysis_result = analyze_workflow_patterns(
                xml_path=xml_path, save_markdown=False
            )
            analysis_result = (
                json.loads(analysis_result)
                if isinstance(analysis_result, str)
                else analysis_result
            )

        classification_results = analysis_result.get("classification_results", [])

        # Format for agent consumption with migration guidance
        detailed_classifications = []
        for processor in classification_results:
            detailed_classifications.append(
                {
                    "name": processor.get("name", "Unknown"),
                    "type": processor.get("type", "Unknown"),
                    "classification": processor.get("classification", "unknown"),
                    "reasoning": processor.get("reasoning", "No reasoning provided"),
                    "migration_action": _get_migration_action(processor),
                    "priority": _get_migration_priority(processor),
                }
            )

        return json.dumps(
            {
                "processor_classifications": detailed_classifications,
                "summary": {
                    "total_processors": len(detailed_classifications),
                    "processors_to_migrate": len(
                        [
                            p
                            for p in detailed_classifications
                            if p["migration_action"] != "prune"
                        ]
                    ),
                    "processors_to_prune": len(
                        [
                            p
                            for p in detailed_classifications
                            if p["migration_action"] == "prune"
                        ]
                    ),
                },
            },
            indent=2,
        )

    except Exception as e:
        return json.dumps(
            {"error": f"Failed to classify processors: {str(e)}", "xml_path": xml_path}
        )


def _get_migration_action(processor: Dict[str, Any]) -> str:
    """Determine what migration action to take for a processor."""
    classification = processor.get("classification", "unknown")

    if classification == "infrastructure":
        return "prune"  # Remove from migration - handled by Databricks natively
    elif classification == "data_transformation":
        return "migrate_transform"  # Core business logic - must migrate
    elif classification == "data_movement":
        return "migrate_io"  # Data sources/sinks - migrate but may simplify
    else:
        return "analyze_further"  # Needs additional analysis


def _get_migration_priority(processor: Dict[str, Any]) -> str:
    """Assign migration priority to help with planning."""
    classification = processor.get("classification", "unknown")
    proc_type = processor.get("type", "")

    # High priority: Sources, sinks, and core transformations
    if proc_type in ["GetFile", "ConsumeKafka", "PutHDFS", "PutSQL", "ExecuteSQL"]:
        return "high"
    elif classification == "data_transformation":
        return "high"
    elif classification == "data_movement":
        return "medium"
    else:
        return "low"

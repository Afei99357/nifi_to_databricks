# utils/workflow_summary.py
# Utility to summarize NiFi workflow analysis results

import json
from collections import Counter
from typing import Any, Dict, List


def summarize_workflow_analysis_from_data(
    analysis_data: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Create a clear summary from the detailed workflow analysis data.

    Args:
        analysis_data: The workflow analysis dictionary

    Returns:
        Dictionary with clear summary statistics and insights
    """

    processors = analysis_data.get("processors_analysis", [])

    # Categorize processors
    data_transformers = []
    data_movers = []
    infrastructure = []
    external_processors = []
    unknown = []

    for proc in processors:
        manipulation_type = proc.get("data_manipulation_type", "unknown")
        transforms_content = proc.get("transforms_data_content", False)

        if manipulation_type == "data_transformation" or transforms_content:
            data_transformers.append(proc)
        elif manipulation_type == "data_movement":
            data_movers.append(proc)
        elif manipulation_type == "infrastructure_only":
            infrastructure.append(proc)
        elif manipulation_type == "external_processing":
            external_processors.append(proc)
        else:
            unknown.append(proc)

    # Count processor types
    processor_types = Counter(
        proc.get("processor_type", "Unknown").split(".")[-1] for proc in processors
    )

    # Identify key business operations
    key_operations = []
    for proc in data_transformers + external_processors:
        ops = proc.get("key_operations", [])
        key_operations.extend(ops)
    key_operations_count = Counter(key_operations)

    # Data impact analysis
    high_impact = [p for p in processors if p.get("data_impact_level") == "high"]
    medium_impact = [p for p in processors if p.get("data_impact_level") == "medium"]
    low_impact = [p for p in processors if p.get("data_impact_level") == "low"]
    no_impact = [p for p in processors if p.get("data_impact_level") == "none"]

    return {
        "workflow_overview": {
            "total_processors": len(processors),
            "actual_data_processors": len(data_transformers) + len(external_processors),
            "infrastructure_processors": len(infrastructure),
            "data_movement_processors": len(data_movers),
            "unknown_processors": len(unknown),
            "data_processing_ratio": (
                round(
                    (len(data_transformers) + len(external_processors))
                    / len(processors)
                    * 100,
                    1,
                )
                if processors
                else 0
            ),
        },
        "data_manipulation_breakdown": {
            "data_transformers": {
                "count": len(data_transformers),
                "processors": [
                    {
                        "name": p.get("name", ""),
                        "type": p.get("processor_type", "").split(".")[-1],
                        "business_purpose": p.get("business_purpose", ""),
                    }
                    for p in data_transformers
                ],
            },
            "external_processors": {
                "count": len(external_processors),
                "processors": [
                    {
                        "name": p.get("name", ""),
                        "type": p.get("processor_type", "").split(".")[-1],
                        "business_purpose": p.get("business_purpose", ""),
                    }
                    for p in external_processors
                ],
            },
            "data_movers": {
                "count": len(data_movers),
                "processors": [
                    {
                        "name": p.get("name", ""),
                        "type": p.get("processor_type", "").split(".")[-1],
                        "business_purpose": p.get("business_purpose", ""),
                    }
                    for p in data_movers
                ],
            },
        },
        "infrastructure_breakdown": {
            "count": len(infrastructure),
            "processors": [
                {
                    "name": p.get("name", ""),
                    "type": p.get("processor_type", "").split(".")[-1],
                    "business_purpose": p.get("business_purpose", ""),
                }
                for p in infrastructure
            ],
        },
        "processor_type_distribution": dict(processor_types.most_common(10)),
        "key_business_operations": dict(key_operations_count.most_common(10)),
        "data_impact_analysis": {
            "high_impact_processors": len(high_impact),
            "medium_impact_processors": len(medium_impact),
            "low_impact_processors": len(low_impact),
            "no_impact_processors": len(no_impact),
            "critical_processors": [
                {
                    "name": p.get("name", ""),
                    "type": p.get("processor_type", "").split(".")[-1],
                    "business_purpose": p.get("business_purpose", ""),
                }
                for p in high_impact
            ],
        },
        "business_insights": {
            "primary_data_operations": list(key_operations_count.most_common(3)),
            "workflow_complexity": (
                "high"
                if len(data_transformers) > 10
                else "medium" if len(data_transformers) > 5 else "low"
            ),
            "automation_potential": (
                "high"
                if len(infrastructure) / len(processors) > 0.4
                else "medium" if len(infrastructure) / len(processors) > 0.2 else "low"
            ),
        },
    }


def print_workflow_summary_from_data(analysis_data: Dict[str, Any]) -> None:
    """
    Print a human-readable summary of the workflow analysis from data.

    Args:
        analysis_data: The workflow analysis dictionary
    """
    summary = summarize_workflow_analysis_from_data(analysis_data)

    print("🔍 NIFI WORKFLOW ANALYSIS SUMMARY")
    print("=" * 60)

    overview = summary["workflow_overview"]
    print(f"📊 OVERVIEW:")
    print(f"   • Total Processors: {overview['total_processors']}")
    print(
        f"   • Actual Data Processing: {overview['actual_data_processors']} ({overview['data_processing_ratio']}%)"
    )
    print(f"   • Data Movement Only: {overview['data_movement_processors']}")
    print(f"   • Infrastructure/Routing: {overview['infrastructure_processors']}")

    print(f"\n🔧 DATA MANIPULATION PROCESSORS ({overview['actual_data_processors']}):")
    transformers = summary["data_manipulation_breakdown"]["data_transformers"]
    external = summary["data_manipulation_breakdown"]["external_processors"]

    if transformers["count"] > 0:
        print(f"   📈 Data Transformers ({transformers['count']}):")
        for proc in transformers["processors"][:5]:  # Show top 5
            print(
                f"      - {proc['name']} ({proc['type']}): {proc['business_purpose']}"
            )
        if transformers["count"] > 5:
            print(f"      ... and {transformers['count'] - 5} more")

    if external["count"] > 0:
        print(f"   🔌 External Processors ({external['count']}):")
        for proc in external["processors"][:5]:  # Show top 5
            print(
                f"      - {proc['name']} ({proc['type']}): {proc['business_purpose']}"
            )

    print(
        f"\n📦 DATA MOVEMENT PROCESSORS ({summary['data_manipulation_breakdown']['data_movers']['count']}):"
    )
    movers = summary["data_manipulation_breakdown"]["data_movers"]["processors"][:3]
    for proc in movers:
        print(f"   • {proc['name']} ({proc['type']}): {proc['business_purpose']}")

    print(
        f"\n🔗 INFRASTRUCTURE PROCESSORS ({summary['infrastructure_breakdown']['count']}):"
    )
    infra = summary["infrastructure_breakdown"]["processors"][:3]
    for proc in infra:
        print(f"   • {proc['name']} ({proc['type']}): {proc['business_purpose']}")

    print(f"\n⭐ KEY BUSINESS OPERATIONS:")
    for operation, count in summary["key_business_operations"].items():
        if count > 1:  # Only show operations done multiple times
            print(f"   • {operation}: {count} times")

    impact = summary["data_impact_analysis"]
    print(f"\n🎯 DATA IMPACT ANALYSIS:")
    print(f"   • High Impact: {impact['high_impact_processors']} processors")
    print(f"   • Medium Impact: {impact['medium_impact_processors']} processors")
    print(
        f"   • Low/No Impact: {impact['low_impact_processors'] + impact['no_impact_processors']} processors"
    )

    if impact["critical_processors"]:
        print(f"\n🚨 CRITICAL PROCESSORS (High Impact):")
        for proc in impact["critical_processors"][:3]:
            print(f"   • {proc['name']} ({proc['type']}): {proc['business_purpose']}")

    insights = summary["business_insights"]
    print(f"\n💡 BUSINESS INSIGHTS:")
    print(f"   • Workflow Complexity: {insights['workflow_complexity'].upper()}")
    print(f"   • Automation Potential: {insights['automation_potential'].upper()}")

    primary_ops = [
        f"{op} ({count})" for op, count in insights["primary_data_operations"]
    ]
    print(f"   • Primary Operations: {', '.join(primary_ops)}")

    print("=" * 60)


def summarize_workflow_analysis(json_file_path: str) -> Dict[str, Any]:
    """
    Create a clear summary from the detailed workflow analysis JSON file.

    Args:
        json_file_path: Path to the workflow analysis JSON file

    Returns:
        Dictionary with clear summary statistics and insights
    """
    with open(json_file_path, "r") as f:
        analysis_data = json.load(f)
    return summarize_workflow_analysis_from_data(analysis_data)


def print_workflow_summary(json_file_path: str) -> None:
    """
    Print a human-readable summary of the workflow analysis from file.

    Args:
        json_file_path: Path to the workflow analysis JSON file
    """
    with open(json_file_path, "r") as f:
        analysis_data = json.load(f)
    print_workflow_summary_from_data(analysis_data)


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        print_workflow_summary(sys.argv[1])
    else:
        print("Usage: python workflow_summary.py <path_to_analysis_json>")

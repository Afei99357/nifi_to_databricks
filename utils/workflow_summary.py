# utils/workflow_summary.py
# Utility to summarize NiFi workflow analysis results

import json
from collections import Counter
from datetime import datetime
from typing import Any, Dict, List


def _deduplicate_processor_list(
    processors: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Deduplicate processors with identical names for cleaner display.
    Shows count when multiple instances exist.
    """

    # Count occurrences of each processor name
    name_counts = Counter(p.get("name", "") for p in processors)

    # Create deduplicated list with instance counts
    seen_names = set()
    deduplicated = []

    for proc in processors:
        name = proc.get("name", "")
        if name not in seen_names:
            seen_names.add(name)

            # Create processor entry with instance count if > 1
            proc_entry = {
                "name": (
                    name
                    if name_counts[name] == 1
                    else f"{name} ({name_counts[name]} instances)"
                ),
                "type": proc.get("processor_type", "").split(".")[-1],
                "business_purpose": proc.get("business_purpose", ""),
                "instance_count": name_counts[name],
            }
            deduplicated.append(proc_entry)

    return deduplicated


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

    # Try both possible key names for processor data
    processors = analysis_data.get("processors_analysis", [])
    if not processors:
        processors = analysis_data.get("classification_results", [])

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
                "processors": _deduplicate_processor_list(data_transformers),
            },
            "external_processors": {
                "count": len(external_processors),
                "processors": _deduplicate_processor_list(external_processors),
            },
            "data_movers": {
                "count": len(data_movers),
                "processors": _deduplicate_processor_list(data_movers),
            },
        },
        "infrastructure_breakdown": {
            "count": len(infrastructure),
            "processors": _deduplicate_processor_list(infrastructure),
        },
        "processor_type_distribution": dict(processor_types.most_common(10)),
        "key_business_operations": dict(key_operations_count.most_common(10)),
        "data_impact_analysis": {
            "high_impact_processors": len(high_impact),
            "medium_impact_processors": len(medium_impact),
            "low_impact_processors": len(low_impact),
            "no_impact_processors": len(no_impact),
            "critical_processors": _deduplicate_processor_list(high_impact),
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
                if len(processors) > 0 and len(infrastructure) / len(processors) > 0.4
                else (
                    "medium"
                    if len(processors) > 0
                    and len(infrastructure) / len(processors) > 0.2
                    else "low"
                )
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
        for proc in transformers["processors"]:  # Show ALL transformers
            print(
                f"      - {proc['name']} ({proc['type']}): {proc['business_purpose']}"
            )

    if external["count"] > 0:
        print(f"   🔌 External Processors ({external['count']}):")
        for proc in external["processors"]:  # Show ALL external processors
            print(
                f"      - {proc['name']} ({proc['type']}): {proc['business_purpose']}"
            )

    print(
        f"\n📦 DATA MOVEMENT PROCESSORS ({summary['data_manipulation_breakdown']['data_movers']['count']}):"
    )
    movers = summary["data_manipulation_breakdown"]["data_movers"][
        "processors"
    ]  # Show ALL data movers
    for proc in movers:
        print(f"   • {proc['name']} ({proc['type']}): {proc['business_purpose']}")

    print(
        f"\n🔗 INFRASTRUCTURE PROCESSORS ({summary['infrastructure_breakdown']['count']}):"
    )
    infra = summary["infrastructure_breakdown"][
        "processors"
    ]  # Show ALL infrastructure processors
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
        for proc in impact["critical_processors"]:  # Show ALL critical processors
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


def print_and_save_workflow_summary(
    json_file_path: str, save_markdown: bool = True, output_path: str = None
) -> str:
    """
    Print workflow summary to console and optionally save as markdown file.

    Args:
        json_file_path: Path to the workflow analysis JSON file
        save_markdown: Whether to save markdown report (default: True)
        output_path: Optional path for markdown file. If None, uses same directory as JSON file.

    Returns:
        Path to the saved markdown file (if save_markdown=True), otherwise None
    """
    with open(json_file_path, "r") as f:
        analysis_data = json.load(f)

    # Print to console
    print_workflow_summary_from_data(analysis_data)

    # Save markdown if requested
    if save_markdown:
        # Generate markdown content
        summary = summarize_workflow_analysis_from_data(analysis_data)
        overview = summary["workflow_overview"]

        # Get original filename if available
        filename = analysis_data.get("workflow_metadata", {}).get(
            "filename", "Unknown Workflow"
        )

        # Generate timestamp
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        markdown = f"""# NiFi Workflow Analysis Report

**Workflow:** {filename}
**Analysis Date:** {timestamp}
**Tool:** NiFi to Databricks Migration Tool

## 📊 Workflow Overview

| Metric | Value | Percentage |
|--------|-------|------------|
| **Total Processors** | {overview['total_processors']} | 100% |
| **Data Processors** | {overview['actual_data_processors']} | {overview['data_processing_ratio']}% |
| **Infrastructure Processors** | {overview['infrastructure_processors']} | {round((overview['infrastructure_processors']/overview['total_processors'])*100, 1) if overview['total_processors'] > 0 else 0}% |
| **Data Movement Processors** | {overview['data_movement_processors']} | {round((overview['data_movement_processors']/overview['total_processors'])*100, 1) if overview['total_processors'] > 0 else 0}% |

## 🔧 Data Manipulation Processors ({overview['actual_data_processors']} processors)

"""

        # Add data transformers
        transformers = summary["data_manipulation_breakdown"]["data_transformers"]
        if transformers["count"] > 0:
            markdown += f"""### 📈 Data Transformers ({transformers['count']} processors)

These processors contain actual business logic and data transformation operations:

"""
            for proc in transformers["processors"]:
                markdown += f"- **{proc['name']}** ({proc['type']}): {proc['business_purpose']}\n"
            markdown += "\n"

        # Add external processors
        external = summary["data_manipulation_breakdown"]["external_processors"]
        if external["count"] > 0:
            markdown += f"""### 🔌 External Processors ({external['count']} processors)

These processors interact with external systems:

"""
            for proc in external["processors"]:
                markdown += f"- **{proc['name']}** ({proc['type']}): {proc['business_purpose']}\n"
            markdown += "\n"

        # Add data movement processors
        data_movers = summary["data_manipulation_breakdown"]["data_movers"]
        if data_movers["count"] > 0:
            markdown += f"""## 📦 Data Movement Processors ({data_movers['count']} processors)

These processors move data without transformation:

"""
            for proc in data_movers["processors"]:
                markdown += f"- **{proc['name']}** ({proc['type']}): {proc['business_purpose']}\n"
            markdown += "\n"

        # Add infrastructure breakdown
        infrastructure = summary["infrastructure_breakdown"]
        markdown += f"""## 🔗 Infrastructure Processors ({infrastructure['count']} processors)

These processors handle routing, logging, flow control, and metadata operations:

"""

        # Show first 10 infrastructure processors, then summarize the rest
        infra_processors = infrastructure["processors"]
        for proc in infra_processors[:10]:
            markdown += (
                f"- **{proc['name']}** ({proc['type']}): {proc['business_purpose']}\n"
            )

        if len(infra_processors) > 10:
            remaining = len(infra_processors) - 10
            markdown += f"- ... and {remaining} more infrastructure processors\n"

        markdown += "\n"

        # Add impact analysis
        impact = summary["data_impact_analysis"]
        markdown += f"""## 🎯 Data Impact Analysis

| Impact Level | Count | Description |
|-------------|-------|-------------|
| **High Impact** | {impact['high_impact_processors']} | Critical data transformation operations |
| **Medium Impact** | {impact['medium_impact_processors']} | Significant data processing or external interactions |
| **Low/No Impact** | {impact['low_impact_processors'] + impact['no_impact_processors']} | Infrastructure and metadata operations |

"""

        # Add critical processors if any
        if impact["critical_processors"]:
            markdown += f"""### 🚨 Critical Processors (High Impact)

These processors require careful migration attention:

"""
            for proc in impact["critical_processors"]:
                markdown += f"- **{proc['name']}** ({proc['type']}): {proc['business_purpose']}\n"
            markdown += "\n"

        # Add business insights
        insights = summary["business_insights"]
        markdown += f"""## 💡 Migration Insights

- **Workflow Complexity:** {insights['workflow_complexity'].upper()}
- **Automation Potential:** {insights['automation_potential'].upper()}

"""

        # Add key business operations
        if summary["key_business_operations"]:
            markdown += """### ⭐ Key Business Operations

"""
            for operation, count in summary["key_business_operations"].items():
                if count > 1:
                    markdown += f"- **{operation}:** {count} times\n"
            markdown += "\n"

        # Add processor type distribution
        if summary["processor_type_distribution"]:
            markdown += """### 📊 Processor Type Distribution

| Processor Type | Count |
|---------------|-------|
"""
            for proc_type, count in summary["processor_type_distribution"].items():
                markdown += f"| {proc_type} | {count} |\n"
            markdown += "\n"

        # Add recommendations
        markdown += f"""## 🏗️ Architecture Recommendations

Based on the analysis:

- **Focus Areas:** The {overview['actual_data_processors']} data processors ({overview['data_processing_ratio']}%) contain the core business logic
- **Migration Priority:** Start with the {impact['high_impact_processors']} high-impact processors
- **Simplification Opportunity:** {overview['infrastructure_processors']} infrastructure processors can potentially be eliminated or simplified in Databricks
- **Architecture Choice:** Consider the processor mix when choosing between Databricks Jobs, DLT Pipeline, or Structured Streaming

## 📈 Summary

This workflow contains **{overview['actual_data_processors']} processors with actual business logic** out of {overview['total_processors']} total processors.
The majority ({round((overview['infrastructure_processors']/overview['total_processors'])*100, 1) if overview['total_processors'] > 0 else 0}%) are infrastructure operations that handle routing, logging, and flow control.

**Migration Focus:** Concentrate effort on the {overview['actual_data_processors']} data processors, particularly the {impact['high_impact_processors']} high-impact ones, while leveraging Databricks' native capabilities to replace most infrastructure processors.

---

*Generated by NiFi to Databricks Migration Tool - Intelligent Workflow Analysis*
"""

        # Determine output path
        if output_path is None:
            import os

            base_path = os.path.splitext(json_file_path)[0]
            output_path = f"{base_path}_analysis_report.md"

        # Save to file
        with open(output_path, "w") as f:
            f.write(markdown)

        print(f"\n📄 Workflow analysis report saved to: {output_path}")
        return output_path

    return None


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        print_and_save_workflow_summary(sys.argv[1], save_markdown=False)
    else:
        print("Usage: python workflow_summary.py <path_to_analysis_json>")

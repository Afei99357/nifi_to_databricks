"""
Simplified NiFi to Databricks migration pipeline without agent complexity.
Direct function call approach for linear migration workflow.
"""

import json
from typing import Any, Dict, Optional

from tools.analysis_tools import (
    analyze_nifi_workflow_detailed,
    classify_processor_types,
)

# Asset discovery removed - focus on business migration guide only
from tools.migration_tools import orchestrate_focused_nifi_migration
from tools.nifi_processor_classifier_tool import (
    analyze_processors_batch,
    analyze_workflow_patterns,
)
from tools.pruning_tools import (
    create_semantic_data_flows,
    detect_data_flow_chains,
    prune_infrastructure_processors,
)


def migrate_nifi_to_databricks_simplified(
    xml_path: str,
    out_dir: str,
    project: str,
    notebook_path: Optional[str] = None,
    max_processors_per_chunk: int = 25,
) -> Dict[str, Any]:
    """
    Simplified NiFi to Databricks migration pipeline using direct function calls.

    This function performs a complete migration through these steps:
    1. Analyze NiFi workflow and classify processors
    2. Prune infrastructure-only processors
    3. Detect semantic data flow chains
    4. Generate comprehensive migration guide with recommendations

    Args:
        xml_path: Path to NiFi XML template file
        out_dir: Output directory for migration artifacts
        project: Project name for generated assets
        notebook_path: Optional notebook path (for reference in guide)
        max_processors_per_chunk: Max processors per chunk for large workflows

    Returns:
        Dictionary containing migration guide and analysis results
    """

    print("🚀 Starting simplified NiFi to Databricks migration...")

    # Step 1: Create output directory structure
    print("📁 Creating output directory structure...")
    import os

    os.makedirs(f"{out_dir}/{project}", exist_ok=True)

    # Step 2: Read XML content
    print("📖 Reading NiFi XML template...")
    with open(xml_path, "r", encoding="utf-8") as f:
        xml_content = f.read()

    # Step 3: Analyze and classify processors (single analysis shared between functions)
    print("🔍 Analyzing workflow and classifying processors...")

    # Single analysis call that both functions can use
    analysis_result = analyze_workflow_patterns(
        xml_path=xml_path, save_markdown=False, output_dir=f"{out_dir}/{project}"
    )

    # Create workflow analysis summary from the single analysis
    workflow_analysis = analyze_nifi_workflow_detailed(
        xml_path,
        save_markdown=False,
        output_dir=f"{out_dir}/{project}",
        _reuse_analysis=analysis_result,  # Pass the analysis to avoid re-running
    )
    print(f"📊 Workflow Analysis: Completed")

    # Create processor classifications from the same analysis
    processor_classifications = classify_processor_types(
        xml_path,
        _reuse_analysis=analysis_result,  # Pass the analysis to avoid re-running
    )
    print(f"🏷️  Processor Classifications: Completed")

    # Parse classification results for pruning
    try:
        class_data = json.loads(processor_classifications)
        classifications = class_data.get("processor_classifications", [])

        # Classifications loaded for pruning

    except Exception as e:
        print(f"🚨 Failed to parse classifications: {e}")

    # Step 4: Prune infrastructure processors
    print("✂️  Pruning infrastructure-only processors...")

    # Debug: Check what we're passing to pruning
    try:
        debug_data = json.loads(processor_classifications)
        if "processor_classifications" in debug_data:
            debug_count = len(debug_data["processor_classifications"])
            print(f"🔍 PRUNING DEBUG: Found {debug_count} processor classifications")
        else:
            print(f"🔍 PRUNING DEBUG: Keys in data: {list(debug_data.keys())}")
    except:
        print(f"🔍 PRUNING DEBUG: Could not parse processor_classifications JSON")

    pruned_result = prune_infrastructure_processors(processor_classifications)
    # print(f"🎯 Pruned Result: {pruned_result}")  # Comment out detailed JSON output

    # DEBUG: Check pruning results
    try:
        if isinstance(pruned_result, str):
            pruned_data = json.loads(pruned_result)
        else:
            pruned_data = pruned_result

        if "error" in pruned_data:
            print(f"🚨 PRUNING ERROR: {pruned_data.get('error', 'Unknown error')}")
        else:
            essential = pruned_data.get("pruned_processors", [])
            print(f"\n🔍 PRUNING DEBUG:")
            print(f"Essential processors after pruning: {len(essential)}")
            if essential:
                print("Essential processor examples:")
                for i, p in enumerate(essential[:3]):
                    print(
                        f"  {i+1}. {p.get('name')} ({p.get('type')}) - {p.get('classification')}"
                    )
    except Exception as e:
        print(f"🚨 Failed to parse pruning results: {e}")

    # Step 5: Detect data flow chains
    print("🔗 Detecting semantic data flow chains...")
    chains_result = detect_data_flow_chains(xml_content, pruned_result)
    # print(f"⛓️  Chains Result: {chains_result}")  # Comment out detailed JSON output

    # Step 6: Create semantic data flows
    print("🌊 Creating semantic data flows...")
    semantic_flows = create_semantic_data_flows(chains_result)
    # print(f"🎨 Semantic Flows: {semantic_flows}")  # Comment out detailed JSON output

    # Step 7: Extract and catalog all workflow assets for manual review
    print("📋 Extracting workflow assets (scripts, paths, tables) for manual review...")

    # Parse analysis result for asset extraction
    if isinstance(analysis_result, str):
        analysis_data = json.loads(analysis_result)
    else:
        analysis_data = analysis_result

    print("📋 Asset discovery skipped - focusing on business migration guide")

    # Generate essential processors report for manual review
    essential_report_path = _generate_essential_processors_report(
        pruned_result, f"{out_dir}/{project}"
    )
    print(f"📋 Essential processors report: {essential_report_path}")

    # Step 8: Generate comprehensive migration guide (essential processors only)
    print(
        "📋 Generating comprehensive migration guide for essential data processors..."
    )

    # Parse pruned_result to get the list of essential processors

    if isinstance(pruned_result, str):
        pruned_data = json.loads(pruned_result)
    else:
        pruned_data = pruned_result

    essential_processors = pruned_data.get("pruned_processors", [])
    print(
        f"📊 Focusing on {len(essential_processors)} essential processors (infrastructure skipped)"
    )

    migration_result = orchestrate_focused_nifi_migration(
        xml_path=xml_path,
        pruned_processors=essential_processors,
        semantic_flows=semantic_flows,
        out_dir=out_dir,
        project=project,
        job=f"{project}_job",
        notebook_path=notebook_path or "",
        run_now=False,  # No deployment - migration guide approach
    )

    # Compile complete results
    complete_result = {
        "migration_result": migration_result,
        "analysis": {
            "workflow_analysis": workflow_analysis,
            "processor_classifications": processor_classifications,
            "pruned_processors": pruned_result,
            "data_flow_chains": chains_result,
            "semantic_flows": semantic_flows,
        },
        # Asset discovery removed - business migration guide focus
        "configuration": {
            "xml_path": xml_path,
            "out_dir": out_dir,
            "project": project,
            "notebook_path": notebook_path,
            "approach": "migration_guide_generation",
            "max_processors_per_chunk": max_processors_per_chunk,
        },
    }

    print("✅ Migration guide generation completed successfully!")
    print(f"📁 Migration guide and analysis saved to: {out_dir}")
    print(f"📋 Check MIGRATION_GUIDE.md for comprehensive migration recommendations")

    return complete_result


def analyze_nifi_workflow_only(xml_path: str) -> Dict[str, Any]:
    """
    Perform only the analysis phase without migration.
    Useful for understanding workflow before committing to migration.

    Args:
        xml_path: Path to NiFi XML template file

    Returns:
        Dictionary containing analysis results
    """

    print("🔍 Analyzing NiFi workflow (analysis only)...")

    # Read XML content
    with open(xml_path, "r", encoding="utf-8") as f:
        xml_content = f.read()

    # Perform analysis steps (single analysis shared between functions)
    # For analysis-only, save to a temp directory next to XML
    import os

    temp_output_dir = os.path.join(os.path.dirname(xml_path), "analysis_temp")

    # Single analysis call that both functions can use
    analysis_result = analyze_workflow_patterns(
        xml_path=xml_path, save_markdown=False, output_dir=temp_output_dir
    )

    # Create workflow analysis and processor classifications from the same analysis
    workflow_analysis = analyze_nifi_workflow_detailed(
        xml_path,
        save_markdown=False,
        output_dir=temp_output_dir,
        _reuse_analysis=analysis_result,
    )
    processor_classifications = classify_processor_types(
        xml_path, _reuse_analysis=analysis_result
    )
    pruned_result = prune_infrastructure_processors(processor_classifications)
    chains_result = detect_data_flow_chains(xml_content, pruned_result)
    semantic_flows = create_semantic_data_flows(chains_result)

    analysis_result = {
        "workflow_analysis": workflow_analysis,
        "processor_classifications": processor_classifications,
        "pruned_processors": pruned_result,
        "data_flow_chains": chains_result,
        "semantic_flows": semantic_flows,
        "xml_path": xml_path,
    }

    print("✅ Analysis completed!")
    return analysis_result


def _generate_essential_processors_report(pruned_result, output_dir: str) -> str:
    """Generate a clean, focused report of essential processors for manual review."""

    # Parse pruned result
    if isinstance(pruned_result, str):
        pruned_data = json.loads(pruned_result)
    else:
        pruned_data = pruned_result

    processors = pruned_data.get("pruned_processors", [])

    # Group by classification for organization
    by_classification = {}
    for proc in processors:
        classification = proc.get("classification", "unknown")
        if classification not in by_classification:
            by_classification[classification] = []
        by_classification[classification].append(proc)

    # Generate the report
    report_lines = [
        "# Essential Processors - Manual Review",
        "",
        "## Summary",
        f"- **Total Essential Processors**: {len(processors)} (after infrastructure pruning)",
    ]

    # Add classification breakdown
    for classification, procs in by_classification.items():
        count = len(procs)
        class_name = classification.replace("_", " ").title()
        report_lines.append(
            f"- **{class_name}**: {count} processor{'s' if count != 1 else ''}"
        )

    report_lines.extend(["", "---", ""])

    # Detail sections by classification
    classification_order = [
        "data_movement",
        "data_transformation",
        "external_processing",
        "unknown",
    ]

    for classification in classification_order:
        if classification not in by_classification:
            continue

        processors_list = by_classification[classification]
        class_name = classification.replace("_", " ").title()

        report_lines.extend([f"## {class_name} Processors", ""])

        for proc in processors_list:
            name = proc.get("name", "Unknown")
            proc_type = proc.get("type", "Unknown")
            properties = proc.get("properties", {})

            report_lines.append(f'### {proc_type} - "{name}"')

            # Extract key details based on processor type
            key_details = _extract_processor_key_details(proc_type, name, properties)
            if key_details:
                report_lines.extend(key_details)

            # Add migration suggestion
            migration_hint = _get_migration_hint(classification, proc_type)
            if migration_hint:
                report_lines.extend([f"- **Migration**: {migration_hint}", ""])
            else:
                report_lines.append("")

    # Write to file
    report_path = f"{output_dir}/essential_processors.md"
    with open(report_path, "w") as f:
        f.write("\n".join(report_lines))

    return report_path


def _extract_processor_key_details(proc_type: str, name: str, properties: dict) -> list:
    """Extract the most important details for manual review."""
    details = []

    if proc_type == "ListFile":
        input_dir = properties.get("Input Directory", "")
        file_filter = properties.get("File Filter", "")
        if input_dir:
            details.append(f"- **Directory**: `{input_dir}`")
        if file_filter and file_filter != ".*":
            details.append(f"- **File Pattern**: `{file_filter}`")

    elif proc_type == "ExecuteStreamCommand":
        command = properties.get("Command Path", "")
        args = properties.get("Command Arguments", "")
        if command:
            details.append(f"- **Command**: `{command}`")
        if args and len(args) < 100:  # Only show short args
            details.append(f"- **Arguments**: `{args}`")
        elif "impala" in args.lower() or "sql" in args.lower():
            details.append("- **Purpose**: Database/SQL operations")
        elif "script" in args.lower():
            details.append("- **Purpose**: Custom script execution")

    elif proc_type == "PutHDFS":
        directory = properties.get("Directory", "")
        if directory:
            details.append(f"- **Output Directory**: `{directory}`")

    elif proc_type == "PutSFTP":
        hostname = properties.get("Hostname", "")
        remote_path = properties.get("Remote Path", "")
        if hostname:
            details.append(f"- **Host**: `{hostname}`")
        if remote_path:
            details.append(f"- **Remote Path**: `{remote_path}`")

    return details


def _get_migration_hint(classification: str, proc_type: str) -> str:
    """Provide concise migration suggestions."""

    if classification == "data_movement":
        if proc_type == "ListFile":
            return "→ Auto Loader with cloud storage monitoring"
        elif proc_type in ["PutHDFS", "PutFile"]:
            return "→ Write to Delta Lake tables"
        elif proc_type == "PutSFTP":
            return "→ Write to cloud storage with appropriate access"
        else:
            return "→ Replace with cloud-native data movement"

    elif classification == "data_transformation":
        if proc_type == "ExecuteStreamCommand":
            return "→ Convert to Databricks SQL or PySpark"
        else:
            return "→ Implement in PySpark DataFrame operations"

    elif classification == "external_processing":
        return "→ Review and convert custom logic to Databricks"

    return "→ Review migration approach based on business logic"

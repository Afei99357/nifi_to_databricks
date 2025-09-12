"""
Simplified NiFi to Databricks migration pipeline without agent complexity.
Direct function call approach for linear migration workflow.
"""

import json
import os
from typing import Any, Dict, Optional

from tools.asset_extraction import (
    generate_asset_summary,
    generate_unknown_processors_json,
)
from tools.improved_classifier import analyze_workflow_patterns
from tools.improved_pruning import (
    detect_data_flow_chains,
    prune_infrastructure_processors,
)
from tools.reporting import generate_essential_processors_report
from tools.simple_table_lineage import generate_simple_lineage_report

# Asset discovery functionality moved to tools/asset_extraction.py


def migrate_nifi_to_databricks_simplified(
    xml_path: str,
    out_dir: str,
    project: str,
    notebook_path: Optional[str] = None,
    max_processors_per_chunk: int = 25,
    progress_callback: Optional[callable] = None,
) -> Dict[str, Any]:
    """
    Simplified NiFi to Databricks migration pipeline using direct function calls.

    This function performs a complete migration through these steps:
    1. Analyze NiFi workflow and classify processors
    2. Prune infrastructure-only processors
    3. Detect data flow chains for reference
    4. Generate processor analysis reports

    Args:
        xml_path: Path to NiFi XML template file
        out_dir: Output directory for migration artifacts
        project: Project name for generated assets
        notebook_path: Optional notebook path (for reference in guide)
        max_processors_per_chunk: Max processors per chunk for large workflows

    Returns:
        Dictionary containing migration guide and analysis results
    """

    def _log(message):
        print(message)
        if progress_callback:
            progress_callback(message)

    _log("🚀 Starting simplified NiFi to Databricks migration...")

    # Step 1: Create output directory structure
    _log("📁 Creating output directory structure...")
    os.makedirs(f"{out_dir}/{project}", exist_ok=True)

    # Step 2: Read XML content
    _log("📖 Reading NiFi XML template...")
    with open(xml_path, "r", encoding="utf-8") as f:
        xml_content = f.read()

    # Step 3: Analyze and classify processors (single analysis shared between functions)
    _log("🔍 Analyzing workflow and classifying processors...")

    # Single analysis call that both functions can use
    analysis_result = analyze_workflow_patterns(
        xml_path=xml_path, save_markdown=False, output_dir=f"{out_dir}/{project}"
    )

    # Convert field names for backward compatibility (data_manipulation_type → classification)
    if "classification_results" in analysis_result:
        for proc in analysis_result["classification_results"]:
            if "data_manipulation_type" in proc and "classification" not in proc:
                proc["classification"] = proc["data_manipulation_type"]

    _log("📊 Analysis data prepared for migration pipeline")

    # Step 4: Prune infrastructure processors
    _log("✂️  Pruning infrastructure-only processors...")

    # Pass the classification results in the format expected by pruning function
    pruned_result = prune_infrastructure_processors(json.dumps(analysis_result))

    # Check pruning results and show essential count
    if isinstance(pruned_result, str):
        pruned_data = json.loads(pruned_result)
    else:
        pruned_data = pruned_result

    if "error" in pruned_data:
        _log(f"⚠️ Pruning error: {pruned_data.get('error', 'Unknown')}")
    else:
        essential_count = len(pruned_data.get("pruned_processors", []))
        _log(f"✅ Pruning complete: {essential_count} essential processors identified")

    # Step 5: Detect data flow chains (for reference only)
    _log("🔗 Detecting data flow chains...")
    chains_result = detect_data_flow_chains(xml_content, pruned_result)

    # Step 6: Analyze connection architecture (fan-in/fan-out hotspots)
    _log("🕸️  Analyzing connection architecture and hotspots...")
    # Generate table lineage analysis
    from tools.simple_table_lineage import analyze_nifi_table_lineage

    lineage_analysis = analyze_nifi_table_lineage(xml_content)
    connection_analysis = generate_simple_lineage_report(lineage_analysis)
    _log("🎯 Table lineage analysis complete")

    # Step 7: Extract and catalog all workflow assets for manual review
    _log("📋 Extracting workflow assets (scripts, paths, tables) for manual review...")

    # Asset discovery skipped - focusing on business migration guide

    # Generate reports content
    essential_processors_tuple = generate_essential_processors_report(pruned_result)
    unknown_processors_content = generate_unknown_processors_json(analysis_result)
    asset_summary_content = generate_asset_summary(analysis_result)
    _log("📋 Reports generated successfully")

    # Step 8: Generate comprehensive migration guide (essential processors only)
    _log("📋 Generating comprehensive migration guide...")

    # Parse pruned_result to get the list of essential processors

    if isinstance(pruned_result, str):
        pruned_data = json.loads(pruned_result)
    else:
        pruned_data = pruned_result

    essential_processors = pruned_data.get("pruned_processors", [])
    _log(f"🎯 Processing {len(essential_processors)} essential processors")

    # Categorize processors for analysis
    data_transformation_procs = []
    data_movement_procs = []
    external_processing_procs = []

    for proc in essential_processors:
        classification = proc.get(
            "classification", proc.get("data_manipulation_type", "unknown")
        )
        if classification == "data_transformation":
            data_transformation_procs.append(proc)
        elif classification == "data_movement":
            data_movement_procs.append(proc)
        elif classification == "external_processing":
            external_processing_procs.append(proc)

    # Create output directory
    os.makedirs(f"{out_dir}/{project}", exist_ok=True)

    migration_result = {
        "migration_type": "focused_analysis",
        "processors_analyzed": len(essential_processors),
        "breakdown": {
            "data_transformation": len(data_transformation_procs),
            "external_processing": len(external_processing_procs),
            "data_movement": len(data_movement_procs),
            "infrastructure_skipped": "All infrastructure processors skipped",
        },
        "output_directory": f"{out_dir}/{project}",
        "approach": "Essential processor analysis and classification complete",
    }

    # Compile complete results
    complete_result = {
        "migration_result": migration_result,
        "analysis": {
            "processor_classifications": analysis_result,
            "pruned_processors": pruned_result,
            "data_flow_chains": chains_result,
        },
        "reports": {
            "essential_processors": essential_processors_tuple,
            "unknown_processors": unknown_processors_content,
            "asset_summary": asset_summary_content,
            "connection_analysis": connection_analysis,
        },
        "configuration": {
            "xml_path": xml_path,
            "out_dir": out_dir,
            "project": project,
            "notebook_path": notebook_path,
            "approach": "migration_guide_generation",
            "max_processors_per_chunk": max_processors_per_chunk,
        },
    }

    _log("✅ Migration analysis completed successfully!")
    _log(f"📋 Generated reports and processor analysis ready for review")

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
    temp_output_dir = os.path.join(os.path.dirname(xml_path), "analysis_temp")

    # Single analysis call that both functions can use
    analysis_result = analyze_workflow_patterns(
        xml_path=xml_path, save_markdown=False, output_dir=temp_output_dir
    )

    # Convert field names for backward compatibility (data_manipulation_type → classification)
    if "classification_results" in analysis_result:
        for proc in analysis_result["classification_results"]:
            if "data_manipulation_type" in proc and "classification" not in proc:
                proc["classification"] = proc["data_manipulation_type"]

    # Process the analysis result through the pipeline
    pruned_result = prune_infrastructure_processors(json.dumps(analysis_result))
    chains_result = detect_data_flow_chains(xml_content, pruned_result)
    # Generate table lineage analysis
    from tools.simple_table_lineage import analyze_nifi_table_lineage

    lineage_analysis = analyze_nifi_table_lineage(xml_content)
    connection_analysis = generate_simple_lineage_report(lineage_analysis)

    # Package all results together
    analysis_result = {
        "processor_classifications": analysis_result,
        "pruned_processors": pruned_result,
        "data_flow_chains": chains_result,
        "connection_analysis": connection_analysis,
        "xml_path": xml_path,
    }

    print("✅ Analysis completed!")
    return analysis_result

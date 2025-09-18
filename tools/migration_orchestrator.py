"""
Simplified NiFi to Databricks migration pipeline without agent complexity.
Direct function call approach for linear migration workflow.
"""

import json
import os
from typing import Any, Dict, Optional

from tools.improved_classifier import (
    analyze_workflow_patterns,
    generate_unknown_processors_json,
)
from tools.improved_pruning import (
    detect_data_flow_chains,
    prune_infrastructure_processors,
)
from tools.reporting import generate_essential_processors_report

# from tools.simple_table_lineage import generate_simple_lineage_report  # REMOVED

# Asset discovery functionality moved to tools/asset_extraction.py


def extract_nifi_assets_only(
    xml_path: str,
    progress_callback: Optional[callable] = None,
) -> Dict[str, Any]:
    """
    Extract only assets from NiFi workflow (no classification/dependencies needed).

    Ultra-lightweight function focused purely on asset discovery:
    - Scripts, paths, hosts, tables from ALL processors
    - No classification or dependency analysis required

    Args:
        xml_path: Path to NiFi XML template file
        progress_callback: Optional callback for progress updates

    Returns:
        Dictionary with asset extraction results
    """

    def _log(msg: str):
        if progress_callback:
            progress_callback(msg)
        print(msg)

    _log("📦 Starting pure asset extraction...")

    try:
        # Step 1: Parse NiFi XML to get raw processor data (no classification needed)
        _log("🔍 Parsing NiFi XML...")
        import json

        from tools.xml_tools import parse_nifi_template_impl

        with open(xml_path, "r", encoding="utf-8") as f:
            xml_content = f.read()

        template_data = parse_nifi_template_impl(xml_content)
        processors = template_data.get("processors", [])

        _log(f"📋 Found {len(processors)} processors, extracting assets...")

        # Step 2: Extract assets from ALL processors - get structured data
        from tools.asset_extraction import extract_assets_from_properties

        # Extract key assets with high confidence
        script_files = set()
        hdfs_paths = set()
        database_hosts = set()
        external_hosts = set()
        table_references = set()

        for proc in processors:
            extract_assets_from_properties(
                proc.get("properties", {}),
                script_files,
                hdfs_paths,
                database_hosts,
                external_hosts,
                table_references,
            )

        _log("✅ Pure asset extraction completed!")

        return {
            "assets": {
                "script_files": sorted(list(script_files)),
                "hdfs_paths": sorted(list(hdfs_paths)),
                "database_hosts": sorted(list(database_hosts)),
                "external_hosts": sorted(list(external_hosts)),
                "table_references": sorted(list(table_references)),
            },
            "summary": {
                "total_processors": len(processors),
                "script_files_count": len(script_files),
                "hdfs_paths_count": len(hdfs_paths),
                "database_hosts_count": len(database_hosts),
                "external_hosts_count": len(external_hosts),
                "table_references_count": len(table_references),
            },
        }

    except Exception as e:
        return {"error": f"Failed to extract assets: {str(e)}"}


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

    # Connection analysis moved to Table Lineage page (03_Table_Lineage.py)

    # Step 7: Extract and catalog all workflow assets for manual review
    _log("📋 Extracting workflow assets (scripts, paths, tables) for manual review...")

    # Asset discovery skipped - focusing on business migration guide

    # Generate migration-focused reports content (dependencies/assets moved to separate pages)
    essential_processors_tuple = generate_essential_processors_report(pruned_result)
    # Extract only the main report, skip dependencies part
    essential_processors_report = (
        essential_processors_tuple[0]
        if isinstance(essential_processors_tuple, tuple)
        else essential_processors_tuple
    )
    unknown_processors_content = generate_unknown_processors_json(analysis_result)
    _log("📋 Migration reports generated successfully")

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
            "essential_processors": essential_processors_report,
            "unknown_processors": unknown_processors_content,
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
    # Generate table lineage analysis (REMOVED - will reimplement differently)
    # from tools.simple_table_lineage import analyze_nifi_table_lineage
    # lineage_analysis = analyze_nifi_table_lineage(xml_content)
    # connection_analysis = generate_simple_lineage_report(lineage_analysis)
    connection_analysis = (
        "Table lineage analysis temporarily disabled - will be reimplemented"
    )

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

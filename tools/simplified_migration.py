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
from tools.asset_discovery_tools import (
    extract_workflow_assets,
    generate_asset_summary_report,
    save_asset_catalog,
)
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
    deploy: bool = False,
    max_processors_per_chunk: int = 25,
) -> Dict[str, Any]:
    """
    Simplified NiFi to Databricks migration pipeline using direct function calls.

    This function performs a complete migration through these steps:
    1. Analyze NiFi workflow and classify processors
    2. Prune infrastructure-only processors
    3. Detect semantic data flow chains
    4. Create optimized Databricks migration

    Args:
        xml_path: Path to NiFi XML template file
        out_dir: Output directory for migration artifacts
        project: Project name for generated assets
        notebook_path: Optional notebook path for deployment
        deploy: Whether to deploy the generated job
        max_processors_per_chunk: Max processors per chunk for large workflows

    Returns:
        Dictionary containing migration results and analysis
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

    # Extract comprehensive asset catalog
    workflow_assets = extract_workflow_assets(analysis_data)

    # Save asset catalog and summary
    asset_catalog_path = save_asset_catalog(workflow_assets, f"{out_dir}/{project}")
    asset_summary_path = generate_asset_summary_report(
        workflow_assets, f"{out_dir}/{project}"
    )

    print(f"📋 Asset catalog saved: {asset_catalog_path}")
    print(f"📄 Asset summary saved: {asset_summary_path}")

    # Show key findings
    asset_summary = workflow_assets.get("asset_summary", {})
    if asset_summary.get("total_script_files", 0) > 0:
        print(
            f"🔍 Found {asset_summary['total_script_files']} script files requiring manual migration"
        )
    if asset_summary.get("total_hdfs_paths", 0) > 0:
        print(
            f"🔍 Found {asset_summary['total_hdfs_paths']} HDFS paths needing Unity Catalog migration"
        )
    if asset_summary.get("total_table_references", 0) > 0:
        print(
            f"🔍 Found {asset_summary['total_table_references']} table references for schema mapping"
        )

    # Step 8: Execute FOCUSED migration (only essential processors)
    print("🎯 Executing focused migration on essential data processors only...")

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
        run_now=deploy,
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
        "asset_discovery": {
            "workflow_assets": workflow_assets,
            "asset_catalog_path": asset_catalog_path,
            "asset_summary_path": asset_summary_path,
            "summary_stats": {
                "script_files": asset_summary.get("total_script_files", 0),
                "hdfs_paths": asset_summary.get("total_hdfs_paths", 0),
                "table_references": asset_summary.get("total_table_references", 0),
                "sql_statements": asset_summary.get("total_sql_statements", 0),
            },
        },
        "configuration": {
            "xml_path": xml_path,
            "out_dir": out_dir,
            "project": project,
            "notebook_path": notebook_path,
            "deploy": deploy,
            "max_processors_per_chunk": max_processors_per_chunk,
        },
    }

    print("✅ Migration completed successfully!")
    print(f"📁 Results saved to: {out_dir}")

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

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

    # DEBUG: Show what the analysis returned
    print(f"DEBUG: Raw analysis result type: {type(analysis_result)}")
    if isinstance(analysis_result, str):
        print(f"DEBUG: Analysis result length: {len(analysis_result)} characters")
        try:
            parsed = json.loads(analysis_result)
            print(
                f"DEBUG: Parsed analysis contains {len(parsed.get('classification_results', []))} classification results"
            )
        except Exception as e:
            print(f"DEBUG: Failed to parse raw analysis: {e}")
    else:
        print(
            f"DEBUG: Analysis result is dict with keys: {list(analysis_result.keys()) if isinstance(analysis_result, dict) else 'not a dict'}"
        )

    # Create workflow analysis summary from the single analysis
    workflow_analysis = analyze_nifi_workflow_detailed(
        xml_path,
        save_markdown=False,
        output_dir=f"{out_dir}/{project}",
        _reuse_analysis=analysis_result,  # Pass the analysis to avoid re-running
    )
    print(f"📊 Workflow Analysis: {workflow_analysis}")

    # DEBUG: Parse and show workflow analysis
    try:
        workflow_data = json.loads(workflow_analysis)
        processor_counts = workflow_data.get("processor_counts", {})
        print(f"DEBUG: Workflow processor counts: {processor_counts}")
        print(f"DEBUG: Total processors found: {processor_counts.get('total', 0)}")
        print(
            f"DEBUG: Data transformation: {processor_counts.get('data_transformation', 0)}"
        )
        print(f"DEBUG: Data movement: {processor_counts.get('data_movement', 0)}")
        print(f"DEBUG: Infrastructure: {processor_counts.get('infrastructure', 0)}")
    except Exception as e:
        print(f"DEBUG: Failed to parse workflow analysis: {e}")

    # Create processor classifications from the same analysis
    processor_classifications = classify_processor_types(
        xml_path,
        _reuse_analysis=analysis_result,  # Pass the analysis to avoid re-running
    )
    print(f"🏷️  Processor Classifications: {processor_classifications}")

    # DEBUG: Parse and show classification summary
    import json

    try:
        classifications_data = json.loads(processor_classifications)
        print(
            f"DEBUG: Classification summary: {classifications_data.get('summary', {})}"
        )
        classifications_list = classifications_data.get("processor_classifications", [])
        print(f"DEBUG: Found {len(classifications_list)} classified processors")
        for i, proc in enumerate(classifications_list[:3]):  # Show first 3
            print(
                f"  {i+1}. {proc.get('name', 'Unknown')} ({proc.get('type', 'Unknown')}) -> {proc.get('classification', 'unknown')}"
            )
        if len(classifications_list) > 3:
            print(f"  ... and {len(classifications_list) - 3} more processors")
    except Exception as e:
        print(f"DEBUG: Failed to parse classifications: {e}")

    # Step 4: Prune infrastructure processors
    print("✂️  Pruning infrastructure-only processors...")
    pruned_result = prune_infrastructure_processors(processor_classifications)
    print(f"🎯 Pruned Result: {pruned_result}")

    # DEBUG: Parse and show pruning results
    try:
        pruned_data = (
            json.loads(pruned_result)
            if isinstance(pruned_result, str)
            else pruned_result
        )
        print(f"DEBUG: Pruning summary:")
        print(f"  Original count: {pruned_data.get('original_count', 'unknown')}")
        print(f"  Pruned count: {pruned_data.get('pruned_count', 'unknown')}")
        print(
            f"  Essential processors: {len(pruned_data.get('pruned_processors', []))}"
        )
        essential_processors = pruned_data.get("pruned_processors", [])
        for i, proc in enumerate(essential_processors[:3]):  # Show first 3 essential
            print(
                f"    {i+1}. {proc.get('name', 'Unknown')} ({proc.get('type', 'Unknown')}) -> {proc.get('classification', 'unknown')}"
            )
        if len(essential_processors) > 3:
            print(
                f"    ... and {len(essential_processors) - 3} more essential processors"
            )
    except Exception as e:
        print(f"DEBUG: Failed to parse pruning results: {e}")

    # Step 5: Detect data flow chains
    print("🔗 Detecting semantic data flow chains...")
    chains_result = detect_data_flow_chains(xml_content, pruned_result)
    print(f"⛓️  Chains Result: {chains_result}")

    # Step 6: Create semantic data flows
    print("🌊 Creating semantic data flows...")
    semantic_flows = create_semantic_data_flows(chains_result)
    print(f"🎨 Semantic Flows: {semantic_flows}")

    # Step 7: Execute FOCUSED migration (only essential processors)
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

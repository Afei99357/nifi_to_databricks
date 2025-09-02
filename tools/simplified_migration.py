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
from tools.migration_tools import orchestrate_intelligent_nifi_migration
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

    # Step 1: Read XML content
    print("📖 Reading NiFi XML template...")
    with open(xml_path, "r", encoding="utf-8") as f:
        xml_content = f.read()

    # Step 2: Analyze and classify processors
    print("🔍 Analyzing workflow and classifying processors...")

    # Get detailed workflow analysis
    workflow_analysis = analyze_nifi_workflow_detailed.func(xml_content)
    print(f"📊 Workflow Analysis: {workflow_analysis}")

    # Get processor classifications
    processor_classifications = classify_processor_types.func(xml_content)
    print(f"🏷️  Processor Classifications: {processor_classifications}")

    # Step 3: Prune infrastructure processors
    print("✂️  Pruning infrastructure-only processors...")
    pruned_result = prune_infrastructure_processors.func(processor_classifications)
    print(f"🎯 Pruned Result: {pruned_result}")

    # Step 4: Detect data flow chains
    print("🔗 Detecting semantic data flow chains...")
    chains_result = detect_data_flow_chains.func(xml_content, pruned_result)
    print(f"⛓️  Chains Result: {chains_result}")

    # Step 5: Create semantic data flows
    print("🌊 Creating semantic data flows...")
    semantic_flows = create_semantic_data_flows.func(chains_result)
    print(f"🎨 Semantic Flows: {semantic_flows}")

    # Step 6: Execute intelligent migration
    print("🧠 Executing intelligent migration with optimized flows...")
    migration_result = orchestrate_intelligent_nifi_migration(
        xml_path=xml_path,
        out_dir=out_dir,
        project=project,
        notebook_path=notebook_path,
        deploy=deploy,
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

    # Perform analysis steps
    workflow_analysis = analyze_nifi_workflow_detailed.func(xml_content)
    processor_classifications = classify_processor_types.func(xml_content)
    pruned_result = prune_infrastructure_processors.func(processor_classifications)
    chains_result = detect_data_flow_chains.func(xml_content, pruned_result)
    semantic_flows = create_semantic_data_flows.func(chains_result)

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

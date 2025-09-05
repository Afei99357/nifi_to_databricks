"""
Simplified NiFi to Databricks migration pipeline without agent complexity.
Direct function call approach for linear migration workflow.
"""

import json
import os
import re
from typing import Any, Dict, Optional

from tools.analysis_tools import (
    analyze_nifi_workflow_detailed,
    classify_processor_types,
)
from tools.improved_classifier import (
    analyze_processors_batch,
    analyze_workflow_patterns,
)
from tools.improved_pruning import (
    create_semantic_data_flows,
    detect_data_flow_chains,
    prune_infrastructure_processors,
)

# Asset discovery removed - focus on business migration guide only
from tools.migration_tools import orchestrate_focused_nifi_migration


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

    # Classifications loaded for pruning - parsing handled in pruning step

    # Step 4: Prune infrastructure processors
    print("✂️  Pruning infrastructure-only processors...")

    pruned_result = prune_infrastructure_processors(processor_classifications)

    # Check pruning results and show essential count
    if isinstance(pruned_result, str):
        pruned_data = json.loads(pruned_result)
    else:
        pruned_data = pruned_result

    if "error" in pruned_data:
        print(f"⚠️ Pruning error: {pruned_data.get('error', 'Unknown')}")
    else:
        essential_count = len(pruned_data.get("pruned_processors", []))
        print(f"✅ Pruning complete: {essential_count} essential processors identified")

    # Step 5: Detect data flow chains
    print("🔗 Detecting semantic data flow chains...")
    chains_result = detect_data_flow_chains(xml_content, pruned_result)

    # Step 6: Create semantic data flows
    print("🌊 Creating semantic data flows...")
    semantic_flows = create_semantic_data_flows(chains_result)

    # Step 7: Extract and catalog all workflow assets for manual review
    print("📋 Extracting workflow assets (scripts, paths, tables) for manual review...")

    # Asset discovery skipped - focusing on business migration guide

    # Generate reports content
    essential_processors_content = _generate_essential_processors_report(pruned_result)
    unknown_processors_content = _generate_unknown_processors_json(analysis_result)
    asset_summary_content = _generate_focused_asset_summary(processor_classifications)
    print("📋 Reports generated successfully")

    # Step 8: Generate comprehensive migration guide (essential processors only)
    print("📋 Generating comprehensive migration guide...")

    # Parse pruned_result to get the list of essential processors

    if isinstance(pruned_result, str):
        pruned_data = json.loads(pruned_result)
    else:
        pruned_data = pruned_result

    essential_processors = pruned_data.get("pruned_processors", [])
    print(f"🎯 Processing {len(essential_processors)} essential processors")

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
        "reports": {
            "essential_processors": essential_processors_content,
            "unknown_processors": unknown_processors_content,
            "asset_summary": asset_summary_content,
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


def _generate_essential_processors_report(pruned_result, output_dir: str = None) -> str:
    """Generate a clean, focused report of essential processors for manual review.

    Args:
        pruned_result: Pruned processor data
        output_dir: Output directory (ignored, kept for compatibility)

    Returns:
        String containing the markdown content
    """

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
            # Use robust type extraction like in migration guide generator
            proc_type = _extract_robust_processor_type(proc)
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

    # Return content instead of writing to file
    return "\n".join(report_lines)


def _extract_robust_processor_type(proc: dict) -> str:
    """Extract processor type with same robust logic as migration guide generator."""
    raw_type = proc.get("type", "")
    full_type = proc.get("full_type", "")

    # Extract processor type with proper fallbacks
    if raw_type and raw_type.strip() and raw_type != "Unknown":
        # Use the short type if it's valid
        proc_type = raw_type.split(".")[-1] if "." in raw_type else raw_type
    elif full_type and full_type.strip() and full_type != "Unknown":
        # Fall back to extracting from full_type
        proc_type = full_type.split(".")[-1] if "." in full_type else full_type
    else:
        # Last resort: use the processor name or Unknown
        proc_name = proc.get("name", "Unknown")
        if proc_name != "Unknown" and proc_name.strip():
            # Extract a reasonable type name from processor name
            # E.g., "Xsite States - Add configuration" -> "UpdateAttribute" (likely)
            if any(
                keyword in proc_name.lower()
                for keyword in ["add configuration", "add queries"]
            ):
                proc_type = "UpdateAttribute"
            elif "split" in proc_name.lower():
                proc_type = "SplitContent"
            elif "move" in proc_name.lower() and "file" in proc_name.lower():
                proc_type = "ExecuteStreamCommand"  # Likely file operation
            else:
                # Use a cleaned version of the name
                clean_name = (
                    proc_name.replace(" - ", "_")
                    .replace(" ", "_")
                    .replace("(", "")
                    .replace(")", "")
                )
                proc_type = f"Custom_{clean_name[:15]}"
        else:
            proc_type = "Unknown"

    # Ensure proc_type is valid (no empty strings)
    if not proc_type or not proc_type.strip():
        proc_type = "Unknown"

    return proc_type


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
        working_dir = properties.get("Working Directory", "")

        if command:
            details.append(f"- **Command**: `{command}`")
        if working_dir:
            details.append(f"- **Working Dir**: `{working_dir}`")
        if args and len(args) < 150:  # Show slightly longer args
            details.append(f"- **Arguments**: `{args}`")
        elif "impala" in args.lower() or "sql" in args.lower():
            details.append("- **Purpose**: Database/SQL operations")
        elif "script" in args.lower():
            details.append("- **Purpose**: Custom script execution")

    elif proc_type == "PutHDFS":
        directory = properties.get("Directory", "")
        conflict_resolution = properties.get("Conflict Resolution Strategy", "")
        if directory:
            details.append(f"- **Output Directory**: `{directory}`")
        if conflict_resolution:
            details.append(f"- **Conflict Strategy**: `{conflict_resolution}`")

    elif proc_type == "PutSFTP":
        hostname = properties.get("Hostname", "")
        remote_path = properties.get("Remote Path", "")
        username = properties.get("Username", "")
        if hostname:
            details.append(f"- **Host**: `{hostname}`")
        if username:
            details.append(f"- **User**: `{username}`")
        if remote_path:
            details.append(f"- **Remote Path**: `{remote_path}`")

    elif proc_type == "UpdateAttribute":
        # Show some dynamic attributes for UpdateAttribute processors
        interesting_props = []
        for key, value in properties.items():
            if (
                key
                not in [
                    "Delete Attributes Expression",
                    "Store State",
                    "canonical-value-lookup-cache-size",
                ]
                and value
            ):
                if len(str(value)) < 100:  # Only short values
                    interesting_props.append(f"- **{key}**: `{value}`")
                if len(interesting_props) >= 2:  # Limit to 2 properties
                    break
        details.extend(interesting_props)

    elif proc_type == "ReplaceText":
        search_value = properties.get("Search Value", "")
        replacement_value = properties.get("Replacement Value", "")
        if search_value:
            details.append(
                f"- **Search**: `{search_value[:50]}{'...' if len(search_value) > 50 else ''}`"
            )
        if replacement_value:
            details.append(
                f"- **Replace**: `{replacement_value[:50]}{'...' if len(replacement_value) > 50 else ''}`"
            )

    elif proc_type.startswith("Custom_") or proc_type == "Unknown":
        # For custom or unknown processors, show most interesting properties
        interesting_props = []
        for key, value in properties.items():
            if value and len(str(value)) < 100:
                interesting_props.append(f"- **{key}**: `{value}`")
                if len(interesting_props) >= 2:
                    break
        details.extend(interesting_props)

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


def _generate_focused_asset_summary(processor_data, output_dir: str = None) -> str:
    """Generate a focused asset summary without the noise of full catalog.

    Args:
        processor_data: Processor data (classifications or pruned result)
        output_dir: Output directory (ignored, kept for compatibility)

    Returns:
        String containing the markdown content
    """

    # Parse processor data (can be classifications or pruned result)
    if isinstance(processor_data, str):
        data = json.loads(processor_data)
    else:
        data = processor_data

    # Handle different input formats
    if "processor_classifications" in data:
        processors = data.get("processor_classifications", [])
    elif "pruned_processors" in data:
        processors = data.get("pruned_processors", [])
    else:
        processors = data.get("processors", [])

    # Extract key assets with high confidence
    script_files = set()
    hdfs_paths = set()
    database_hosts = set()
    external_hosts = set()

    for proc in processors:
        properties = proc.get("properties", {})
        proc_name = proc.get("name", "")

        # Intelligent asset extraction from all properties
        for prop_name, prop_value in properties.items():
            if prop_value and isinstance(prop_value, str):
                prop_lower = prop_value.lower()

                # 1. SCRIPT/EXECUTABLE FILES - intelligent pattern matching
                # Common script/executable extensions
                script_extensions = [
                    ".sh",
                    ".py",
                    ".sql",
                    ".jar",
                    ".pl",
                    ".r",
                    ".rb",
                    ".js",
                    ".scala",
                    ".groovy",
                ]

                # Direct file path detection
                for ext in script_extensions:
                    if prop_value.endswith(ext):
                        script_files.add(prop_value)

                # Path-based script detection (covers command arguments)
                script_pattern = (
                    r'/[^\s;"\']*\.(?:sh|py|sql|jar|pl|r|rb|js|scala|groovy|exe|bat)\b'
                )
                script_matches = re.findall(script_pattern, prop_value, re.IGNORECASE)
                for script_path in script_matches:
                    script_files.add(script_path)

                # 2. FILESYSTEM PATHS - intelligent pattern detection
                # HDFS patterns (more comprehensive)
                hdfs_patterns = [
                    r"hdfs://[^\s]+",  # hdfs:// protocol
                    r"/user/[^\s]+",  # /user/ paths
                    r"/etl/[^\s]+",  # /etl/ paths
                    r"/hdfs/[^\s]+",  # /hdfs/ paths
                    r"/warehouse/[^\s]+",  # warehouse paths
                    r"/tmp/[^\s]+",  # temp paths
                    r"/data/[^\s]+",  # data paths
                ]

                for pattern in hdfs_patterns:
                    hdfs_matches = re.findall(pattern, prop_value)
                    for path in hdfs_matches:
                        cleaned_path = path.strip().rstrip(";")
                        if len(cleaned_path) > 8 and not cleaned_path.startswith("${"):
                            hdfs_paths.add(cleaned_path)

                # 3. WORKING DIRECTORIES - removed (not needed for migration planning)

                # 4. DATABASE CONNECTIONS - intelligent host extraction
                # Look for database-related keywords and extract hostnames
                db_keywords = [
                    "jdbc:",
                    "impala",
                    "hive",
                    "mysql",
                    "postgres",
                    "oracle",
                    "sqlserver",
                    "mongodb",
                    "cassandra",
                ]
                if any(keyword in prop_lower for keyword in db_keywords):
                    # Extract various hostname patterns
                    hostname_patterns = [
                        r"[\w\-\.]+\.[\w\-\.]*\.(?:com|org|net|edu|gov)",  # domain names
                        r"jdbc:[^/]+//([^:/]+)",  # JDBC URLs
                        r"://([^:/]+)",  # protocol://host
                    ]
                    for pattern in hostname_patterns:
                        host_matches = re.findall(pattern, prop_value, re.IGNORECASE)
                        for host in host_matches:
                            if len(host) > 3 and "." in host:
                                database_hosts.add(host)

                # 5. EXTERNAL HOSTS - intelligent detection
                # Look for external service indicators
                external_indicators = [
                    "ftp",
                    "sftp",
                    "ssh",
                    "http",
                    "https",
                    "smtp",
                    "ldap",
                ]
                host_related_props = [
                    "hostname",
                    "host",
                    "server",
                    "url",
                    "endpoint",
                    "address",
                ]

                # Method 1: Property name suggests external host
                if any(keyword in prop_name.lower() for keyword in host_related_props):
                    if (
                        "." in prop_value
                        and not prop_value.startswith("/")
                        and len(prop_value) < 200
                        and len(prop_value) > 3
                    ):
                        external_hosts.add(prop_value.strip())

                # Method 2: Content suggests external service
                if any(indicator in prop_lower for indicator in external_indicators):
                    # Extract hostnames from URLs or connection strings
                    url_pattern = r"(?:https?|ftp|sftp)://([^/:\s]+)"
                    url_hosts = re.findall(url_pattern, prop_value, re.IGNORECASE)
                    for host in url_hosts:
                        external_hosts.add(host)

    # Generate summary report
    report_lines = [
        "# NiFi Asset Summary",
        "",
        "## Overview",
        f"- **Total Processors Analyzed**: {len(processors)}",
        f"- **Script Files Found**: {len(script_files)}",
        f"- **HDFS Paths Found**: {len(hdfs_paths)}",
        f"- **Database Hosts Found**: {len(database_hosts)}",
        f"- **External Hosts Found**: {len(external_hosts)}",
        "",
    ]

    if script_files:
        report_lines.extend(["## Script Files Requiring Migration", ""])
        for script in sorted(script_files):
            report_lines.append(f"- `{script}`")
        report_lines.append("")

    if database_hosts:
        report_lines.extend(["## Database Connections", ""])
        for host in sorted(database_hosts):
            report_lines.append(f"- **Impala**: `{host}`")
        report_lines.append("")

    if hdfs_paths:
        report_lines.extend(["## HDFS Paths for Unity Catalog Migration", ""])
        for path in sorted(hdfs_paths):
            report_lines.append(f"- `{path}`")
        report_lines.append("")

    if external_hosts:
        report_lines.extend(["## External Host Dependencies", ""])
        for host in sorted(external_hosts):
            report_lines.append(f"- **SFTP**: `{host}`")
        report_lines.append("")

    # Migration recommendations
    report_lines.extend(
        [
            "## Migration Recommendations",
            "",
            "### High Priority",
        ]
    )

    if script_files:
        report_lines.append("- **Convert shell scripts** to PySpark/Databricks SQL")
    if database_hosts:
        report_lines.append(
            "- **Replace Impala connections** with Databricks SQL compute"
        )
    if hdfs_paths:
        report_lines.append("- **Migrate HDFS data** to Unity Catalog managed tables")

    if external_hosts:
        report_lines.extend(
            [
                "",
                "### Medium Priority",
                "- **Replace SFTP transfers** with cloud storage integration",
            ]
        )

    # Return content instead of writing to file
    return "\n".join(report_lines)


def _generate_unknown_processors_json(analysis_result, output_dir: str = None) -> dict:
    """Generate JSON with unknown processors for manual review.

    Args:
        analysis_result: Analysis result data
        output_dir: Output directory (ignored, kept for compatibility)

    Returns:
        Dictionary containing unknown processors data
    """
    # Find unknown processors
    unknown = []
    if hasattr(analysis_result, "get"):
        for proc in analysis_result.get("classification_results", []):
            if proc.get("data_manipulation_type") == "unknown":
                unknown.append(
                    {
                        "name": proc.get("name"),
                        "type": proc.get("processor_type"),
                        "reason": "LLM failed - needs manual classification",
                    }
                )

    # Return data object instead of writing to file
    return {"unknown_processors": unknown, "count": len(unknown)}

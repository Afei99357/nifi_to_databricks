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
from tools.networkx_complete_flow_analysis import generate_connection_analysis_reports

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
    connection_analysis = generate_connection_analysis_reports(
        xml_content, pruned_result
    )
    _log(
        f"🎯 Connection analysis complete: {connection_analysis['connection_summary']['complexity_reduction']} complexity reduction"
    )

    # Step 7: Extract and catalog all workflow assets for manual review
    _log("📋 Extracting workflow assets (scripts, paths, tables) for manual review...")

    # Asset discovery skipped - focusing on business migration guide

    # Generate reports content
    essential_processors_content = _generate_essential_processors_report(pruned_result)
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
            "essential_processors": essential_processors_content,
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
    connection_analysis = generate_connection_analysis_reports(
        xml_content, pruned_result
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


def _find_placeholder_dependencies(
    essential_processors: list, all_processors: list
) -> list:
    """Find processors that define variables used as placeholders in essential processors."""
    import re

    # Extract all ${variable} placeholders from essential processors
    placeholders = set()
    for proc in essential_processors:
        properties = proc.get("properties", {})
        for prop_value in properties.values():
            if isinstance(prop_value, str):
                # Find all ${variable} patterns
                matches = re.findall(r"\$\{([^}]+)\}", prop_value)
                placeholders.update(matches)

    if not placeholders:
        return []

    # Find processors that define these variables
    dependency_processors = []
    for proc in all_processors:
        properties = proc.get("properties", {})
        proc_name = proc.get("name", "")

        # Check if this processor defines any of the needed placeholders
        defines_placeholder = False
        for prop_name, prop_value in properties.items():
            # Check if property name matches a placeholder
            if prop_name in placeholders:
                defines_placeholder = True
                break

        # Also check for SQL-defining UpdateAttribute processors
        proc_type = proc.get("processor_type", "")
        if defines_placeholder or (
            "UpdateAttribute" in proc_type
            and any(
                keyword in proc_name
                for keyword in [
                    "Add Queries",
                    "Add Configuration",
                    "Add Variables",
                    "States",
                    "Steps",
                ]
            )
        ):
            dependency_processors.append(
                {
                    **proc,
                    "dependency_reason": (
                        f"Defines variables: {', '.join(prop_name for prop_name in properties.keys() if prop_name in placeholders)}"
                        if defines_placeholder
                        else "Contains SQL queries/configuration"
                    ),
                }
            )

    return dependency_processors


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
    removed_processors = pruned_data.get("removed_processors", [])

    # Find dependency processors that define variables used by essential processors
    all_processors = processors + removed_processors
    dependency_processors = _find_placeholder_dependencies(processors, all_processors)

    # Group by classification for organization
    by_classification = {}
    for proc in processors:
        classification = proc.get(
            "classification", proc.get("data_manipulation_type", "unknown")
        )
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

    if dependency_processors:
        report_lines.append(
            f"- **Essential Dependencies**: {len(dependency_processors)} processors (define variables used by essential processors)"
        )

    # Add classification breakdown
    for classification, procs in by_classification.items():
        count = len(procs)
        class_name = classification.replace("_", " ").title()
        report_lines.append(
            f"- **{class_name}**: {count} processor{'s' if count != 1 else ''}"
        )

    report_lines.extend(["", "---", ""])

    # Add Essential Dependencies section (categorized)
    if dependency_processors:
        # Categorize dependency processors
        categories = {
            "sql_queries": [],
            "configuration": [],
            "data_extraction": [],
            "dynamic_values": [],
            "routing_logic": [],
        }

        for proc in dependency_processors:
            proc_name = proc.get("name", "").lower()
            proc_type = proc.get("processor_type", "")
            properties = proc.get("properties", {})

            # Categorize based on processor name and properties
            if "add queries" in proc_name:
                categories["sql_queries"].append(proc)
            elif "add configuration" in proc_name or "add config" in proc_name:
                categories["configuration"].append(proc)
            elif (
                "extracttext" in proc_type.lower() or "filename breakdown" in proc_name
            ):
                categories["data_extraction"].append(proc)
            elif "generateflowfile" in proc_type.lower() or any(
                keyword in proc_name
                for keyword in ["time", "timestamp", "current", "mem_limit"]
            ):
                categories["dynamic_values"].append(proc)
            elif (
                "routeonattribute" in proc_type.lower() or "split loaders" in proc_name
            ):
                categories["routing_logic"].append(proc)
            else:
                # Default to configuration if it defines multiple variables
                if (
                    len([k for k in properties.keys() if not k.startswith("Delete")])
                    > 3
                ):
                    categories["configuration"].append(proc)
                else:
                    categories["data_extraction"].append(proc)

        report_lines.extend(
            [
                "## Essential Dependencies",
                "",
                "*These processors define variables/queries used by essential processors above*",
                "",
            ]
        )

        # SQL Query Providers section
        if categories["sql_queries"]:
            report_lines.extend(
                [
                    "### SQL Query Providers",
                    "*Define business logic SQL queries used by ExecuteStreamCommand processors*",
                    "",
                ]
            )
            for i, proc in enumerate(
                categories["sql_queries"][:10], 1
            ):  # Limit to top 10
                name = proc.get("name", "Unknown")
                proc_type = proc.get("processor_type", "Unknown").split(".")[-1]

                # Count SQL queries this processor defines
                properties = proc.get("properties", {})
                query_props = [
                    k
                    for k in properties.keys()
                    if "query_" in k
                    and any(
                        keyword in str(properties[k]).upper()
                        for keyword in ["SELECT", "INSERT", "ALTER", "REFRESH"]
                    )
                ]

                report_lines.append(f'{i}. **{proc_type}** - "{name}"')
                if query_props:
                    report_lines.append(
                        f"   - Defines {len(query_props)} SQL queries: {', '.join(query_props[:3])}{'...' if len(query_props) > 3 else ''}"
                    )
                report_lines.append("")

        # Configuration Providers section
        if categories["configuration"]:
            report_lines.extend(
                [
                    "### Configuration Providers",
                    "*Set infrastructure variables (tables, paths, memory) used by processing*",
                    "",
                ]
            )
            for i, proc in enumerate(
                categories["configuration"][:10], 1
            ):  # Limit to top 10
                name = proc.get("name", "Unknown")
                proc_type = proc.get("processor_type", "Unknown").split(".")[-1]
                reason = proc.get("dependency_reason", "")

                report_lines.append(f'{i}. **{proc_type}** - "{name}"')
                if "Defines variables:" in reason:
                    vars_part = reason.split("Defines variables: ")[1]
                    var_list = vars_part.split(", ")
                    if len(var_list) > 5:
                        report_lines.append(
                            f"   - Sets {len(var_list)} variables: {', '.join(var_list[:5])}..."
                        )
                    else:
                        report_lines.append(
                            f"   - Sets variables: {', '.join(var_list)}"
                        )
                report_lines.append("")

        # Other categories (show only if they exist and are significant)
        other_categories = [
            (
                "data_extraction",
                "Data Extraction Processors",
                "Extract values from file content/names for downstream processing",
            ),
            (
                "dynamic_values",
                "Dynamic Value Generators",
                "Generate timestamps, calculations, and runtime values",
            ),
            (
                "routing_logic",
                "Routing Logic Processors",
                "Set conditional variables based on data content",
            ),
        ]

        for cat_key, cat_title, cat_desc in other_categories:
            if categories[cat_key]:
                report_lines.extend([f"### {cat_title}", f"*{cat_desc}*", ""])
                for i, proc in enumerate(categories[cat_key][:5], 1):  # Limit to top 5
                    name = proc.get("name", "Unknown")
                    proc_type = proc.get("processor_type", "Unknown").split(".")[-1]
                    report_lines.append(f'{i}. **{proc_type}** - "{name}"')
                report_lines.append("")

        report_lines.extend(["---", ""])

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

        # Add visual separator between sections
        report_lines.extend(["", "---", "", f"## {class_name} Processors", ""])

        processor_index = 1  # Reset index for each section
        for proc in processors_list:
            name = proc.get("name", "Unknown")
            # Use robust type extraction like in migration guide generator
            proc_type = _extract_robust_processor_type(proc)
            properties = proc.get("properties", {})

            # Add LLM classification indicator
            analysis_method = proc.get("analysis_method", "")
            llm_indicator = (
                " [LLM: Yes]" if analysis_method == "llm_batch" else " [LLM: No]"
            )

            report_lines.append(
                f'### {processor_index}. {proc_type} - "{name}"{llm_indicator}'
            )
            processor_index += 1

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
    raw_type = proc.get("processor_type", "") or proc.get("type", "")
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
        command_path = properties.get("Command Path", "")
        args = properties.get("Command Arguments", "")
        working_dir = properties.get("Working Directory", "")

        # Combine command path and arguments to show full command line
        # Convert semicolon-separated arguments to space-separated for proper command line
        full_command = command_path
        if args:
            # Replace semicolons with spaces for proper command line format
            formatted_args = args.replace(";", " ")
            full_command = (
                f"{command_path} {formatted_args}" if command_path else formatted_args
            )

        if command_path:
            details.append(f"- **Command Path**: `{command_path}`")
        if args and len(args) < 150:  # Show arguments separately for clarity
            details.append(f"- **Arguments**: `{args}`")
        if full_command and len(full_command) < 200:  # Show combined command line
            details.append(f"- **Full Command**: `{full_command}`")
        if working_dir:
            details.append(f"- **Working Dir**: `{working_dir}`")

        # Add purpose detection based on command/args content
        command_lower = (command_path + " " + args).lower()
        if "impala" in command_lower or "sql" in command_lower:
            details.append("- **Purpose**: Database/SQL operations")
        elif "script" in command_lower or command_path.endswith((".sh", ".py", ".pl")):
            details.append("- **Purpose**: Custom script execution")
        elif any(cmd in command_lower for cmd in ["mv", "cp", "move", "copy"]):
            details.append("- **Purpose**: File operations")

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

"""
Enhanced reporting for NiFi to Databricks migration.
Generates detailed reports of essential processors and their dependencies.
"""

import json
import re
from typing import Any, Dict, List


def _find_placeholder_dependencies(
    essential_processors: list, all_processors: list
) -> list:
    """Find processors that define variables used as placeholders in essential processors."""

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
            elif any(
                keyword in proc_name.lower()
                for keyword in ["split", "flowfile per", "per type", "separate"]
            ):
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
        if command_path or args:
            combined = f"{command_path} {args}".lower()
            if "impala-shell" in combined or "hive" in combined:
                details.append("- **Purpose**: Database/SQL operations")
            elif any(
                cmd in combined for cmd in ["/bin/mv", "/bin/cp", "/bin/hdfs", "dfs"]
            ):
                details.append("- **Purpose**: File operations")
            elif ".sh" in combined or "scripts/" in combined:
                details.append("- **Purpose**: Custom script execution")

    elif proc_type == "PutHDFS":
        output_dir = properties.get("Directory", "") or properties.get(
            "Output Directory", ""
        )
        conflict = properties.get("Conflict Resolution Strategy", "")
        if output_dir:
            details.append(f"- **Output Directory**: `{output_dir}`")
        if conflict:
            details.append(f"- **Conflict Strategy**: {conflict}")

    elif proc_type == "FetchFile":
        file_to_fetch = properties.get("File to Fetch", "")
        completion = properties.get("Completion Strategy", "")
        if file_to_fetch:
            details.append(f"- **File to Fetch**: `{file_to_fetch}`")
        if completion:
            details.append(f"- **Completion Strategy**: {completion}")

    elif proc_type == "SplitContent":
        byte_format = properties.get("Byte Sequence Format", "")
        byte_seq = properties.get("Byte Sequence", "")
        if byte_format:
            details.append(f"- **Byte Sequence Format**: {byte_format}")
        if byte_seq:
            details.append(f"- **Byte Sequence**: {byte_seq}")

    return details


def generate_essential_processors_report(
    pruned_result, output_dir: str = None
) -> tuple[str, str]:
    """Generate a clean, focused report of essential processors for manual review.

    Args:
        pruned_result: Pruned processor data
        output_dir: Output directory (ignored, kept for compatibility)

    Returns:
        Tuple of (main_report, dependencies_report) as separate markdown strings
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

    # Generate the main report (essential processors only)
    main_report_lines = [
        "# Essential Processors - Manual Review",
        "",
        "## Summary",
        f"- **Total Essential Processors**: {len(processors)} (after infrastructure pruning)",
    ]

    # Add classification breakdown
    for classification, procs in by_classification.items():
        count = len(procs)
        class_name = classification.replace("_", " ").title()
        main_report_lines.append(
            f"- **{class_name}**: {count} processor{'s' if count != 1 else ''}"
        )

    # Generate the dependencies report separately
    dependencies_report_lines = []

    if dependency_processors:
        dependencies_report_lines.extend(
            [
                "# Essential Dependencies",
                "",
                f"**{len(dependency_processors)} processors** define variables/queries used by essential processors",
                "",
            ]
        )
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

        # SQL Query Providers section
        if categories["sql_queries"]:
            dependencies_report_lines.extend(
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

                dependencies_report_lines.append(f'{i}. **{proc_type}** - "{name}"')
                if query_props:
                    dependencies_report_lines.append(
                        f"    - Defines {len(query_props)} SQL queries: {', '.join(query_props)}"
                    )
                dependencies_report_lines.append("")

        # Configuration Providers section
        if categories["configuration"]:
            dependencies_report_lines.extend(
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

                dependencies_report_lines.append(f'{i}. **{proc_type}** - "{name}"')
                if "Defines variables:" in reason:
                    vars_part = reason.split("Defines variables: ")[1]
                    var_list = vars_part.split(", ")
                    dependencies_report_lines.append(
                        f"    - Sets variables: {', '.join(var_list)}"
                    )
                dependencies_report_lines.append("")

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
                dependencies_report_lines.extend(
                    [f"### {cat_title}", f"*{cat_desc}*", ""]
                )
                for i, proc in enumerate(categories[cat_key][:5], 1):  # Limit to top 5
                    name = proc.get("name", "Unknown")
                    proc_type = proc.get("processor_type", "Unknown").split(".")[-1]
                    dependencies_report_lines.append(f'{i}. **{proc_type}** - "{name}"')
                dependencies_report_lines.append("")

    # Add main processor details to main report
    classification_order = [
        "data_movement",
        "data_transformation",
        "external_processing",
        "unknown",
    ]

    processors_lists = []
    for classification in classification_order:
        if classification in by_classification:
            processors_list = by_classification[classification]
            class_name = classification.replace("_", " ").title()
            processors_lists.append((f"{class_name} Processors", processors_list))

    processor_index = 1
    for section_name, processors_list in processors_lists:
        if not processors_list:
            continue

        main_report_lines.extend([f"## {section_name}", ""])

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

            main_report_lines.append(
                f'### {processor_index}. {proc_type} - "{name}"{llm_indicator}'
            )
            processor_index += 1

            # Extract key details based on processor type
            key_details = _extract_processor_key_details(proc_type, name, properties)
            if key_details:
                main_report_lines.extend(key_details)

            main_report_lines.append("")

    # Return both reports as separate strings
    main_report = "\n".join(main_report_lines)
    dependencies_report = "\n".join(dependencies_report_lines)

    return main_report, dependencies_report

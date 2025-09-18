"""
External Script Extraction for NiFi workflows.
Extracts script files, executable paths, and external dependencies that need migration planning.
Based on asset_extraction.py but focused specifically on scripts and executables.
"""

import json
import re
from typing import Any, Dict, List, Set

# Script and executable patterns
SCRIPT_EXTENSIONS = [
    ".sh",
    ".py",
    ".sql",
    ".jar",
    ".class",
    ".scala",
    ".R",
    ".pl",
    ".rb",
]
SCRIPT_PATH_PATTERN = re.compile(r"/[\w/.-]+\.(?:sh|py|sql|jar|class|scala|R|pl|rb)")
EXECUTABLE_COMMANDS = re.compile(
    r"\b(?:bash|sh|python|java|scala|spark-submit|hdfs|impala-shell|beeline|hive)\b"
)
HOST_PATTERN = re.compile(r"\b(?:[a-zA-Z0-9-]+\.){1,}[a-zA-Z]{2,}\b")


def _is_script_false_positive(value: str) -> bool:
    """Filter out non-script patterns and obvious false positives."""
    if not value or not isinstance(value, str):
        return True

    value = value.strip()

    # Skip empty or very short values
    if len(value) < 3:
        return True

    # Skip regex patterns
    if value.startswith("^") or value.endswith("$") or ".*" in value or "\\." in value:
        return True

    # Skip variable references without actual paths
    if "${" in value and "}" in value and not value.startswith("/"):
        return True

    # Skip wildcards and glob patterns
    if "*" in value or "?" in value or "[" in value:
        return True

    # Skip multiple items in one value
    if ";" in value and "/" not in value:  # Allow shell commands with semicolons
        return True

    # Skip error messages or descriptive text
    error_indicators = [
        "failed",
        "error",
        "exception",
        "unable to",
        "cannot",
        "not found",
    ]
    if any(indicator in value.lower() for indicator in error_indicators):
        return True

    # Skip configuration-like values
    config_indicators = ["true", "false", "null", "none", "auto", "default"]
    if value.lower() in config_indicators:
        return True

    return False


def _extract_scripts_from_command(command_text: str) -> Set[str]:
    """Extract script paths from command line text."""
    scripts = set()

    if not command_text or _is_script_false_positive(command_text):
        return scripts

    # Look for script file paths in command arguments
    script_matches = SCRIPT_PATH_PATTERN.findall(command_text)
    for script_path in script_matches:
        if not _is_script_false_positive(script_path):
            scripts.add(script_path.strip())

    # Look for relative script names that might be in working directory
    lines = command_text.split("\n")
    for line in lines:
        line = line.strip()
        if not line or line.startswith("#"):  # Skip comments
            continue

        # Split by spaces to find script arguments
        tokens = line.split()
        for token in tokens:
            token = token.strip("\"'`;")  # Remove quotes and separators
            if any(token.endswith(ext) for ext in SCRIPT_EXTENSIONS):
                if not _is_script_false_positive(token):
                    scripts.add(token)

    return scripts


def _classify_script_type(script_path: str) -> str:
    """Classify script by file extension and naming patterns."""
    script_lower = script_path.lower()

    if script_lower.endswith(".sh"):
        return "shell_script"
    elif script_lower.endswith(".py"):
        return "python_script"
    elif script_lower.endswith(".sql"):
        return "sql_script"
    elif script_lower.endswith(".jar"):
        return "java_application"
    elif script_lower.endswith(".class"):
        return "java_class"
    elif script_lower.endswith(".scala"):
        return "scala_script"
    elif script_lower.endswith(".R"):
        return "r_script"
    elif script_lower.endswith(".pl"):
        return "perl_script"
    elif script_lower.endswith(".rb"):
        return "ruby_script"
    else:
        return "unknown_script"


def _extract_external_hosts_from_scripts(script_content: str) -> Set[str]:
    """Extract external host dependencies from script content."""
    hosts = set()

    if not script_content or _is_script_false_positive(script_content):
        return hosts

    # Extract URLs and hosts
    try:
        # Look for URLs
        url_pattern = re.compile(r"https?://([^/\s]+)")
        url_matches = url_pattern.findall(script_content)
        for host in url_matches:
            hosts.add(host.lower())

        # Look for host patterns (domains)
        host_matches = HOST_PATTERN.findall(script_content)
        for host in host_matches:
            # Filter out obvious non-hosts
            if not any(
                skip in host.lower()
                for skip in ["example.com", "localhost", "test.com"]
            ):
                hosts.add(host.lower())

    except Exception:
        pass  # Continue if regex fails

    return hosts


def extract_scripts_from_processor(processor: Dict[str, Any]) -> Dict[str, Any]:
    """Extract script information from a single processor."""
    processor_id = processor.get("id", "")
    processor_name = processor.get("name", "")
    processor_type = processor.get("type", "")
    properties = processor.get("properties", {})

    result = {
        "processor_id": processor_id,
        "processor_name": processor_name,
        "processor_type": (
            processor_type.split(".")[-1] if processor_type else "Unknown"
        ),
        "scripts": [],
        "executables": [],
        "external_hosts": [],
        "script_count": 0,
        "has_external_dependencies": False,
    }

    script_files = set()
    executable_commands = set()
    external_hosts = set()

    # Process all properties
    for prop_name, prop_value in properties.items():
        if not prop_value or not isinstance(prop_value, str):
            continue

        prop_value = prop_value.strip()
        prop_name_lower = prop_name.lower()

        if _is_script_false_positive(prop_value):
            continue

        # 1. Direct script file paths
        if (
            any(prop_value.endswith(ext) for ext in SCRIPT_EXTENSIONS)
            and "/" in prop_value
        ):
            script_files.add(prop_value)

        # 2. Command-related properties
        command_indicators = [
            "command",
            "script",
            "executable",
            "arguments",
            "args",
            "cmd",
        ]
        if any(indicator in prop_name_lower for indicator in command_indicators):
            # Extract scripts from command text
            command_scripts = _extract_scripts_from_command(prop_value)
            script_files.update(command_scripts)

            # Check for executable commands
            if EXECUTABLE_COMMANDS.search(prop_value):
                executable_commands.add(prop_value[:100])  # Truncate long commands

        # 3. Script paths using regex
        script_matches = SCRIPT_PATH_PATTERN.findall(prop_value)
        for script_match in script_matches:
            if not _is_script_false_positive(script_match):
                script_files.add(script_match)

        # 4. External host dependencies
        script_hosts = _extract_external_hosts_from_scripts(prop_value)
        external_hosts.update(script_hosts)

    # Build result structure
    for script_file in script_files:
        script_info = {
            "path": script_file,
            "type": _classify_script_type(script_file),
            "property_source": "detected_from_properties",
            "migration_priority": "high" if script_file.startswith("/") else "medium",
        }
        result["scripts"].append(script_info)

    for executable in executable_commands:
        exec_info = {
            "command": executable,
            "type": "executable_command",
            "migration_priority": "high",
        }
        result["executables"].append(exec_info)

    result["external_hosts"] = list(external_hosts)
    result["script_count"] = len(script_files) + len(executable_commands)
    result["has_external_dependencies"] = len(external_hosts) > 0

    return result


def extract_all_scripts_from_nifi_xml(xml_path: str) -> List[Dict[str, Any]]:
    """
    Extract all script references from NiFi XML file.

    Args:
        xml_path: Path to NiFi XML file

    Returns:
        List of script extraction results per processor
    """
    from tools.xml_tools import parse_nifi_template_impl

    # Parse processors from XML
    with open(xml_path, "r", encoding="utf-8") as f:
        xml_content = f.read()

    template_data = parse_nifi_template_impl(xml_content)
    processors = template_data.get("processors", [])

    all_script_results = []

    for processor in processors:
        script_result = extract_scripts_from_processor(processor)

        # Only include processors that have scripts or external dependencies
        if (
            script_result["script_count"] > 0
            or script_result["has_external_dependencies"]
        ):
            all_script_results.append(script_result)

    return all_script_results


def generate_script_migration_summary(script_results: List[Dict[str, Any]]) -> str:
    """Generate a migration-focused summary of script dependencies."""

    total_processors = len(script_results)
    total_scripts = sum(result["script_count"] for result in script_results)

    # Categorize scripts by type
    script_types = {}
    high_priority_scripts = []
    external_dependencies = set()

    for result in script_results:
        for script in result["scripts"]:
            script_type = script["type"]
            script_types[script_type] = script_types.get(script_type, 0) + 1

            if script["migration_priority"] == "high":
                high_priority_scripts.append(
                    {
                        "path": script["path"],
                        "processor": result["processor_name"],
                        "type": script_type,
                    }
                )

        external_dependencies.update(result["external_hosts"])

    # Generate markdown report
    lines = [
        "# NiFi Script Migration Analysis",
        "",
        "## Summary",
        f"- **Processors with Scripts**: {total_processors}",
        f"- **Total Scripts Found**: {total_scripts}",
        f"- **External Dependencies**: {len(external_dependencies)}",
        "",
    ]

    if script_types:
        lines.extend(["## Scripts by Type", ""])
        for script_type, count in sorted(script_types.items()):
            lines.append(f"- **{script_type.replace('_', ' ').title()}**: {count}")
        lines.append("")

    if high_priority_scripts:
        lines.extend(
            [
                "## High Priority Scripts (Absolute Paths)",
                "",
                "These scripts require immediate attention for migration:",
                "",
            ]
        )
        for script in high_priority_scripts[:20]:  # Limit to first 20
            lines.append(
                f"- `{script['path']}` ({script['type']}) - Used in: {script['processor']}"
            )

        if len(high_priority_scripts) > 20:
            lines.append(f"- ... and {len(high_priority_scripts) - 20} more")
        lines.append("")

    if external_dependencies:
        lines.extend(
            [
                "## External Host Dependencies",
                "",
                "External systems that scripts depend on:",
                "",
            ]
        )
        for host in sorted(external_dependencies)[:15]:  # Limit to first 15
            lines.append(f"- `{host}`")

        if len(external_dependencies) > 15:
            lines.append(f"- ... and {len(external_dependencies) - 15} more")
        lines.append("")

    lines.extend(
        [
            "## Migration Recommendations",
            "",
            "1. **High Priority Scripts**: Migrate absolute path scripts first",
            "2. **External Dependencies**: Ensure external hosts are accessible from Databricks",
            "3. **Script Types**: Consider converting shell scripts to Databricks notebooks",
            "4. **JAR Files**: Upload to Databricks libraries or DBFS",
            "5. **SQL Scripts**: Convert to Databricks SQL notebooks or jobs",
            "",
        ]
    )

    return "\n".join(lines)


def extract_scripts_summary_json(xml_path: str) -> str:
    """
    Extract scripts and return JSON summary suitable for API responses.

    Args:
        xml_path: Path to NiFi XML file

    Returns:
        JSON string with script extraction summary
    """
    try:
        script_results = extract_all_scripts_from_nifi_xml(xml_path)

        summary = {
            "total_processors_with_scripts": len(script_results),
            "total_scripts": sum(result["script_count"] for result in script_results),
            "script_types": {},
            "high_priority_scripts": [],
            "external_dependencies": [],
            "processors": script_results,
        }

        # Aggregate data
        external_deps = set()
        for result in script_results:
            for script in result["scripts"]:
                script_type = script["type"]
                summary["script_types"][script_type] = (
                    summary["script_types"].get(script_type, 0) + 1
                )

                if script["migration_priority"] == "high":
                    summary["high_priority_scripts"].append(
                        {
                            "path": script["path"],
                            "processor_name": result["processor_name"],
                            "processor_id": result["processor_id"],
                            "type": script_type,
                        }
                    )

            external_deps.update(result["external_hosts"])

        summary["external_dependencies"] = list(external_deps)

        return json.dumps(summary, indent=2)

    except Exception as e:
        return json.dumps(
            {
                "error": f"Failed to extract scripts: {str(e)}",
                "total_processors_with_scripts": 0,
                "total_scripts": 0,
            }
        )

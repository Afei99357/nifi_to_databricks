"""
NiFi Asset Discovery Tools - Extract and catalog all external file/script/table references
for manual review during migration planning.
"""

import json
import re
from pathlib import Path
from typing import Any, Dict, List, Set
from urllib.parse import urlparse


def extract_processor_assets(processor: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract all external assets (files, scripts, tables, paths) from a single processor.

    Args:
        processor: Processor data with properties

    Returns:
        Dict with categorized assets found in this processor
    """
    processor_name = processor.get("name", "Unknown")
    processor_type = processor.get("processor_type", processor.get("type", "Unknown"))
    properties = processor.get("properties", {})

    # Initialize asset categories
    assets = {
        "processor_name": processor_name,
        "processor_type": processor_type,
        "script_files": [],
        "working_directories": [],
        "hdfs_paths": [],
        "file_paths": [],
        "table_references": [],
        "sql_statements": [],
        "host_references": [],
        "database_connections": [],
        "raw_properties": properties,
    }

    # Extract from all property values
    for prop_key, prop_value in properties.items():
        if not prop_value:
            continue

        prop_str = str(prop_value)

        # 1. Script Files and Commands
        if any(
            keyword in prop_key.lower()
            for keyword in ["command", "script", "path", "executable"]
        ):
            script_paths = _extract_script_paths(prop_str)
            assets["script_files"].extend(script_paths)

        # 2. Working Directories
        if "working" in prop_key.lower() and "directory" in prop_key.lower():
            if prop_str.startswith("/"):
                assets["working_directories"].append(
                    {"property": prop_key, "path": prop_str}
                )

        # 3. HDFS Paths
        hdfs_paths = _extract_hdfs_paths(prop_str)
        for hdfs_path in hdfs_paths:
            assets["hdfs_paths"].append(
                {
                    "property": prop_key,
                    "path": hdfs_path,
                    "type": _classify_hdfs_path(hdfs_path),
                }
            )

        # 4. File System Paths
        file_paths = _extract_file_paths(prop_str)
        for file_path in file_paths:
            assets["file_paths"].append({"property": prop_key, "path": file_path})

        # 5. Table References
        table_refs = _extract_table_references(prop_str)
        for table_ref in table_refs:
            assets["table_references"].append(
                {
                    "property": prop_key,
                    "table": table_ref,
                    "type": _classify_table_reference(table_ref),
                }
            )

        # 6. SQL Statements
        sql_statements = _extract_sql_statements(prop_str)
        for sql_stmt in sql_statements:
            assets["sql_statements"].append(
                {
                    "property": prop_key,
                    "statement": sql_stmt,
                    "type": _classify_sql_statement(sql_stmt),
                }
            )

        # 7. Host/Server References
        hosts = _extract_host_references(prop_str)
        for host in hosts:
            assets["host_references"].append({"property": prop_key, "host": host})

        # 8. Database Connection Strings
        db_connections = _extract_database_connections(prop_str)
        for db_conn in db_connections:
            assets["database_connections"].append(
                {"property": prop_key, "connection": db_conn}
            )

    # Remove empty categories and deduplicate
    cleaned_assets = {}
    for category, items in assets.items():
        if category in ["processor_name", "processor_type", "raw_properties"]:
            cleaned_assets[category] = items
        elif isinstance(items, list) and items:
            # Deduplicate while preserving order and structure
            seen = set()
            unique_items = []
            for item in items:
                # Create a hashable identifier
                if isinstance(item, dict):
                    identifier = f"{item.get('property', '')}-{item.get('path', '')}{item.get('table', '')}{item.get('statement', '')}{item.get('host', '')}"
                else:
                    identifier = str(item)

                if identifier not in seen:
                    seen.add(identifier)
                    unique_items.append(item)

            cleaned_assets[category] = unique_items

    return cleaned_assets


def _extract_script_paths(text: str) -> List[str]:
    """Extract script file paths from text."""
    paths = []

    # Pattern for Unix-style paths ending in common script extensions
    script_patterns = [
        r'/[^\s,;"\'<>]+\.(?:sh|py|pl|rb|js|sql|jar)',  # /path/to/script.sh
        r'/users/[^\s,;"\'<>]+',  # /users/... paths
        r'/opt/[^\s,;"\'<>]+',  # /opt/... paths
        r'/usr/local/[^\s,;"\'<>]+',  # /usr/local/... paths
    ]

    for pattern in script_patterns:
        matches = re.findall(pattern, text, re.IGNORECASE)
        paths.extend(matches)

    return list(set(paths))  # Remove duplicates


def _extract_hdfs_paths(text: str) -> List[str]:
    """Extract HDFS paths from text."""
    paths = []

    # HDFS URI patterns
    hdfs_patterns = [
        r'hdfs://[^\s,;"\'<>]+',  # hdfs://nameservice/path
        r'/user/[^\s,;"\'<>]+',  # /user/hive/warehouse/...
        r'/tmp/[^\s,;"\'<>]+',  # /tmp/...
        r'/data/[^\s,;"\'<>]+',  # /data/...
        r'/warehouse/[^\s,;"\'<>]+',  # /warehouse/...
    ]

    for pattern in hdfs_patterns:
        matches = re.findall(pattern, text)
        paths.extend(matches)

    return list(set(paths))


def _extract_file_paths(text: str) -> List[str]:
    """Extract general file system paths."""
    paths = []

    # General file path patterns (but not URLs)
    file_patterns = [
        r'/[a-zA-Z][^\s,;"\'<>]*',  # Unix absolute paths starting with /[letter]
    ]

    for pattern in file_patterns:
        matches = re.findall(pattern, text)
        # Filter out URLs and known HDFS paths
        for match in matches:
            if not match.startswith(("http", "hdfs", "ftp", "jdbc")):
                paths.append(match)

    return list(set(paths))


def _extract_table_references(text: str) -> List[str]:
    """Extract database table references."""
    tables = []

    # Table reference patterns
    table_patterns = [
        r"\b[a-z_]+\.[a-z_]+\.[a-z_]+\b",  # catalog.schema.table
        r"\b[a-z_]+\.[a-z_]+\b",  # schema.table
        r"FROM\s+([a-z_][a-z0-9_]*)",  # FROM table_name
        r"INTO\s+([a-z_][a-z0-9_]*)",  # INTO table_name
        r"UPDATE\s+([a-z_][a-z0-9_]*)",  # UPDATE table_name
    ]

    for pattern in table_patterns:
        matches = re.findall(pattern, text, re.IGNORECASE)
        tables.extend(matches)

    return list(set(tables))


def _extract_sql_statements(text: str) -> List[str]:
    """Extract SQL statements from text."""
    statements = []

    # Look for SQL keywords at start of statements
    sql_keywords = [
        "SELECT",
        "INSERT",
        "UPDATE",
        "DELETE",
        "CREATE",
        "DROP",
        "ALTER",
        "REFRESH",
        "SHOW",
    ]

    for keyword in sql_keywords:
        # Find statements starting with SQL keywords
        pattern = rf"\b{keyword}\b[^;]*(?:;|\Z)"
        matches = re.findall(pattern, text, re.IGNORECASE | re.DOTALL)
        statements.extend(matches)

    return [stmt.strip() for stmt in statements if stmt.strip()]


def _extract_host_references(text: str) -> List[str]:
    """Extract host/server references."""
    hosts = []

    # Host patterns
    host_patterns = [
        r"[a-z0-9.-]+\.(?:com|net|org|edu|gov)",  # domain names
        r"[a-z0-9.-]+:\d+",  # host:port
        r"\b(?:\d{1,3}\.){3}\d{1,3}\b",  # IP addresses
    ]

    for pattern in host_patterns:
        matches = re.findall(pattern, text, re.IGNORECASE)
        hosts.extend(matches)

    return list(set(hosts))


def _extract_database_connections(text: str) -> List[str]:
    """Extract database connection strings."""
    connections = []

    # Connection string patterns
    connection_patterns = [
        r'jdbc:[^\s,;"\'<>]+',  # JDBC URLs
        r'postgresql://[^\s,;"\'<>]+',  # PostgreSQL
        r'mysql://[^\s,;"\'<>]+',  # MySQL
        r'oracle:[^\s,;"\'<>]+',  # Oracle
    ]

    for pattern in connection_patterns:
        matches = re.findall(pattern, text, re.IGNORECASE)
        connections.extend(matches)

    return list(set(connections))


def _classify_hdfs_path(path: str) -> str:
    """Classify HDFS path type."""
    path_lower = path.lower()

    if "/user/hive/warehouse/" in path_lower:
        return "hive_warehouse"
    elif "/tmp/" in path_lower:
        return "temporary"
    elif "/data/" in path_lower:
        return "data_storage"
    elif "/user/" in path_lower:
        return "user_directory"
    elif ".parq" in path_lower:
        return "parquet_file"
    else:
        return "other"


def _classify_table_reference(table: str) -> str:
    """Classify table reference type."""
    if "." in table:
        parts = table.split(".")
        if len(parts) == 3:
            return "catalog.schema.table"
        elif len(parts) == 2:
            return "schema.table"
    return "table_name"


def _classify_sql_statement(statement: str) -> str:
    """Classify SQL statement type."""
    statement_upper = statement.upper().strip()

    if statement_upper.startswith("SELECT"):
        return "query"
    elif statement_upper.startswith(("INSERT", "UPDATE", "DELETE")):
        return "dml"
    elif statement_upper.startswith(("CREATE", "DROP", "ALTER")):
        return "ddl"
    elif statement_upper.startswith("REFRESH"):
        return "maintenance"
    else:
        return "other"


def extract_workflow_assets(workflow_analysis: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract all assets from a complete workflow analysis.

    Args:
        workflow_analysis: Complete workflow analysis with processors

    Returns:
        Comprehensive asset catalog for the entire workflow
    """

    # Get processors from analysis (handle different formats)
    processors = []
    if "classification_results" in workflow_analysis:
        processors = workflow_analysis["classification_results"]
    elif "processors_analysis" in workflow_analysis:
        processors = workflow_analysis["processors_analysis"]
    elif isinstance(workflow_analysis, list):
        processors = workflow_analysis

    workflow_assets = {
        "workflow_metadata": {
            "total_processors": len(processors),
            "analysis_timestamp": workflow_analysis.get("timestamp", "unknown"),
            "source_file": workflow_analysis.get("source_file", "unknown"),
        },
        "asset_summary": {
            "total_script_files": 0,
            "total_hdfs_paths": 0,
            "total_table_references": 0,
            "total_sql_statements": 0,
            "unique_working_directories": set(),
            "unique_hosts": set(),
        },
        "processor_assets": [],
        "consolidated_assets": {
            "all_script_files": [],
            "all_hdfs_paths": [],
            "all_table_references": [],
            "all_sql_statements": [],
            "all_working_directories": [],
            "all_host_references": [],
            "all_database_connections": [],
        },
    }

    # Process each processor
    for processor in processors:
        processor_assets = extract_processor_assets(processor)

        # Only include processors that have assets
        if any(
            isinstance(v, list) and v
            for k, v in processor_assets.items()
            if k not in ["processor_name", "processor_type", "raw_properties"]
        ):
            workflow_assets["processor_assets"].append(processor_assets)

            # Update summary counts
            workflow_assets["asset_summary"]["total_script_files"] += len(
                processor_assets.get("script_files", [])
            )
            workflow_assets["asset_summary"]["total_hdfs_paths"] += len(
                processor_assets.get("hdfs_paths", [])
            )
            workflow_assets["asset_summary"]["total_table_references"] += len(
                processor_assets.get("table_references", [])
            )
            workflow_assets["asset_summary"]["total_sql_statements"] += len(
                processor_assets.get("sql_statements", [])
            )

            # Collect unique items
            for wd in processor_assets.get("working_directories", []):
                workflow_assets["asset_summary"]["unique_working_directories"].add(
                    wd.get("path", "")
                )
            for host in processor_assets.get("host_references", []):
                workflow_assets["asset_summary"]["unique_hosts"].add(
                    host.get("host", "")
                )

            # Consolidate all assets
            for category in workflow_assets["consolidated_assets"]:
                category_key = category.replace("all_", "")
                if category_key in processor_assets:
                    workflow_assets["consolidated_assets"][category].extend(
                        processor_assets[category_key]
                    )

    # Convert sets to lists for JSON serialization
    workflow_assets["asset_summary"]["unique_working_directories"] = list(
        workflow_assets["asset_summary"]["unique_working_directories"]
    )
    workflow_assets["asset_summary"]["unique_hosts"] = list(
        workflow_assets["asset_summary"]["unique_hosts"]
    )

    return workflow_assets


def save_asset_catalog(workflow_assets: Dict[str, Any], output_path: str) -> str:
    """
    Save the asset catalog to a JSON file.

    Args:
        workflow_assets: Asset catalog from extract_workflow_assets
        output_path: Path to save the catalog

    Returns:
        Path to saved catalog file
    """

    catalog_path = Path(output_path) / "nifi_asset_catalog.json"
    catalog_path.parent.mkdir(parents=True, exist_ok=True)

    with open(catalog_path, "w") as f:
        json.dump(workflow_assets, f, indent=2, default=str)

    return str(catalog_path)


def generate_asset_summary_report(
    workflow_assets: Dict[str, Any], output_path: str
) -> str:
    """
    Generate a human-readable asset summary report.

    Args:
        workflow_assets: Asset catalog from extract_workflow_assets
        output_path: Path to save the report

    Returns:
        Path to saved report file
    """

    summary = workflow_assets.get("asset_summary", {})
    consolidated = workflow_assets.get("consolidated_assets", {})

    report = f"""# NiFi Workflow Asset Discovery Report

## Summary
- **Total Processors with Assets**: {len(workflow_assets.get('processor_assets', []))}
- **Script Files Found**: {summary.get('total_script_files', 0)}
- **HDFS Paths Found**: {summary.get('total_hdfs_paths', 0)}
- **Table References Found**: {summary.get('total_table_references', 0)}
- **SQL Statements Found**: {summary.get('total_sql_statements', 0)}
- **Unique Working Directories**: {len(summary.get('unique_working_directories', []))}
- **Unique Hosts**: {len(summary.get('unique_hosts', []))}

## Critical Scripts Requiring Manual Migration
"""

    # Group script files by processor
    script_by_processor = {}
    processor_assets = workflow_assets.get("processor_assets", [])
    for proc_asset in processor_assets:
        proc_name = proc_asset.get("processor_name", "Unknown")
        proc_type = proc_asset.get("processor_type", "Unknown")
        for script in proc_asset.get("script_files", []):
            if script not in script_by_processor:
                script_by_processor[script] = []
            script_by_processor[script].append(f"{proc_name} ({proc_type})")

    for script in sorted(script_by_processor.keys()):
        processors = ", ".join(script_by_processor[script])
        report += f"- `{script}` ← Used by: {processors}\n"

    report += "\n## HDFS Paths Requiring Unity Catalog Migration\n"

    # Group HDFS paths by processor
    hdfs_by_processor = {}
    for proc_asset in processor_assets:
        proc_name = proc_asset.get("processor_name", "Unknown")
        proc_type = proc_asset.get("processor_type", "Unknown")
        for hdfs_path in proc_asset.get("hdfs_paths", []):
            path_str = (
                hdfs_path.get("path", "")
                if isinstance(hdfs_path, dict)
                else str(hdfs_path)
            )
            if path_str not in hdfs_by_processor:
                hdfs_by_processor[path_str] = []
            hdfs_by_processor[path_str].append(f"{proc_name} ({proc_type})")

    for path in sorted(hdfs_by_processor.keys()):
        processors = hdfs_by_processor[path]
        if len(processors) > 3:
            # Too many processors - show count only to avoid redundancy
            report += f"- `{path}` ← Used by: {len(processors)} processors\n"
        else:
            # Few processors - show specific names
            processors_str = ", ".join(processors)
            report += f"- `{path}` ← Used by: {processors_str}\n"

    report += "\n## Table References for Schema Mapping\n"

    # Group table references by processor
    table_by_processor = {}
    for proc_asset in processor_assets:
        proc_name = proc_asset.get("processor_name", "Unknown")
        proc_type = proc_asset.get("processor_type", "Unknown")
        for table_ref in proc_asset.get("table_references", []):
            table_str = (
                table_ref.get("table", "")
                if isinstance(table_ref, dict)
                else str(table_ref)
            )
            if table_str not in table_by_processor:
                table_by_processor[table_str] = []
            table_by_processor[table_str].append(f"{proc_name} ({proc_type})")

    for table in sorted(table_by_processor.keys()):
        processors = table_by_processor[table]
        if len(processors) > 3:
            # Too many processors - show count only to avoid redundancy
            report += f"- `{table}` ← Used by: {len(processors)} processors\n"
        else:
            # Few processors - show specific names
            processors_str = ", ".join(processors)
            report += f"- `{table}` ← Used by: {processors_str}\n"

    report += "\n## Working Directories\n"

    for wd in summary.get("unique_working_directories", []):
        if wd:
            report += f"- `{wd}`\n"

    report += "\n## External Hosts/Connections\n"

    for host in summary.get("unique_hosts", []):
        if host:
            report += f"- `{host}`\n"

    report_path = Path(output_path) / "nifi_asset_summary.md"
    report_path.parent.mkdir(parents=True, exist_ok=True)

    with open(report_path, "w") as f:
        f.write(report)

    return str(report_path)


# Integration function for the migration pipeline
def discover_and_catalog_assets(xml_path: str, output_dir: str) -> Dict[str, str]:
    """
    Main function to discover and catalog assets during migration.

    Args:
        xml_path: Path to NiFi XML file
        output_dir: Output directory for catalogs

    Returns:
        Dict with paths to generated catalog files
    """

    # This would integrate with existing analysis
    # For now, return placeholder that shows the integration pattern
    return {
        "asset_catalog": f"{output_dir}/nifi_asset_catalog.json",
        "asset_summary": f"{output_dir}/nifi_asset_summary.md",
    }

"""
Enhanced asset extraction for NiFi workflows.
Extracts scripts, paths, database hosts, external hosts, and table references.
"""

import json
import re
from typing import Set
from urllib.parse import urlparse

# Asset discovery patterns
SCRIPT_EXTS = [".sh", ".py", ".sql", ".jar", ".class"]
SCRIPT_PATH_RX = re.compile(r"/[\w/.-]+\.(?:sh|py|sql|jar|class)")
HOST_RX = re.compile(r"\b(?:[a-zA-Z0-9-]+\.){1,}[a-zA-Z]{2,}\b")


def _is_false_positive_asset(value: str) -> bool:
    """Simple filter to skip patterns, variables, and obvious non-executable content."""
    if not value:
        return True

    # Skip regex patterns
    if value.startswith("^") or value.endswith("$") or ".*" in value:
        return True

    # Skip variable references
    if "${" in value and "}" in value:
        return True

    # Skip wildcards and patterns
    if "*" in value or "?" in value:
        return True

    # Skip multiple items in one value
    if ";" in value or ", " in value:
        return True

    # Skip error messages or descriptive text
    if any(
        word in value.lower() for word in ["failed", "error", "feel like", "ingest -"]
    ):
        return True

    return False


def extract_assets_from_properties(
    properties: dict,
    script_files: set,
    hdfs_paths: set,
    database_hosts: set,
    external_hosts: set,
    table_references: set,
):
    """Enhanced asset extraction with comprehensive table reference detection."""

    # First pass: collect location and table properties by environment
    location_table_pairs = {}

    for prop_name, prop_value in (properties or {}).items():
        if not prop_value or not isinstance(prop_value, str):
            continue
        pv = prop_value.strip()

        # Skip obvious false positives
        if _is_false_positive_asset(pv):
            continue

        prop_lower = prop_name.lower()

        # Detect environment-specific location/table pairs (prod_, staging_, dev_, test_)
        for env_prefix in ["prod_", "staging_", "dev_", "test_", "source_", "target_"]:
            if prop_lower.startswith(env_prefix):
                env_key = env_prefix.rstrip("_")
                if env_key not in location_table_pairs:
                    location_table_pairs[env_key] = {}

                prop_suffix = prop_lower[len(env_prefix) :]
                if prop_suffix in ["location", "path", "directory"]:
                    location_table_pairs[env_key]["location"] = pv
                elif prop_suffix in ["table", "table_name"]:
                    location_table_pairs[env_key]["table"] = pv
                elif prop_suffix in ["database", "db", "schema"]:
                    location_table_pairs[env_key]["database"] = pv
                break

    # Second pass: combine location and table information
    for env, info in location_table_pairs.items():
        location = info.get("location", "")
        table = info.get("table", "")
        database = info.get("database", "")

        if table and not any(char in table for char in ["${", "*", "?", ";"]):
            # Case 1: Table already contains database.table format - use as is
            if "." in table and len(table.split(".")) == 2:
                db, tbl = table.split(".")
                table_references.add(f"{db.strip()}.{tbl.strip()}")
            # Case 2: We have both database and table (standalone table name)
            elif database and not any(
                char in database for char in ["${", "*", "?", ";"]
            ):
                table_references.add(f"{database.strip()}.{table.strip()}")
            # Case 3: Extract database from location path if available
            elif location and location.startswith("/"):
                path_parts = [
                    p for p in location.split("/") if p and not p.startswith("${")
                ]
                if len(path_parts) >= 1:
                    # Use last meaningful path component as database
                    db_from_path = path_parts[-1]
                    table_references.add(f"{db_from_path}.{table}")
            # Case 4: Standalone table name
            elif table.replace("_", "").replace("-", "").isalnum():
                table_references.add(f"unknown_db.{table}")

    # Third pass: handle remaining table/location properties not caught by environment prefixes
    for prop_name, prop_value in (properties or {}).items():
        if not prop_value or not isinstance(prop_value, str):
            continue
        pv = prop_value.strip()

        if _is_false_positive_asset(pv):
            continue

        prop_lower = prop_name.lower()

        # Skip if already processed in environment-specific pairs
        if any(
            prop_lower.startswith(env + "_")
            for env in ["prod", "staging", "dev", "test", "source", "target"]
        ):
            continue

        # Handle standalone table/database/schema properties
        if any(keyword in prop_lower for keyword in ["table", "database", "schema"]):
            if (
                "table" in prop_lower
                and pv
                and not any(char in pv for char in ["${", "*", "?", ";"])
            ):
                # Handle database.table format
                if "." in pv and len(pv.split(".")) == 2:
                    db, table = pv.split(".")
                    table_references.add(f"{db.strip()}.{table.strip()}")
                elif pv.replace("_", "").replace("-", "").isalnum():
                    table_references.add(f"unknown_db.{pv}")

        # Extract location references (often contain database/table info)
        if "location" in prop_lower and pv.startswith("/"):
            # Look for patterns like /warehouse/database/table or /data/database/table
            path_parts = [p for p in pv.split("/") if p and not p.startswith("${")]
            if len(path_parts) >= 2:
                # Last two parts might be database.table
                potential_table = f"{path_parts[-2]}.{path_parts[-1]}"
                table_references.add(potential_table)

        # Extract table names from SQL queries (ExecuteSQL, PutSQL, etc.)
        if any(
            sql_keyword in prop_lower for sql_keyword in ["sql", "query", "statement"]
        ):
            if len(pv) > 10 and any(
                keyword in pv.upper()
                for keyword in ["SELECT", "FROM", "INSERT", "UPDATE", "DELETE", "JOIN"]
            ):
                # Simple SQL parsing to extract table names
                sql_upper = pv.upper()
                # Extract table names after FROM clause
                from_patterns = [
                    r"FROM\s+([a-zA-Z_][a-zA-Z0-9_.]*)",  # FROM table_name
                    r"JOIN\s+([a-zA-Z_][a-zA-Z0-9_.]*)",  # JOIN table_name
                    r"INSERT\s+INTO\s+([a-zA-Z_][a-zA-Z0-9_.]*)",  # INSERT INTO table_name
                    r"UPDATE\s+([a-zA-Z_][a-zA-Z0-9_.]*)",  # UPDATE table_name
                ]
                for pattern in from_patterns:
                    matches = re.findall(pattern, sql_upper)
                    for match in matches:
                        # Clean up table name (remove aliases, whitespace)
                        table_name = match.strip()
                        if table_name and not any(
                            skip in table_name.lower()
                            for skip in ["select", "where", "order", "group"]
                        ):
                            if "." in table_name:
                                table_references.add(table_name.lower())
                            else:
                                table_references.add(f"unknown_db.{table_name.lower()}")

        # scripts - only capture real executable paths
        if any(pv.lower().endswith(ext) for ext in SCRIPT_EXTS) and pv.startswith("/"):
            script_files.add(pv)
        for m in SCRIPT_PATH_RX.findall(pv):
            if not _is_false_positive_asset(m):
                script_files.add(m)

        # HDFS/file-ish paths
        for pattern in (
            r"hdfs://[^\s]+",
            r"/user/[^\s]+",
            r"/etl/[^\s]+",
            r"/hdfs/[^\s]+",
            r"/warehouse/[^\s]+",
            r"/tmp/[^\s]+",
            r"/data/[^\s]+",
        ):
            for path in re.findall(pattern, pv):
                p = path.strip().rstrip(";")
                if p and not p.startswith("${"):
                    hdfs_paths.add(p)

        # URLs → external hosts
        try:
            u = urlparse(pv)
            if u.scheme and u.hostname:
                external_hosts.add(u.hostname.lower())
        except Exception:
            pass

        # Host-like tokens (domains/IPs)
        for tok in re.findall(HOST_RX, pv):
            # classify JDBC/DB tokens as database_hosts when appropriate
            if "jdbc:" in pv.lower() or any(
                k in pv.lower()
                for k in ("impala", "hive", "postgres", "oracle", "sqlserver", "mysql")
            ):
                database_hosts.add(tok.lower())
            else:
                external_hosts.add(tok.lower())


def generate_asset_summary(processor_data, output_dir: str = None) -> str:
    """Generate a focused asset summary for migration planning.

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
    if "classification_results" in data:
        processors = data.get("classification_results", [])
    elif "processor_classifications" in data:
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

    # Generate summary report
    report_lines = [
        "# NiFi Asset Summary",
        "",
        "## Overview",
        f"- **Total Processors Analyzed**: {len(processors)}",
        f"- **Script Files Found**: {len(script_files)}",
        f"- **HDFS Paths Found**: {len(hdfs_paths)}",
        f"- **Database Hosts Found**: {len(database_hosts)}",
        f"- **Table References Found**: {len(table_references)}",
        f"- **External Hosts Found**: {len(external_hosts)}",
        "",
    ]

    if script_files:
        report_lines.extend(["## Script Files Requiring Migration", ""])
        for script in sorted(script_files):
            report_lines.append(f"- `{script}`")
        report_lines.append("")

    if table_references:
        report_lines.extend(["## Table References for Unity Catalog Migration", ""])
        for table in sorted(table_references):
            if "." in table:
                report_lines.append(f"- **Database.Table**: `{table}`")
            else:
                report_lines.append(f"- **Table**: `{table}`")
        report_lines.append("")

    if database_hosts:
        report_lines.extend(["## Database Connections", ""])
        for host in sorted(database_hosts):
            report_lines.append(f"- **Database**: `{host}`")
        report_lines.append("")

    if hdfs_paths:
        report_lines.extend(["## HDFS Paths for Unity Catalog Migration", ""])
        for path in sorted(hdfs_paths):
            report_lines.append(f"- `{path}`")
        report_lines.append("")

    if external_hosts:
        report_lines.extend(["## External Host Dependencies", ""])
        for host in sorted(external_hosts):
            report_lines.append(f"- **External Host**: `{host}`")
        report_lines.append("")

    # Migration recommendations section removed - not useful for users

    return "\n".join(report_lines)

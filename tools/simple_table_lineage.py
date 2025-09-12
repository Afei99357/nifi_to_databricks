#!/usr/bin/env python3
"""
Simple table lineage analysis without NetworkX dependency.
Focuses on table extraction from NiFi XML workflows.
"""

import json
import re
import xml.etree.ElementTree as ET
from typing import Any, Dict, List, Set


def _txt(elem, *paths):
    """Helper for safe text extraction with component/* fallbacks"""
    for p in paths:
        n = elem.find(p)
        if n is not None:
            t = (n.text or "").strip()
            if t:
                return t
    return ""


def _extract_tables_from_processor(
    processor_type: str, properties: Dict[str, Any]
) -> Dict[str, Set[str]]:
    """Enhanced table extraction based on processor type and properties"""
    result = {
        "input_tables": set(),
        "output_tables": set(),
        "input_files": set(),
        "output_files": set(),
    }

    processor_type = (processor_type or "").lower()

    # Enhanced table extraction for ExecuteStreamCommand processors
    def _is_false_positive_table_ref(table_name: str) -> bool:
        """Filter out false positive table names"""
        if not table_name:
            return True
        # Skip obvious false positives in table names themselves
        if any(
            pattern in table_name.lower()
            for pattern in [
                ".sh",
                ".py",
                ".jar",
                ".class",
                ".xml",
                ".keytab",
                ".na",
                ".com",
                ".error",
                "impala.",
                "execution.",
                "nxp.",
            ]
        ):
            return True
        return False

    # Look for table references using enhanced extraction
    if "executestreamcommand" in processor_type:
        # First pass: Environment-specific table properties
        env_tables = {}
        for key, value in properties.items():
            if not value or not isinstance(value, str):
                continue

            key_lower = key.lower()
            # Look for environment-prefixed properties
            for env_prefix in [
                "prod_",
                "staging_",
                "dev_",
                "test_",
                "source_",
                "target_",
            ]:
                if key_lower.startswith(env_prefix):
                    env_key = env_prefix.rstrip("_")
                    if env_key not in env_tables:
                        env_tables[env_key] = {}

                    prop_suffix = key_lower[len(env_prefix) :]
                    if prop_suffix in ["table", "table_name"]:
                        env_tables[env_key]["table"] = value
                    elif prop_suffix in ["database", "db", "schema"]:
                        env_tables[env_key]["database"] = value
                    break

        # Process environment-specific tables
        for env, info in env_tables.items():
            table = info.get("table", "")
            database = info.get("database", "")
            if table:
                if "." in table and len(table.split(".")) == 2:
                    # Already in database.table format
                    result["input_tables"].add(table.lower())
                    result["output_tables"].add(table.lower())
                elif database:
                    # Combine database and table
                    full_table = f"{database}.{table}"
                    result["input_tables"].add(full_table.lower())
                    result["output_tables"].add(full_table.lower())

        # Second pass: SQL query extraction
        for key, value in properties.items():
            if not value or not isinstance(value, str):
                continue

            key_lower = key.lower()
            # Look for SQL queries in properties
            if any(
                sql_keyword in key_lower
                for sql_keyword in ["sql", "query", "statement"]
            ):
                if len(value) > 10 and any(
                    keyword in value.upper()
                    for keyword in [
                        "SELECT",
                        "FROM",
                        "INSERT",
                        "UPDATE",
                        "DELETE",
                        "JOIN",
                        "CREATE",
                        "ALTER",
                        "REFRESH",
                    ]
                ):

                    # Extract table names from SQL
                    sql_upper = value.upper()
                    sql_patterns = [
                        r"FROM\s+([a-zA-Z_][a-zA-Z0-9_.]*)",
                        r"JOIN\s+([a-zA-Z_][a-zA-Z0-9_.]*)",
                        r"INSERT\s+INTO\s+([a-zA-Z_][a-zA-Z0-9_.]*)",
                        r"UPDATE\s+([a-zA-Z_][a-zA-Z0-9_.]*)",
                        r"CREATE\s+TABLE\s+([a-zA-Z_][a-zA-Z0-9_.]*)",
                        r"ALTER\s+TABLE\s+([a-zA-Z_][a-zA-Z0-9_.]*)",
                        r"REFRESH\s+([a-zA-Z_][a-zA-Z0-9_.]*)",
                    ]

                    for pattern in sql_patterns:
                        matches = re.findall(pattern, sql_upper)
                        for match in matches:
                            table_name = match.strip()
                            if table_name and not any(
                                skip in table_name.lower()
                                for skip in ["select", "where", "order", "group"]
                            ):
                                if "." in table_name:
                                    result["input_tables"].add(table_name.lower())
                                    result["output_tables"].add(table_name.lower())

        # Third pass: General schema.table pattern matching with smart filtering
        for key, value in properties.items():
            if not value or not isinstance(value, str):
                continue

            # Look for schema.table patterns
            table_patterns = re.findall(
                r"\b([a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*)\b", value
            )

            for table in table_patterns:
                table_lower = table.lower()

                # Enhanced filtering - only include valid database schemas and exclude false positives
                if any(
                    db_schema in table_lower
                    for db_schema in [
                        "mfg_",
                        "bqa.",
                        "staging",
                        "prod_",
                        "temp.",
                        "data.",
                    ]
                ) and not _is_false_positive_table_ref(table_lower):
                    # Determine direction based on property context
                    key_lower = key.lower()
                    if (
                        "output" in key_lower
                        or "target" in key_lower
                        or "destination" in key_lower
                    ):
                        result["output_tables"].add(table_lower)
                    elif "input" in key_lower or "source" in key_lower:
                        result["input_tables"].add(table_lower)
                    else:
                        result["input_tables"].add(table_lower)
                        result["output_tables"].add(table_lower)

    # Handle other processor types with simple extraction
    elif "executesql" in processor_type:
        # Look for SQL queries with table references
        sql_query = properties.get("SQL Query", properties.get("SQL select query", ""))
        if sql_query:
            # Simple regex to find table names after FROM and INSERT INTO
            from_tables = re.findall(
                r"FROM\s+([a-zA-Z_][a-zA-Z0-9_.]*)", sql_query.upper()
            )
            insert_tables = re.findall(
                r"INSERT\s+INTO\s+([a-zA-Z_][a-zA-Z0-9_.]*)", sql_query.upper()
            )

            result["input_tables"].update(t.lower() for t in from_tables)
            result["output_tables"].update(t.lower() for t in insert_tables)

    elif "getfile" in processor_type or "listfile" in processor_type:
        # Input files only
        directory = properties.get("Input Directory", "")
        if directory:
            result["input_files"].add(directory)

    elif "puthdfs" in processor_type or "putfile" in processor_type:
        # Output files
        directory = properties.get("Directory", properties.get("Output Directory", ""))
        if directory:
            result["output_files"].add(directory)

    return result


def analyze_nifi_table_lineage(xml_content: str) -> Dict[str, Any]:
    """Analyze table lineage without NetworkX dependency"""
    try:
        root = ET.fromstring(xml_content)
    except ET.ParseError as e:
        return {"error": f"Failed to parse XML: {e}"}

    # Extract all processors
    all_processors = []

    # Find processors at all levels
    for proc in root.findall(".//processors"):
        proc_id = proc.get("id", "unknown")
        proc_type = _txt(proc, "type", "class")
        proc_name = _txt(proc, "name")

        # Extract properties
        properties = {}
        config = proc.find("config")
        if config is not None:
            props_elem = config.find("properties")
            if props_elem is not None:
                for entry in props_elem.findall("entry"):
                    key_elem = entry.find("key")
                    value_elem = entry.find("value")
                    if key_elem is not None and value_elem is not None:
                        key = key_elem.text or ""
                        value = value_elem.text or ""
                        properties[key] = value

        all_processors.append(
            {
                "id": proc_id,
                "type": proc_type,
                "name": proc_name,
                "properties": properties,
            }
        )

    # Extract table information from each processor
    all_tables = set()
    all_files = set()
    processor_table_map = {}

    for proc in all_processors:
        table_info = _extract_tables_from_processor(proc["type"], proc["properties"])

        tables = table_info["input_tables"].union(table_info["output_tables"])
        files = table_info["input_files"].union(table_info["output_files"])

        if tables or files:
            processor_table_map[proc["name"]] = {
                "tables": tables,
                "files": files,
                "input_tables": table_info["input_tables"],
                "output_tables": table_info["output_tables"],
            }

        all_tables.update(tables)
        all_files.update(files)

    # Generate simple lineage analysis
    table_usage = {}
    for proc_name, info in processor_table_map.items():
        for table in info["tables"]:
            if table not in table_usage:
                table_usage[table] = {"processors": [], "in_degree": 0, "out_degree": 0}
            table_usage[table]["processors"].append(proc_name)

            if table in info["input_tables"]:
                table_usage[table]["in_degree"] += 1
            if table in info["output_tables"]:
                table_usage[table]["out_degree"] += 1

    # Find critical tables (most connected)
    critical_tables = []
    for table, usage in table_usage.items():
        connectivity = usage["in_degree"] + usage["out_degree"]
        if connectivity > 0:
            critical_tables.append(
                {
                    "table": table,
                    "connectivity": connectivity,
                    "in_degree": usage["in_degree"],
                    "out_degree": usage["out_degree"],
                    "processors": usage["processors"],
                }
            )

    critical_tables.sort(key=lambda x: x["connectivity"], reverse=True)

    # Simple lineage chains (same table in different processors)
    lineage_chains = []
    for table in all_tables:
        processors_using_table = [
            proc_name
            for proc_name, info in processor_table_map.items()
            if table in info["tables"]
        ]
        if len(processors_using_table) > 1:
            lineage_chains.append(
                {
                    "source_table": table,
                    "target_table": table,
                    "processors": [{"name": name} for name in processors_using_table],
                    "processor_count": len(processors_using_table),
                }
            )

    return {
        "table_lineage": {
            "total_tables": len(all_tables),
            "total_files": len(all_files),
            "lineage_chains": lineage_chains,
            "critical_tables": critical_tables[:10],  # Top 10
            "all_tables": sorted(all_tables),
            "all_files": sorted(all_files),
        }
    }


def generate_simple_lineage_report(analysis_result: Dict[str, Any]) -> str:
    """Generate a simple table lineage report"""
    if "table_lineage" not in analysis_result:
        return "No table lineage data available."

    data = analysis_result["table_lineage"]

    lines = [
        "# 📊 Table-Level Data Lineage Analysis",
        "",
        "## Overview",
        f"- **Total Tables**: {data['total_tables']} table references",
        f"- **Total Files**: {data['total_files']} file references",
        f"- **Lineage Chains**: {len(data['lineage_chains'])} data flows",
        f"- **Critical Tables**: {len(data['critical_tables'])} high-connectivity tables",
        "",
    ]

    if data["all_tables"]:
        lines.extend(["## 🗄️ Database Tables Found", ""])
        for table in data["all_tables"]:
            lines.append(f"- `{table}`")
        lines.append("")

    if data["critical_tables"]:
        lines.extend(["## 🎯 Critical Tables", ""])
        for table in data["critical_tables"][:5]:
            lines.append(f"- **`{table['table']}`**")
            lines.append(
                f"  - Connectivity: {table['connectivity']} ({table['in_degree']} readers, {table['out_degree']} writers)"
            )
            lines.append(f"  - Used in: {', '.join(table['processors'])}")
        lines.append("")

    if data["lineage_chains"]:
        lines.extend(["## 🔄 Data Flow Analysis", ""])
        for chain in data["lineage_chains"][:5]:
            processors_text = " → ".join([p["name"] for p in chain["processors"]])
            lines.append(f"- **Table**: `{chain['source_table']}`")
            lines.append(f"  - **Flow**: {processors_text}")
            lines.append(
                f"  - **Complexity**: {chain['processor_count']} processing steps"
            )
        lines.append("")

    lines.extend(
        [
            "## 🎯 Migration Recommendations",
            "",
            "### Immediate Actions:",
            "- **Critical Tables** → Design Unity Catalog schemas for high-connectivity tables",
            "- **Source Tables** → Plan data ingestion architecture",
            "- **Sink Tables** → Design output data architecture",
            "",
            "### Data Architecture Planning:",
            "- **Complex Chains** → Consider Delta Live Tables for multi-stage pipelines",
            "- **Simple Chains** → Standard Databricks Jobs with table dependencies",
            "- **High-connectivity Tables** → Central data assets requiring careful schema design",
        ]
    )

    return "\n".join(lines)


# Alias functions for compatibility with Streamlit app
def build_complete_nifi_graph_with_tables(xml_content: str):
    """Compatibility function - returns analysis result instead of graph"""
    return analyze_nifi_table_lineage(xml_content)


def analyze_complete_workflow_with_tables(analysis_result, k: int = 10):
    """Compatibility function - returns the analysis result"""
    if isinstance(analysis_result, dict) and "table_lineage" in analysis_result:
        return analysis_result
    else:
        # If it's not already analyzed, analyze it
        return {
            "table_lineage": {
                "total_tables": 0,
                "total_files": 0,
                "lineage_chains": [],
                "critical_tables": [],
            }
        }


def generate_table_lineage_report(analysis_result):
    """Generate table lineage report"""
    return generate_simple_lineage_report(analysis_result)

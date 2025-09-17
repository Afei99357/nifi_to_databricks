"""
Table extraction for NiFi workflows.
Extracts all table references from NiFi XML regardless of database type (SQL, NoSQL, Hive, HBase, etc.)
"""

import re
from typing import Any, Dict, List, Set


def extract_all_tables_from_nifi_xml(xml_path: str) -> List[Dict[str, Any]]:
    """
    Extract all table references from NiFi XML file.

    Args:
        xml_path: Path to NiFi XML file

    Returns:
        List of table dictionaries with metadata
    """
    from tools.xml_tools import parse_nifi_template_impl

    # Parse processors from XML
    with open(xml_path, "r", encoding="utf-8") as f:
        xml_content = f.read()

    # Use the full template parser to get processors with properties
    template_data = parse_nifi_template_impl(xml_content)
    processors = template_data.get("processors", [])
    controller_services = template_data.get("controller_services", [])

    # Create mapping of controller service ID to connection details
    controller_service_map = _build_controller_service_map(controller_services)

    all_tables = []

    for processor in processors:
        tables = extract_table_references_from_processor(
            processor, controller_service_map
        )
        all_tables.extend(tables)

    # Remove duplicates based on table name and data source
    unique_tables = _remove_duplicate_tables(all_tables)

    return unique_tables


def _build_controller_service_map(
    controller_services: List[Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    """Build mapping of controller service ID to connection details."""
    service_map = {}

    for service in controller_services:
        service_id = service.get("id")
        service_type = service.get("type", "")
        properties = service.get("properties", {})

        if not service_id:
            continue

        connection_info = {}

        # Extract database connection URL
        if "Database Connection URL" in properties:
            connection_url = properties["Database Connection URL"]
            connection_info["connection_url"] = connection_url
            connection_info["source_detail"] = _parse_connection_url(connection_url)

        # Extract HBase configuration
        elif "Hadoop Configuration Files" in properties:
            config_files = properties["Hadoop Configuration Files"]
            connection_info["config_files"] = config_files
            connection_info["source_detail"] = f"HBase Cluster ({config_files})"

        # Extract hostname for other services
        elif "Hostname" in properties:
            hostname = properties["Hostname"]
            connection_info["hostname"] = hostname
            connection_info["source_detail"] = f"SFTP: {hostname}"

        # Set service type
        connection_info["service_type"] = service_type
        connection_info["service_name"] = service.get("name", "")

        service_map[service_id] = connection_info

    return service_map


def _parse_connection_url(connection_url: str) -> str:
    """Parse database connection URL to extract readable source information."""
    if not connection_url:
        return "Unknown Database"

    try:
        # Handle Oracle JDBC URLs
        if "oracle:thin:" in connection_url.lower():
            # Extract host:port/database from oracle:thin:@host:port:database
            parts = connection_url.split("@")
            if len(parts) > 1:
                host_db = parts[1]
                return f"Oracle: {host_db}"

        # Handle Hive JDBC URLs
        elif "hive2://" in connection_url.lower():
            # Extract host:port/database from hive2://host:port/database
            parts = connection_url.replace("jdbc:hive2://", "").split(";")[0]
            return f"Hive: {parts}"

        # Handle other JDBC URLs
        elif "jdbc:" in connection_url.lower():
            # Generic JDBC parsing
            url_part = connection_url.replace("jdbc:", "").split("//")
            if len(url_part) > 1:
                return f"Database: {url_part[1].split('?')[0]}"

        return connection_url

    except Exception:
        return connection_url


def extract_table_references_from_processor(
    processor: Dict[str, Any],
    controller_service_map: Dict[str, Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """
    Extract all table references from a single processor.

    Args:
        processor: Processor dictionary from XML parsing

    Returns:
        List of table dictionaries
    """
    tables = []
    properties = processor.get("properties", {})
    processor_type = processor.get("type", "")
    processor_name = processor.get("name", "")
    processor_id = processor.get("id", "")

    # Initialize controller service map if not provided
    if controller_service_map is None:
        controller_service_map = {}

    # Find controller service references in processor properties
    controller_service_info = _find_controller_service_for_processor(
        properties, controller_service_map
    )

    for prop_name, prop_value in properties.items():
        if not prop_value or not isinstance(prop_value, str):
            continue

        prop_value = prop_value.strip()

        # 1. Direct table name properties
        table_from_property = _extract_table_from_property(
            prop_name,
            prop_value,
            processor_type,
            processor_name,
            processor_id,
            controller_service_info,
        )
        if table_from_property:
            tables.append(table_from_property)

        # 2. Extract from SQL queries
        sql_tables = _extract_tables_from_sql_property(
            prop_name,
            prop_value,
            processor_type,
            processor_name,
            processor_id,
            controller_service_info,
        )
        tables.extend(sql_tables)

        # 3. Extract from Hive warehouse paths
        hive_table = _extract_hive_table_from_path_property(
            prop_name,
            prop_value,
            processor_type,
            processor_name,
            processor_id,
            controller_service_info,
        )
        if hive_table:
            tables.append(hive_table)

    return tables


def _find_controller_service_for_processor(
    properties: Dict[str, Any], controller_service_map: Dict[str, Dict[str, Any]]
) -> Dict[str, Any]:
    """Find controller service information for a processor."""
    # Look for controller service references in processor properties
    for prop_name, prop_value in properties.items():
        if isinstance(prop_value, str) and prop_value in controller_service_map:
            return controller_service_map[prop_value]

    return {}


def _extract_table_from_property(
    prop_name: str,
    prop_value: str,
    processor_type: str,
    processor_name: str,
    processor_id: str,
    controller_service_info: Dict[str, Any] = None,
) -> Dict[str, Any]:
    """Extract table reference from direct table name properties."""

    # Properties that typically contain table names
    table_property_keywords = [
        "table",
        "collection",
        "topic",
        "index",
        "bucket",
        "schema",
        "dataset",
        "entity",
        "model",
    ]

    prop_lower = prop_name.lower()

    # Check if property name suggests it contains a table name
    if any(keyword in prop_lower for keyword in table_property_keywords):
        if _is_valid_table_name(prop_value):
            return {
                "table_name": prop_value,
                "data_source": _detect_data_source(processor_type),
                "property_name": prop_name,
                "processor_name": processor_name,
                "processor_type": (
                    processor_type.split(".")[-1] if processor_type else "Unknown"
                ),
                "processor_id": processor_id,
                "detection_method": "property_name",
            }

    return None


def _extract_tables_from_sql_property(
    prop_name: str,
    prop_value: str,
    processor_type: str,
    processor_name: str,
    processor_id: str,
    controller_service_info: Dict[str, Any] = None,
) -> List[Dict[str, Any]]:
    """Extract table references from SQL query properties."""

    # Properties that typically contain SQL
    sql_property_keywords = ["sql", "query", "statement", "command"]

    prop_lower = prop_name.lower()

    # Check if property contains SQL
    if any(keyword in prop_lower for keyword in sql_property_keywords):
        sql_tables = _extract_tables_from_sql(prop_value)

        return [
            {
                "table_name": table,
                "data_source": "SQL Database",
                "property_name": prop_name,
                "processor_name": processor_name,
                "processor_type": (
                    processor_type.split(".")[-1] if processor_type else "Unknown"
                ),
                "processor_id": processor_id,
                "detection_method": "sql_parsing",
            }
            for table in sql_tables
        ]

    return []


def _extract_hive_table_from_path_property(
    prop_name: str,
    prop_value: str,
    processor_type: str,
    processor_name: str,
    processor_id: str,
    controller_service_info: Dict[str, Any] = None,
) -> Dict[str, Any]:
    """Extract Hive table from warehouse path properties."""

    # Check if property contains Hive warehouse path
    if "/warehouse/" in prop_value or "/hive/warehouse/" in prop_value:
        hive_table = _extract_hive_table_from_path(prop_value)
        if hive_table:
            return {
                "table_name": hive_table,
                "data_source": "Hive",
                "property_name": prop_name,
                "processor_name": processor_name,
                "processor_type": (
                    processor_type.split(".")[-1] if processor_type else "Unknown"
                ),
                "processor_id": processor_id,
                "detection_method": "hive_path",
            }

    return None


def _detect_data_source(processor_type: str) -> str:
    """Detect data source type from processor type."""
    if not processor_type:
        return "Unknown"

    type_lower = processor_type.lower()

    # Map processor types to data sources
    source_mapping = {
        "hbase": "HBase",
        "mongo": "MongoDB",
        "cassandra": "Cassandra",
        "kafka": "Kafka",
        "elasticsearch": "Elasticsearch",
        "solr": "Solr",
        "redis": "Redis",
        "sql": "SQL Database",
        "jdbc": "SQL Database",
        "database": "SQL Database",
        "oracle": "Oracle",
        "mysql": "MySQL",
        "postgres": "PostgreSQL",
        "sqlserver": "SQL Server",
        "hive": "Hive",
        "delta": "Delta Lake",
        "parquet": "Parquet",
        "avro": "Avro",
    }

    for keyword, source in source_mapping.items():
        if keyword in type_lower:
            return source

    return "Unknown"


def _extract_tables_from_sql(sql_content: str) -> List[str]:
    """Extract table references from SQL queries."""
    tables = set()

    # Common SQL patterns for table references
    patterns = [
        # FROM clause
        r"\bFROM\s+([a-zA-Z_][a-zA-Z0-9_.]*)",
        # JOIN clauses
        r"\b(?:INNER\s+|LEFT\s+|RIGHT\s+|FULL\s+|CROSS\s+)?JOIN\s+([a-zA-Z_][a-zA-Z0-9_.]*)",
        # INSERT INTO
        r"\bINSERT\s+INTO\s+(?:TABLE\s+)?([a-zA-Z_][a-zA-Z0-9_.]*)",
        # UPDATE
        r"\bUPDATE\s+([a-zA-Z_][a-zA-Z0-9_.]*)",
        # CREATE TABLE
        r"\bCREATE\s+(?:EXTERNAL\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([a-zA-Z_][a-zA-Z0-9_.]*)",
        # DROP TABLE
        r"\bDROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?([a-zA-Z_][a-zA-Z0-9_.]*)",
        # ALTER TABLE
        r"\bALTER\s+TABLE\s+([a-zA-Z_][a-zA-Z0-9_.]*)",
        # TRUNCATE TABLE
        r"\bTRUNCATE\s+(?:TABLE\s+)?([a-zA-Z_][a-zA-Z0-9_.]*)",
        # DELETE FROM
        r"\bDELETE\s+FROM\s+([a-zA-Z_][a-zA-Z0-9_.]*)",
        # WITH clause (CTE)
        r"\bWITH\s+([a-zA-Z_][a-zA-Z0-9_.]*)\s+AS",
        # MERGE/UPSERT
        r"\bMERGE\s+(?:INTO\s+)?([a-zA-Z_][a-zA-Z0-9_.]*)",
    ]

    for pattern in patterns:
        matches = re.findall(pattern, sql_content, re.IGNORECASE | re.MULTILINE)
        for match in matches:
            # Clean up table name
            table_name = match.strip()
            if _is_valid_table_name(table_name):
                tables.add(table_name)

    return list(tables)


def _extract_hive_table_from_path(path: str) -> str:
    """Extract Hive table from warehouse path."""
    # Patterns for Hive warehouse paths
    # Examples:
    # /user/hive/warehouse/stdf.db/tx_reference/
    # /warehouse/stdf.db/tx_reference/
    patterns = [
        r"/(?:user/)?hive/warehouse/([^/]+\.db)/([^/]+)",
        r"/warehouse/([^/]+\.db)/([^/]+)",
    ]

    for pattern in patterns:
        match = re.search(pattern, path)
        if match:
            db_name = match.group(1).replace(".db", "")
            table_name = match.group(2)
            # Return as database.table format
            return f"{db_name}.{table_name}"

    return None


def _is_valid_table_name(table_name: str) -> bool:
    """Validate if a string looks like a valid table name."""
    if not table_name:
        return False

    # Skip variables
    if "${" in table_name:
        return False

    # Skip obvious non-tables
    invalid_chars = ["*", "?", ";", " ", "\n", "\t", "(", ")", "[", "]"]
    if any(char in table_name for char in invalid_chars):
        return False

    # Skip URLs
    if table_name.startswith(("http://", "https://", "ftp://", "jdbc:")):
        return False

    # Skip file paths
    if table_name.startswith("/") or "\\" in table_name:
        return False

    # Must look like a table name (letters, numbers, dots, underscores)
    if re.match(r"^[a-zA-Z_][a-zA-Z0-9_.]*$", table_name):
        return True

    return False


def _remove_duplicate_tables(tables: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Remove duplicate table entries."""
    unique_tables = []
    seen = set()

    for table in tables:
        # Create unique key based on table name and data source
        key = (table["table_name"].lower(), table["data_source"])

        if key not in seen:
            unique_tables.append(table)
            seen.add(key)

    # Sort by table name for consistent output
    return sorted(unique_tables, key=lambda x: x["table_name"].lower())


def get_table_summary(tables: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Generate summary statistics for extracted tables."""
    if not tables:
        return {
            "total_tables": 0,
            "data_sources": {},
            "detection_methods": {},
            "top_processors": {},
        }

    # Count by data source
    data_sources = {}
    for table in tables:
        source = table["data_source"]
        data_sources[source] = data_sources.get(source, 0) + 1

    # Count by detection method
    detection_methods = {}
    for table in tables:
        method = table["detection_method"]
        detection_methods[method] = detection_methods.get(method, 0) + 1

    # Count by processor type
    processor_types = {}
    for table in tables:
        proc_type = table["processor_type"]
        processor_types[proc_type] = processor_types.get(proc_type, 0) + 1

    return {
        "total_tables": len(tables),
        "data_sources": data_sources,
        "detection_methods": detection_methods,
        "top_processors": dict(
            sorted(processor_types.items(), key=lambda x: x[1], reverse=True)[:5]
        ),
    }

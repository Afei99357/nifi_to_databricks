"""
Table extraction for NiFi workflows.
Extracts all table references from NiFi XML regardless of database type (SQL, NoSQL, Hive, HBase, etc.)
"""

import re
from collections import Counter
from typing import Any, Dict, List, Set, Tuple

# NiFi Expression Language pattern
EL_PATTERN = re.compile(r"\$\{[^}]+\}")


def _normalize_el(value: str, replacement: str = "elvar") -> str:
    """Replace NiFi Expression Language ${...} with safe tokens."""
    return EL_PATTERN.sub(replacement, value)


def _canonical_type(name: str) -> str:
    """Canonicalize processor type to short, lowercase form."""
    if not name:
        return "unknown"
    short = name.split(".")[-1]
    return short.lower()


def extract_all_tables_from_nifi_xml(xml_path: str) -> List[Dict[str, Any]]:
    """
    Extract all table references from NiFi XML file using multi-pass validation.

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

    # Multi-pass extraction pipeline (simplified for better results)
    all_tables = []
    for processor in processors:
        tables = extract_table_references_from_processor(
            processor, controller_service_map
        )
        all_tables.extend(tables)

    # Apply basic anti-pattern filtering only
    filtered_tables = []
    anti_patterns = _get_anti_patterns()
    for table in all_tables:
        if table["table_name"].lower() not in anti_patterns:
            filtered_tables.append(table)

    all_tables = filtered_tables

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

    # Skip GenerateFlowFile processors - they create workflow entities, not database tables
    if "GenerateFlowFile" in processor_type:
        return None

    # Properties that typically contain table names
    table_property_keywords = [
        "table",
        "collection",
        "topic",
        "index",
        "bucket",
        "schema",
        "dataset",
        "model",
    ]

    prop_lower = prop_name.lower()

    # Be more selective about what to skip - only obvious config keys
    obvious_config_names = {"entity"}
    if prop_lower in obvious_config_names:
        return None

    # Check if property name suggests it contains a table name
    if any(keyword in prop_lower for keyword in table_property_keywords):
        # Create context for validation
        context = {
            "processor_type": processor_type,
            "property_name": prop_name,
        }

        if _is_valid_table_name(prop_value, context):
            return {
                "table_name": prop_value,
                "property_name": prop_name,
                "processor_name": processor_name,
                "processor_type": _canonical_type(processor_type),
                "processor_id": processor_id,
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
        # Create context for SQL parsing
        sql_context = {
            "processor_type": processor_type,
            "property_name": prop_name,
        }

        sql_tables = _extract_tables_from_sql(prop_value, sql_context)

        return [
            {
                "table_name": table,
                "property_name": prop_name,
                "processor_name": processor_name,
                "processor_type": _canonical_type(processor_type),
                "processor_id": processor_id,
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
                "property_name": prop_name,
                "processor_name": processor_name,
                "processor_type": _canonical_type(processor_type),
                "processor_id": processor_id,
            }

    return None


def _extract_tables_from_sql(
    sql_content: str, context: Dict[str, Any] = None
) -> List[str]:
    """Extract table references from SQL queries with context-aware validation."""
    tables = set()

    if context is None:
        context = {}

    # Preprocess SQL to handle common issues
    sql_clean = _preprocess_sql_for_parsing(sql_content)

    # Enhanced SQL patterns with better context awareness
    table_patterns = _get_context_aware_sql_patterns()

    for pattern_info in table_patterns:
        pattern = pattern_info["pattern"]
        priority = pattern_info["priority"]
        context_hints = pattern_info.get("context", {})

        matches = re.findall(pattern, sql_clean, re.IGNORECASE | re.MULTILINE)
        for match in matches:
            # Handle tuple matches from groups
            if isinstance(match, tuple):
                table_name = next((m for m in match if m), "").strip()
            else:
                table_name = match.strip()

            if not table_name:
                continue

            # Clean up captured subquery tokens
            candidate = table_name.strip().strip(",")
            if candidate.startswith("(") or candidate.endswith(")"):
                continue

            # Normalize EL before validation
            candidate = _normalize_el(candidate)

            # Create enhanced context for validation
            validation_context = {
                **context,
                "sql_pattern_priority": priority,
                "sql_context": context_hints,
                "from_sql": True,
            }

            if _is_valid_table_name(candidate, validation_context):
                tables.add(candidate)

    return list(tables)


def _preprocess_sql_for_parsing(sql_content: str) -> str:
    """Preprocess SQL to improve parsing accuracy."""
    # Remove comments
    sql_clean = re.sub(r"--.*?$", "", sql_content, flags=re.MULTILINE)
    sql_clean = re.sub(r"/\*.*?\*/", "", sql_clean, flags=re.DOTALL)

    # Normalize whitespace
    sql_clean = re.sub(r"\s+", " ", sql_clean)

    # Remove backticks and quotes around identifiers
    sql_clean = re.sub(r'[`"\']([a-zA-Z_][a-zA-Z0-9_.]*)["`\']', r"\1", sql_clean)

    return sql_clean.strip()


def _get_context_aware_sql_patterns() -> List[Dict[str, Any]]:
    """Get SQL patterns with priority and context information."""
    return [
        {
            "pattern": r"\bFROM\s+([a-zA-Z_][a-zA-Z0-9_.]*)",
            "priority": "high",
            "context": {"clause": "from", "confidence": 0.9},
        },
        {
            "pattern": r"\b(?:INNER\s+|LEFT\s+|RIGHT\s+|FULL\s+|CROSS\s+)?JOIN\s+([a-zA-Z_][a-zA-Z0-9_.]*)",
            "priority": "high",
            "context": {"clause": "join", "confidence": 0.9},
        },
        {
            "pattern": r"\bINSERT\s+INTO\s+(?:TABLE\s+)?([a-zA-Z_][a-zA-Z0-9_.]*)",
            "priority": "high",
            "context": {"clause": "insert", "confidence": 0.95},
        },
        {
            "pattern": r"\bUPDATE\s+([a-zA-Z_][a-zA-Z0-9_.]*)",
            "priority": "high",
            "context": {"clause": "update", "confidence": 0.95},
        },
        {
            "pattern": r"\bCREATE\s+(?:EXTERNAL\s+)?TABLE\s+IF\s+NOT\s+EXISTS\s+([a-zA-Z_][a-zA-Z0-9_.]*)",
            "priority": "high",
            "context": {"clause": "create", "confidence": 0.98},
        },
        {
            "pattern": r"\bCREATE\s+(?:EXTERNAL\s+)?TABLE\s+(?!IF\s+NOT\s+EXISTS)([a-zA-Z_][a-zA-Z0-9_.]*)",
            "priority": "high",
            "context": {"clause": "create", "confidence": 0.98},
        },
        {
            "pattern": r"\bDROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?([a-zA-Z_][a-zA-Z0-9_.]*)",
            "priority": "medium",
            "context": {"clause": "drop", "confidence": 0.8},
        },
        {
            "pattern": r"\bALTER\s+TABLE\s+([a-zA-Z_][a-zA-Z0-9_.]*)",
            "priority": "medium",
            "context": {"clause": "alter", "confidence": 0.8},
        },
        {
            "pattern": r"\bTRUNCATE\s+(?:TABLE\s+)?([a-zA-Z_][a-zA-Z0-9_.]*)",
            "priority": "medium",
            "context": {"clause": "truncate", "confidence": 0.8},
        },
        {
            "pattern": r"\bDELETE\s+FROM\s+([a-zA-Z_][a-zA-Z0-9_.]*)",
            "priority": "high",
            "context": {"clause": "delete", "confidence": 0.9},
        },
        {
            "pattern": r"\bMERGE\s+(?:INTO\s+)?([a-zA-Z_][a-zA-Z0-9_.]*)",
            "priority": "medium",
            "context": {"clause": "merge", "confidence": 0.8},
        },
        {
            "pattern": r"\bWITH\s+([a-zA-Z_][a-zA-Z0-9_.]*)\s+AS",
            "priority": "low",
            "context": {
                "clause": "with",
                "confidence": 0.3,
                "note": "CTE name, might not be real table",
            },
        },
    ]


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


def _is_valid_table_name(table_name: str, context: Dict[str, Any] = None) -> bool:
    """Validate if a string looks like a valid table name using pattern recognition."""
    if not table_name:
        return False

    # Initialize context if not provided
    if context is None:
        context = {}

    # NEW: Normalize NiFi EL before validation
    normalized = _normalize_el(table_name)

    # Calculate table name score using normalized name
    score = _calculate_table_name_score(normalized, context)

    # More forgiving threshold for real NiFi flows
    VALIDITY_THRESHOLD = 0.35

    return score >= VALIDITY_THRESHOLD


def _calculate_table_name_score(table_name: str, context: Dict[str, Any]) -> float:
    """Calculate a score (0-1) indicating how likely this is a real table name."""
    score = 0.0

    # Basic format validation (required)
    if not _passes_basic_format_checks(table_name):
        return 0.0

    # Pattern recognition scoring
    score += _score_table_name_patterns(table_name) * 0.4
    score += _score_naming_conventions(table_name) * 0.3
    score += _score_context_clues(table_name, context) * 0.2
    score += _score_database_formats(table_name) * 0.1

    return min(score, 1.0)


def _passes_basic_format_checks(table_name: str) -> bool:
    """Basic format validation that must pass for any table name."""

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

    # Skip too short names (likely not real table names)
    if len(table_name) < 2:
        return False

    # Allow leading digits if qualified name or previously quoted/EL
    if not re.match(r"^[a-zA-Z_0-9][a-zA-Z0-9_.]*$", table_name):
        return False

    return True


def _score_table_name_patterns(table_name: str) -> float:
    """Score based on common table name patterns."""
    score = 0.0
    name_lower = table_name.lower()

    # Common table name patterns (positive indicators)
    positive_patterns = [
        r"^[a-z_]+_(?:table|tbl|data|log|hist|temp|staging)$",  # suffix patterns
        r"^(?:dim|fact|stage|temp|raw|clean)_[a-z_]+$",  # prefix patterns
        r"^[a-z]+\.[a-z_]+$",  # database.table format
        r"^[a-z_]+(?:_[0-9]+)?$",  # table with version numbers
        r"^[a-z_]{3,20}$",  # reasonable length names
    ]

    for pattern in positive_patterns:
        if re.match(pattern, name_lower):
            score += 0.3
            break

    # Length scoring
    if 3 <= len(table_name) <= 30:
        score += 0.2
    elif len(table_name) > 30:
        score -= 0.1

    # Database.table format bonus
    if "." in table_name and len(table_name.split(".")) == 2:
        parts = table_name.split(".")
        if all(len(part) >= 2 for part in parts):
            score += 0.3

    return min(score, 1.0)


def _score_naming_conventions(table_name: str) -> float:
    """Score based on common database naming conventions."""
    score = 0.0
    name_lower = table_name.lower()

    # Common database table indicators
    table_indicators = [
        "table",
        "tbl",
        "data",
        "log",
        "hist",
        "temp",
        "staging",
        "archive",
        "backup",
        "cache",
        "queue",
        "event",
        "audit",
        "dim",
        "fact",
        "raw",
        "clean",
        "processed",
        "transformed",
    ]

    # Business domain indicators
    domain_indicators = [
        "user",
        "customer",
        "order",
        "product",
        "invoice",
        "payment",
        "transaction",
        "account",
        "profile",
        "session",
        "metric",
        "stdf",
        "probe",
        "wafer",
        "lot",
        "unit",
        "test",
        "measurement",
    ]

    # Check for table indicators
    for indicator in table_indicators:
        if indicator in name_lower:
            score += 0.4
            break

    # Check for domain indicators
    for indicator in domain_indicators:
        if indicator in name_lower:
            score += 0.3
            break

    # Underscore convention (common in databases)
    if "_" in table_name:
        score += 0.2

    # All lowercase (common convention)
    if table_name.islower():
        score += 0.1

    return min(score, 1.0)


def _score_context_clues(table_name: str, context: Dict[str, Any]) -> float:
    """Score based on processor context and property information."""
    score = 0.0

    # Canonicalize processor type for consistent matching
    processor_type = _canonical_type(context.get("processor_type", ""))
    property_name = context.get("property_name", "").lower()
    detection_method = context.get("detection_method", "")

    # Processor type context
    data_processors = [
        "hbase",
        "sql",
        "jdbc",
        "mongo",
        "cassandra",
        "hive",
        "executesql",
    ]
    if any(proc in processor_type for proc in data_processors):
        score += 0.5

    # Property name context (more permissive for real NiFi property names)
    table_properties = ["table", "collection", "dataset", "model", "schema"]
    if any(prop in property_name for prop in table_properties):
        score += 0.4

    # Detection method context
    if detection_method == "sql_parsing":
        sql_context = context.get("sql_context", {})
        sql_confidence = sql_context.get("confidence", 0.5)
        score += sql_confidence * 0.3

    # Controller service context boost
    data_source = context.get("data_source", "").lower()
    if data_source in {"sql database", "hive"}:
        score += 0.2

    # Pattern learning from historical data (fixed casing issue)
    patterns = context.get("patterns", {})
    if patterns:
        processor_patterns = patterns.get("processor_patterns", {})
        processor_types = processor_patterns.get("processor_types", {})

        # Boost score if this processor type commonly has tables
        if processor_type in processor_types:
            frequency = processor_types[processor_type]
            total = patterns.get("total_candidates", 1)
            if (
                frequency / total > 0.1
            ):  # More than 10% of tables from this processor type
                score += 0.2

    # Skip GenerateFlowFile context
    if "generateflowfile" in processor_type:
        score -= 0.8

    # Penalize if from non-SQL property but claiming to be SQL table
    if (
        context.get("from_sql")
        and "sql" not in property_name
        and "query" not in property_name
    ):
        score -= 0.1

    return max(score, 0.0)


def _score_database_formats(table_name: str) -> float:
    """Score based on database-specific format patterns."""
    score = 0.0

    # HBase table format (namespace:table)
    if ":" in table_name and len(table_name.split(":")) == 2:
        score += 0.8

    # SQL qualified names (2-part or 3-part: schema.table or catalog.schema.table)
    if "." in table_name:
        parts = table_name.split(".")
        if 2 <= len(parts) <= 3 and all(
            re.match(r"^[a-zA-Z_0-9][a-zA-Z0-9_]*$", p) for p in parts
        ):
            score += 0.6

    # MongoDB collection patterns
    if table_name.endswith("s") and len(table_name) > 4:  # plural collections
        score += 0.2

    return min(score, 1.0)


def _get_anti_patterns() -> Set[str]:
    """Get set of known anti-patterns that are definitely not table names."""
    return {
        # SQL keywords
        "select",
        "from",
        "where",
        "join",
        "inner",
        "left",
        "right",
        "outer",
        "full",
        "union",
        "order",
        "group",
        "having",
        "limit",
        "offset",
        "distinct",
        "count",
        "sum",
        "avg",
        "max",
        "min",
        "case",
        "when",
        "then",
        "else",
        "end",
        "as",
        "and",
        "or",
        "not",
        "null",
        "true",
        "false",
        "exists",
        "in",
        "between",
        "like",
        "is",
        "desc",
        "asc",
        "by",
        "into",
        "values",
        "insert",
        "update",
        "delete",
        "create",
        "drop",
        "alter",
        "table",
        "index",
        "view",
        "database",
        "schema",
        "column",
        "primary",
        "foreign",
        "key",
        "constraint",
        "default",
        "auto_increment",
        "unique",
        "cascade",
        "on",
        "references",
        "check",
        # Detected false positives from real data
        "because",
        "dates",
        "grps",
        "leadlag",
        "now",
        "startgrouping",
        "if",
        "then",
        "else",
        "when",
        "case",
        "end",
        # Configuration properties
        "prod_table",
        "staging_table",
        "external_table",
        "entity",
        "table_name",
        "database_name",
        "schema_name",
        "connection_url",
        "driver_class",
        "host",
        "port",
        "username",
        "password",
        # Common non-table words
        "true",
        "false",
        "yes",
        "no",
        "none",
        "null",
        "empty",
        "default",
        "auto",
        "manual",
        "enabled",
        "disabled",
        "active",
        "inactive",
    }


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

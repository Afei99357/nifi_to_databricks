"""
Table extraction for NiFi workflows.
Extracts all table references from NiFi XML regardless of database type (SQL, NoSQL, Hive, HBase, etc.)
"""

import re
from collections import Counter, defaultdict
from typing import Any, Dict, List, Set, Tuple

from tools.variable_extraction import find_variable_definitions, find_variable_usage
from tools.xml_tools import parse_nifi_template_impl

# NiFi Expression Language pattern
EL_PATTERN = re.compile(r"\$\{([^}]+)\}")  # Capturing group for variable name


def _normalize_el(value: str, replacement: str = "elvar") -> str:
    """Replace NiFi Expression Language ${...} with safe tokens.

    Preserves variable names by keeping them as ${var_name} format but with
    sanitized characters for pattern matching.
    """

    def replace_with_varname(match):
        # Extract variable name from ${var_name}
        var_expr = match.group(1)  # e.g., "db_schema" or "tblout:equals('foo')"
        var_name = var_expr.split(":")[0]  # Strip EL functions
        # Return sanitized format that preserves identity
        return f"${{{var_name}}}"

    return EL_PATTERN.sub(replace_with_varname, value)


def _canonical_type(name: str) -> str:
    """Canonicalize processor type to short, lowercase form."""
    if not name:
        return "unknown"
    short = name.split(".")[-1]
    return short.lower()


def _build_connection_maps(
    connections: List[Dict[str, Any]],
) -> Tuple[Dict[str, List[str]], Dict[str, List[str]]]:
    incoming: Dict[str, List[str]] = defaultdict(list)
    outgoing: Dict[str, List[str]] = defaultdict(list)
    for connection in connections:
        source = connection.get("source")
        destination = connection.get("destination")
        if source and destination:
            outgoing[source].append(destination)
            incoming[destination].append(source)
    return incoming, outgoing


def _collect_literal_variable_assignments(
    variable_definitions: Dict[str, List[Dict[str, Any]]],
) -> Dict[str, Dict[str, str]]:
    literal_assignments: Dict[str, Dict[str, str]] = defaultdict(dict)
    for var_name, entries in variable_definitions.items():
        for entry in entries:
            if entry.get("definition_type") != "static":
                continue
            pid = entry.get("processor_id")
            value = entry.get("property_value")
            if not pid or value is None:
                continue
            literal_assignments[pid][var_name] = value
    return literal_assignments


def _resolve_variables_for_processor(
    processor_id: str,
    incoming_map: Dict[str, List[str]],
    literal_assignments: Dict[str, Dict[str, str]],
    cache: Dict[str, Dict[str, str]],
    visiting: Set[str] | None = None,
) -> Dict[str, str]:
    if processor_id in cache:
        return cache[processor_id]

    if visiting is None:
        visiting = set()
    if processor_id in visiting:
        return {}

    visiting.add(processor_id)
    resolved: Dict[str, str] = {}

    for upstream in incoming_map.get(processor_id, []):
        upstream_vars = _resolve_variables_for_processor(
            upstream, incoming_map, literal_assignments, cache, visiting
        )
        if upstream_vars:
            resolved.update(upstream_vars)

    if processor_id in literal_assignments:
        resolved.update(literal_assignments[processor_id])

    visiting.remove(processor_id)
    cache[processor_id] = resolved
    return resolved


EL_CAPTURE_PATTERN = re.compile(r"\$\{([^}]+)\}")


def _resolve_expression_language(value: str, variables: Dict[str, str]) -> str:
    if not value or not isinstance(value, str):
        return value

    resolved = value
    for _ in range(3):
        changed = False

        def replace(match: re.Match[str]) -> str:
            nonlocal changed
            raw = match.group(1)
            key = raw.split(":", 1)[0]
            if key in variables:
                changed = True
                return variables[key]
            return match.group(0)

        new_value = EL_CAPTURE_PATTERN.sub(replace, resolved)
        if not changed:
            break
        resolved = new_value
    return resolved


def extract_all_tables_from_nifi_xml(xml_path: str) -> List[Dict[str, Any]]:
    """
    Extract all table references from NiFi XML file using multi-pass validation.

    Args:
        xml_path: Path to NiFi XML file

    Returns:
        List of table dictionaries with metadata
    """
    with open(xml_path, "r", encoding="utf-8") as f:
        xml_content = f.read()

    template_data = parse_nifi_template_impl(xml_content)
    processors = template_data.get("processors", [])
    controller_services = template_data.get("controller_services", [])
    connections = template_data.get("connections", [])

    processor_lookup = {p["id"]: p for p in processors}
    incoming_map, _ = _build_connection_maps(connections)

    variable_definitions = find_variable_definitions(processors)
    variable_usage = find_variable_usage(processors)
    literal_assignments = _collect_literal_variable_assignments(variable_definitions)
    resolver_cache: Dict[str, Dict[str, str]] = {}

    controller_service_map = _build_controller_service_map(controller_services)

    # Multi-pass extraction pipeline (simplified for better results)
    all_tables = []
    processor_table_entries: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    seen: Set[Tuple[str, str, str]] = set()

    for processor in processors:
        tables = extract_table_references_from_processor(
            processor,
            controller_service_map=controller_service_map,
            incoming_map=incoming_map,
            literal_assignments=literal_assignments,
            resolver_cache=resolver_cache,
        )
        for entry in tables:
            key = (
                entry.get("processor_id", ""),
                entry.get("table_name", ""),
                entry.get("io_type", ""),
            )
            if key in seen:
                continue
            seen.add(key)
            processor_table_entries[entry.get("processor_id", "")].append(entry)
            all_tables.append(entry)

    # Propagate table references to processors that consume variables
    for var_name, usages in variable_usage.items():
        definition_entries = variable_definitions.get(var_name, [])
        if not definition_entries:
            continue
        source_processor_ids = [
            entry.get("processor_id")
            for entry in definition_entries
            if entry.get("processor_id")
        ]
        if not source_processor_ids:
            continue

        # Gather tables defined by source processors
        source_tables: List[Dict[str, Any]] = []
        for spid in source_processor_ids:
            source_tables.extend(
                entry
                for entry in processor_table_entries.get(spid, [])
                if entry.get("source") == "sql"
            )
        if not source_tables:
            continue

        for usage in usages:
            dest_pid = usage.get("processor_id")
            if not dest_pid or dest_pid == "unknown":
                continue
            dest_proc = processor_lookup.get(dest_pid, {})
            dest_type = _canonical_type(dest_proc.get("type", "unknown"))
            if dest_type not in INHERIT_TABLE_TYPES:
                continue
            for table_entry in source_tables:
                key = (
                    dest_pid,
                    table_entry.get("table_name", ""),
                    table_entry.get("io_type", ""),
                )
                if key in seen:
                    continue
                inherited = dict(table_entry)
                inherited.update(
                    {
                        "processor_id": dest_pid,
                        "processor_name": dest_proc.get("name", "unknown"),
                        "processor_type": _canonical_type(
                            dest_proc.get("type", "unknown")
                        ),
                        "property_name": usage.get("property_name"),
                        "source": table_entry.get("source", "variable"),
                        "origin_processor_id": table_entry.get("processor_id"),
                        "origin_property_name": table_entry.get("property_name"),
                    }
                )
                seen.add(key)
                processor_table_entries[dest_pid].append(inherited)
                all_tables.append(inherited)

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
    controller_service_map: Dict[str, Dict[str, Any]] | None = None,
    incoming_map: Dict[str, List[str]] | None = None,
    literal_assignments: Dict[str, Dict[str, str]] | None = None,
    resolver_cache: Dict[str, Dict[str, str]] | None = None,
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

    resolver_cache = resolver_cache or {}
    incoming_map = incoming_map or {}
    literal_assignments = literal_assignments or {}

    var_values = _resolve_variables_for_processor(
        processor_id, incoming_map, literal_assignments, resolver_cache
    )

    for prop_name, prop_value in properties.items():
        if not prop_value or not isinstance(prop_value, str):
            continue

        prop_value = prop_value.strip()
        resolved_value = _resolve_expression_language(prop_value, var_values)

        table_from_property = _extract_table_from_property(
            prop_name,
            resolved_value,
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
            resolved_value,
            processor_type,
            processor_name,
            processor_id,
            controller_service_info,
        )
        tables.extend(sql_tables)

        # 3. Extract from Hive warehouse paths
        hive_table = _extract_hive_table_from_path_property(
            prop_name,
            resolved_value,
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
            ptype_lower = processor_type.lower()
            io_type = "unknown"
            if any(
                token in ptype_lower
                for token in (
                    "putsql",
                    "puthive",
                    "putdatabaserecord",
                    "putrecord",
                    "putdatabase",
                    "putpostgresql",
                )
            ):
                io_type = "write"
            elif any(
                token in ptype_lower
                for token in (
                    "querydatabasetable",
                    "generatetablefetch",
                    "selecthiveql",
                    "fetch",
                )
            ):
                io_type = "read"
            return {
                "table_name": prop_value,
                "property_name": prop_name,
                "processor_name": processor_name,
                "processor_type": _canonical_type(processor_type),
                "processor_id": processor_id,
                "source": "property",
                "io_type": io_type,
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

        results = []
        for table in sql_tables:
            entry = {
                "table_name": table["table_name"],
                "property_name": prop_name,
                "processor_name": processor_name,
                "processor_type": _canonical_type(processor_type),
                "processor_id": processor_id,
                "source": "sql",
                "sql_clause": table.get("clause"),
                "io_type": table.get("io_type", "unknown"),
                "confidence": table.get("confidence"),
            }
            results.append(entry)

        return results

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
            prop_lower = prop_name.lower()
            io_type = "unknown"
            if any(
                token in prop_lower
                for token in ("output", "target", "destination", "landing")
            ):
                io_type = "write"
            return {
                "table_name": hive_table,
                "property_name": prop_name,
                "processor_name": processor_name,
                "processor_type": _canonical_type(processor_type),
                "processor_id": processor_id,
                "source": "path",
                "io_type": io_type,
            }

    return None


def _extract_tables_from_sql(
    sql_content: str, context: Dict[str, Any] = None
) -> List[Dict[str, Any]]:
    """Extract table references from SQL queries with context-aware validation."""

    if context is None:
        context = {}

    sql_clean = _preprocess_sql_for_parsing(sql_content)
    if not sql_clean:
        return []

    tables: Dict[str, Dict[str, Any]] = {}
    write_clauses = {
        "insert",
        "update",
        "delete",
        "create",
        "drop",
        "alter",
        "truncate",
        "merge",
    }

    for pattern_info in _get_context_aware_sql_patterns():
        pattern = pattern_info["pattern"]
        priority = pattern_info["priority"]
        context_hints = pattern_info.get("context", {})

        matches = re.findall(pattern, sql_clean, re.IGNORECASE | re.MULTILINE)
        for match in matches:
            # Patterns now return tuples with (EL_prefix, table_name)
            # EL_prefix may be None or empty string if no EL variable present
            if isinstance(match, tuple) and len(match) >= 2:
                el_prefix = match[0] or ""  # Optional ${var}. prefix
                table_name = match[1] or ""  # Actual table name
                # Combine them to preserve ${var}.table_name format
                candidate = el_prefix + table_name
            else:
                # Fallback for patterns without EL support (shouldn't happen with new patterns)
                candidate = match if isinstance(match, str) else str(match)

            candidate = candidate.strip().strip(",")
            if not candidate:
                continue

            if candidate.startswith("(") or candidate.endswith(")"):
                continue

            candidate = _normalize_el(candidate)

            validation_context = {
                **context,
                "sql_pattern_priority": priority,
                "sql_context": context_hints,
                "from_sql": True,
            }

            if not _is_valid_table_name(candidate, validation_context):
                continue

            record = tables.setdefault(
                candidate,
                {
                    "table_name": candidate,
                    "clauses": set(),
                    "io_type": "read",
                    "confidence": 0.0,
                },
            )

            clause = (context_hints.get("clause") or "").lower()
            if clause:
                record["clauses"].add(clause)
                if clause in write_clauses:
                    record["io_type"] = "write"

            confidence = context_hints.get("confidence") or 0.0
            if confidence > record["confidence"]:
                record["confidence"] = confidence

    results: List[Dict[str, Any]] = []
    for entry in tables.values():
        clause_str = "/".join(sorted(entry["clauses"])) if entry["clauses"] else None
        results.append(
            {
                "table_name": entry["table_name"],
                "clause": clause_str,
                "io_type": entry["io_type"],
                "confidence": entry["confidence"] or None,
            }
        )

    return results


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
    """Get SQL patterns with priority and context information.

    Patterns support EL-prefixed table names like ${db_schema}.table_name by using
    two capturing groups: (${var}.)? for optional EL prefix and (table_name) for the actual table.
    """
    # Common pattern suffix for table names with optional EL variable prefix
    # Captures: (${var}.)? (table_name)
    table_pattern = r"(\$\{[^}]+\}\.)?([a-zA-Z_][a-zA-Z0-9_.]*)"

    return [
        {
            "pattern": rf"\bFROM\s+{table_pattern}",
            "priority": "high",
            "context": {"clause": "from", "confidence": 0.9},
        },
        {
            "pattern": rf"\b(?:INNER\s+|LEFT\s+|RIGHT\s+|FULL\s+|CROSS\s+)?JOIN\s+{table_pattern}",
            "priority": "high",
            "context": {"clause": "join", "confidence": 0.9},
        },
        {
            "pattern": rf"\bINSERT\s+(?:INTO|OVERWRITE)\s+(?:TABLE\s+)?{table_pattern}",
            "priority": "high",
            "context": {"clause": "insert", "confidence": 0.95},
        },
        {
            "pattern": rf"\bUPDATE\s+{table_pattern}",
            "priority": "high",
            "context": {"clause": "update", "confidence": 0.95},
        },
        {
            "pattern": rf"\bCREATE\s+(?:EXTERNAL\s+)?TABLE\s+IF\s+NOT\s+EXISTS\s+{table_pattern}",
            "priority": "high",
            "context": {"clause": "create", "confidence": 0.98},
        },
        {
            "pattern": rf"\bCREATE\s+(?:EXTERNAL\s+)?TABLE\s+(?!IF\s+NOT\s+EXISTS){table_pattern}",
            "priority": "high",
            "context": {"clause": "create", "confidence": 0.98},
        },
        {
            "pattern": rf"\bDROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?{table_pattern}",
            "priority": "medium",
            "context": {"clause": "drop", "confidence": 0.8},
        },
        {
            "pattern": rf"\bALTER\s+TABLE\s+{table_pattern}",
            "priority": "medium",
            "context": {"clause": "alter", "confidence": 0.8},
        },
        {
            "pattern": rf"\bTRUNCATE\s+(?:TABLE\s+)?{table_pattern}",
            "priority": "medium",
            "context": {"clause": "truncate", "confidence": 0.8},
        },
        {
            "pattern": rf"\bDELETE\s+FROM\s+{table_pattern}",
            "priority": "high",
            "context": {"clause": "delete", "confidence": 0.9},
        },
        {
            "pattern": rf"\bMERGE\s+(?:INTO\s+)?{table_pattern}",
            "priority": "medium",
            "context": {"clause": "merge", "confidence": 0.8},
        },
        {
            "pattern": rf"\bWITH\s+{table_pattern}\s+AS",
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

    # Allow EL variables in table names: ${var}.table or table with ${var} anywhere
    # Strip ${...} patterns temporarily for validation
    clean_name = re.sub(r"\$\{[^}]+\}\.?", "", table_name)

    # If everything was EL variables, that's invalid
    if not clean_name:
        return False

    # Allow leading digits if qualified name or previously quoted/EL
    if not re.match(r"^[a-zA-Z_0-9][a-zA-Z0-9_.]*$", clean_name):
        return False

    return True


def _score_table_name_patterns(table_name: str) -> float:
    """Score based on common table name patterns."""
    score = 0.0
    name_lower = table_name.lower()

    # Special handling for EL-normalized table names (${var}.table_name or ${var})
    # These come from unresolved ${var}.table_name patterns
    if "${" in name_lower:
        # Strip EL variables and score the actual table name
        actual_table = re.sub(r"\$\{[^}]+\}\.?", "", name_lower)
        if len(actual_table) >= 3:
            score += 0.5  # High confidence - came from SQL with EL variables
        return min(score, 1.0)

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

    # SQL parsing context boost
    if context.get("from_sql"):
        sql_context = context.get("sql_context", {})
        sql_confidence = sql_context.get("confidence", 0.5)
        score += sql_confidence * 0.3

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
    # Strip EL variables for validation since ${var}.table is valid
    if "." in table_name:
        parts = table_name.split(".")
        if 2 <= len(parts) <= 3:
            # Strip EL variables from each part before validation
            clean_parts = [re.sub(r"\$\{[^}]+\}", "", p) for p in parts]
            # Filter out empty parts (which were pure EL variables)
            clean_parts = [p for p in clean_parts if p]
            # Check if remaining parts are valid identifiers
            if clean_parts and all(
                re.match(r"^[a-zA-Z_0-9][a-zA-Z0-9_]*$", p) for p in clean_parts
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
        # Create unique key based on table name and processor type
        key = (table["table_name"].lower(), table["processor_type"])

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
            "top_processors": {},
            "property_types": {},
        }

    # Count by processor type
    processor_types = {}
    for table in tables:
        proc_type = table["processor_type"]
        processor_types[proc_type] = processor_types.get(proc_type, 0) + 1

    # Count by property type
    property_types = {}
    for table in tables:
        prop_name = table["property_name"]
        property_types[prop_name] = property_types.get(prop_name, 0) + 1

    return {
        "total_tables": len(tables),
        "top_processors": dict(
            sorted(processor_types.items(), key=lambda x: x[1], reverse=True)[:5]
        ),
        "property_types": dict(
            sorted(property_types.items(), key=lambda x: x[1], reverse=True)[:5]
        ),
    }


INHERIT_TABLE_TYPES = {
    "executestreamcommand",
    "putsql",
    "puthiveql",
    "putdatabaserecord",
    "putdatabase",
    "putdatabasetable",
}

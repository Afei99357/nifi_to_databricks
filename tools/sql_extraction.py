"""SQL extraction from NiFi ExecuteStreamCommand processors.

Extracts CREATE TABLE schemas and INSERT OVERWRITE transformations from
Command Arguments properties.
"""

import re
from typing import Any, Dict, List, Optional

from tools.variable_extraction import find_variable_definitions
from tools.xml_tools import parse_nifi_template_impl


def extract_sql_from_nifi_workflow(
    xml_path: str, variable_definitions: Optional[Dict[str, List]] = None
) -> Dict[str, Any]:
    """Extract all SQL statements from NiFi workflow.

    Args:
        xml_path: Path to NiFi XML template file
        variable_definitions: Optional dict of variable definitions from variable extraction

    Returns:
        Dictionary with extracted schemas and transformations:
        {
            "schemas": {
                "table_name": {
                    "database": "be_ei",
                    "table": "emt_log_new",
                    "columns": [...],
                    "partition_columns": [...],
                    "stored_as": "PARQUET",
                    "sort_by": ["mid"],
                    "comment": "..."
                }
            },
            "transformations": {
                "table_name": {
                    "target_table": "be_ei.emt_log_new",
                    "partition_spec": {...},
                    "column_mappings": [...],
                    "order_by": [...],
                    "source_table": "${tempTable}"
                }
            }
        }
    """
    with open(xml_path, "r") as f:
        xml_content = f.read()

    template_data = parse_nifi_template_impl(xml_content)
    processors = template_data.get("processors", [])

    # Extract variable definitions if not provided
    if variable_definitions is None:
        variable_definitions = find_variable_definitions(processors)

    schemas = {}
    transformations = {}

    for processor in processors:
        proc_type = processor.get("type", "")
        processor_id = processor.get("id")  # NEW: Track which processor has this SQL

        # Look for ExecuteStreamCommand processors
        if "ExecuteStreamCommand" in proc_type:
            command_args = processor.get("properties", {}).get("Command Arguments", "")

            if command_args:
                # Extract SQL from command arguments
                sql_statements = extract_sql_from_command_args(command_args)

                # Parse CREATE TABLE statements
                for stmt in sql_statements:
                    if stmt["type"] == "CREATE_TABLE":
                        schema = parse_create_table_statement(
                            stmt["sql"], variable_definitions
                        )
                        if schema:
                            table_name = schema["table"]
                            schema["processor_id"] = (
                                processor_id  # NEW: Link to processor
                            )
                            schemas[table_name] = schema

                    # Parse ALTER TABLE ADD PARTITION for LOCATION
                    elif stmt["type"] == "ALTER_TABLE":
                        location_info = parse_alter_table_location(stmt["sql"])
                        if location_info:
                            # Find matching CREATE TABLE schema and add location
                            table_name = location_info["table"]
                            if table_name in schemas:
                                schemas[table_name]["location"] = location_info[
                                    "location"
                                ]

                    # Parse INSERT OVERWRITE statements
                    elif stmt["type"] == "INSERT_OVERWRITE":
                        transform = parse_insert_overwrite_statement(
                            stmt["sql"], variable_definitions
                        )
                        if transform:
                            table_name = _extract_table_name_from_fqn(
                                transform["target_table"]
                            )
                            transform["processor_id"] = (
                                processor_id  # NEW: Link to processor
                            )
                            transformations[table_name] = transform

    return {"schemas": schemas, "transformations": transformations}


def extract_sql_from_command_args(command_args: str) -> List[Dict[str, Any]]:
    """Extract SQL statements from Command Arguments property.

    Handles various command formats:
    - impala-shell ... -q;"SQL HERE" (Impala shell format)
    - beeline -u ${IMPALA_SERVER} -e "SQL HERE"
    - impala-shell -i ${IMPALA_SERVER} -q "SQL HERE"
    - Direct SQL statements

    Args:
        command_args: Command Arguments property value

    Returns:
        List of SQL statements with type classification
    """
    statements = []

    # Try impala-shell with -q;" format first (common in NiFi)
    impala_shell_match = re.search(r'-q;\s*"(.+)"', command_args, re.DOTALL)
    if impala_shell_match:
        sql_content = impala_shell_match.group(1)
    else:
        # Try to extract SQL from beeline -e "..." wrapper
        beeline_match = re.search(r'-e\s+["\'](.+?)["\']', command_args, re.DOTALL)
        if beeline_match:
            sql_content = beeline_match.group(1)
        else:
            # Try impala-shell -q "..." wrapper
            impala_match = re.search(r'-q\s+["\'](.+?)["\']', command_args, re.DOTALL)
            if impala_match:
                sql_content = impala_match.group(1)
            else:
                # Assume direct SQL
                sql_content = command_args

    # Split by semicolon (simple split - good enough for most cases)
    parts = sql_content.split(";")

    for part in parts:
        sql = part.strip()
        if not sql:
            continue

        stmt_type = classify_sql_statement(sql)
        if stmt_type:
            statements.append({"type": stmt_type, "sql": sql})

    return statements


def classify_sql_statement(sql: str) -> Optional[str]:
    """Classify SQL statement type.

    Args:
        sql: SQL statement

    Returns:
        Statement type: CREATE_TABLE, INSERT_OVERWRITE, USE, INVALIDATE_METADATA, etc.
        or None if not recognized
    """
    sql_upper = sql.upper().strip()

    if sql_upper.startswith("CREATE TABLE") or sql_upper.startswith(
        "CREATE EXTERNAL TABLE"
    ):
        return "CREATE_TABLE"
    elif "INSERT OVERWRITE" in sql_upper:
        return "INSERT_OVERWRITE"
    elif sql_upper.startswith("USE "):
        return "USE"
    elif sql_upper.startswith("INVALIDATE METADATA"):
        return "INVALIDATE_METADATA"
    elif sql_upper.startswith("COMPUTE STATS"):
        return "COMPUTE_STATS"
    elif sql_upper.startswith("DROP TABLE"):
        return "DROP_TABLE"
    elif sql_upper.startswith("ALTER TABLE"):
        return "ALTER_TABLE"

    return None


def _resolve_variable(
    table_name: str, variable_definitions: Optional[Dict[str, List]]
) -> str:
    """Resolve NiFi EL variables like ${tempTable} to their actual values.

    Args:
        table_name: Table name that may contain ${variable}
        variable_definitions: Dict of variable definitions

    Returns:
        Resolved table name, or original EL expression if:
        - Variable is not defined (external)
        - Variable is dynamic (contains other ${vars})
    """
    if not variable_definitions or "${" not in table_name:
        return table_name

    # Extract variable name from ${varname}
    var_match = re.search(r"\$\{([^}]+)\}", table_name)
    if not var_match:
        return table_name

    var_name = var_match.group(1)

    # Look up variable definition
    if var_name in variable_definitions:
        definitions = variable_definitions[var_name]
        if definitions:
            # Use the first static definition if available
            for defn in definitions:
                if defn.get("definition_type") == "static":
                    resolved_value = defn.get("property_value", "")
                    # Replace ${varname} with the resolved value
                    return table_name.replace(f"${{{var_name}}}", resolved_value)

            # If only dynamic definitions exist, keep the dynamic value as-is
            # This helps document that it's a dynamic variable pattern
            first_defn = definitions[0]
            if first_defn.get("definition_type") == "dynamic":
                # Keep the EL expression to show it's a variable
                return table_name

    # Can't resolve (external variable) - return original with EL expression
    return table_name


def parse_create_table_statement(
    sql: str, variable_definitions: Optional[Dict[str, List]] = None
) -> Optional[Dict[str, Any]]:
    """Parse CREATE TABLE statement to extract schema.

    Args:
        sql: CREATE TABLE SQL statement
        variable_definitions: Optional dict of variable definitions for resolving ${vars}

    Returns:
        Schema dictionary or None if parsing fails
    """
    # Check if this is an EXTERNAL table
    is_external = bool(re.search(r"CREATE\s+EXTERNAL\s+TABLE", sql, re.IGNORECASE))

    # Extract table name (database.table or just table or ${variable})
    # Pattern supports: db.table, table, ${variable}
    table_name_match = re.search(
        r"CREATE\s+(?:EXTERNAL\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([a-zA-Z0-9_.${}]+)",
        sql,
        re.IGNORECASE,
    )
    if not table_name_match:
        return None

    full_table_name = table_name_match.group(1)

    # Resolve variables like ${tempTable}
    full_table_name = _resolve_variable(full_table_name, variable_definitions)

    if "." in full_table_name:
        database, table = full_table_name.split(".", 1)
    else:
        database = ""
        table = full_table_name

    # Extract columns (between first '(' and first ')')
    # Find the matching closing paren for column definitions
    first_paren = sql.find("(")
    if first_paren == -1:
        return None

    # Find matching closing paren
    paren_count = 1
    pos = first_paren + 1
    while pos < len(sql) and paren_count > 0:
        if sql[pos] == "(":
            paren_count += 1
        elif sql[pos] == ")":
            paren_count -= 1
        pos += 1

    if paren_count != 0:
        return None

    columns_text = sql[first_paren + 1 : pos - 1]
    columns = parse_column_definitions(columns_text)

    # Extract partition columns
    partition_match = re.search(
        r"PARTITIONED BY\s*\((.*?)\)", sql, re.IGNORECASE | re.DOTALL
    )
    partition_columns = []
    if partition_match:
        partition_text = partition_match.group(1)
        partition_columns = parse_column_definitions(partition_text)

    # Extract storage format
    stored_as_match = re.search(r"STORED AS\s+(\w+)", sql, re.IGNORECASE)
    stored_as = stored_as_match.group(1).upper() if stored_as_match else "TEXTFILE"

    # Extract sort by
    sort_by_match = re.search(r"SORT BY\s*\((.*?)\)", sql, re.IGNORECASE)
    sort_by = []
    if sort_by_match:
        sort_by_text = sort_by_match.group(1)
        sort_by = [col.strip() for col in sort_by_text.split(",")]

    # Extract comment
    comment_match = re.search(r"COMMENT\s+['\"](.+?)['\"]", sql, re.IGNORECASE)
    comment = comment_match.group(1) if comment_match else ""

    # Detect if this is a temp/staging table based on naming pattern
    is_temp_table = bool(
        re.search(
            r"(temp|tmp|staging|stg|_tmp\b|\${temp)",
            table,
            re.IGNORECASE,
        )
    )

    return {
        "database": database,
        "table": table,
        "columns": columns,
        "partition_columns": partition_columns,
        "stored_as": stored_as,
        "sort_by": sort_by,
        "comment": comment,
        "is_external": is_external,
        "is_temp_table": is_temp_table,
        "original_sql": sql,
    }


def parse_column_definitions(columns_text: str) -> List[Dict[str, Any]]:
    """Parse column definitions from CREATE TABLE.

    Args:
        columns_text: Column definitions text (e.g., "mid STRING, seq_num INTEGER")

    Returns:
        List of column dictionaries
    """
    columns = []

    # Split by comma (simple approach - works for most cases)
    parts = columns_text.split(",")

    for part in parts:
        part = part.strip()
        if not part:
            continue

        # Parse: column_name TYPE [COMMENT 'comment']
        col_match = re.match(
            r"([a-zA-Z0-9_]+)\s+([A-Z]+(?:\([^)]+\))?)"
            r"(?:\s+COMMENT\s+['\"](.+?)['\"])?",
            part,
            re.IGNORECASE,
        )
        if col_match:
            col_name = col_match.group(1)
            col_type = col_match.group(2).upper()
            col_comment = col_match.group(3) if col_match.group(3) else ""

            columns.append({"name": col_name, "type": col_type, "comment": col_comment})

    return columns


def parse_insert_overwrite_statement(
    sql: str, variable_definitions: Optional[Dict[str, List]] = None
) -> Optional[Dict[str, Any]]:
    """Parse INSERT OVERWRITE statement to extract transformations.

    Args:
        sql: INSERT OVERWRITE SQL statement
        variable_definitions: Optional dict of variable definitions for resolving ${vars}

    Returns:
        Transformation dictionary or None if parsing fails
    """
    # Extract target table (supports ${variables})
    table_match = re.search(
        r"INSERT\s+OVERWRITE\s+TABLE\s+([a-zA-Z0-9_.${}]+)", sql, re.IGNORECASE
    )
    if not table_match:
        return None

    target_table = table_match.group(1)
    # Resolve variables in target table
    target_table = _resolve_variable(target_table, variable_definitions)

    # Extract partition specification
    partition_match = re.search(
        r"PARTITION\s*\((.*?)\)", sql, re.IGNORECASE | re.DOTALL
    )
    partition_spec = {}
    if partition_match:
        partition_text = partition_match.group(1)
        partition_spec = parse_partition_spec(partition_text)

    # Extract SELECT clause
    select_match = re.search(
        r"SELECT\s+(.*?)\s+FROM\s+", sql, re.IGNORECASE | re.DOTALL
    )
    if not select_match:
        return None

    select_text = select_match.group(1)
    column_mappings = parse_select_expressions(select_text)

    # Extract source table (supports ${variables})
    from_match = re.search(r"FROM\s+([a-zA-Z0-9_.${}]+)", sql, re.IGNORECASE)
    source_table = from_match.group(1) if from_match else ""
    # Resolve variables in source table
    source_table = _resolve_variable(source_table, variable_definitions)

    # Extract ORDER BY
    order_by_match = re.search(r"ORDER BY\s+(.*?)(?:;|$)", sql, re.IGNORECASE)
    order_by = []
    if order_by_match:
        order_by_text = order_by_match.group(1).strip()
        order_by = [col.strip() for col in order_by_text.split(",")]

    return {
        "target_table": target_table,
        "partition_spec": partition_spec,
        "column_mappings": column_mappings,
        "source_table": source_table,
        "order_by": order_by,
        "original_sql": sql,
    }


def parse_partition_spec(partition_text: str) -> Dict[str, str]:
    """Parse partition specification.

    Args:
        partition_text: Partition spec (e.g., "site='${fab}', year=${year}")

    Returns:
        Dictionary mapping partition column to value
    """
    partition_spec = {}

    # Split by comma
    parts = partition_text.split(",")

    for part in parts:
        part = part.strip()
        if "=" in part:
            key, value = part.split("=", 1)
            key = key.strip()
            value = value.strip().strip("'\"")
            partition_spec[key] = value

    return partition_spec


def parse_select_expressions(select_text: str) -> List[Dict[str, Any]]:
    """Parse SELECT expressions to extract column transformations.

    Args:
        select_text: SELECT clause content

    Returns:
        List of column mappings with transformations
    """
    column_mappings = []

    # Split by comma (simple approach)
    parts = select_text.split(",")

    for part in parts:
        part = part.strip()
        if not part:
            continue

        # Check for AS alias
        as_match = re.search(r"(.*?)\s+AS\s+([a-zA-Z0-9_]+)", part, re.IGNORECASE)
        if as_match:
            expression = as_match.group(1).strip()
            target = as_match.group(2).strip()

            # Check if expression has transformation
            if "(" in expression or expression.upper() != target.upper():
                # Has transformation
                source = extract_source_column_from_expression(expression)
                column_mappings.append(
                    {"source": source, "target": target, "transform": expression}
                )
            else:
                # Direct mapping
                column_mappings.append(
                    {"source": target, "target": target, "transform": None}
                )
        else:
            # No AS clause - direct column reference
            col_name = part.strip()

            # Check for function call without AS
            if "(" in col_name:
                # Has transformation but no AS
                source = extract_source_column_from_expression(col_name)
                # Use source as target (common pattern)
                column_mappings.append(
                    {"source": source, "target": source, "transform": col_name}
                )
            else:
                # Simple column reference
                column_mappings.append(
                    {"source": col_name, "target": col_name, "transform": None}
                )

    return column_mappings


def extract_source_column_from_expression(expression: str) -> str:
    """Extract source column name from transformation expression.

    Args:
        expression: SQL expression (e.g., "TRIM(ts_state_start)", "CAST(seq_num AS INT)")

    Returns:
        Source column name
    """
    # For TRIM(col_name)
    trim_match = re.match(
        r"TRIM\s*\(\s*([a-zA-Z0-9_]+)\s*\)", expression, re.IGNORECASE
    )
    if trim_match:
        return trim_match.group(1)

    # For CAST(col_name AS TYPE)
    cast_match = re.match(
        r"CAST\s*\(\s*([a-zA-Z0-9_]+)\s+AS\s+\w+\s*\)", expression, re.IGNORECASE
    )
    if cast_match:
        return cast_match.group(1)

    # For other functions like NOW(), return empty
    if expression.upper() in ["NOW()", "CURRENT_TIMESTAMP()", "CURRENT_TIMESTAMP"]:
        return ""

    # Fallback: return expression as-is
    return expression


def _extract_table_name_from_fqn(fqn: str) -> str:
    """Extract table name from fully qualified name.

    Args:
        fqn: Fully qualified name (database.table or ${var}.table or just table)

    Returns:
        Table name only
    """
    if "." in fqn:
        return fqn.split(".")[-1]
    return fqn


def parse_alter_table_location(sql: str) -> Optional[Dict[str, str]]:
    """Parse ALTER TABLE ADD PARTITION statement to extract LOCATION.

    Args:
        sql: ALTER TABLE SQL statement

    Returns:
        Dictionary with table and location, or None if not a LOCATION statement
    """
    # Match: ALTER TABLE ${tempTable} ADD ... PARTITION ... LOCATION '${landingPath}'
    location_match = re.search(
        r"ALTER\s+TABLE\s+([a-zA-Z0-9_.${}]+).*?LOCATION\s+['\"](.+?)['\"]",
        sql,
        re.IGNORECASE | re.DOTALL,
    )

    if location_match:
        table_name = location_match.group(1)
        location = location_match.group(2)
        return {"table": table_name, "location": location}

    return None

"""
Databricks DDL Generator

Takes parsed Hive/Impala table information and generates
Databricks Delta table DDL statements.
"""

from typing import Dict, List, Tuple

from hive_ddl_parser import parse_hive_ddl


class DatabricksDDLGenerator:
    """Generator for Databricks DDL from Hive table information."""

    def __init__(self, parsed_table: Dict):
        """
        Initialize generator with parsed table information.

        Args:
            parsed_table: Dictionary from HiveDDLParser.parse()
        """
        self.table_info = parsed_table

    def optimize_column_types(
        self, columns: List[Tuple[str, str]], convert_timestamps: bool = True
    ) -> List[Tuple[str, str]]:
        """
        Optimize column types for Databricks.

        Args:
            columns: List of (column_name, column_type) tuples
            convert_timestamps: Whether to convert _ts STRING columns to TIMESTAMP

        Returns:
            List of optimized columns
        """
        optimized = []
        for col_name, col_type in columns:
            new_type = col_type.upper()

            # Convert STRING to TIMESTAMP for _ts columns
            if convert_timestamps and col_name.endswith("_ts") and "STRING" in new_type:
                new_type = "TIMESTAMP"

            optimized.append((col_name, new_type))

        return optimized

    def generate_delta_ddl(
        self,
        optimize_types: bool = True,
        include_comments: bool = True,
    ) -> str:
        """
        Generate managed Delta table DDL for Databricks.

        Args:
            optimize_types: Whether to optimize column types
            include_comments: Whether to include comments and documentation

        Returns:
            Complete DDL string
        """
        schema_name = self.table_info.get("schema_name") or "default"
        table_name = self.table_info["table_name"]
        columns = self.table_info["columns"]
        partition_columns = self.table_info["partition_columns"]

        # Optimize column types if requested
        if optimize_types:
            columns = self.optimize_column_types(columns)

        # Combine all columns (Delta doesn't separate data and partition columns in schema)
        all_columns = columns + partition_columns

        # Generate column definitions
        col_defs = []
        for col_name, col_type in all_columns:
            comment = ""
            if (
                include_comments
                and col_name.endswith("_ts")
                and "TIMESTAMP" in col_type
            ):
                comment = "  -- Converted from STRING"
            col_defs.append(f"  {col_name} {col_type}{comment}")

        columns_str = ",\n".join(col_defs)

        # Generate partition clause
        if partition_columns:
            partition_names = [col[0] for col in partition_columns]
            partition_clause = f"PARTITIONED BY ({', '.join(partition_names)})"
        else:
            partition_clause = ""

        # Build DDL
        ddl_parts = []

        if include_comments:
            ddl_parts.append(
                "-- ============================================================"
            )
            ddl_parts.append(f"-- Delta Table: {schema_name}.{table_name}")
            ddl_parts.append("-- Source: Hive table")
            ddl_parts.append("-- Target: Databricks Delta Lake (managed table)")
            ddl_parts.append(
                "-- ============================================================\n"
            )

        # Create schema
        ddl_parts.append("-- Step 1: Create schema")
        ddl_parts.append(f"CREATE SCHEMA IF NOT EXISTS {schema_name};\n")

        # Create table
        ddl_parts.append("-- Step 2: Create Delta table")
        ddl_parts.append(f"CREATE TABLE {schema_name}.{table_name} (")
        ddl_parts.append(columns_str)
        ddl_parts.append(")")
        ddl_parts.append("USING DELTA")
        if partition_clause:
            ddl_parts.append(partition_clause)
        ddl_parts.append("TBLPROPERTIES (")
        ddl_parts.append("  'delta.autoOptimize.optimizeWrite' = 'true',")
        ddl_parts.append("  'delta.autoOptimize.autoCompact' = 'true'")
        ddl_parts.append(");")

        if include_comments:
            ddl_parts.append("\n-- Step 3: Verify table")
            ddl_parts.append(f"DESCRIBE EXTENDED {schema_name}.{table_name};")

            if partition_columns:
                ddl_parts.append("\n-- Step 4: Check partitions (after data load)")
                ddl_parts.append(f"-- SHOW PARTITIONS {schema_name}.{table_name};")

            ddl_parts.append("\n-- Step 5: Optimize table")
            ddl_parts.append(f"OPTIMIZE {schema_name}.{table_name};")

        return "\n".join(ddl_parts)


def convert_hive_to_databricks(
    hive_ddl: str,
    optimize_types: bool = True,
) -> str:
    """
    Convert Hive DDL to Databricks managed Delta table DDL.
    Creates empty table structure without data or location.

    Args:
        hive_ddl: The Hive CREATE TABLE DDL
        optimize_types: Whether to optimize column types (STRING ending with _ts → TIMESTAMP)

    Returns:
        Databricks DDL string for managed Delta table

    Example:
        >>> hive_ddl = '''
        ... CREATE EXTERNAL TABLE my_schema.my_table (
        ...   id INT,
        ...   created_ts STRING
        ... )
        ... PARTITIONED BY (date STRING)
        ... STORED AS PARQUET
        ... LOCATION 'hdfs://namenode/path'
        ... '''
        >>> databricks_ddl = convert_hive_to_databricks(hive_ddl)
        >>> print(databricks_ddl)
    """
    # Parse Hive DDL
    parsed = parse_hive_ddl(hive_ddl)

    # Generate Databricks DDL (managed table, no location)
    generator = DatabricksDDLGenerator(parsed)
    return generator.generate_delta_ddl(optimize_types)


if __name__ == "__main__":
    # Example usage
    example_ddl = """
    CREATE EXTERNAL TABLE obf_schema.obf_table_raw (
      col_a STRING,
      col_b_ts STRING,
      col_c_ts STRING,
      col_d INT
    )
    PARTITIONED BY (
      part_a_ts STRING,
      part_b_ts STRING
    )
    STORED AS PARQUET
    LOCATION 'hdfs://files-dev-server-1/user/hive/warehouse/obf_tables/obf_schema/obf_table_name_raw'
    """

    print("=" * 80)
    print("DATABRICKS MANAGED DELTA TABLE DDL")
    print("(Creates empty table structure, no data or location)")
    print("=" * 80)
    print(convert_hive_to_databricks(example_ddl))

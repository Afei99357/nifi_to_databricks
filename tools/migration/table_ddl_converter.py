"""
Table DDL Converter - Hive/HDFS to Databricks

Converts Hive external table DDLs to Databricks-compatible Delta table DDLs.
Handles path conversion, type optimization, and partitioning strategy.
"""

from typing import Dict, List, Optional, Tuple


def convert_hdfs_to_databricks_path(hdfs_path: str, target_type: str = "dbfs") -> str:
    """
    Convert HDFS path to Databricks-compatible path.

    Args:
        hdfs_path: Original HDFS path
        target_type: Target storage type - 'dbfs', 'azure', 'aws', 's3', or 'unity_catalog'

    Returns:
        Converted path string
    """
    # Remove hdfs:// prefix and extract the base path
    base_path = hdfs_path.replace("hdfs://", "").split("/", 1)[-1]

    if target_type == "dbfs":
        return f"dbfs:/mnt/warehouse/{base_path}"
    elif target_type == "azure":
        # Replace with your Azure storage account details
        return f"abfss://<container>@<storage_account>.dfs.core.windows.net/{base_path}"
    elif target_type in ["aws", "s3"]:
        # Replace with your S3 bucket details
        return f"s3://<bucket_name>/{base_path}"
    elif target_type == "unity_catalog":
        # Extract table path components for Unity Catalog
        parts = base_path.split("/")
        return f"/Volumes/<catalog>/<schema>/<volume>/{'/'.join(parts[-2:])}"
    else:
        raise ValueError(
            f"Unknown target_type: {target_type}. Use 'dbfs', 'azure', 'aws', 's3', or 'unity_catalog'"
        )


def optimize_timestamp_columns(
    columns: List[Tuple[str, str]], convert_timestamps: bool = True
) -> List[Tuple[str, str]]:
    """
    Optimize column types, especially converting STRING to TIMESTAMP where appropriate.

    Args:
        columns: List of (column_name, column_type) tuples
        convert_timestamps: Whether to convert _ts STRING columns to TIMESTAMP

    Returns:
        List of optimized (column_name, column_type) tuples
    """
    optimized = []
    for col_name, col_type in columns:
        new_type = col_type

        if convert_timestamps and col_name.endswith("_ts") and col_type == "STRING":
            new_type = "TIMESTAMP"
        elif convert_timestamps and col_name.startswith("part_") and "_ts" in col_name:
            # Partition timestamp columns might be better as DATE or STRING
            # Keep as STRING for flexibility, or use DATE if daily partitions
            new_type = "STRING"  # or "DATE" depending on granularity

        optimized.append((col_name, new_type))

    return optimized


def generate_external_parquet_ddl(
    schema_name: str,
    table_name: str,
    columns: List[Tuple[str, str]],
    partition_columns: List[Tuple[str, str]],
    location: str,
) -> str:
    """
    Generate DDL for external Parquet table (minimal changes from Hive).

    Args:
        schema_name: Database/schema name
        table_name: Table name
        columns: List of (column_name, column_type) tuples
        partition_columns: List of (partition_column_name, partition_type) tuples
        location: Databricks storage location

    Returns:
        DDL string
    """
    col_defs = ",\n  ".join([f"{name} {dtype}" for name, dtype in columns])
    part_defs = ",\n  ".join([f"{name} {dtype}" for name, dtype in partition_columns])

    ddl = f"""-- External Parquet Table (Direct Migration)
CREATE EXTERNAL TABLE {schema_name}.{table_name} (
  {col_defs}
)
PARTITIONED BY (
  {part_defs}
)
STORED AS PARQUET
LOCATION '{location}';

-- Repair partitions (if data already exists)
MSCK REPAIR TABLE {schema_name}.{table_name};
"""
    return ddl


def generate_delta_table_ddl(
    schema_name: str,
    table_name: str,
    columns: List[Tuple[str, str]],
    partition_columns: List[Tuple[str, str]],
    location: str,
    optimize_types: bool = True,
    table_properties: Optional[Dict[str, str]] = None,
) -> str:
    """
    Generate DDL for managed Delta table (recommended for Databricks).

    Args:
        schema_name: Database/schema name
        table_name: Table name
        columns: List of (column_name, column_type) tuples
        partition_columns: List of (partition_column_name, partition_type) tuples
        location: Databricks storage location
        optimize_types: Whether to optimize column types (e.g., STRING -> TIMESTAMP)
        table_properties: Optional table properties dict

    Returns:
        DDL string
    """
    # Combine all columns (data + partition columns for Delta)
    all_columns = columns + partition_columns

    if optimize_types:
        all_columns = optimize_timestamp_columns(all_columns)

    col_defs = ",\n  ".join([f"{name} {dtype}" for name, dtype in all_columns])
    part_names = ", ".join([name for name, _ in partition_columns])

    # Build table properties
    props = table_properties or {}
    default_props = {
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
    }
    props = {**default_props, **props}
    props_str = ",\n  ".join([f"'{k}' = '{v}'" for k, v in props.items()])

    ddl = f"""-- Delta Table (Recommended for Databricks)
CREATE TABLE {schema_name}.{table_name} (
  {col_defs}
)
USING DELTA
PARTITIONED BY ({part_names})
LOCATION '{location}'
TBLPROPERTIES (
  {props_str}
);

-- Optional: Add table comment
COMMENT ON TABLE {schema_name}.{table_name} IS 'Migrated from Hive external table';

-- Optional: Optimize table for better query performance
OPTIMIZE {schema_name}.{table_name};

-- Optional: Z-order by commonly filtered columns
-- OPTIMIZE {schema_name}.{table_name} ZORDER BY (column1, column2);
"""
    return ddl


def generate_conversion_query(
    source_table: str,
    target_table: str,
    timestamp_conversions: Optional[Dict[str, str]] = None,
) -> str:
    """
    Generate SQL to convert data from source to target with type conversions.

    Args:
        source_table: Source table name (schema.table)
        target_table: Target table name (schema.table)
        timestamp_conversions: Dict mapping column_name -> timestamp_format

    Returns:
        SQL query string
    """
    if timestamp_conversions:
        # Generate column list with conversions
        conversions = []
        for col_name, format_str in timestamp_conversions.items():
            conversions.append(
                f"  to_timestamp({col_name}, '{format_str}') AS {col_name}"
            )

        # Add other columns as-is
        select_clause = ",\n".join(conversions) + ",\n  *"  # Add all other columns
    else:
        select_clause = "*"

    query = f"""-- Convert data from source to target table
INSERT OVERWRITE {target_table}
SELECT {select_clause}
FROM {source_table};

-- Verify row count
SELECT
  'Source' AS table_type,
  COUNT(*) AS row_count
FROM {source_table}
UNION ALL
SELECT
  'Target' AS table_type,
  COUNT(*) AS row_count
FROM {target_table};
"""
    return query


def generate_full_migration_ddl(
    schema_name: str,
    table_name: str,
    columns: List[Tuple[str, str]],
    partition_columns: List[Tuple[str, str]],
    hdfs_location: str,
    target_storage: str = "dbfs",
    migration_approach: str = "delta",
) -> str:
    """
    Generate complete migration DDL with all options.

    Args:
        schema_name: Database/schema name
        table_name: Table name
        columns: List of (column_name, column_type) tuples
        partition_columns: List of (partition_column_name, partition_type) tuples
        hdfs_location: Original HDFS location
        target_storage: Target storage type
        migration_approach: 'external' or 'delta'

    Returns:
        Complete DDL script
    """
    databricks_location = convert_hdfs_to_databricks_path(hdfs_location, target_storage)

    ddl = f"""-- ============================================================
-- Table Migration DDL: {schema_name}.{table_name}
-- Source: HDFS External Table
-- Target: Databricks {'Delta' if migration_approach == 'delta' else 'External Parquet'}
-- ============================================================

-- Step 1: Create schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS {schema_name};

-- Step 2: Create table
"""

    if migration_approach == "external":
        ddl += generate_external_parquet_ddl(
            schema_name, table_name, columns, partition_columns, databricks_location
        )
    else:
        ddl += generate_delta_table_ddl(
            schema_name, table_name, columns, partition_columns, databricks_location
        )

    ddl += f"""
-- Step 3: Verify table creation
DESCRIBE EXTENDED {schema_name}.{table_name};

-- Step 4: Check partitions
SHOW PARTITIONS {schema_name}.{table_name};

-- Step 5: Sample data query
SELECT * FROM {schema_name}.{table_name} LIMIT 10;

-- ============================================================
-- Notes:
-- Original HDFS location: {hdfs_location}
-- New Databricks location: {databricks_location}
-- ============================================================
"""
    return ddl


# Example usage
if __name__ == "__main__":
    # Example: Convert the provided DDL
    schema = "obf_schema"
    table = "obf_table_raw"

    # Define columns (from the original DDL)
    data_columns = [
        ("col_a", "STRING"),
        ("col_b_ts", "STRING"),
        ("col_c_ts", "STRING"),
        ("col_d", "INT"),
        ("col_e", "STRING"),
        ("col_f", "INT"),
        ("col_g", "STRING"),
        ("col_h", "DOUBLE"),
        ("col_i", "DOUBLE"),
        ("col_j", "DOUBLE"),
        ("col_k", "DOUBLE"),
        ("col_l_ts", "STRING"),
        ("col_m", "INT"),
        ("col_n", "STRING"),
        ("col_o", "STRING"),
        ("col_p_ts", "STRING"),
        ("col_q_ts", "STRING"),
        ("col_r", "STRING"),
        ("col_s", "STRING"),
        ("col_t", "DOUBLE"),
        ("col_u", "DOUBLE"),
        ("col_v", "DOUBLE"),
        ("col_w", "STRING"),
        ("col_x_ts", "STRING"),
        ("col_y", "INT"),
        ("col_z_ts", "STRING"),
    ]

    partition_cols = [
        ("part_a_ts", "STRING"),
        ("part_b_ts", "STRING"),
        ("part_suffix", "STRING"),
    ]

    hdfs_path = "hdfs://files-dev-server-1/user/hive/warehouse/obf_tables/obf_schema/obf_table_name_raw"

    # Generate Delta table DDL
    print("=" * 80)
    print("DELTA TABLE DDL (RECOMMENDED)")
    print("=" * 80)
    print(
        generate_full_migration_ddl(
            schema, table, data_columns, partition_cols, hdfs_path, "dbfs", "delta"
        )
    )

    print("\n" + "=" * 80)
    print("EXTERNAL PARQUET TABLE DDL (MINIMAL CHANGES)")
    print("=" * 80)
    print(
        generate_full_migration_ddl(
            schema, table, data_columns, partition_cols, hdfs_path, "dbfs", "external"
        )
    )

# Databricks notebook source
# MAGIC %md
# MAGIC # Table Migration: Hive/HDFS to Databricks
# MAGIC
# MAGIC This notebook migrates external Hive tables from HDFS to Databricks Delta tables.
# MAGIC
# MAGIC ## Migration Steps
# MAGIC 1. Configure source and target details
# MAGIC 2. Read data from HDFS/Hive
# MAGIC 3. Convert and write to Delta format
# MAGIC 4. Create Delta table
# MAGIC 5. Verify migration
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Access to source HDFS/Hive data
# MAGIC - Target storage location configured
# MAGIC - Appropriate permissions

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

import sys
from pathlib import Path
from typing import Any, Dict, List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

# Type stubs for Databricks-specific functions
try:
    dbutils  # type: ignore
    display  # type: ignore
    spark  # type: ignore
except NameError:
    pass  # Only available in Databricks environment


# Configuration helper functions
def _has_dbutils():
    try:
        dbutils  # type: ignore  # noqa: F821
        return True
    except NameError:
        return False


def get_arg(name: str, default: str = "") -> str:
    if not _has_dbutils():
        return default
    try:
        return dbutils.widgets.get(name)  # type: ignore  # noqa: F821
    except Exception:
        return default


def ensure_text_widget(name: str, default: str, label: str):
    if not _has_dbutils():
        return
    try:
        _ = dbutils.widgets.get(name)  # type: ignore  # noqa: F821
    except Exception:
        dbutils.widgets.text(name, default, label)  # type: ignore  # noqa: F821


# Create widgets
ensure_text_widget("source_path", "", "1. Source HDFS/Hive Path")
ensure_text_widget("target_path", "dbfs:/mnt/warehouse/", "2. Target Databricks Path")
ensure_text_widget("schema_name", "", "3. Target Schema Name")
ensure_text_widget("table_name", "", "4. Target Table Name")
ensure_text_widget("partition_columns", "", "5. Partition Columns (comma-separated)")
ensure_text_widget(
    "convert_timestamps", "true", "6. Convert _ts columns to TIMESTAMP (true/false)"
)

# Get values
source_path = get_arg("source_path")
target_path = get_arg("target_path")
schema_name = get_arg("schema_name")
table_name = get_arg("table_name")
partition_cols_str = get_arg("partition_columns", "")
convert_timestamps = get_arg("convert_timestamps", "true").lower() in {
    "true",
    "1",
    "yes",
}

# Parse partition columns
partition_columns = (
    [col.strip() for col in partition_cols_str.split(",") if col.strip()]
    if partition_cols_str
    else []
)

print("✓ Configuration loaded:")
print(f"  Source Path:        {source_path}")
print(f"  Target Path:        {target_path}")
print(f"  Schema:             {schema_name}")
print(f"  Table:              {table_name}")
print(f"  Partition Columns:  {partition_columns}")
print(f"  Convert Timestamps: {convert_timestamps}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Read Source Data

# COMMAND ----------


def read_source_data(path: str, source_format: str = "parquet") -> DataFrame:
    """
    Read data from source location.

    Args:
        path: Source path (HDFS, Hive, etc.)
        source_format: Source format (parquet, orc, etc.)

    Returns:
        DataFrame with source data
    """
    print(f"Reading data from: {path}")

    # Try reading as Parquet first (most common for external tables)
    try:
        df = spark.read.format(source_format).load(path)
        print(f"✓ Successfully read {df.count():,} rows from {source_format} format")
        return df
    except Exception as e:
        print(f"✗ Error reading as {source_format}: {e}")
        raise


# Read source data
try:
    source_df = read_source_data(source_path)
    print("\nSource schema:")
    source_df.printSchema()
    print(f"\nSample data (first 5 rows):")
    display(source_df.limit(5))
except Exception as e:
    print(f"✗ Failed to read source data: {e}")
    dbutils.notebook.exit(f"Failed to read source: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Transform Data (Optional Type Conversions)

# COMMAND ----------


def convert_timestamp_columns(
    df: DataFrame, timestamp_format: str = "yyyy-MM-dd HH:mm:ss"
) -> DataFrame:
    """
    Convert STRING columns ending with '_ts' to TIMESTAMP type.

    Args:
        df: Input DataFrame
        timestamp_format: Format string for timestamp parsing

    Returns:
        DataFrame with converted timestamp columns
    """
    converted_df = df
    converted_cols = []

    for field in df.schema.fields:
        col_name = field.name
        col_type = str(field.dataType)

        # Convert _ts STRING columns to TIMESTAMP
        if col_name.endswith("_ts") and "String" in col_type:
            print(f"Converting {col_name} from STRING to TIMESTAMP")
            converted_df = converted_df.withColumn(
                col_name, F.to_timestamp(F.col(col_name), timestamp_format)
            )
            converted_cols.append(col_name)

    if converted_cols:
        print(f"\n✓ Converted {len(converted_cols)} columns to TIMESTAMP:")
        for col in converted_cols:
            print(f"  - {col}")
    else:
        print("\nℹ No timestamp columns to convert")

    return converted_df


# Apply transformations
if convert_timestamps:
    print("Applying timestamp conversions...")
    transformed_df = convert_timestamp_columns(source_df)
    print("\nTransformed schema:")
    transformed_df.printSchema()
else:
    print("Skipping timestamp conversions (convert_timestamps=false)")
    transformed_df = source_df

# Show sample of transformed data
print("\nTransformed data sample:")
display(transformed_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Write to Delta Format

# COMMAND ----------


def write_delta_table(
    df: DataFrame,
    target_path: str,
    partition_cols: List[str],
    mode: str = "overwrite",
) -> None:
    """
    Write DataFrame to Delta format with partitioning.

    Args:
        df: DataFrame to write
        target_path: Target Delta table path
        partition_cols: List of partition column names
        mode: Write mode (overwrite, append, etc.)
    """
    print(f"Writing data to Delta format at: {target_path}")
    print(f"Partition columns: {partition_cols}")
    print(f"Write mode: {mode}")

    writer = df.write.format("delta").mode(mode)

    if partition_cols:
        writer = writer.partitionBy(*partition_cols)

    # Enable auto-optimization
    writer = writer.option("overwriteSchema", "true")

    writer.save(target_path)

    row_count = df.count()
    print(f"✓ Successfully wrote {row_count:,} rows to Delta table")


# Construct full target path
full_target_path = f"{target_path.rstrip('/')}/{schema_name}/{table_name}"

# Write data
try:
    write_delta_table(transformed_df, full_target_path, partition_columns)
except Exception as e:
    print(f"✗ Failed to write Delta table: {e}")
    dbutils.notebook.exit(f"Failed to write Delta: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Create Delta Table in Metastore

# COMMAND ----------

# Create schema if it doesn't exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
print(f"✓ Schema '{schema_name}' ready")

# Create table
full_table_name = f"{schema_name}.{table_name}"

# Drop table if it exists (for clean migration)
try:
    spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
    print(f"✓ Dropped existing table '{full_table_name}' (if existed)")
except Exception:
    pass

# Create Delta table pointing to the data
create_table_sql = f"""
CREATE TABLE {full_table_name}
USING DELTA
LOCATION '{full_target_path}'
"""

spark.sql(create_table_sql)
print(f"✓ Created Delta table: {full_table_name}")

# Set table properties for optimization
spark.sql(
    f"""
ALTER TABLE {full_table_name}
SET TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
)
"""
)
print("✓ Enabled auto-optimization")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Verification and Statistics

# COMMAND ----------

print("=" * 80)
print(f"MIGRATION COMPLETE: {full_table_name}")
print("=" * 80)

# Show table details
print("\n1. Table Description:")
display(spark.sql(f"DESCRIBE EXTENDED {full_table_name}"))

# Show partitions
if partition_columns:
    print(f"\n2. Partitions:")
    display(spark.sql(f"SHOW PARTITIONS {full_table_name}"))

# Row count
row_count = spark.sql(f"SELECT COUNT(*) as count FROM {full_table_name}").collect()[0][
    0
]
print(f"\n3. Total Rows: {row_count:,}")

# Sample data
print("\n4. Sample Data:")
display(spark.sql(f"SELECT * FROM {full_table_name} LIMIT 10"))

# Schema
print("\n5. Table Schema:")
spark.sql(f"DESCRIBE {full_table_name}").show(100, False)

# Partition statistics
if partition_columns:
    print("\n6. Partition Statistics:")
    partition_group = ", ".join(partition_columns)
    stats_sql = f"""
    SELECT {partition_group}, COUNT(*) as row_count
    FROM {full_table_name}
    GROUP BY {partition_group}
    ORDER BY {partition_group}
    """
    display(spark.sql(stats_sql))

print("\n✓ Verification complete!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Optional: Optimize Table

# COMMAND ----------

print(f"Optimizing {full_table_name}...")

# Run OPTIMIZE
spark.sql(f"OPTIMIZE {full_table_name}")
print("✓ Table optimized")

# Optional: Z-ORDER by commonly filtered columns
# Uncomment and modify for your use case
# z_order_cols = ["col_a", "col_b_ts"]  # Replace with your columns
# spark.sql(f"OPTIMIZE {full_table_name} ZORDER BY ({', '.join(z_order_cols)})")
# print(f"✓ Z-ordered by: {z_order_cols}")

# Show table statistics after optimization
print("\nTable statistics after optimization:")
display(spark.sql(f"DESCRIBE DETAIL {full_table_name}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Migration Summary

# COMMAND ----------

summary = f"""
{'='*80}
MIGRATION SUMMARY
{'='*80}

Source Path:          {source_path}
Target Path:          {full_target_path}
Table Name:           {full_table_name}
Partition Columns:    {', '.join(partition_columns) if partition_columns else 'None'}
Timestamp Conversion: {'Yes' if convert_timestamps else 'No'}
Total Rows:           {row_count:,}

Next Steps:
1. Verify data integrity with sample queries
2. Update downstream applications to use new table
3. Test query performance
4. Update NiFi flows to write to new Delta location
5. Monitor for any issues
6. Decommission old HDFS table after validation period

Query Example:
  SELECT * FROM {full_table_name} WHERE <your_conditions>;

{'='*80}
"""

print(summary)

# Return success
dbutils.notebook.exit("Migration completed successfully")

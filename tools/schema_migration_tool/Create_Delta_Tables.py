# Databricks notebook source
# MAGIC %md
# MAGIC # Create Delta Tables from Hive DDL
# MAGIC
# MAGIC This notebook converts Hive/Impala DDL statements to Databricks Delta tables.
# MAGIC
# MAGIC **Features:**
# MAGIC - Automatic type optimization (STRING `_ts` columns → TIMESTAMP)
# MAGIC - Managed Delta tables with auto-optimization
# MAGIC - Single file or batch processing
# MAGIC - Dry-run mode to preview DDL
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - Hive DDL files uploaded to Volumes
# MAGIC - Appropriate permissions on target catalog/schema
# MAGIC - Active cluster (serverless or all-purpose)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC Import the table creation functions

# COMMAND ----------

import sys

# Add the schema migration tool to Python path
sys.path.append(
    "/Workspace/Users/eliao@bpcs.com/nifi_to_databricks_test/tools/schema_migration_tool"
)

from create_delta_tables import create_tables_from_hive_ddl

print("✓ Imports successful")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC
# MAGIC Set your target catalog and schema

# COMMAND ----------

# Target Databricks catalog and schema
TARGET_CATALOG = "eliao"
TARGET_SCHEMA = "nifi_to_databricks"

print(f"Target location: {TARGET_CATALOG}.{TARGET_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Option 1: Single File Processing
# MAGIC
# MAGIC Convert a single Hive DDL file to Delta table

# COMMAND ----------

# Single file - update the path to your DDL file
SINGLE_FILE_PATH = "/Volumes/eliao/nifi_to_databricks/test_data_files/test.sql"

result = create_tables_from_hive_ddl(
    input_file=SINGLE_FILE_PATH,
    catalog=TARGET_CATALOG,
    schema=TARGET_SCHEMA,
    optimize_types=True,  # Convert _ts columns to TIMESTAMP
)

print(f"\n{'='*80}")
print("RESULT")
print(f"{'='*80}")
print(f"✓ Successfully created: {result['success_count']} table(s)")
print(f"✗ Failed: {result['fail_count']} table(s)")
print(f"Total processed: {result['total']} file(s)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Single File - Dry Run
# MAGIC
# MAGIC Preview the DDL without creating tables

# COMMAND ----------

# Dry run - just show what would be created
result = create_tables_from_hive_ddl(
    input_file=SINGLE_FILE_PATH,
    catalog=TARGET_CATALOG,
    schema=TARGET_SCHEMA,
    optimize_types=True,
    dry_run=True,  # Only preview, don't create
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Option 2: Batch Processing
# MAGIC
# MAGIC Process multiple DDL files from a directory

# COMMAND ----------

# Batch processing - update the path to your directory containing .sql files
BATCH_DIRECTORY = "/Volumes/eliao/nifi_to_databricks/hive_ddls/"

result = create_tables_from_hive_ddl(
    input_dir=BATCH_DIRECTORY,
    catalog=TARGET_CATALOG,
    schema=TARGET_SCHEMA,
    optimize_types=True,  # Convert _ts columns to TIMESTAMP
)

print(f"\n{'='*80}")
print("BATCH PROCESSING RESULT")
print(f"{'='*80}")
print(f"✓ Successfully created: {result['success_count']} table(s)")
print(f"✗ Failed: {result['fail_count']} table(s)")
print(f"Total processed: {result['total']} file(s)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Batch Processing - Dry Run
# MAGIC
# MAGIC Preview all tables without creating them

# COMMAND ----------

# Dry run for batch - see DDL for all files
result = create_tables_from_hive_ddl(
    input_dir=BATCH_DIRECTORY,
    catalog=TARGET_CATALOG,
    schema=TARGET_SCHEMA,
    optimize_types=True,
    dry_run=True,  # Only preview, don't create
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Created Tables
# MAGIC
# MAGIC Check that tables were created successfully

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Show all tables in the target schema
# MAGIC SHOW TABLES IN eliao.nifi_to_databricks;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Describe a specific table (update table name)
# MAGIC DESCRIBE EXTENDED eliao.nifi_to_databricks.obf_table_raw;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Advanced Options

# COMMAND ----------

# MAGIC %md
# MAGIC ### Without Type Optimization
# MAGIC
# MAGIC Keep _ts columns as STRING instead of converting to TIMESTAMP

# COMMAND ----------

# Process without type optimization
result = create_tables_from_hive_ddl(
    input_file=SINGLE_FILE_PATH,
    catalog=TARGET_CATALOG,
    schema=TARGET_SCHEMA,
    optimize_types=False,  # Keep original STRING types
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Custom Catalog and Schema
# MAGIC
# MAGIC Create tables in different catalog/schema combinations

# COMMAND ----------

# Create in dev environment
result_dev = create_tables_from_hive_ddl(
    input_file=SINGLE_FILE_PATH, catalog="dev", schema="bronze"
)

# Create in prod environment
result_prod = create_tables_from_hive_ddl(
    input_file=SINGLE_FILE_PATH, catalog="prod", schema="bronze"
)

print(f"Dev: {result_dev['success_count']} tables created")
print(f"Prod: {result_prod['success_count']} tables created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Troubleshooting

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check if schema exists

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW SCHEMAS IN eliao LIKE 'nifi*';

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check catalog permissions

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW GRANTS ON CATALOG eliao;

# COMMAND ----------

# MAGIC %md
# MAGIC ### View table properties

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View Delta table properties
# MAGIC SHOW TBLPROPERTIES eliao.nifi_to_databricks.obf_table_raw;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notes
# MAGIC
# MAGIC - Tables are created as **managed Delta tables** (no LOCATION clause)
# MAGIC - Partitioning is preserved from the Hive table
# MAGIC - Auto-optimization is enabled by default
# MAGIC - Tables are created **empty** (structure only, no data)
# MAGIC - To load data, use separate `INSERT INTO` or data migration tools

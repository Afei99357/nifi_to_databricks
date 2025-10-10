"""
Simple Example: How to use the table DDL converter

Run this script to generate Databricks DDL for your own tables.
"""

from table_ddl_converter import generate_full_migration_ddl

# =============================================================================
# STEP 1: Define your table structure
# =============================================================================

# Your schema and table name
SCHEMA_NAME = "my_database"
TABLE_NAME = "my_table"

# Define your columns as a list of (column_name, column_type) tuples
# Get these from: DESCRIBE your_hive_table;
DATA_COLUMNS = [
    ("id", "INT"),
    ("customer_name", "STRING"),
    ("email", "STRING"),
    ("created_date", "STRING"),  # Will be converted to TIMESTAMP
    ("last_updated_ts", "STRING"),  # Will be converted to TIMESTAMP
    ("amount", "DOUBLE"),
    ("status", "STRING"),
]

# Define your partition columns
# Get these from: SHOW PARTITIONS your_hive_table;
PARTITION_COLUMNS = [
    ("year", "INT"),
    ("month", "INT"),
]

# Your original HDFS location
# Get this from: DESCRIBE EXTENDED your_hive_table;
HDFS_LOCATION = "hdfs://namenode:8020/user/hive/warehouse/my_database.db/my_table"

# =============================================================================
# STEP 2: Choose your target storage
# =============================================================================

# Options:
# - "dbfs" - Databricks File System
# - "azure" - Azure Blob Storage
# - "aws" or "s3" - AWS S3
# - "unity_catalog" - Unity Catalog Volumes

TARGET_STORAGE = "dbfs"

# =============================================================================
# STEP 3: Choose migration approach
# =============================================================================

# Options:
# - "external" - External Parquet table (quick, minimal changes)
# - "delta" - Delta table (recommended, better performance and features)

MIGRATION_APPROACH = "delta"

# =============================================================================
# STEP 4: Generate DDL
# =============================================================================

print("=" * 80)
print(f"Generating DDL for: {SCHEMA_NAME}.{TABLE_NAME}")
print("=" * 80)

ddl = generate_full_migration_ddl(
    schema_name=SCHEMA_NAME,
    table_name=TABLE_NAME,
    columns=DATA_COLUMNS,
    partition_columns=PARTITION_COLUMNS,
    hdfs_location=HDFS_LOCATION,
    target_storage=TARGET_STORAGE,
    migration_approach=MIGRATION_APPROACH,
)

print(ddl)

# =============================================================================
# STEP 5: Save to file (optional)
# =============================================================================

output_file = f"{TABLE_NAME}_migration.sql"
with open(output_file, "w") as f:
    f.write(ddl)

print("\n" + "=" * 80)
print(f"DDL saved to: {output_file}")
print("=" * 80)

# =============================================================================
# NEXT STEPS
# =============================================================================

print(
    """
Next steps:
1. Review the generated SQL file
2. Upload to Databricks and run the CREATE TABLE statement
3. Use the databricks_table_migration.py notebook to migrate data
4. Or manually copy data: INSERT INTO target_table SELECT * FROM source_table

For data migration with the notebook, configure these widgets:
- source_path: {hdfs}
- target_path: dbfs:/mnt/warehouse/
- schema_name: {schema}
- table_name: {table}
- partition_columns: {parts}
- convert_timestamps: true
""".format(
        hdfs=HDFS_LOCATION,
        schema=SCHEMA_NAME,
        table=TABLE_NAME,
        parts=",".join([col[0] for col in PARTITION_COLUMNS]),
    )
)

# Table Migration Tools: Hive/HDFS to Databricks

This directory contains tools for migrating external Hive tables from HDFS to Databricks Delta tables.

## Contents

1. **`table_ddl_converter.py`** - Python script to generate Databricks-compatible DDL from Hive DDL
2. **`databricks_table_migration.py`** - Databricks notebook for end-to-end table migration
3. **`README.md`** - This documentation

## Quick Start

### Option 1: Generate DDL Only (No Data Migration)

Use this when you just need the table structure in Databricks:

```bash
# Run the DDL converter
cd tools/migration
python table_ddl_converter.py
```

This will output:
- Delta table DDL (recommended)
- External Parquet table DDL (minimal changes)

### Option 2: Full Migration with Data

Use the Databricks notebook for complete migration:

1. Upload `databricks_table_migration.py` to Databricks Workspace
2. Open the notebook in Databricks
3. Configure parameters in the widgets
4. Run all cells

## Detailed Usage

### 1. DDL Converter (`table_ddl_converter.py`)

#### Basic Usage

```python
from tools.migration.table_ddl_converter import generate_full_migration_ddl

# Define your table structure
schema = "my_schema"
table = "my_table"

data_columns = [
    ("col_a", "STRING"),
    ("col_b", "INT"),
    ("col_c_ts", "STRING"),  # Will be converted to TIMESTAMP if optimize_types=True
]

partition_columns = [
    ("date_partition", "STRING"),
]

hdfs_path = "hdfs://namenode:8020/user/hive/warehouse/my_schema.db/my_table"

# Generate Delta table DDL
ddl = generate_full_migration_ddl(
    schema_name=schema,
    table_name=table,
    columns=data_columns,
    partition_columns=partition_columns,
    hdfs_location=hdfs_path,
    target_storage="dbfs",  # or "azure", "aws", "unity_catalog"
    migration_approach="delta"  # or "external"
)

print(ddl)
```

#### Path Conversion Options

```python
from tools.migration.table_ddl_converter import convert_hdfs_to_databricks_path

hdfs_path = "hdfs://namenode/user/hive/warehouse/schema/table"

# DBFS (Databricks File System)
dbfs_path = convert_hdfs_to_databricks_path(hdfs_path, "dbfs")
# Result: "dbfs:/mnt/warehouse/user/hive/warehouse/schema/table"

# Azure Blob Storage
azure_path = convert_hdfs_to_databricks_path(hdfs_path, "azure")
# Result: "abfss://<container>@<storage_account>.dfs.core.windows.net/..."

# AWS S3
s3_path = convert_hdfs_to_databricks_path(hdfs_path, "s3")
# Result: "s3://<bucket_name>/..."

# Unity Catalog Volume
uc_path = convert_hdfs_to_databricks_path(hdfs_path, "unity_catalog")
# Result: "/Volumes/<catalog>/<schema>/<volume>/..."
```

#### Type Optimization

```python
from tools.migration.table_ddl_converter import optimize_timestamp_columns

columns = [
    ("id", "INT"),
    ("created_ts", "STRING"),  # Will be converted
    ("updated_ts", "STRING"),  # Will be converted
    ("name", "STRING"),  # Will stay as STRING
]

optimized = optimize_timestamp_columns(columns, convert_timestamps=True)
# Result: [("id", "INT"), ("created_ts", "TIMESTAMP"),
#          ("updated_ts", "TIMESTAMP"), ("name", "STRING")]
```

### 2. Databricks Migration Notebook

#### Parameters

Configure these widgets in the notebook:

| Parameter | Description | Example |
|-----------|-------------|---------|
| `source_path` | Source HDFS/Hive path | `hdfs://namenode/user/hive/warehouse/schema/table` |
| `target_path` | Target Databricks path | `dbfs:/mnt/warehouse/` |
| `schema_name` | Target schema name | `production` |
| `table_name` | Target table name | `my_table` |
| `partition_columns` | Comma-separated partition columns | `date_partition,region` |
| `convert_timestamps` | Convert _ts STRING columns to TIMESTAMP | `true` |

#### Migration Steps

The notebook performs these steps automatically:

1. **Configuration** - Load parameters from widgets
2. **Read Source** - Read data from HDFS/Hive
3. **Transform** - Convert timestamp columns (optional)
4. **Write Delta** - Write data in Delta format with partitioning
5. **Create Table** - Register table in metastore
6. **Verify** - Run verification queries
7. **Optimize** - Run OPTIMIZE and optional Z-ORDER
8. **Summary** - Display migration summary

#### Example Workflow

```python
# 1. Configure widgets (done in notebook UI)
source_path = "hdfs://namenode/warehouse/obf_schema.db/obf_table_raw"
target_path = "dbfs:/mnt/warehouse/"
schema_name = "obf_schema"
table_name = "obf_table_raw"
partition_columns = "part_a_ts,part_b_ts,part_suffix"
convert_timestamps = "true"

# 2. Run all cells
# The notebook will:
#   - Read ~1M rows from source
#   - Convert 10 _ts columns to TIMESTAMP
#   - Write Delta with 3 partition columns
#   - Create table obf_schema.obf_table_raw
#   - Run OPTIMIZE
#   - Display statistics
```

## Migration Strategies

### Strategy 1: Lift and Shift (External Parquet)

**When to use:**
- Quick migration needed
- Data stays in original format
- Minimal changes to queries

**Pros:**
- Fastest migration
- No data movement
- Nearly identical to Hive

**Cons:**
- No Delta benefits (ACID, time travel, etc.)
- No type optimization
- Limited performance improvements

**DDL:**
```sql
CREATE EXTERNAL TABLE schema.table (...)
STORED AS PARQUET
LOCATION 'dbfs:/mnt/warehouse/...';
```

### Strategy 2: Convert to Delta (Recommended)

**When to use:**
- Long-term production use
- Need ACID transactions
- Want performance optimization

**Pros:**
- Full Delta features (ACID, time travel, schema evolution)
- Better query performance
- Auto-optimization
- Updates/deletes supported

**Cons:**
- Requires data copy/conversion
- Takes longer for large tables
- May need type adjustments

**DDL:**
```sql
CREATE TABLE schema.table (...)
USING DELTA
PARTITIONED BY (...)
LOCATION 'dbfs:/mnt/warehouse/...';
```

### Strategy 3: Delta with Type Optimization

**When to use:**
- Building for long-term
- Want best practices
- Can tolerate schema changes

**Pros:**
- All Delta benefits
- Proper data types (TIMESTAMP instead of STRING)
- Better compression
- More efficient queries

**Cons:**
- Longest migration time
- Requires testing type conversions
- May need query updates

**Example Conversions:**
```sql
-- Before (Hive)
col_created_ts STRING  -- "2024-01-01 10:30:00"

-- After (Databricks optimized)
col_created_ts TIMESTAMP  -- Proper timestamp type
```

## Common Migration Patterns

### Pattern 1: Daily Partitioned Tables

```python
# Original Hive table
partition_columns = [("date_partition", "STRING")]  # Format: "2024-01-01"

# Databricks Delta - keep as STRING or convert to DATE
partition_columns = [("date_partition", "DATE")]  # More efficient
```

### Pattern 2: Multi-Level Partitioning

```python
# Original Hive (fine-grained partitioning)
partition_columns = [
    ("year", "INT"),
    ("month", "INT"),
    ("day", "INT"),
    ("region", "STRING")
]

# Databricks Delta - consider simplifying
partition_columns = [("date_partition", "DATE")]  # Simpler
# Use Z-ORDER for secondary dimensions
# OPTIMIZE table ZORDER BY (region)
```

### Pattern 3: Timestamp-Heavy Tables

```python
# Original Hive (all STRING timestamps)
columns = [
    ("event_ts", "STRING"),
    ("created_ts", "STRING"),
    ("updated_ts", "STRING"),
]

# Databricks Delta - convert to proper types
columns = [
    ("event_ts", "TIMESTAMP"),
    ("created_ts", "TIMESTAMP"),
    ("updated_ts", "TIMESTAMP"),
]

# In notebook, specify timestamp format
timestamp_format = "yyyy-MM-dd HH:mm:ss"
```

## Example: Complete Migration

Here's a complete example migrating the table from your DDL:

### Step 1: Analyze Source Table

```sql
-- In Hive/source system
DESCRIBE EXTENDED obf_schema.obf_table_raw;
SHOW PARTITIONS obf_schema.obf_table_raw;
SELECT COUNT(*) FROM obf_schema.obf_table_raw;
```

### Step 2: Generate DDL

```python
# Run this locally or in Databricks
from tools.migration.table_ddl_converter import generate_full_migration_ddl

schema = "obf_schema"
table = "obf_table_raw"

data_columns = [
    ("col_a", "STRING"),
    ("col_b_ts", "STRING"),
    # ... all 26 columns
]

partition_columns = [
    ("part_a_ts", "STRING"),
    ("part_b_ts", "STRING"),
    ("part_suffix", "STRING"),
]

hdfs_path = "hdfs://files-dev-server-1/user/hive/warehouse/obf_tables/obf_schema/obf_table_name_raw"

# Generate Delta DDL
delta_ddl = generate_full_migration_ddl(
    schema, table, data_columns, partition_columns,
    hdfs_path, "dbfs", "delta"
)

# Save to file
with open("obf_table_migration.sql", "w") as f:
    f.write(delta_ddl)
```

### Step 3: Migrate Data (Databricks Notebook)

Upload `databricks_table_migration.py` to Databricks and configure:

```
source_path: hdfs://files-dev-server-1/user/hive/warehouse/obf_tables/obf_schema/obf_table_name_raw
target_path: dbfs:/mnt/warehouse/
schema_name: obf_schema
table_name: obf_table_raw
partition_columns: part_a_ts,part_b_ts,part_suffix
convert_timestamps: true
```

### Step 4: Verify Migration

```sql
-- In Databricks
SELECT COUNT(*) FROM obf_schema.obf_table_raw;

SELECT part_a_ts, part_b_ts, COUNT(*) as row_count
FROM obf_schema.obf_table_raw
GROUP BY part_a_ts, part_b_ts
ORDER BY part_a_ts, part_b_ts;

-- Compare with source
-- Source count: X rows
-- Target count: X rows (should match)
```

### Step 5: Optimize

```sql
-- Run optimization
OPTIMIZE obf_schema.obf_table_raw;

-- Optional: Z-order by frequently filtered columns
OPTIMIZE obf_schema.obf_table_raw ZORDER BY (col_a, part_suffix);

-- Check table statistics
DESCRIBE DETAIL obf_schema.obf_table_raw;
```

### Step 6: Update Applications

1. Update NiFi flows to write to new Delta location
2. Update downstream queries to use new table
3. Test end-to-end
4. Monitor performance

## Troubleshooting

### Issue: "Path does not exist"

**Problem:** Source HDFS path is not accessible from Databricks

**Solution:**
1. Verify HDFS connectivity from Databricks
2. Or copy data to Databricks-accessible storage first:
   ```python
   # Copy from HDFS to DBFS
   dbutils.fs.cp("hdfs://...", "dbfs:/tmp/migration/", recurse=True)
   ```

### Issue: "Cannot parse timestamp"

**Problem:** Timestamp format doesn't match expected format

**Solution:**
Modify the `convert_timestamp_columns` function with your format:
```python
# In notebook cell
timestamp_format = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"  # ISO format
# or
timestamp_format = "MM/dd/yyyy HH:mm:ss"  # US format
```

### Issue: "Too many partitions"

**Problem:** Fine-grained partitioning creates millions of partitions

**Solution:**
Simplify partitioning strategy:
```python
# Instead of: year, month, day, hour
partition_columns = ["date_partition"]  # Just date

# Use Z-ORDER for hour-level filtering
OPTIMIZE table ZORDER BY (hour_column)
```

### Issue: "Schema mismatch"

**Problem:** Column types don't match between source and target

**Solution:**
```python
# Explicitly cast in migration:
transformed_df = source_df.select(
    F.col("col_a").cast("string"),
    F.col("col_b").cast("int"),
    # ... etc
)
```

## Best Practices

1. **Test with subset first** - Migrate a single partition for testing
2. **Verify row counts** - Always compare source vs target counts
3. **Monitor performance** - Compare query times before/after
4. **Keep source data** - Don't delete source until fully validated
5. **Document changes** - Track schema changes and transformations
6. **Use Delta** - Prefer Delta over external tables for production
7. **Optimize regularly** - Run OPTIMIZE on schedule
8. **Enable auto-optimization** - Set table properties for auto-compaction

## Performance Considerations

### Partitioning

```python
# Good: Daily partitions for time-series data
partition_columns = ["date"]

# Avoid: Too many partitions
# partition_columns = ["year", "month", "day", "hour", "region"]

# Use Z-ORDER instead:
OPTIMIZE table ZORDER BY (hour, region)
```

### File Size

```python
# Configure optimal file size (128MB-1GB)
spark.conf.set("spark.sql.files.maxPartitionBytes", "134217728")  # 128MB
```

### Write Performance

```python
# Enable adaptive query execution
spark.conf.set("spark.sql.adaptive.enabled", "true")

# Enable auto-optimization
ALTER TABLE table_name SET TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
)
```

## Resources

- [Databricks Delta Lake Documentation](https://docs.databricks.com/delta/index.html)
- [Table Migration Best Practices](https://docs.databricks.com/migration/index.html)
- [Partitioning Strategies](https://docs.databricks.com/delta/best-practices.html#partitioning)
- [OPTIMIZE and Z-ORDER](https://docs.databricks.com/delta/optimize.html)

## Support

For issues or questions about these migration tools, please refer to the main project documentation or create an issue in the repository.

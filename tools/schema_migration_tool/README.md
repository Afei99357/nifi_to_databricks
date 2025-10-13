# Schema Migration Tool

Convert Hive/Impala tables to Databricks Delta tables directly.

## Overview

This tool creates Databricks Delta tables from Hive DDL statements, automatically handling:
- Type optimization (STRING `_ts` columns → TIMESTAMP)
- Schema conversion (EXTERNAL TABLE → managed Delta table)
- Catalog/schema mapping
- Direct table creation via spark.sql()

## Quick Start

### Single Table

```bash
python create_tables_databricks.py \
  --input hive_table.sql \
  --catalog my_catalog \
  --schema my_schema
```

### Multiple Tables (Batch Processing)

```bash
python create_tables_databricks.py \
  --input-dir hive_ddls/ \
  --catalog my_catalog \
  --schema my_schema
```

### Dry Run (Preview DDL)

```bash
python create_tables_databricks.py \
  --input hive_table.sql \
  --catalog my_catalog \
  --schema my_schema \
  --dry-run
```

## Input Format

**Hive DDL Example:**
```sql
CREATE EXTERNAL TABLE sales.orders (
  order_id INT,
  customer_name STRING,
  order_timestamp_ts STRING,
  total_amount DOUBLE
)
PARTITIONED BY (year INT, month INT)
STORED AS PARQUET
LOCATION 'hdfs://namenode/warehouse/sales.db/orders'
```

## What Gets Created

**Databricks Delta Table:**
```sql
CREATE SCHEMA IF NOT EXISTS my_catalog.my_schema;

CREATE TABLE my_catalog.my_schema.orders (
  order_id INT,
  customer_name STRING,
  order_timestamp_ts TIMESTAMP,  -- Automatically converted
  total_amount DOUBLE,
  year INT,
  month INT
)
USING DELTA
PARTITIONED BY (year, month)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);
```

**Key differences from Hive:**
- ✅ Managed Delta table (no LOCATION clause)
- ✅ STRING `_ts` columns converted to TIMESTAMP
- ✅ Uses specified catalog and schema
- ✅ Auto-optimization enabled
- ✅ Empty table (structure only, no data)

## Usage in Databricks

### Notebook Example

```python
%sh
python3 /Workspace/path/to/create_tables_databricks.py \
  --input /Volumes/catalog/schema/files/hive.sql \
  --catalog prod \
  --schema bronze
```

### Process Multiple Files

```python
%sh
python3 /Workspace/path/to/create_tables_databricks.py \
  --input-dir /Volumes/catalog/schema/hive_ddls/ \
  --catalog prod \
  --schema bronze
```

## Command Options

```bash
Required:
  --catalog     Target Databricks catalog name
  --schema      Target Databricks schema name

Input (choose one):
  -i, --input           Single Hive DDL file
  --input-dir           Directory of Hive DDL files (.sql)

Options:
  --optimize            Optimize types (default: enabled)
  --no-optimize         Keep original types
  --dry-run             Show DDL without creating tables
```

## Features

### Automatic Type Optimization

Columns ending with `_ts` are converted from STRING to TIMESTAMP:
- `created_ts STRING` → `created_ts TIMESTAMP`
- `updated_ts STRING` → `updated_ts TIMESTAMP`

Disable with `--no-optimize` if needed.

### Catalog/Schema Substitution

The tool automatically updates all schema references:
- `CREATE SCHEMA sales` → `CREATE SCHEMA my_catalog.my_schema`
- `CREATE TABLE sales.orders` → `CREATE TABLE my_catalog.my_schema.orders`

### Batch Processing

Process entire directories of DDL files:
```bash
hive_ddls/
├── table1.sql
├── table2.sql
└── table3.sql

# Creates all tables at once
python create_tables_databricks.py \
  --input-dir hive_ddls/ \
  --catalog prod \
  --schema bronze
```

## Workflow

### Step 1: Extract Hive DDL

In Hive/Impala:
```sql
SHOW CREATE TABLE my_schema.my_table;
```

Save output to a `.sql` file.

### Step 2: Create Table in Databricks

```bash
python create_tables_databricks.py \
  -i my_table.sql \
  --catalog prod \
  --schema bronze
```

### Step 3: Verify (Optional)

In Databricks:
```sql
SHOW CREATE TABLE prod.bronze.my_table;
DESCRIBE EXTENDED prod.bronze.my_table;
```

### Step 4: Load Data (Separate Step)

```sql
-- Copy data from Hive (if needed)
INSERT INTO prod.bronze.my_table
SELECT * FROM hive_metastore.old_schema.my_table;
```

## Architecture

**Files:**
- `create_tables_databricks.py` - Main tool (creates tables directly)
- `hive_ddl_parser.py` - Parses Hive DDL using regex
- `databricks_ddl_generator.py` - Generates Databricks DDL (used internally)

**Flow:**
```
Hive DDL → parse_hive_ddl() → convert_to_databricks_ddl() → spark.sql() → Table created
```

## Limitations

- Only supports `CREATE EXTERNAL TABLE` statements
- Complex nested types (STRUCT, ARRAY, MAP) are passed through as-is
- Hive table properties (TBLPROPERTIES) are not migrated
- SerDe properties are not converted
- This creates table structure only (no data migration)

## Best Practices

1. **Use Dry Run First**: Test with `--dry-run` to verify DDL before creating
2. **Start with Development**: Create tables in dev catalog first
3. **Batch Processing**: Process multiple tables together for efficiency
4. **Verify Results**: Check created tables with `SHOW CREATE TABLE`
5. **Document Mappings**: Keep track of Hive → Databricks schema mappings

## Troubleshooting

### Issue: "ImportError: SparkSession"

**Problem:** Script not running in Databricks environment

**Solution:** This script must be run in Databricks notebooks or jobs with PySpark available

### Issue: "Table already exists"

**Problem:** Table already created in target catalog/schema

**Solution:** Drop existing table first or use different schema name

### Issue: Columns not converted to TIMESTAMP

**Problem:** Columns don't end with `_ts`

**Solution:** Use `--no-optimize` and manually update types, or rename columns to follow `*_ts` convention

### Issue: Wrong catalog/schema

**Problem:** Tables created in wrong location

**Solution:** Double-check `--catalog` and `--schema` parameters

## FAQ

**Q: Does this migrate data?**
A: No, it only creates empty table structures. Use `INSERT INTO` or data migration tools for data.

**Q: Can I customize the catalog/schema?**
A: Yes, use `--catalog` and `--schema` parameters to specify any target location.

**Q: What if I want to see the DDL without creating tables?**
A: Use `--dry-run` flag to preview the DDL.

**Q: Can I process multiple DDL files at once?**
A: Yes, use `--input-dir` to process entire directories.

**Q: What about the LOCATION clause?**
A: Removed - creates managed tables in Databricks default warehouse location.

## Version

Current version: 2.0.0 (Direct creation mode)

## License

Part of the nifi_to_databricks migration toolkit.

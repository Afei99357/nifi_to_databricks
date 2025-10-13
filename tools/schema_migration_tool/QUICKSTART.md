# Quick Start Guide

## What Does This Tool Do?

Creates Databricks Delta tables directly from Hive DDL statements - no intermediate files needed!

**Input:** Hive `CREATE EXTERNAL TABLE` DDL
**Output:** Delta table created in Databricks catalog

## One Command Setup

```bash
python create_tables_databricks.py \
  --input hive_table.sql \
  --catalog my_catalog \
  --schema my_schema
```

That's it! Your table is now in Databricks.

## Step-by-Step Example

### Step 1: Get Hive DDL

In Hive/Impala:
```sql
SHOW CREATE TABLE sales.orders;
```

Save the output to `orders.sql`:
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

### Step 2: Create Table in Databricks

In Databricks notebook:
```python
%sh
python3 /Workspace/path/to/create_tables_databricks.py \
  --input /Volumes/catalog/schema/files/orders.sql \
  --catalog prod \
  --schema bronze
```

### Step 3: Verify

```sql
%sql
DESCRIBE EXTENDED prod.bronze.orders;
```

Done! Your table structure is now in Databricks.

## What You Get

**Original Hive Table:**
```sql
CREATE EXTERNAL TABLE sales.orders (
  order_timestamp_ts STRING,  -- Note: STRING type
  ...
)
STORED AS PARQUET
LOCATION 'hdfs://...'
```

**Databricks Delta Table:**
```sql
CREATE TABLE prod.bronze.orders (
  order_timestamp_ts TIMESTAMP,  -- Auto-converted to TIMESTAMP!
  ...
)
USING DELTA  -- Delta Lake format
-- No LOCATION clause = managed table
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);
```

**Automatic Changes:**
- ✅ `STRING` columns ending with `_ts` → `TIMESTAMP`
- ✅ `EXTERNAL TABLE` → managed Delta table
- ✅ `PARQUET` → `DELTA` format
- ✅ HDFS location → removed (uses Databricks default)
- ✅ Adds auto-optimization properties

## Common Use Cases

### Single Table

```bash
python create_tables_databricks.py \
  -i table.sql \
  --catalog dev \
  --schema bronze
```

### Multiple Tables (Batch)

```bash
# Put all your Hive DDL files in a directory
hive_ddls/
├── orders.sql
├── customers.sql
└── products.sql

# Create all at once
python create_tables_databricks.py \
  --input-dir hive_ddls/ \
  --catalog dev \
  --schema bronze
```

### Dry Run (Preview Only)

```bash
# See what will be created without actually creating it
python create_tables_databricks.py \
  -i table.sql \
  --catalog dev \
  --schema bronze \
  --dry-run
```

### Without Type Optimization

```bash
# Keep STRING types as-is (don't convert _ts to TIMESTAMP)
python create_tables_databricks.py \
  -i table.sql \
  --catalog dev \
  --schema bronze \
  --no-optimize
```

## Real Workflow Example

### Extract from Hive

```bash
# Get DDL for all tables in a schema
hive -e "SHOW TABLES IN my_schema;" | while read table; do
  hive -e "SHOW CREATE TABLE my_schema.$table;" > "${table}.sql"
done
```

### Create in Databricks

```python
%sh
# Upload the DDL files to Volumes first
# Then run the tool

python3 /Workspace/path/to/create_tables_databricks.py \
  --input-dir /Volumes/catalog/schema/hive_ddls/ \
  --catalog prod \
  --schema bronze
```

### Load Data (Separate Step)

```sql
%sql
-- Copy data from Hive to Databricks
INSERT INTO prod.bronze.orders
SELECT * FROM hive_metastore.sales.orders;
```

## Command Reference

```bash
# Required parameters
--catalog CATALOG       # Target Databricks catalog
--schema SCHEMA         # Target Databricks schema

# Input (choose one)
-i, --input FILE        # Single Hive DDL file
--input-dir DIR         # Directory of DDL files

# Optional
--optimize              # Convert _ts columns to TIMESTAMP (default: on)
--no-optimize           # Keep original types
--dry-run               # Preview DDL without creating tables
```

## Tips

1. **Start with dry-run** to see what will be created:
   ```bash
   python create_tables_databricks.py -i table.sql --catalog dev --schema bronze --dry-run
   ```

2. **Use dev catalog first** for testing:
   ```bash
   # Test in dev
   python create_tables_databricks.py -i table.sql --catalog dev --schema bronze

   # Then prod
   python create_tables_databricks.py -i table.sql --catalog prod --schema bronze
   ```

3. **Check the results** in Databricks:
   ```sql
   SHOW CREATE TABLE prod.bronze.my_table;
   ```

4. **Batch process** for efficiency:
   ```bash
   # Process all tables at once
   python create_tables_databricks.py --input-dir hive_ddls/ --catalog prod --schema bronze
   ```

## Troubleshooting

**Error: "ImportError: SparkSession"**
- You must run this in a Databricks notebook or job
- Use `%sh` magic command in notebooks

**Error: "Table already exists"**
- Drop the existing table first: `DROP TABLE catalog.schema.table;`
- Or use a different catalog/schema

**Columns not converted to TIMESTAMP**
- Check if column names end with `_ts`
- Or use `--no-optimize` to keep original types

**Wrong catalog/schema**
- Double-check `--catalog` and `--schema` parameters
- Tables are created in `catalog.schema.table_name` format

## What's Next?

After creating tables:

1. **Verify structure**: `DESCRIBE EXTENDED catalog.schema.table;`
2. **Load data**: Use `INSERT INTO` or data migration tools
3. **Test queries**: Run sample queries to verify
4. **Set permissions**: Grant access to appropriate users
5. **Document**: Keep track of Hive → Databricks mappings

## Summary

```
Hive DDL file → create_tables_databricks.py → Delta table in Databricks
                                            ↓
                            (Optional: SHOW CREATE TABLE to view DDL)
```

One command, no intermediate files, tables ready to use!

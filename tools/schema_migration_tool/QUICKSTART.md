# Quick Start Guide

## What is the Input?

**The input is your Hive/Impala CREATE TABLE statement (DDL) as text.**

Example of what your input looks like:
```sql
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
```

## How to Get Your Input (Hive DDL)

### Option 1: From Hive CLI
```bash
# Connect to Hive
hive

# Get the DDL
hive> SHOW CREATE TABLE your_schema.your_table;
```

### Option 2: From Impala
```bash
# Connect to Impala
impala-shell

# Get the DDL
[server:21000] > SHOW CREATE TABLE your_schema.your_table;
```

### Option 3: From Hue/Web UI
1. Open your Hive/Impala query editor
2. Run: `SHOW CREATE TABLE your_schema.your_table;`
3. Copy the output

## Step-by-Step Usage

### Example 1: Using a File

**Step 1:** Save your Hive DDL to a file

Create a file `example_hive_table.sql`:
```sql
CREATE EXTERNAL TABLE sales.orders (
  order_id INT,
  customer_name STRING,
  order_date STRING,
  amount DOUBLE
)
PARTITIONED BY (year INT, month INT)
STORED AS PARQUET
LOCATION 'hdfs://namenode/warehouse/sales.db/orders'
```

**Step 2:** Run the converter
```bash
cd /home/eric/Projects/nifi_to_databricks

python3 tools/schema_migration_tool/convert_ddl.py \
  --input example_hive_table.sql \
  --output example_databricks_table.sql
```

**Step 3:** Check the output
```bash
cat example_databricks_table.sql
```

You'll get:
```sql
-- ============================================================
-- Delta Table: sales.orders
-- Source: PARQUET external table
-- Target: Databricks Delta Lake
-- ============================================================

-- Step 1: Create schema
CREATE SCHEMA IF NOT EXISTS sales;

-- Step 2: Create Delta table
CREATE TABLE sales.orders (
  order_id INT,
  customer_name STRING,
  order_date STRING,
  amount DOUBLE,
  year INT,
  month INT
)
USING DELTA
PARTITIONED BY (year, month)
LOCATION 'dbfs:/mnt/warehouse/warehouse/sales.db/orders'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);

-- Step 3: Verify table
DESCRIBE EXTENDED sales.orders;

-- Step 4: Check partitions (after data load)
-- SHOW PARTITIONS sales.orders;

-- Step 5: Optimize table
OPTIMIZE sales.orders;

-- Original location: hdfs://namenode/warehouse/sales.db/orders
-- New location: dbfs:/mnt/warehouse/warehouse/sales.db/orders
```

### Example 2: Using Copy-Paste (No File)

**Step 1:** Copy your Hive DDL

**Step 2:** Run the converter with stdin
```bash
cd /home/eric/Projects/nifi_to_databricks

# Type or paste your DDL, then press Ctrl+D when done
python3 tools/schema_migration_tool/convert_ddl.py
```

**Step 3:** Paste your DDL and press `Ctrl+D`

The output will be printed to your screen.

### Example 3: Using Python Directly

Create a Python script `convert_my_table.py`:
```python
from tools.schema_migration_tool import convert_hive_to_databricks

# Your Hive DDL as a string
hive_ddl = """
CREATE EXTERNAL TABLE my_db.my_table (
  id INT,
  name STRING,
  created_ts STRING
)
PARTITIONED BY (date STRING)
STORED AS PARQUET
LOCATION 'hdfs://namenode/warehouse/my_db.db/my_table'
"""

# Convert to Databricks DDL
databricks_ddl = convert_hive_to_databricks(
    hive_ddl,
    target_storage="dbfs",     # or "azure", "aws", "unity_catalog"
    migration_type="delta",    # or "external"
    optimize_types=True        # Convert _ts columns to TIMESTAMP
)

# Print the result
print(databricks_ddl)

# Or save to file
with open("output.sql", "w") as f:
    f.write(databricks_ddl)
```

Run it:
```bash
python3 convert_my_table.py
```

## Command Options

```bash
python3 tools/schema_migration_tool/convert_ddl.py \
  --input <file>           # Input Hive DDL file (or use stdin)
  --output <file>          # Output Databricks DDL file (or use stdout)
  --type delta             # "delta" or "external" (default: delta)
  --storage dbfs           # "dbfs", "azure", "aws", "unity_catalog" (default: dbfs)
  --optimize               # Convert STRING timestamps to TIMESTAMP (default: on)
  --no-optimize            # Keep original types
```

## Real Example with YOUR Table

Your Hive DDL:
```sql
CREATE EXTERNAL TABLE obf_schema.obf_table_raw (
  col_a STRING,
  col_b_ts STRING,
  col_c_ts STRING,
  col_d INT,
  col_e STRING,
  col_f INT,
  col_g STRING,
  col_h DOUBLE,
  col_i DOUBLE,
  col_j DOUBLE,
  col_k DOUBLE,
  col_l_ts STRING,
  col_m INT,
  col_n STRING,
  col_o STRING,
  col_p_ts STRING,
  col_q_ts STRING,
  col_r STRING,
  col_s STRING,
  col_t DOUBLE,
  col_u DOUBLE,
  col_v DOUBLE,
  col_w STRING,
  col_x_ts STRING,
  col_y INT,
  col_z_ts STRING
)
PARTITIONED BY (
  part_a_ts STRING,
  part_b_ts STRING,
  part_suffix STRING
)
STORED AS PARQUET
LOCATION 'hdfs://files-dev-server-1/user/hive/warehouse/obf_tables/obf_schema/obf_table_name_raw'
```

**To convert it:**

1. Save the above to `obf_table.sql`

2. Run:
```bash
python3 tools/schema_migration_tool/convert_ddl.py \
  --input obf_table.sql \
  --output obf_table_databricks.sql \
  --type delta \
  --storage dbfs
```

3. You'll get a Databricks DDL with:
   - All `_ts` columns converted to TIMESTAMP
   - HDFS path converted to dbfs path
   - Delta table format
   - Auto-optimization enabled

## What If I Don't Have a Hive DDL?

If you only have the table name, you need to get the DDL first:

```bash
# Method 1: Hive
hive -e "SHOW CREATE TABLE your_schema.your_table;" > my_table.sql

# Method 2: Impala
impala-shell -q "SHOW CREATE TABLE your_schema.your_table;" > my_table.sql

# Then convert
python3 tools/schema_migration_tool/convert_ddl.py --input my_table.sql
```

## Testing Without a Real Table

You can test with the example that's already in the code:

```bash
cd /home/eric/Projects/nifi_to_databricks

# This runs with a built-in example
python3 -m tools.schema_migration_tool.databricks_ddl_generator
```

This will show you what the output looks like!

## Common Workflows

### Workflow 1: Single Table
```bash
# 1. Get DDL from Hive
hive -e "SHOW CREATE TABLE my_schema.my_table;" > hive_ddl.sql

# 2. Convert
python3 tools/schema_migration_tool/convert_ddl.py \
  -i hive_ddl.sql -o databricks_ddl.sql

# 3. Review
cat databricks_ddl.sql

# 4. Execute in Databricks
# Copy and paste the SQL into Databricks SQL Editor
```

### Workflow 2: Multiple Tables
```bash
# 1. Get all table names
hive -e "SHOW TABLES IN my_schema;" > tables.txt

# 2. Get DDL for each table
while read table; do
  hive -e "SHOW CREATE TABLE my_schema.$table;" > "hive_${table}.sql"
done < tables.txt

# 3. Convert all tables
for file in hive_*.sql; do
  output="databricks_${file#hive_}"
  python3 tools/schema_migration_tool/convert_ddl.py -i "$file" -o "$output"
done

# 4. Review all outputs
ls databricks_*.sql
```

### Workflow 3: Direct Pipe
```bash
# Get DDL and convert in one command
hive -e "SHOW CREATE TABLE my_schema.my_table;" | \
  python3 tools/schema_migration_tool/convert_ddl.py > databricks_output.sql
```

## Summary

**Input:**  A text file containing your Hive `CREATE EXTERNAL TABLE` statement

**Output:** A text file containing the Databricks `CREATE TABLE` statement

**Usage:**
```bash
python3 tools/schema_migration_tool/convert_ddl.py \
  --input <your_hive_ddl.sql> \
  --output <databricks_ddl.sql>
```

That's it! The tool does all the parsing and conversion automatically.

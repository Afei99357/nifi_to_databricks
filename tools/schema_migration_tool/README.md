# Schema Migration Tool

Convert Hive/Impala `CREATE EXTERNAL TABLE` DDL statements to Databricks-compatible DDL.

## Features

- **Automatic DDL Parsing** - Parses Hive/Impala DDL and extracts table structure
- **Path Conversion** - Converts HDFS paths to Databricks storage paths
- **Type Optimization** - Automatically converts STRING timestamp columns to TIMESTAMP
- **Multiple Formats** - Generate Delta table (recommended) or External Parquet DDL
- **CLI Tool** - Simple command-line interface
- **Python API** - Programmatic access for automation

## Quick Start

### Input: Hive DDL
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

### Output: Databricks Delta DDL
```sql
CREATE TABLE obf_schema.obf_table_raw (
  col_a STRING,
  col_b_ts TIMESTAMP,  -- Converted from STRING
  col_c_ts TIMESTAMP,  -- Converted from STRING
  col_d INT,
  part_a_ts STRING,
  part_b_ts STRING
)
USING DELTA
PARTITIONED BY (part_a_ts, part_b_ts)
LOCATION 'dbfs:/mnt/warehouse/user/hive/warehouse/obf_tables/obf_schema/obf_table_name_raw'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);
```

## Installation

No installation required - just use the Python files directly:

```bash
cd tools/schema_migration_tool
```

## Usage

### Option 1: Command Line

```bash
# Basic usage - file to file
python convert_ddl.py --input hive_table.sql --output databricks_table.sql

# Read from stdin
cat hive_table.sql | python convert_ddl.py > databricks_table.sql

# Specify migration type
python convert_ddl.py -i hive.sql -o databricks.sql --type delta

# Choose storage target
python convert_ddl.py -i hive.sql --storage azure

# Disable type optimization
python convert_ddl.py -i hive.sql --no-optimize
```

**CLI Options:**
- `-i, --input` - Input Hive DDL file (default: stdin)
- `-o, --output` - Output Databricks DDL file (default: stdout)
- `-t, --type` - Migration type: `delta` or `external` (default: delta)
- `-s, --storage` - Storage type: `dbfs`, `azure`, `aws`, `unity_catalog` (default: dbfs)
- `--optimize` - Optimize column types (default: enabled)
- `--no-optimize` - Disable type optimization

### Option 2: Python API

```python
from tools.schema_migration_tool import convert_hive_to_databricks

# Your Hive DDL
hive_ddl = """
CREATE EXTERNAL TABLE my_schema.my_table (
  id INT,
  created_ts STRING,
  name STRING
)
PARTITIONED BY (date STRING)
STORED AS PARQUET
LOCATION 'hdfs://namenode/warehouse/my_schema.db/my_table'
"""

# Convert to Databricks Delta DDL
databricks_ddl = convert_hive_to_databricks(
    hive_ddl,
    target_storage="dbfs",      # or "azure", "aws", "unity_catalog"
    migration_type="delta",     # or "external"
    optimize_types=True         # Convert STRING timestamps to TIMESTAMP
)

print(databricks_ddl)
```

### Option 3: Interactive Python

```python
from tools.schema_migration_tool import HiveDDLParser, DatabricksDDLGenerator

# Parse Hive DDL
parser = HiveDDLParser(your_hive_ddl)
parsed = parser.parse()

# Show what was parsed
print(parser.summary())

# Generate Databricks DDL
generator = DatabricksDDLGenerator(parsed)
delta_ddl = generator.generate_delta_ddl(target_storage="dbfs")
print(delta_ddl)
```

## Examples

### Example 1: Simple Table

**Input (Hive):**
```sql
CREATE EXTERNAL TABLE sales.orders (
  order_id INT,
  customer_id INT,
  order_date STRING,
  amount DOUBLE
)
STORED AS PARQUET
LOCATION 'hdfs://namenode/warehouse/sales.db/orders'
```

**Command:**
```bash
echo "CREATE EXTERNAL TABLE ..." | python convert_ddl.py
```

**Output (Databricks Delta):**
```sql
CREATE SCHEMA IF NOT EXISTS sales;

CREATE TABLE sales.orders (
  order_id INT,
  customer_id INT,
  order_date STRING,
  amount DOUBLE
)
USING DELTA
LOCATION 'dbfs:/mnt/warehouse/warehouse/sales.db/orders'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);
```

### Example 2: Partitioned Table with Timestamps

**Input (Hive):**
```sql
CREATE EXTERNAL TABLE events.user_activity (
  user_id INT,
  event_type STRING,
  event_ts STRING,
  properties STRING
)
PARTITIONED BY (
  date_partition STRING,
  hour_partition INT
)
STORED AS PARQUET
LOCATION 'hdfs://namenode/warehouse/events.db/user_activity'
```

**Command:**
```bash
python convert_ddl.py -i hive_events.sql -o databricks_events.sql --type delta
```

**Output (Databricks Delta with optimized types):**
```sql
CREATE TABLE events.user_activity (
  user_id INT,
  event_type STRING,
  event_ts TIMESTAMP,  -- Converted from STRING
  properties STRING,
  date_partition STRING,
  hour_partition INT
)
USING DELTA
PARTITIONED BY (date_partition, hour_partition)
LOCATION 'dbfs:/mnt/warehouse/warehouse/events.db/user_activity'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);
```

### Example 3: Azure Storage

**Command:**
```bash
python convert_ddl.py -i hive.sql --storage azure
```

**Output:**
```sql
...
LOCATION 'abfss://<container>@<storage_account>.dfs.core.windows.net/warehouse/...'
...
```

### Example 4: External Parquet (Minimal Migration)

**Command:**
```bash
python convert_ddl.py -i hive.sql --type external --no-optimize
```

**Output:**
```sql
CREATE EXTERNAL TABLE my_schema.my_table (
  col_a STRING,
  col_b_ts STRING  -- Kept as STRING
)
PARTITIONED BY (
  date_partition STRING
)
STORED AS PARQUET
LOCATION 'dbfs:/mnt/warehouse/...';
```

## Storage Path Conversion

The tool automatically converts HDFS paths to Databricks-compatible paths:

| Source (HDFS) | Target Type | Result |
|---------------|-------------|---------|
| `hdfs://namenode/user/hive/warehouse/db/table` | `dbfs` | `dbfs:/mnt/warehouse/user/hive/warehouse/db/table` |
| `hdfs://namenode/user/hive/warehouse/db/table` | `azure` | `abfss://<container>@<storage>.dfs.core.windows.net/user/hive/warehouse/db/table` |
| `hdfs://namenode/user/hive/warehouse/db/table` | `aws` | `s3://<bucket>/user/hive/warehouse/db/table` |
| `hdfs://namenode/user/hive/warehouse/db/table` | `unity_catalog` | `/Volumes/<catalog>/<schema>/<volume>/db/table` |

**Note:** For Azure, AWS, and Unity Catalog, you'll need to replace the placeholders (`<container>`, `<storage_account>`, `<bucket>`, `<catalog>`, etc.) with your actual values.

## Type Optimization

When `--optimize` is enabled (default), the tool converts STRING columns ending with `_ts` to TIMESTAMP:

| Hive Type | Column Name Pattern | Databricks Type |
|-----------|---------------------|-----------------|
| `STRING` | `*_ts` | `TIMESTAMP` |
| `STRING` | Other | `STRING` |
| `INT`, `DOUBLE`, etc. | Any | Unchanged |

**Example:**
- `created_ts STRING` → `created_ts TIMESTAMP`
- `updated_ts STRING` → `updated_ts TIMESTAMP`
- `name STRING` → `name STRING` (no change)

## Migration Types

### Delta Table (Recommended)

**When to use:**
- Production workloads
- Need ACID transactions
- Want time travel / versioning
- Need update/delete support

**Advantages:**
- Full ACID transactions
- Time travel (query historical versions)
- Schema evolution
- Better query performance
- Auto-optimization
- Update/delete support

**Command:**
```bash
python convert_ddl.py -i hive.sql --type delta
```

### External Parquet Table

**When to use:**
- Quick migration / testing
- Data stays in original format
- Read-only access

**Advantages:**
- Fastest migration
- No data movement
- Minimal changes from Hive

**Command:**
```bash
python convert_ddl.py -i hive.sql --type external
```

## Workflow

### Step 1: Get Hive DDL

```sql
-- In Hive/Impala
SHOW CREATE TABLE my_schema.my_table;
```

Copy the output to a file (e.g., `hive_table.sql`)

### Step 2: Convert DDL

```bash
python convert_ddl.py -i hive_table.sql -o databricks_table.sql
```

### Step 3: Review Generated DDL

Open `databricks_table.sql` and review:
- Table and schema names
- Column types (especially timestamp conversions)
- Storage location path
- Partition strategy

### Step 4: Execute in Databricks

```sql
-- In Databricks SQL Editor or Notebook
%sql
-- Copy and paste the generated DDL
CREATE TABLE ...
```

### Step 5: Migrate Data

If you need to copy data (not just create table structure):

```sql
-- In Databricks
INSERT INTO target_schema.target_table
SELECT * FROM source_schema.source_table;
```

Or use the data migration notebook (to be created).

## Troubleshooting

### Issue: "Could not extract table name"

**Problem:** DDL format not recognized

**Solution:** Ensure your DDL starts with `CREATE EXTERNAL TABLE` or `CREATE TABLE`

### Issue: Columns missing from output

**Problem:** Complex DDL with nested types or comments

**Solution:** Simplify the DDL or report the issue with an example

### Issue: Wrong storage path

**Problem:** Path conversion didn't work as expected

**Solution:** Manually edit the `LOCATION` clause in the generated DDL

### Issue: Timestamp columns not converted

**Problem:** Columns don't end with `_ts`

**Solution:**
1. Use `--no-optimize` and manually edit the DDL, or
2. Rename columns to follow the `*_ts` convention

## Advanced Usage

### Custom Type Conversions

```python
from tools.schema_migration_tool import HiveDDLParser, DatabricksDDLGenerator

parser = HiveDDLParser(hive_ddl)
parsed = parser.parse()

# Custom column type modification
custom_columns = []
for col_name, col_type in parsed['columns']:
    if col_name == 'special_column':
        custom_columns.append((col_name, 'DECIMAL(10,2)'))
    else:
        custom_columns.append((col_name, col_type))

parsed['columns'] = custom_columns

generator = DatabricksDDLGenerator(parsed)
ddl = generator.generate_delta_ddl()
print(ddl)
```

### Batch Processing

```bash
# Convert all DDL files in a directory
for file in hive_tables/*.sql; do
    output="databricks_tables/$(basename $file)"
    python convert_ddl.py -i "$file" -o "$output"
done
```

### Integration with CI/CD

```yaml
# Example GitHub Actions workflow
- name: Convert Hive DDL to Databricks
  run: |
    python tools/schema_migration_tool/convert_ddl.py \
      -i schemas/hive/my_table.sql \
      -o schemas/databricks/my_table.sql \
      --type delta \
      --storage dbfs
```

## Limitations

- Only supports `CREATE EXTERNAL TABLE` statements
- Complex nested types (STRUCT, ARRAY, MAP) are passed through as-is
- Comments in DDL might not be preserved
- Table properties (TBLPROPERTIES) from Hive are not migrated
- SerDe properties are not converted

## Best Practices

1. **Review Generated DDL** - Always review before executing
2. **Test with Small Tables** - Test the process with non-critical tables first
3. **Verify Data Types** - Check timestamp conversions are correct
4. **Update Storage Paths** - Replace placeholder storage account names
5. **Consider Partitioning** - Review if the partition strategy still makes sense
6. **Use Delta Format** - Prefer Delta tables for production workloads
7. **Document Changes** - Keep track of schema changes made during migration

## FAQ

**Q: Does this tool migrate data?**
A: No, it only converts DDL (table structure). Use Databricks notebooks or `INSERT INTO` for data migration.

**Q: Can I convert Managed tables?**
A: Yes, the tool works with both `CREATE EXTERNAL TABLE` and `CREATE TABLE` statements.

**Q: Will this work with Impala DDL?**
A: Yes, Hive and Impala DDL syntax is very similar and should work.

**Q: Can I use this in production?**
A: The tool generates standard SQL DDL. Always review the output before executing in production.

**Q: How do I handle complex types?**
A: Complex types (STRUCT, ARRAY, MAP) are preserved as-is. Review and adjust manually if needed.

## Support

For issues or questions:
1. Check the examples in this README
2. Review the generated DDL for obvious issues
3. Report bugs with sample DDL (anonymized if needed)

## Version

Current version: 1.0.0

## License

Part of the nifi_to_databricks migration toolkit.

# Scripts

Command-line tools for batch processing NiFi workflows.

## Batch Table Extraction

Extract table references from multiple NiFi XML files and save results to CSV.

### Basic Usage

**Using uv (recommended):**
```bash
uv run python scripts/batch_table_extraction.py \
  --input-dir ./xml_files \
  --output-file tables.csv
```

**Using Python directly (requires virtual environment):**
```bash
# First activate virtual environment
source .venv/bin/activate

# Then run the script
python scripts/batch_table_extraction.py \
  --input-dir ./xml_files \
  --output-file tables.csv
```

### Options

| Option | Short | Description | Default |
|--------|-------|-------------|---------|
| `--input-dir` | `-i` | Directory containing NiFi XML files | *Required* |
| `--output-file` | `-o` | Output CSV file path | *Required* |
| `--recursive` | `-r` | Search subdirectories | `False` |
| `--pattern` | `-p` | File pattern to match | `*.xml` |
| `--verbose` | `-v` | Show detailed progress | `False` |
| `--continue-on-error` | | Continue if a file fails | `False` |

### Examples

All examples use `uv run python`. If using Python directly, activate `.venv` first and omit `uv run`.

**Process all XML files in a directory:**
```bash
uv run python scripts/batch_table_extraction.py \
  -i ./nifi_pipeline_file \
  -o all_tables.csv
```

**Recursive search with verbose output:**
```bash
uv run python scripts/batch_table_extraction.py \
  -i ./workflows \
  -o tables.csv \
  --recursive \
  --verbose
```

**Process specific file pattern:**
```bash
uv run python scripts/batch_table_extraction.py \
  -i ./xml_files \
  -o icn8_tables.csv \
  --pattern "*ICN8*.xml"
```

**Continue processing if some files fail:**
```bash
uv run python scripts/batch_table_extraction.py \
  -i ./xml_files \
  -o results.csv \
  --continue-on-error
```

### Output Format

The CSV output includes the following columns:

| Column | Description |
|--------|-------------|
| `source_file` | XML filename |
| `source_path` | Full path to XML file |
| `table_name` | Extracted table name |
| `processor_name` | Processor that uses this table |
| `processor_type` | Type of processor |
| `processor_id` | Processor ID |
| `processor_group` | Parent group name |
| `property_name` | Property containing table reference |
| `source` | Source type (sql, property, etc.) |
| `sql_clause` | SQL clause where table appears (from, join, etc.) |
| `io_type` | I/O type (read, write, unknown) |
| `confidence` | Confidence score (0.0-1.0) |
| `origin_processor_id` | Original processor ID (for variable references) |
| `origin_property_name` | Original property name (for variable references) |

### Summary Statistics

The tool displays summary information after processing:

```
============================================================
SUMMARY
============================================================
Files processed:        5/5
Files with tables:      3
Total table references: 130
Unique tables:          69
Unique processors:      42
```

### Getting Help

View all available options:

```bash
uv run python scripts/batch_table_extraction.py --help
```

## Other Scripts

Additional scripts for NiFi workflow analysis:

- `export_external_filenames.py` - Export external script filenames from workflows
  - See `export_external_filenames_usage.txt` for usage details

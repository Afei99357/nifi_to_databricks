# Databricks notebook source
# MAGIC %md
# MAGIC # Batch Table Extraction for Databricks
# MAGIC
# MAGIC Extract table references from multiple NiFi XML files and save results to CSV.
# MAGIC
# MAGIC ## Setup Instructions
# MAGIC
# MAGIC 1. Upload your `tools/` folder to Databricks Workspace at `/Workspace/Users/<your-email>/nifi_tools/tools/`
# MAGIC 2. Upload XML files to a directory (e.g., `/Workspace/xml_files/` or DBFS)
# MAGIC 3. Run this notebook
# MAGIC
# MAGIC ## Parameters
# MAGIC
# MAGIC Use the widgets below to configure the extraction:
# MAGIC - **Input Directory**: Directory containing NiFi XML files
# MAGIC - **Output File**: Path for the output CSV file
# MAGIC - **Recursive Search**: Whether to search subdirectories
# MAGIC - **File Pattern**: Pattern to match XML files (e.g., *.xml)
# MAGIC - **Tools Path**: Path where you uploaded the tools folder

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup and Configuration

# COMMAND ----------

import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd

# Type stubs for Databricks-specific functions (ignored by mypy)
try:
    dbutils  # type: ignore
    display  # type: ignore
except NameError:
    pass  # Only available in Databricks environment

# Create widgets for parameters
dbutils.widgets.text("input_dir", "/Workspace/xml_files", "1. Input XML Directory")
dbutils.widgets.text(
    "output_file", "/Workspace/results/tables.csv", "2. Output CSV File"
)
dbutils.widgets.dropdown("recursive", "False", ["True", "False"], "3. Recursive Search")
dbutils.widgets.text("file_pattern", "*.xml", "4. File Pattern")
dbutils.widgets.text(
    "tools_path", "/Workspace/Users/<your-email>/nifi_tools", "5. Tools Path"
)

# Get parameter values
input_dir = dbutils.widgets.get("input_dir")
output_file = dbutils.widgets.get("output_file")
recursive = dbutils.widgets.get("recursive") == "True"
file_pattern = dbutils.widgets.get("file_pattern")
tools_path = dbutils.widgets.get("tools_path")

# Add tools path to Python path
sys.path.insert(0, tools_path)

print(f"✓ Configuration loaded:")
print(f"  Input Directory: {input_dir}")
print(f"  Output File: {output_file}")
print(f"  Recursive: {recursive}")
print(f"  File Pattern: {file_pattern}")
print(f"  Tools Path: {tools_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Import Dependencies

# COMMAND ----------

# Import table extraction tool
try:
    from tools.table_extraction import extract_all_tables_from_nifi_xml

    print("✓ Successfully imported table extraction module")
except ImportError as e:
    print(f"✗ Failed to import table extraction module: {e}")
    print(
        f"\nPlease ensure you have uploaded the 'tools/' folder to: {tools_path}/tools/"
    )
    dbutils.notebook.exit("Import failed - check tools path")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Define Extraction Functions

# COMMAND ----------


def find_xml_files(input_dir: str, pattern: str, recursive: bool) -> List[str]:
    """Find all XML files matching the pattern."""
    input_path = Path(input_dir)

    if recursive:
        files = list(input_path.rglob(pattern))
    else:
        files = list(input_path.glob(pattern))

    return sorted([str(f) for f in files])


def process_single_file(xml_path: str) -> List[Dict[str, Any]]:
    """Extract tables from a single XML file."""
    results = extract_all_tables_from_nifi_xml(xml_path)

    # Add source file information
    file_name = Path(xml_path).name
    for table in results:
        table["source_file"] = file_name
        table["source_path"] = xml_path

    return results


def process_all_files(xml_files: List[str]) -> Tuple[List[Dict[str, Any]], List[str]]:
    """Process all XML files and combine results."""
    all_tables = []
    errors = []

    for i, xml_file in enumerate(xml_files, 1):
        print(f"[{i}/{len(xml_files)}] Processing: {Path(xml_file).name}")

        try:
            tables = process_single_file(xml_file)
            all_tables.extend(tables)
            print(f"  ✓ Found {len(tables)} table reference(s)")

        except Exception as e:
            error_msg = f"{Path(xml_file).name}: {str(e)}"
            errors.append(error_msg)
            print(f"  ✗ Error: {error_msg}")

    return all_tables, errors


# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Run Table Extraction

# COMMAND ----------

# Find XML files
print(f"Searching for XML files in: {input_dir}")
if recursive:
    print("  (including subdirectories)")

xml_files = find_xml_files(input_dir, file_pattern, recursive)

if not xml_files:
    print(f"✗ No XML files found matching pattern '{file_pattern}'")
    dbutils.notebook.exit(f"No XML files found in {input_dir}")

print(f"Found {len(xml_files)} XML file(s)\n")

# Process all files
all_tables, errors = process_all_files(xml_files)

print(f"\n{'='*60}")
print("EXTRACTION COMPLETE")
print(f"{'='*60}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Save Results to CSV

# COMMAND ----------

if not all_tables:
    print("⚠ No tables found in any XML files")
else:
    # Create DataFrame
    df = pd.DataFrame(all_tables)

    # Reorder columns for better readability
    priority_cols = [
        "source_file",
        "source_path",
        "table_name",
        "processor_name",
        "processor_type",
        "processor_id",
        "processor_group",
        "property_name",
    ]

    other_cols = [c for c in df.columns if c not in priority_cols]
    column_order = [c for c in priority_cols if c in df.columns] + other_cols
    df = df[column_order]

    # Create output directory if needed
    output_path = Path(output_file)
    output_dir = output_path.parent

    # Save to CSV
    # Note: For DBFS paths, you may need to use /dbfs/ prefix
    if str(output_file).startswith("/dbfs/"):
        save_path = output_file
    elif str(output_file).startswith("dbfs:/"):
        save_path = output_file.replace("dbfs:/", "/dbfs/")
    else:
        # Workspace path - may need adjustment based on your setup
        save_path = output_file

    df.to_csv(save_path, index=False)
    print(f"✓ Wrote {len(all_tables)} table entries to {output_file}")

    # Display preview
    print(f"\nPreview of results (first 10 rows):")
    display(df.head(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Summary Statistics

# COMMAND ----------

if all_tables:
    unique_tables = set(t["table_name"] for t in all_tables)
    unique_processors = set(t["processor_name"] for t in all_tables)
    unique_files = set(t["source_file"] for t in all_tables)

    print(f"{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    print(f"Files processed:        {len(xml_files) - len(errors)}/{len(xml_files)}")
    print(f"Files with tables:      {len(unique_files)}")
    print(f"Total table references: {len(all_tables)}")
    print(f"Unique tables:          {len(unique_tables)}")
    print(f"Unique processors:      {len(unique_processors)}")

    if errors:
        print(f"\n⚠ {len(errors)} file(s) had errors:")
        for error in errors:
            print(f"  - {error}")

    # Create summary DataFrame for visualization
    summary_data = {
        "Metric": [
            "Files Processed",
            "Files with Tables",
            "Total Table References",
            "Unique Tables",
            "Unique Processors",
        ],
        "Count": [
            len(xml_files) - len(errors),
            len(unique_files),
            len(all_tables),
            len(unique_tables),
            len(unique_processors),
        ],
    }

    summary_df = pd.DataFrame(summary_data)
    display(summary_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Analysis - Top Tables and Processors

# COMMAND ----------

if all_tables:
    # Top tables by reference count
    table_counts = pd.DataFrame(all_tables)["table_name"].value_counts().head(10)
    print("Top 10 Most Referenced Tables:")
    print(table_counts)

    # Top processors by table count
    processor_counts = (
        pd.DataFrame(all_tables)["processor_name"].value_counts().head(10)
    )
    print("\nTop 10 Processors with Most Table References:")
    print(processor_counts)

    # Visualize top tables
    display(table_counts.to_frame().reset_index())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Download Results
# MAGIC
# MAGIC The results have been saved to: `{output_file}`
# MAGIC
# MAGIC To download:
# MAGIC - For Workspace paths: Use the Databricks file browser
# MAGIC - For DBFS paths: Use `dbutils.fs.cp()` or download via UI
# MAGIC
# MAGIC Example to copy to DBFS for download:
# MAGIC ```python
# MAGIC dbutils.fs.cp(output_file, "dbfs:/FileStore/tables.csv")
# MAGIC # Then download from: https://<databricks-instance>/files/tables.csv
# MAGIC ```

# COMMAND ----------

# Optional: Copy results to DBFS for easy download
if not str(output_file).startswith("dbfs:/"):
    try:
        dbfs_path = "dbfs:/FileStore/nifi_tables_output.csv"
        dbutils.fs.cp(f"file:{output_file}", dbfs_path, recurse=False)
        print(f"✓ Results also copied to DBFS: {dbfs_path}")
        print(f"  Download URL: https://<your-workspace>/files/nifi_tables_output.csv")
    except Exception as e:
        print(f"Note: Could not copy to DBFS: {e}")

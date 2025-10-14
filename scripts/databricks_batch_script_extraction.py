# Databricks notebook source
# MAGIC %md
# MAGIC # Batch Script Extraction for Databricks
# MAGIC
# MAGIC Extract script references (external files and inline scripts) from multiple NiFi XML files and save results to CSV.
# MAGIC
# MAGIC ## Setup Instructions
# MAGIC
# MAGIC 1. Upload your `tools/` folder to Databricks Workspace at `/Workspace/Users/<your-email>/nifi_tools/tools/`
# MAGIC 2. Upload XML files to a directory (e.g., `/Workspace/xml_files/` or `/Volumes/...`)
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


# --- Safe widgets helper for Databricks notebooks ---
def _has_dbutils():
    try:
        dbutils  # type: ignore  # noqa: F821
        return True
    except NameError:
        return False


def get_arg(name: str, default: str = "") -> str:
    """
    Robustly fetch a widget/argument value:
    - If widget exists -> get it
    - Else try getArgument (older runtime compatibility)
    - Else return default
    """
    if not _has_dbutils():
        return default

    # Try widget get
    try:
        return dbutils.widgets.get(name)  # type: ignore  # noqa: F821
    except Exception:
        pass

    # Try older API
    try:
        return dbutils.widgets.getArgument(name, default)  # type: ignore  # noqa: F821
    except Exception:
        return default


def ensure_text_widget(name: str, default: str, label: str):
    if not _has_dbutils():
        return
    try:
        # If it already exists, don't recreate; just ensure there's a value
        _ = dbutils.widgets.get(name)  # type: ignore  # noqa: F821
    except Exception:
        dbutils.widgets.text(name, default, label)  # type: ignore  # noqa: F821


# ---- Define + ensure widgets exist (idempotent) ----
ensure_text_widget("input_dir", "/Workspace/xml_files", "1. Input XML Directory")
ensure_text_widget(
    "output_file", "/Workspace/results/scripts.csv", "2. Output CSV File"
)
ensure_text_widget("recursive", "False", "3. Recursive Search (True/False)")
ensure_text_widget("file_pattern", "*.xml", "4. File Pattern")
ensure_text_widget(
    "tools_path", "/Workspace/Users/<your-email>/nifi_tools", "5. Tools Path"
)

# ---- Read args with fallbacks ----
input_dir = get_arg("input_dir", "/Workspace/xml_files")
output_file = get_arg("output_file", "/Workspace/results/scripts.csv")
recursive_s = get_arg("recursive", "False")
file_pattern = get_arg("file_pattern", "*.xml")
tools_path = get_arg("tools_path", "/Workspace/Users/<your-email>/nifi_tools")

# Parse boolean robustly
recursive = str(recursive_s).strip().lower() in {"true", "1", "yes", "y", "t"}

# Add tools path to Python path
if tools_path and tools_path not in sys.path:
    sys.path.insert(0, tools_path)

# Echo config in the notebook output
if _has_dbutils():
    print("✓ Configuration loaded:")
    print(f"  Input Directory: {input_dir}")
    print(f"  Output File:     {output_file}")
    print(f"  Recursive:       {recursive}")
    print(f"  File Pattern:    {file_pattern}")
    print(f"  Tools Path:      {tools_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Import Dependencies

# COMMAND ----------

# Import script extraction tool
try:
    from tools.script_extraction import extract_all_scripts_from_nifi_xml

    print("✓ Successfully imported script extraction module")
except ImportError as e:
    print(f"✗ Failed to import script extraction module: {e}")
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
    """Extract scripts from a single XML file."""
    results = extract_all_scripts_from_nifi_xml(xml_path)

    # Flatten results into individual script entries
    all_scripts = []
    file_name = Path(xml_path).name

    for processor_result in results:
        processor_name = processor_result.get("processor_name", "Unknown")
        processor_type = processor_result.get("processor_type", "Unknown")
        processor_id = processor_result.get("processor_id", "Unknown")
        processor_group = processor_result.get("processor_group", "Root")

        # Process external scripts
        for ext_script in processor_result.get("external_scripts", []):
            all_scripts.append(
                {
                    "processor_id": processor_id,
                    "script_type": ext_script.get("type", "unknown"),
                    "source": "external",
                    "property_name": ext_script.get("property_source", ""),
                }
            )

        # Process inline scripts
        for inline_script in processor_result.get("inline_scripts", []):
            property_name = inline_script.get("property_name", "")

            all_scripts.append(
                {
                    "processor_id": processor_id,
                    "script_type": inline_script.get("script_type", "unknown"),
                    "source": "inline",
                    "property_name": property_name,
                }
            )

    return all_scripts


def process_all_files(xml_files: List[str]) -> Tuple[List[Dict[str, Any]], List[str]]:
    """Process all XML files and combine results."""
    all_scripts = []
    errors = []

    for i, xml_file in enumerate(xml_files, 1):
        print(f"[{i}/{len(xml_files)}] Processing: {Path(xml_file).name}")

        try:
            scripts = process_single_file(xml_file)
            all_scripts.extend(scripts)
            print(f"  ✓ Found {len(scripts)} script reference(s)")

        except Exception as e:
            error_msg = f"{Path(xml_file).name}: {str(e)}"
            errors.append(error_msg)
            print(f"  ✗ Error: {error_msg}")

    return all_scripts, errors


# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Run Script Extraction

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
all_scripts, errors = process_all_files(xml_files)

print(f"\n{'='*60}")
print("EXTRACTION COMPLETE")
print(f"{'='*60}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Save Results to CSV

# COMMAND ----------

if not all_scripts:
    print("⚠ No scripts found in any XML files")
else:
    # Create DataFrame
    df = pd.DataFrame(all_scripts)

    # Ensure column order: processor_id, script_type, source, property_name
    column_order = ["processor_id", "script_type", "source", "property_name"]
    df = df[column_order]

    # Handle different path types in Databricks (DBFS, Workspace, Unity Catalog Volumes)
    if str(output_file).startswith("/dbfs/"):
        save_path = output_file
    elif str(output_file).startswith("dbfs:/"):
        save_path = output_file.replace("dbfs:/", "/dbfs/")
    elif str(output_file).startswith("/Volumes/"):
        # Unity Catalog Volume path
        save_path = output_file
        # If path is a directory, append default filename
        if not save_path.endswith(".csv"):
            save_path = str(Path(save_path) / "batch_scripts_output.csv")
    else:
        # Workspace path
        save_path = output_file
        if not save_path.endswith(".csv"):
            save_path = str(Path(save_path) / "batch_scripts_output.csv")

    df.to_csv(save_path, index=False)
    print(f"✓ Wrote {len(all_scripts)} script entries to {save_path}")

    # Display preview
    print(f"\nPreview of results (first 10 rows):")
    display(df.head(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Summary Statistics

# COMMAND ----------

if all_scripts:
    # Count by type
    external_scripts = [s for s in all_scripts if s["source"] == "external"]
    inline_scripts = [s for s in all_scripts if s["source"] == "inline"]

    # Unique counts
    unique_processors = set(s["processor_id"] for s in all_scripts)
    unique_script_types = set(s["script_type"] for s in all_scripts)

    print(f"{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    print(f"Files processed:         {len(xml_files) - len(errors)}/{len(xml_files)}")
    print(f"Total script references: {len(all_scripts)}")
    print(f"  - External scripts:    {len(external_scripts)}")
    print(f"  - Inline scripts:      {len(inline_scripts)}")
    print(f"Unique processors:       {len(unique_processors)}")
    print(f"Script types found:      {', '.join(sorted(unique_script_types))}")

    if errors:
        print(f"\n⚠ {len(errors)} file(s) had errors:")
        for error in errors:
            print(f"  - {error}")

    # Create summary DataFrame for visualization
    summary_data = {
        "Metric": [
            "Files Processed",
            "Total Scripts",
            "External Scripts",
            "Inline Scripts",
            "Unique Processors",
        ],
        "Count": [
            len(xml_files) - len(errors),
            len(all_scripts),
            len(external_scripts),
            len(inline_scripts),
            len(unique_processors),
        ],
    }

    summary_df = pd.DataFrame(summary_data)
    display(summary_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Analysis - Top Scripts and Processors

# COMMAND ----------

if all_scripts:
    # Script type distribution
    script_type_counts = df["script_type"].value_counts()
    print("Script Types Distribution:")
    print(script_type_counts)

    # Top processors by script count
    processor_counts = df["processor_id"].value_counts().head(10)
    print("\nTop 10 Processors with Most Scripts:")
    print(processor_counts)

    # External vs Inline breakdown
    source_counts = df["source"].value_counts()
    print("\nExternal vs Inline Scripts:")
    print(source_counts)

    # Visualize distributions
    display(script_type_counts.to_frame().reset_index())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Inline Script Analysis

# COMMAND ----------

if all_scripts:
    # Filter to only inline scripts
    inline_df = df[df["source"] == "inline"].copy()

    if not inline_df.empty:
        print(f"Found {len(inline_df)} inline scripts")

        # Show inline scripts by type
        inline_by_type = inline_df["script_type"].value_counts()
        print("\nInline Scripts by Type:")
        display(inline_by_type.to_frame().reset_index())

        # Show sample of inline scripts
        print("\nSample Inline Scripts:")
        display(inline_df.head(20))
    else:
        print("No inline scripts found")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Download Results
# MAGIC
# MAGIC The results have been saved to the configured output path.
# MAGIC
# MAGIC **CSV Columns:**
# MAGIC - `processor_id` - Unique processor identifier
# MAGIC - `script_type` - Script language (sql, bash, python, unknown, etc.)
# MAGIC - `source` - Script source ("inline" or "external")
# MAGIC - `property_name` - Property name containing the script

# COMMAND ----------

# Optional: Copy results to DBFS for easy download
if not str(output_file).startswith("dbfs:/"):
    try:
        dbfs_path = "dbfs:/FileStore/nifi_scripts_output.csv"
        dbutils.fs.cp(f"file:{save_path}", dbfs_path, recurse=False)
        print(f"✓ Results also copied to DBFS: {dbfs_path}")
        print(f"  Download URL: https://<your-workspace>/files/nifi_scripts_output.csv")
    except Exception as e:
        print(f"Note: Could not copy to DBFS: {e}")

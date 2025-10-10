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

# Create widgets for parameters
dbutils.widgets.text("input_dir", "/Workspace/xml_files", "1. Input XML Directory")
dbutils.widgets.text(
    "output_file", "/Workspace/results/scripts.csv", "2. Output CSV File"
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
                    "source_file": file_name,
                    "source_path": xml_path,
                    "script_path": ext_script.get("path", ""),
                    "script_type": ext_script.get("type", "unknown"),
                    "source_type": "External File",
                    "processor_name": processor_name,
                    "processor_type": processor_type,
                    "processor_group": processor_group,
                    "processor_id": processor_id,
                    "line_count": ext_script.get("line_count"),
                    "referenced_queries": "",
                    "inline_script_content": "",
                    "content_preview": "",
                }
            )

        # Process inline scripts
        for inline_script in processor_result.get("inline_scripts", []):
            property_name = inline_script.get("property_name", "")
            line_count = inline_script.get("line_count", 0)
            referenced_queries = inline_script.get("referenced_queries", [])
            full_content = inline_script.get("content", "")

            # Create script path with query references if available
            script_path = f"{property_name} ({line_count} lines)"
            if referenced_queries:
                refs = ", ".join(referenced_queries)
                script_path = f"{script_path} → {refs}"

            # Create content preview (first 200 chars)
            content_preview = (
                full_content[:200] + "..." if len(full_content) > 200 else full_content
            )

            all_scripts.append(
                {
                    "source_file": file_name,
                    "source_path": xml_path,
                    "script_path": script_path,
                    "script_type": inline_script.get("script_type", "unknown"),
                    "source_type": "Inline Script",
                    "processor_name": processor_name,
                    "processor_type": processor_type,
                    "processor_group": processor_group,
                    "processor_id": processor_id,
                    "line_count": line_count,
                    "referenced_queries": (
                        ", ".join(referenced_queries) if referenced_queries else ""
                    ),
                    "inline_script_content": full_content,
                    "content_preview": content_preview,
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

    # Reorder columns for better readability
    priority_cols = [
        "source_file",
        "source_path",
        "script_path",
        "script_type",
        "source_type",
        "processor_name",
        "processor_type",
        "processor_group",
        "processor_id",
        "line_count",
        "referenced_queries",
        "content_preview",
        "inline_script_content",
    ]

    column_order = [c for c in priority_cols if c in df.columns]
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
    # Show columns except full inline content for preview
    preview_df = df.drop(columns=["inline_script_content"], errors="ignore")
    display(preview_df.head(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Summary Statistics

# COMMAND ----------

if all_scripts:
    # Count by type
    external_scripts = [s for s in all_scripts if s["source_type"] == "External File"]
    inline_scripts = [s for s in all_scripts if s["source_type"] == "Inline Script"]

    # Unique counts
    unique_processors = set(s["processor_name"] for s in all_scripts)
    unique_files = set(s["source_file"] for s in all_scripts)
    unique_script_types = set(s["script_type"] for s in all_scripts)

    print(f"{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    print(f"Files processed:        {len(xml_files) - len(errors)}/{len(xml_files)}")
    print(f"Files with scripts:     {len(unique_files)}")
    print(f"Total script references: {len(all_scripts)}")
    print(f"  - External scripts:   {len(external_scripts)}")
    print(f"  - Inline scripts:     {len(inline_scripts)}")
    print(f"Unique processors:      {len(unique_processors)}")
    print(f"Script types found:     {', '.join(sorted(unique_script_types))}")

    if errors:
        print(f"\n⚠ {len(errors)} file(s) had errors:")
        for error in errors:
            print(f"  - {error}")

    # Create summary DataFrame for visualization
    summary_data = {
        "Metric": [
            "Files Processed",
            "Files with Scripts",
            "Total Scripts",
            "External Scripts",
            "Inline Scripts",
            "Unique Processors",
        ],
        "Count": [
            len(xml_files) - len(errors),
            len(unique_files),
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
    processor_counts = df["processor_name"].value_counts().head(10)
    print("\nTop 10 Processors with Most Scripts:")
    print(processor_counts)

    # External vs Inline breakdown
    source_type_counts = df["source_type"].value_counts()
    print("\nExternal vs Inline Scripts:")
    print(source_type_counts)

    # Visualize distributions
    display(script_type_counts.to_frame().reset_index())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Inline Script Analysis

# COMMAND ----------

if all_scripts:
    # Filter to only inline scripts
    inline_df = df[df["source_type"] == "Inline Script"].copy()

    if not inline_df.empty:
        print(f"Found {len(inline_df)} inline scripts")

        # Show top 5 longest inline scripts
        longest_scripts = inline_df.nlargest(5, "line_count")[
            [
                "processor_name",
                "script_path",
                "script_type",
                "line_count",
                "content_preview",
            ]
        ]
        print("\nTop 5 Longest Inline Scripts:")
        display(longest_scripts)

        # Scripts with query references
        with_refs = inline_df[inline_df["referenced_queries"] != ""]
        if not with_refs.empty:
            print(f"\n{len(with_refs)} inline scripts reference other queries")
            display(
                with_refs[["processor_name", "script_path", "referenced_queries"]].head(
                    10
                )
            )
    else:
        print("No inline scripts found")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Download Results
# MAGIC
# MAGIC The results have been saved to the configured output path.
# MAGIC
# MAGIC **CSV Columns:**
# MAGIC - `source_file` - XML filename
# MAGIC - `source_path` - Full path to XML
# MAGIC - `script_path` - Script path or property name
# MAGIC - `script_type` - python, shell, sql, groovy, etc.
# MAGIC - `source_type` - "External File" or "Inline Script"
# MAGIC - `processor_name` - Processor name
# MAGIC - `processor_type` - Processor type
# MAGIC - `processor_group` - Group name
# MAGIC - `processor_id` - Processor ID
# MAGIC - `line_count` - Number of lines
# MAGIC - `referenced_queries` - Query references (for inline scripts)
# MAGIC - `content_preview` - First 200 chars preview
# MAGIC - `inline_script_content` - Full script content (for inline scripts only)

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

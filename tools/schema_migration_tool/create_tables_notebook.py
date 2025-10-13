"""
Create Databricks Tables from Hive DDL - Notebook Version

This version is designed to be imported and used directly in Databricks notebooks.
It uses the notebook's existing Spark session instead of creating a new one.

Usage in Databricks Notebook:
    import sys
    sys.path.append("/Workspace/path/to/schema_migration_tool")

    from create_tables_notebook import create_tables_from_hive_ddl

    # Single file
    create_tables_from_hive_ddl(
        input_file="/Volumes/catalog/schema/files/hive.sql",
        catalog="my_catalog",
        schema="my_schema"
    )

    # Multiple files
    create_tables_from_hive_ddl(
        input_dir="/Volumes/catalog/schema/hive_ddls/",
        catalog="my_catalog",
        schema="my_schema"
    )

    # Dry run
    create_tables_from_hive_ddl(
        input_file="/Volumes/catalog/schema/files/hive.sql",
        catalog="my_catalog",
        schema="my_schema",
        dry_run=True
    )
"""

from pathlib import Path
from typing import Optional

from databricks_ddl_generator import convert_hive_to_databricks


def create_table_in_databricks_notebook(
    ddl: str, catalog: str, schema: str, spark_session, dry_run: bool = False
) -> bool:
    """
    Create table in Databricks using provided spark session.

    Args:
        ddl: Databricks DDL string
        catalog: Target catalog name
        schema: Target schema name
        spark_session: Existing Spark session from notebook
        dry_run: If True, only print DDL without executing

    Returns:
        True if successful, False otherwise
    """
    try:
        # Update DDL to use specified catalog and schema
        lines = ddl.split("\n")
        updated_lines = []

        for line in lines:
            # Update CREATE SCHEMA line
            if "CREATE SCHEMA IF NOT EXISTS" in line:
                line = f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema};"

            # Update CREATE TABLE line
            elif "CREATE TABLE" in line and "(" not in line:
                # Extract table name from "CREATE TABLE schema.table_name ("
                parts = line.split("CREATE TABLE")[1].strip().split("(")[0].strip()
                if "." in parts:
                    table_name = parts.split(".")[1]
                else:
                    table_name = parts
                line = line.replace(parts, f"{catalog}.{schema}.{table_name}")

            # Update DESCRIBE, SHOW PARTITIONS, OPTIMIZE commands
            elif any(
                cmd in line
                for cmd in ["DESCRIBE EXTENDED", "SHOW PARTITIONS", "OPTIMIZE"]
            ):
                # Extract old schema.table reference
                for word in line.split():
                    if "." in word and not word.startswith("--"):
                        old_ref = word.rstrip(";")
                        if "." in old_ref:
                            table_name = old_ref.split(".")[1]
                        else:
                            table_name = old_ref
                        line = line.replace(old_ref, f"{catalog}.{schema}.{table_name}")
                        break

            updated_lines.append(line)

        updated_ddl = "\n".join(updated_lines)

        if dry_run:
            print("=" * 80)
            print("DRY RUN - DDL TO BE EXECUTED:")
            print("=" * 80)
            print(updated_ddl)
            print("=" * 80)
            return True

        # Execute DDL statements
        print(f"Creating table in {catalog}.{schema}...")

        # Split by semicolons and execute each statement
        statements = [
            s.strip()
            for s in updated_ddl.split(";")
            if s.strip() and not s.strip().startswith("--")
        ]

        for statement in statements:
            # Skip comment-only lines
            if statement.startswith("--") or not statement.strip():
                continue

            try:
                print(f"Executing: {statement[:80]}...")
                spark_session.sql(statement)
                print("✓ Success")
            except Exception as e:
                # Some statements like DESCRIBE might fail if table doesn't exist yet
                if (
                    "DESCRIBE" in statement
                    or "SHOW PARTITIONS" in statement
                    or "OPTIMIZE" in statement
                ):
                    print(f"⚠ Skipped (optional): {str(e)[:100]}")
                else:
                    raise

        print(f"✓ Table created successfully in {catalog}.{schema}")
        return True

    except Exception as e:
        print(f"Error creating table: {e}")
        return False


def create_tables_from_hive_ddl(
    input_file: Optional[str] = None,
    input_dir: Optional[str] = None,
    catalog: str = None,
    schema: str = None,
    optimize_types: bool = True,
    dry_run: bool = False,
) -> dict:
    """
    Create Databricks tables from Hive DDL files using notebook's Spark session.

    Args:
        input_file: Single Hive DDL file path
        input_dir: Directory containing Hive DDL files (.sql)
        catalog: Target Databricks catalog name
        schema: Target Databricks schema name
        optimize_types: Whether to optimize column types (STRING _ts → TIMESTAMP)
        dry_run: If True, only print DDL without creating tables

    Returns:
        Dictionary with success_count, fail_count, total

    Example:
        result = create_tables_from_hive_ddl(
            input_file="/Volumes/cat/schema/files/table.sql",
            catalog="dev",
            schema="bronze"
        )
        print(f"Created {result['success_count']} tables")
    """
    # Get Spark session from notebook
    try:
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()
    except Exception as e:
        print(f"Error: Could not get Spark session: {e}")
        print(
            "Make sure you're running this in a Databricks notebook with an active cluster."
        )
        return {"success_count": 0, "fail_count": 0, "total": 0}

    # Validate inputs
    if not catalog or not schema:
        print("Error: Must specify both catalog and schema")
        return {"success_count": 0, "fail_count": 0, "total": 0}

    if not input_file and not input_dir:
        print("Error: Must specify either input_file or input_dir")
        return {"success_count": 0, "fail_count": 0, "total": 0}

    if input_file and input_dir:
        print("Error: Cannot specify both input_file and input_dir")
        return {"success_count": 0, "fail_count": 0, "total": 0}

    # Collect input files
    input_files = []
    if input_file:
        input_path = Path(input_file)
        if not input_path.exists():
            print(f"Error: Input file not found: {input_file}")
            return {"success_count": 0, "fail_count": 0, "total": 0}
        input_files.append(input_path)
    else:
        input_dir_path = Path(input_dir)
        if not input_dir_path.exists():
            print(f"Error: Input directory not found: {input_dir}")
            return {"success_count": 0, "fail_count": 0, "total": 0}
        input_files = list(input_dir_path.glob("*.sql"))
        if not input_files:
            print(f"Error: No .sql files found in {input_dir}")
            return {"success_count": 0, "fail_count": 0, "total": 0}

    # Process each file
    success_count = 0
    fail_count = 0

    for file_path in input_files:
        print(f"\n{'='*80}")
        print(f"Processing: {file_path.name}")
        print(f"{'='*80}")

        try:
            # Read Hive DDL
            hive_ddl = file_path.read_text()

            if not hive_ddl.strip():
                print(f"⚠ Skipping empty file: {file_path.name}")
                continue

            # Convert to Databricks DDL
            databricks_ddl = convert_hive_to_databricks(
                hive_ddl,
                optimize_types=optimize_types,
            )

            # Create table in Databricks
            success = create_table_in_databricks_notebook(
                databricks_ddl, catalog, schema, spark, dry_run=dry_run
            )

            if success:
                success_count += 1
            else:
                fail_count += 1

        except Exception as e:
            print(f"✗ Error processing {file_path.name}: {e}")
            fail_count += 1

    # Summary
    print(f"\n{'='*80}")
    print("SUMMARY")
    print(f"{'='*80}")
    print(f"✓ Successful: {success_count}")
    print(f"✗ Failed: {fail_count}")
    print(f"Total: {len(input_files)}")

    return {
        "success_count": success_count,
        "fail_count": fail_count,
        "total": len(input_files),
    }

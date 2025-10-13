#!/usr/bin/env python3
"""
Create Databricks Tables Directly from Hive DDL

Reads Hive DDL and creates tables directly in Databricks catalog.

Usage:
    # Create tables in specified catalog and schema
    python create_tables_databricks.py --input hive.sql --catalog my_catalog --schema my_schema

    # Process multiple DDL files
    python create_tables_databricks.py --input-dir hive_ddls/ --catalog my_catalog --schema my_schema

    # Dry run (show DDL without creating)
    python create_tables_databricks.py --input hive.sql --catalog my_catalog --schema my_schema --dry-run
"""

import argparse
import sys
from pathlib import Path
from typing import Optional

from databricks_ddl_generator import convert_hive_to_databricks


def create_table_in_databricks(
    ddl: str, catalog: str, schema: str, dry_run: bool = False
) -> bool:
    """
    Create table in Databricks using spark.sql().

    Args:
        ddl: Databricks DDL string
        catalog: Target catalog name
        schema: Target schema name
        dry_run: If True, only print DDL without executing

    Returns:
        True if successful, False otherwise
    """
    try:
        # Import spark (only available in Databricks)
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()

        # Update DDL to use specified catalog and schema
        # Replace schema name in DDL with catalog.schema
        lines = ddl.split("\n")
        updated_lines = []

        for line in lines:
            # Update CREATE SCHEMA line
            if "CREATE SCHEMA IF NOT EXISTS" in line:
                old_schema = (
                    line.split("CREATE SCHEMA IF NOT EXISTS")[1].strip().rstrip(";")
                )
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
                spark.sql(statement)
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

    except ImportError:
        print(
            "Error: This script must be run in Databricks environment with PySpark",
            file=sys.stderr,
        )
        return False
    except Exception as e:
        print(f"Error creating table: {e}", file=sys.stderr)
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Create Databricks tables directly from Hive DDL",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Create single table
  python create_tables_databricks.py -i hive.sql --catalog dev --schema bronze

  # Process directory of DDL files
  python create_tables_databricks.py --input-dir hive_ddls/ --catalog prod --schema silver

  # Dry run (show what would be created)
  python create_tables_databricks.py -i hive.sql --catalog dev --schema bronze --dry-run

  # Disable type optimization
  python create_tables_databricks.py -i hive.sql --catalog dev --schema bronze --no-optimize

Note: Must be run in Databricks environment (notebook or job).
        """,
    )

    parser.add_argument(
        "-i",
        "--input",
        type=str,
        help="Input Hive DDL file",
    )

    parser.add_argument(
        "--input-dir",
        type=str,
        help="Directory containing Hive DDL files (.sql)",
    )

    parser.add_argument(
        "--catalog",
        type=str,
        required=True,
        help="Target Databricks catalog name",
    )

    parser.add_argument(
        "--schema",
        type=str,
        required=True,
        help="Target Databricks schema name",
    )

    parser.add_argument(
        "--optimize",
        action="store_true",
        default=True,
        help="Optimize column types (e.g., STRING ending with _ts to TIMESTAMP) (default: True)",
    )

    parser.add_argument(
        "--no-optimize",
        action="store_false",
        dest="optimize",
        help="Don't optimize column types",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show DDL without executing (for testing)",
    )

    args = parser.parse_args()

    # Validate inputs
    if not args.input and not args.input_dir:
        print("Error: Must specify either --input or --input-dir", file=sys.stderr)
        sys.exit(1)

    if args.input and args.input_dir:
        print("Error: Cannot specify both --input and --input-dir", file=sys.stderr)
        sys.exit(1)

    # Collect input files
    input_files = []
    if args.input:
        input_path = Path(args.input)
        if not input_path.exists():
            print(f"Error: Input file not found: {args.input}", file=sys.stderr)
            sys.exit(1)
        input_files.append(input_path)
    else:
        input_dir = Path(args.input_dir)
        if not input_dir.exists():
            print(
                f"Error: Input directory not found: {args.input_dir}", file=sys.stderr
            )
            sys.exit(1)
        input_files = list(input_dir.glob("*.sql"))
        if not input_files:
            print(f"Error: No .sql files found in {args.input_dir}", file=sys.stderr)
            sys.exit(1)

    # Process each file
    success_count = 0
    fail_count = 0

    for input_file in input_files:
        print(f"\n{'='*80}")
        print(f"Processing: {input_file.name}")
        print(f"{'='*80}")

        try:
            # Read Hive DDL
            hive_ddl = input_file.read_text()

            if not hive_ddl.strip():
                print(f"⚠ Skipping empty file: {input_file.name}")
                continue

            # Convert to Databricks DDL
            databricks_ddl = convert_hive_to_databricks(
                hive_ddl,
                optimize_types=args.optimize,
            )

            # Create table in Databricks
            success = create_table_in_databricks(
                databricks_ddl, args.catalog, args.schema, dry_run=args.dry_run
            )

            if success:
                success_count += 1
            else:
                fail_count += 1

        except Exception as e:
            print(f"✗ Error processing {input_file.name}: {e}", file=sys.stderr)
            fail_count += 1

    # Summary
    print(f"\n{'='*80}")
    print(f"SUMMARY")
    print(f"{'='*80}")
    print(f"✓ Successful: {success_count}")
    print(f"✗ Failed: {fail_count}")
    print(f"Total: {len(input_files)}")

    if fail_count > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()

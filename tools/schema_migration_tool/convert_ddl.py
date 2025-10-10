#!/usr/bin/env python3
"""
DDL Conversion CLI Tool

Simple command-line tool to convert Hive/Impala DDL to Databricks DDL.

Usage:
    # Read from file
    python convert_ddl.py --input hive_table.sql --output databricks_table.sql

    # Read from stdin
    cat hive_table.sql | python convert_ddl.py

    # Specify options
    python convert_ddl.py --input hive.sql --type delta --storage dbfs --optimize
"""

import argparse
import sys
from pathlib import Path

from databricks_ddl_generator import convert_hive_to_databricks


def main():
    parser = argparse.ArgumentParser(
        description="Convert Hive/Impala DDL to Databricks DDL",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Convert from file to file
  python convert_ddl.py -i hive_table.sql -o databricks_table.sql

  # Convert with Delta format (recommended)
  python convert_ddl.py -i hive.sql -o databricks.sql --type delta

  # Convert for Azure storage
  python convert_ddl.py -i hive.sql --storage azure

  # Read from stdin, write to stdout
  cat hive_table.sql | python convert_ddl.py

  # External Parquet table (minimal changes)
  python convert_ddl.py -i hive.sql --type external --no-optimize
        """,
    )

    parser.add_argument(
        "-i",
        "--input",
        type=str,
        help="Input Hive DDL file (default: stdin)",
    )

    parser.add_argument(
        "-o",
        "--output",
        type=str,
        help="Output Databricks DDL file (default: stdout)",
    )

    parser.add_argument(
        "-t",
        "--type",
        type=str,
        choices=["delta", "external"],
        default="delta",
        help="Migration type: 'delta' (recommended) or 'external' (default: delta)",
    )

    parser.add_argument(
        "-s",
        "--storage",
        type=str,
        choices=["dbfs", "azure", "aws", "s3", "unity_catalog"],
        default="dbfs",
        help="Target storage type (default: dbfs)",
    )

    parser.add_argument(
        "--optimize",
        action="store_true",
        default=True,
        help="Optimize column types (e.g., STRING to TIMESTAMP) (default: True)",
    )

    parser.add_argument(
        "--no-optimize",
        action="store_false",
        dest="optimize",
        help="Don't optimize column types",
    )

    args = parser.parse_args()

    # Read input
    if args.input:
        input_path = Path(args.input)
        if not input_path.exists():
            print(f"Error: Input file not found: {args.input}", file=sys.stderr)
            sys.exit(1)
        hive_ddl = input_path.read_text()
    else:
        # Read from stdin
        hive_ddl = sys.stdin.read()

    if not hive_ddl.strip():
        print("Error: No input DDL provided", file=sys.stderr)
        sys.exit(1)

    # Convert
    try:
        databricks_ddl = convert_hive_to_databricks(
            hive_ddl,
            target_storage=args.storage,
            migration_type=args.type,
            optimize_types=args.optimize,
        )
    except Exception as e:
        print(f"Error converting DDL: {e}", file=sys.stderr)
        sys.exit(1)

    # Write output
    if args.output:
        output_path = Path(args.output)
        output_path.write_text(databricks_ddl)
        print(f"Converted DDL written to: {args.output}", file=sys.stderr)
    else:
        # Write to stdout
        print(databricks_ddl)


if __name__ == "__main__":
    main()

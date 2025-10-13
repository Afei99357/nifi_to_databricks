#!/usr/bin/env python3
"""
DDL Conversion CLI Tool

Simple command-line tool to convert Hive/Impala DDL to Databricks managed Delta table DDL.
Creates empty table structure without data or location.

Usage:
    # Read from file
    python convert_ddl.py --input hive_table.sql --output databricks_table.sql

    # Read from stdin
    cat hive_table.sql | python convert_ddl.py

    # Disable type optimization
    python convert_ddl.py --input hive.sql --no-optimize
"""

import argparse
import sys
from pathlib import Path

from databricks_ddl_generator import convert_hive_to_databricks


def main():
    parser = argparse.ArgumentParser(
        description="Convert Hive/Impala DDL to Databricks managed Delta table DDL",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Convert from file to file
  python convert_ddl.py -i hive_table.sql -o databricks_table.sql

  # Read from stdin, write to stdout
  cat hive_table.sql | python convert_ddl.py

  # Disable type optimization
  python convert_ddl.py -i hive.sql --no-optimize

Note: Creates empty managed Delta tables without LOCATION clause.
      Databricks will store data in the default warehouse location.
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

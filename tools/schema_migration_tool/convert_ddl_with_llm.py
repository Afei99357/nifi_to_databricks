#!/usr/bin/env python3
"""
DDL Conversion with Optional LLM Enhancement

Converts Hive DDL to Databricks DDL using rule-based converter,
with optional Databricks LLM validation and enhancement.

Usage:
    # Basic conversion (rule-based only, fast & free)
    python convert_ddl_with_llm.py --input hive.sql --output databricks.sql

    # With LLM validation
    python convert_ddl_with_llm.py --input hive.sql --output databricks.sql --validate

    # With LLM suggestions
    python convert_ddl_with_llm.py --input hive.sql --output databricks.sql --suggest

    # Full LLM enhancement (validate + suggest + enhance)
    python convert_ddl_with_llm.py --input hive.sql --output databricks.sql --llm-full
"""

import argparse
import json
import sys
from pathlib import Path

from databricks_ddl_generator import convert_hive_to_databricks

# LLM enhancement (optional, requires Databricks)
try:
    from databricks_llm_enhancer import (
        convert_with_llm_validation,
        enhance_ddl_with_comments,
        suggest_optimizations,
        validate_ddl_conversion,
    )

    LLM_AVAILABLE = True
except ImportError:
    LLM_AVAILABLE = False
    print(
        "⚠️  Databricks LLM enhancement not available (requires databricks-sdk and mlflow)",
        file=sys.stderr,
    )


def main():
    parser = argparse.ArgumentParser(
        description="Convert Hive DDL to Databricks DDL with optional LLM enhancement",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic conversion (fast, free, rule-based)
  python convert_ddl_with_llm.py -i hive.sql -o databricks.sql

  # With LLM validation
  python convert_ddl_with_llm.py -i hive.sql -o databricks.sql --validate

  # With suggestions
  python convert_ddl_with_llm.py -i hive.sql -o databricks.sql --suggest

  # Full LLM enhancement
  python convert_ddl_with_llm.py -i hive.sql -o databricks.sql --llm-full

Note: LLM features require Databricks authentication and model serving endpoint.
        """,
    )

    parser.add_argument("-i", "--input", type=str, help="Input Hive DDL file")
    parser.add_argument("-o", "--output", type=str, help="Output Databricks DDL file")
    parser.add_argument(
        "-t",
        "--type",
        type=str,
        choices=["delta", "external"],
        default="delta",
        help="Migration type (default: delta)",
    )
    parser.add_argument(
        "-s",
        "--storage",
        type=str,
        choices=["dbfs", "azure", "aws", "s3", "unity_catalog"],
        default="dbfs",
        help="Target storage (default: dbfs)",
    )
    parser.add_argument(
        "--optimize",
        action="store_true",
        default=True,
        help="Optimize types (default: True)",
    )
    parser.add_argument(
        "--no-optimize",
        action="store_false",
        dest="optimize",
        help="Don't optimize types",
    )

    # LLM options
    parser.add_argument(
        "--validate",
        action="store_true",
        help="Validate conversion with Databricks LLM",
    )
    parser.add_argument(
        "--suggest",
        action="store_true",
        help="Get optimization suggestions from LLM",
    )
    parser.add_argument(
        "--enhance",
        action="store_true",
        help="Enhance DDL with LLM-generated comments",
    )
    parser.add_argument(
        "--llm-full",
        action="store_true",
        help="Full LLM enhancement (validate + suggest + enhance)",
    )
    parser.add_argument(
        "--endpoint",
        type=str,
        default="databricks-meta-llama-3-1-70b-instruct",
        help="Databricks serving endpoint name",
    )

    args = parser.parse_args()

    # Check if LLM features requested but not available
    if (
        args.validate or args.suggest or args.enhance or args.llm_full
    ) and not LLM_AVAILABLE:
        print(
            "❌ Error: LLM features requested but databricks-sdk/mlflow not available",
            file=sys.stderr,
        )
        print("   Install with: pip install databricks-sdk mlflow", file=sys.stderr)
        sys.exit(1)

    # Read input
    if args.input:
        input_path = Path(args.input)
        if not input_path.exists():
            print(f"❌ Error: Input file not found: {args.input}", file=sys.stderr)
            sys.exit(1)
        hive_ddl = input_path.read_text()
    else:
        hive_ddl = sys.stdin.read()

    if not hive_ddl.strip():
        print("❌ Error: No input DDL provided", file=sys.stderr)
        sys.exit(1)

    # Step 1: Rule-based conversion (always done)
    print("🔄 Converting with rule-based converter...", file=sys.stderr)
    try:
        databricks_ddl = convert_hive_to_databricks(
            hive_ddl,
            target_storage=args.storage,
            migration_type=args.type,
            optimize_types=args.optimize,
        )
        print("✅ Rule-based conversion complete", file=sys.stderr)
    except Exception as e:
        print(f"❌ Error in rule-based conversion: {e}", file=sys.stderr)
        sys.exit(1)

    # Step 2: LLM enhancement (optional)
    if args.llm_full or args.validate or args.suggest:
        print("\n🤖 Enhancing with Databricks LLM...", file=sys.stderr)

        if args.llm_full:
            # Full enhancement
            result = convert_with_llm_validation(
                hive_ddl, databricks_ddl, endpoint_name=args.endpoint, verbose=True
            )

            # Print validation results
            print("\n" + "=" * 80, file=sys.stderr)
            print("VALIDATION RESULTS", file=sys.stderr)
            print("=" * 80, file=sys.stderr)
            print(
                json.dumps(result["validation"], indent=2),
                file=sys.stderr,
            )

            # Print suggestions
            if result["suggestions"]:
                print("\n" + "=" * 80, file=sys.stderr)
                print("OPTIMIZATION SUGGESTIONS", file=sys.stderr)
                print("=" * 80, file=sys.stderr)
                for i, suggestion in enumerate(result["suggestions"], 1):
                    print(f"{i}. {suggestion}", file=sys.stderr)

            # Optionally enhance DDL
            if args.enhance:
                print("\n💬 Adding comments to DDL...", file=sys.stderr)
                databricks_ddl = enhance_ddl_with_comments(
                    hive_ddl, databricks_ddl, endpoint_name=args.endpoint
                )

        else:
            # Individual features
            if args.validate:
                validation = validate_ddl_conversion(
                    hive_ddl, databricks_ddl, endpoint_name=args.endpoint
                )
                print(
                    "\nValidation:", json.dumps(validation, indent=2), file=sys.stderr
                )

            if args.suggest:
                suggestions = suggest_optimizations(
                    databricks_ddl, endpoint_name=args.endpoint
                )
                print("\nSuggestions:", file=sys.stderr)
                for i, suggestion in enumerate(suggestions, 1):
                    print(f"  {i}. {suggestion}", file=sys.stderr)

            if args.enhance:
                databricks_ddl = enhance_ddl_with_comments(
                    hive_ddl, databricks_ddl, endpoint_name=args.endpoint
                )

    # Output result
    if args.output:
        output_path = Path(args.output)
        output_path.write_text(databricks_ddl)
        print(f"\n✅ Converted DDL written to: {args.output}", file=sys.stderr)
    else:
        print(databricks_ddl)


if __name__ == "__main__":
    main()

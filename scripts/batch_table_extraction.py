#!/usr/bin/env python3
"""
Batch table extraction from multiple NiFi XML files.

Extract table references from all NiFi XML files in a directory and save
results to a CSV file.

Usage:
    python scripts/batch_table_extraction.py --input-dir ./xml_files --output-file results.csv

Examples:
    # Basic usage
    python scripts/batch_table_extraction.py \\
        --input-dir ./nifi_pipeline_file \\
        --output-file all_tables.csv

    # Recursive search with verbose output
    python scripts/batch_table_extraction.py \\
        --input-dir ./workflows \\
        --output-file tables.csv \\
        --recursive \\
        --verbose

    # Continue processing if some files fail
    python scripts/batch_table_extraction.py \\
        --input-dir ./xml_files \\
        --output-file results.csv \\
        --continue-on-error
"""

import argparse
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd

# Add parent directory to path to import tools
sys.path.insert(0, str(Path(__file__).parent.parent))

from tools.table_extraction import extract_all_tables_from_nifi_xml


def find_xml_files(input_dir: Path, pattern: str, recursive: bool) -> List[Path]:
    """
    Find all XML files matching the pattern.

    Args:
        input_dir: Directory to search
        pattern: File pattern (e.g., "*.xml")
        recursive: Whether to search subdirectories

    Returns:
        List of Path objects for matching XML files
    """
    if recursive:
        return sorted(input_dir.rglob(pattern))
    else:
        return sorted(input_dir.glob(pattern))


def process_single_file(xml_path: Path, verbose: bool = False) -> List[Dict[str, Any]]:
    """
    Extract tables from a single XML file.

    Args:
        xml_path: Path to XML file
        verbose: Whether to print detailed progress

    Returns:
        List of table extraction results with source file info added
    """
    if verbose:
        print(f"  Processing: {xml_path.name}")

    results = extract_all_tables_from_nifi_xml(str(xml_path))

    # Add source file information to each table entry
    for table in results:
        table["source_file"] = xml_path.name
        table["source_path"] = str(xml_path)

    if verbose:
        print(f"    Found {len(results)} table reference(s)")

    return results


def process_all_files(
    xml_files: List[Path], verbose: bool, continue_on_error: bool
) -> Tuple[List[Dict[str, Any]], List[str]]:
    """
    Process all XML files and combine results.

    Args:
        xml_files: List of XML file paths to process
        verbose: Whether to print detailed progress
        continue_on_error: Whether to continue if a file fails

    Returns:
        Tuple of (all_tables, errors)
    """
    all_tables = []
    errors = []

    for i, xml_file in enumerate(xml_files, 1):
        if verbose or len(xml_files) > 1:
            print(f"[{i}/{len(xml_files)}] {xml_file.name}")

        try:
            tables = process_single_file(xml_file, verbose=False)
            all_tables.extend(tables)

            if verbose:
                print(f"  ✓ Found {len(tables)} table reference(s)")

        except Exception as e:
            error_msg = f"{xml_file.name}: {str(e)}"
            errors.append(error_msg)

            if continue_on_error:
                print(f"  ✗ Error: {error_msg}")
            else:
                print(f"  ✗ Error processing {xml_file.name}: {str(e)}")
                raise

    return all_tables, errors


def write_results_to_csv(tables: List[Dict], output_file: Path, verbose: bool):
    """
    Write combined table extraction results to CSV.

    Args:
        tables: List of table extraction results
        output_file: Output CSV file path
        verbose: Whether to print detailed info
    """
    if not tables:
        print("⚠ No tables found in any XML files")
        return

    df = pd.DataFrame(tables)

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

    # Put priority columns first, then the rest
    other_cols = [c for c in df.columns if c not in priority_cols]
    column_order = [c for c in priority_cols if c in df.columns] + other_cols
    df = df[column_order]

    # Create output directory if it doesn't exist
    output_file.parent.mkdir(parents=True, exist_ok=True)

    df.to_csv(output_file, index=False)
    print(f"✓ Wrote {len(tables)} table entries to {output_file}")

    if verbose:
        print(f"\nColumns: {', '.join(df.columns)}")


def print_summary(all_tables: List[Dict], xml_file_count: int, error_count: int):
    """
    Print summary statistics.

    Args:
        all_tables: List of all table extraction results
        xml_file_count: Total number of XML files processed
        error_count: Number of files that had errors
    """
    if not all_tables:
        return

    unique_tables = set(t["table_name"] for t in all_tables)
    unique_processors = set(t["processor_name"] for t in all_tables)
    unique_files = set(t["source_file"] for t in all_tables)

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Files processed:        {xml_file_count - error_count}/{xml_file_count}")
    print(f"Files with tables:      {len(unique_files)}")
    print(f"Total table references: {len(all_tables)}")
    print(f"Unique tables:          {len(unique_tables)}")
    print(f"Unique processors:      {len(unique_processors)}")

    if error_count > 0:
        print(f"Errors:                 {error_count}")


def main():
    """Main entry point for the batch table extraction CLI."""
    parser = argparse.ArgumentParser(
        description="Extract table references from NiFi XML files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Extract tables from all XML files in a directory
  python scripts/batch_table_extraction.py --input-dir ./xml_files --output-file tables.csv

  # Recursive search with verbose output
  python scripts/batch_table_extraction.py -i ./workflows -o tables.csv -r -v

  # Continue on errors
  python scripts/batch_table_extraction.py -i ./xml_files -o results.csv --continue-on-error
        """,
    )

    parser.add_argument(
        "-i",
        "--input-dir",
        required=True,
        type=Path,
        help="Directory containing NiFi XML files",
    )
    parser.add_argument(
        "-o",
        "--output-file",
        required=True,
        type=Path,
        help="Output CSV file path",
    )
    parser.add_argument(
        "-r",
        "--recursive",
        action="store_true",
        help="Search subdirectories recursively",
    )
    parser.add_argument(
        "-p",
        "--pattern",
        default="*.xml",
        help="File pattern to match (default: %(default)s)",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Show verbose output"
    )
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Continue processing if a file fails",
    )

    args = parser.parse_args()

    # Validate input directory
    if not args.input_dir.exists():
        print(f"✗ Error: Input directory does not exist: {args.input_dir}")
        sys.exit(1)

    if not args.input_dir.is_dir():
        print(f"✗ Error: Input path is not a directory: {args.input_dir}")
        sys.exit(1)

    # Find XML files
    print(f"Searching for XML files in: {args.input_dir}")
    if args.recursive:
        print("  (including subdirectories)")

    xml_files = find_xml_files(args.input_dir, args.pattern, args.recursive)

    if not xml_files:
        print(f"✗ No XML files found matching pattern '{args.pattern}'")
        sys.exit(0)

    print(f"Found {len(xml_files)} XML file(s)\n")

    # Process all files
    all_tables, errors = process_all_files(
        xml_files, args.verbose, args.continue_on_error
    )

    # Write results
    print()
    write_results_to_csv(all_tables, args.output_file, args.verbose)

    # Report errors
    if errors:
        print(f"\n⚠ {len(errors)} file(s) had errors:")
        for error in errors:
            print(f"  - {error}")

    # Print summary
    print_summary(all_tables, len(xml_files), len(errors))

    # Exit with error code if there were any failures
    if errors and not args.continue_on_error:
        sys.exit(1)


if __name__ == "__main__":
    main()

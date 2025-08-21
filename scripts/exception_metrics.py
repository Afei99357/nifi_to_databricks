#!/usr/bin/env python3
"""Exception handling metrics tracking and analysis."""

import argparse
import json
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

# Patterns to check for banned exception handling
BANNED_PATTERNS = [
    (
        r"except\s*Exception\s*:",
        "Generic Exception clause - use specific exception types",
    ),
    (
        r"except\s*Exception\s+as\s+\w+\s*:",
        "Exception as variable - use specific types",
    ),
    (r"except\s*:", "Bare except clause - use specific exception types"),
]

# Metrics patterns to count
METRIC_PATTERNS = {
    "try_blocks": r"try\s*:",
    "except_blocks": r"except\s+",
    "finally_blocks": r"finally\s*:",
    "with_statements": r"with\s+",
    "raise_statements": r"raise\s+",
    "assert_statements": r"assert\s+",
}

# Key metrics to track (lower is better for all)
TRACKED_METRICS = ["banned_patterns", "try_blocks", "except_blocks", "finally_blocks"]


def check_file(file_path: Path) -> Tuple[List[Tuple[int, str, str]], Dict[str, int]]:
    """Check a single file for banned patterns and collect metrics."""
    issues: List[Tuple[int, str, str]] = []
    metrics = {name: 0 for name in METRIC_PATTERNS.keys()}

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()

        lines = content.split("\n")

        for line_num, line in enumerate(lines, 1):
            # Check for banned patterns
            for pattern, message in BANNED_PATTERNS:
                if re.search(pattern, line):
                    issues.append((line_num, line.strip(), message))

            # Count metrics
            for metric_name, pattern in METRIC_PATTERNS.items():
                if re.search(pattern, line):
                    metrics[metric_name] += 1

    except Exception:
        # Skip files we can't read
        pass

    return issues, metrics


def get_python_files(directory: Path) -> List[Path]:
    """Get Python files from directory."""
    exclude_dirs = {".venv", "__pycache__", ".git", "node_modules", "build", "dist"}
    python_files = []

    for file_path in directory.rglob("*.py"):
        if not any(exclude_dir in file_path.parts for exclude_dir in exclude_dirs):
            python_files.append(file_path)

    return python_files


def load_history():
    """Load metrics history."""
    history_file = Path(".exception_metrics.json")
    if not history_file.exists():
        return []

    with open(history_file) as f:
        content = f.read().strip()
        if not content:
            return []
        return json.loads(content)


def save_metrics(metrics):
    """Save current metrics."""
    history = load_history()
    history.append({"timestamp": datetime.now().isoformat(), "metrics": metrics})
    with open(".exception_metrics.json", "w") as f:
        json.dump(history, f, indent=2)


def check_degradation(current_metrics):
    """Check if any metric got worse."""
    history = load_history()
    if len(history) < 2:
        return []

    previous = history[-2]["metrics"]
    issues = []

    for metric in TRACKED_METRICS:
        if metric in current_metrics and metric in previous:
            if current_metrics[metric] > previous[metric]:
                issues.append(
                    f"{metric}: {previous[metric]} → {current_metrics[metric]}"
                )

    return issues


def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description="Track exception handling metrics in Python files"
    )
    parser.add_argument(
        "directory", type=Path, help="Directory to scan for Python files"
    )

    args = parser.parse_args()

    if not args.directory.exists():
        print(f"Error: Directory '{args.directory}' does not exist", file=sys.stderr)
        sys.exit(1)

    if not args.directory.is_dir():
        print(f"Error: '{args.directory}' is not a directory", file=sys.stderr)
        sys.exit(1)

    total_metrics = {name: 0 for name in METRIC_PATTERNS.keys()}
    files_with_issues: List[Tuple[str, List[Tuple[int, str, str]]]] = []
    python_files = get_python_files(args.directory)

    for file_path in python_files:
        issues, metrics = check_file(file_path)
        for name, count in metrics.items():
            total_metrics[name] += count
        if issues:
            files_with_issues.append((str(file_path), issues))

    # Count banned patterns
    banned_count = sum(len(issues) for _, issues in files_with_issues)

    # Prepare metrics for tracking
    tracked_metrics = {
        "banned_patterns": banned_count,
        "try_blocks": total_metrics["try_blocks"],
        "except_blocks": total_metrics["except_blocks"],
        "finally_blocks": total_metrics["finally_blocks"],
    }

    # Save and check for degradation
    save_metrics(tracked_metrics)
    degradation = check_degradation(tracked_metrics)

    # Output results
    output_data = {
        "file_count": len(python_files),
        "banned_patterns": banned_count,
        "metrics": tracked_metrics,
        "files_with_issues": files_with_issues[:10],  # Limit to first 10 files
        "total_files_with_issues": len(files_with_issues),
    }

    if len(files_with_issues) > 10:
        output_data["note"] = (
            f"Showing first 10 of {len(files_with_issues)} files with issues"
        )

    print(json.dumps(output_data, indent=2))

    if degradation:
        print("🚫 METRICS DEGRADED:", file=sys.stderr)
        for issue in degradation:
            print(f"  ❌ {issue}", file=sys.stderr)
        sys.exit(1)
    else:
        print("✅ Metrics stable or improved", file=sys.stderr)


if __name__ == "__main__":
    main()

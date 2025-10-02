#!/usr/bin/env python3
"""Export unique external script references from NiFi templates."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Iterable, Set

# Ensure repository root is on the import path
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.script_extraction import extract_all_scripts_from_nifi_xml  # noqa: E402


def _discover_xml_files(input_path: Path, pattern: str, recursive: bool) -> Iterable[Path]:
    if input_path.is_file():
        if input_path.suffix.lower() == ".xml":
            yield input_path
        return

    if not input_path.is_dir():
        raise FileNotFoundError(f"Input path {input_path} does not exist or is not a directory")

    iterator = input_path.rglob(pattern) if recursive else input_path.glob(pattern)
    for path in iterator:
        if path.is_file() and path.suffix.lower() == ".xml":
            yield path


def _collect_external_paths(xml_file: Path) -> Set[str]:
    script_results = extract_all_scripts_from_nifi_xml(str(xml_file))
    unique_paths: Set[str] = set()
    for result in script_results:
        for script in result.get("external_scripts", []) or []:
            path = str(script.get("path") or "").strip()
            if path:
                unique_paths.add(path)
    return unique_paths


def _write_output(output_dir: Path, xml_file: Path, paths: Iterable[str]) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / f"{xml_file.stem}_external_scripts.txt"
    sorted_paths = sorted({p for p in paths if p})
    output_file.write_text("\n".join(sorted_paths), encoding="utf-8")
    return output_file


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Export unique external script filenames referenced by NiFi templates."
    )
    parser.add_argument(
        "input",
        type=Path,
        help="Path to a NiFi XML file or a directory containing XML files.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("external_script_exports"),
        help="Directory where the output .txt files will be written (default: %(default)s).",
    )
    parser.add_argument(
        "--pattern",
        default="*.xml",
        help="Glob pattern used to discover XML files inside directories (default: %(default)s).",
    )
    parser.add_argument(
        "--recursive",
        action="store_true",
        help="Search for XML files recursively when the input is a directory.",
    )
    parser.add_argument(
        "--skip-empty",
        action="store_true",
        help="Skip writing a file when no external scripts are discovered.",
    )

    args = parser.parse_args()

    xml_files = list(_discover_xml_files(args.input, args.pattern, args.recursive))
    if not xml_files:
        print(f"No XML files found under {args.input}", file=sys.stderr)
        return 1

    total_paths = 0
    written_files = 0

    for xml_file in xml_files:
        external_paths = _collect_external_paths(xml_file)
        if not external_paths and args.skip_empty:
            print(f"[skip] {xml_file} (no external scripts)")
            continue

        output_file = _write_output(args.output_dir, xml_file, external_paths)
        written_files += 1
        total_paths += len(external_paths)
        print(f"[write] {output_file} ({len(external_paths)} paths)")

    print(
        f"Completed. Processed {len(xml_files)} template(s); "
        f"wrote {written_files} file(s) containing {total_paths} unique paths."
    )
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    raise SystemExit(main())

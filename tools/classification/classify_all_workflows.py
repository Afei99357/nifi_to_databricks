"""CLI to batch classify NiFi workflows using the declarative rules engine."""

from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from pathlib import Path
from typing import Iterable, List, Sequence

if __package__ in {None, ""}:
    PROJECT_ROOT = Path(__file__).resolve().parents[2]
    if str(PROJECT_ROOT) not in sys.path:
        sys.path.insert(0, str(PROJECT_ROOT))

from tools.classification.rules_engine import classify_workflow

DEFAULT_INPUT = "nifi_pipeline_file"
DEFAULT_OUTPUT = Path("derived_classification")
SUMMARY_FILENAME = "summary.csv"


def _gather_templates(input_arg: str) -> List[Path]:
    """Resolve an input argument into a list of NiFi template paths."""
    candidate = Path(input_arg)
    if candidate.exists():
        if candidate.is_file():
            return [candidate]
        if candidate.is_dir():
            return sorted(candidate.rglob("*.xml"))
    return sorted(Path().glob(input_arg))


def _ensure_output_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _flatten_rows(template: Path, payload: dict) -> Sequence[dict]:
    workflow = payload.get("workflow", {})
    template_name = workflow.get("filename", template.name)
    template_path = workflow.get("path", str(template))

    rows = []
    for record in payload.get("classifications", []):
        features = record.get("feature_evidence", {})
        sql = features.get("sql", {})
        scripts = features.get("scripts", {})
        connections = features.get("connections", {})
        row = {
            "template": template_name,
            "template_path": template_path,
            "processor_id": record.get("processor_id"),
            "processor_name": record.get("name"),
            "processor_type": record.get("processor_type"),
            "short_type": record.get("short_type"),
            "parent_group": record.get("parent_group"),
            "migration_category": record.get("migration_category"),
            "databricks_target": record.get("databricks_target"),
            "confidence": record.get("confidence"),
            "classification_source": record.get("classification_source"),
            "rule": record.get("rule"),
            "sql_has_dml": sql.get("has_dml"),
            "sql_has_metadata_only": sql.get("has_metadata_only"),
            "scripts_inline_count": scripts.get("inline_count"),
            "scripts_external_count": scripts.get("external_count"),
            "uses_variables": features.get("uses_variables"),
            "incoming_count": len(connections.get("incoming", [])),
            "outgoing_count": len(connections.get("outgoing", [])),
        }
        rows.append(row)
    return rows


def _write_summary(rows: Iterable[dict], csv_path: Path, *, append: bool) -> None:
    rows = list(rows)
    if not rows:
        return

    fieldnames = [
        "template",
        "template_path",
        "processor_id",
        "processor_name",
        "processor_type",
        "short_type",
        "parent_group",
        "migration_category",
        "databricks_target",
        "confidence",
        "classification_source",
        "rule",
        "sql_has_dml",
        "sql_has_metadata_only",
        "scripts_inline_count",
        "scripts_external_count",
        "uses_variables",
        "incoming_count",
        "outgoing_count",
    ]

    write_header = not append or not csv_path.exists()
    mode = "a" if append else "w"
    with csv_path.open(mode, newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        writer.writerows(rows)


def classify_all(
    input_arg: str,
    output_dir: Path,
    *,
    append_summary: bool,
    verbose: bool = True,
) -> dict:
    """Classify all templates from ``input_arg`` into ``output_dir``."""

    start = time.perf_counter()
    templates = _gather_templates(input_arg)
    if verbose:
        print(f"Found {len(templates)} template(s) under {input_arg}")

    if not templates:
        return {"templates": 0, "processors": 0, "ambiguous": 0, "duration": 0.0}

    _ensure_output_dir(output_dir)
    summary_rows: List[dict] = []
    total_processors = 0
    total_ambiguous = 0

    for template in templates:
        result = classify_workflow(str(template))
        json_path = output_dir / f"{template.stem}.json"
        with json_path.open("w", encoding="utf-8") as fh:
            json.dump(result, fh, indent=2, ensure_ascii=False)
        rows = _flatten_rows(template, result)
        summary_rows.extend(rows)
        total_processors += len(rows)
        total_ambiguous += len(result.get("ambiguous", []))
        if verbose:
            print(
                f"Processed {template.name}: {len(rows)} processors, "
                f"{len(result.get('ambiguous', []))} ambiguous"
            )

    summary_path = output_dir / SUMMARY_FILENAME
    _write_summary(summary_rows, summary_path, append=append_summary)

    duration = time.perf_counter() - start
    stats = {
        "templates": len(templates),
        "processors": total_processors,
        "ambiguous": total_ambiguous,
        "duration": duration,
        "summary_path": str(summary_path),
    }
    if verbose:
        print(
            "Run complete: "
            f"{stats['processors']} processors across {stats['templates']} template(s); "
            f"{stats['ambiguous']} ambiguous; {duration:.2f}s"
        )
        print(f"Summary CSV: {summary_path}")
    return stats


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input",
        default=DEFAULT_INPUT,
        help="Directory, file, or glob pattern for NiFi XML templates",
    )
    parser.add_argument(
        "--output",
        default=str(DEFAULT_OUTPUT),
        help="Directory to store classification artefacts",
    )
    parser.add_argument(
        "--append-summary",
        action="store_true",
        help="Append to an existing summary CSV instead of overwriting",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress progress logging",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    output_dir = Path(args.output)
    stats = classify_all(
        args.input,
        output_dir,
        append_summary=args.append_summary,
        verbose=not args.quiet,
    )
    return 0 if stats["processors"] >= 0 else 1


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    raise SystemExit(main())

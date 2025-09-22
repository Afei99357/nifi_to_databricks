"""Generate candidate classification rules from derived summary CSVs."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional

import pandas as pd
import yaml

from .rules_engine import load_rules


@dataclass
class SuggestionConfig:
    summary_path: Path
    output_path: Path
    only_new: bool
    include_evidence: bool
    top_n_evidence: int


def _load_summary(summary_path: Path) -> pd.DataFrame:
    if not summary_path.exists():
        raise FileNotFoundError(f"Summary CSV not found: {summary_path}")
    df = pd.read_csv(summary_path)
    if "processor_type" not in df.columns:
        raise ValueError("Summary CSV must include a 'processor_type' column")
    return df


def _covered_processor_types(existing_rules) -> set[str]:
    covered: set[str] = set()
    for rule in existing_rules:
        if rule.processor_types:
            covered.update(rule.processor_types)
    return covered


def _format_notes(group: pd.DataFrame) -> str:
    total = len(group)
    fields = {
        "sql_has_dml": group.get("sql_has_dml", pd.Series(dtype=float)).mean(),
        "sql_has_metadata_only": group.get(
            "sql_has_metadata_only", pd.Series(dtype=float)
        ).mean(),
        "inline_scripts": group.get(
            "scripts_inline_count", pd.Series(dtype=float)
        ).mean(),
        "external_scripts": group.get(
            "scripts_external_count", pd.Series(dtype=float)
        ).mean(),
        "uses_variables": group.get("uses_variables", pd.Series(dtype=float)).mean(),
    }
    parts = [f"Derived from {total} processor(s)"]
    for field, value in fields.items():
        if pd.notna(value):
            parts.append(f"avg {field}={value:.2f}")
    return "; ".join(parts)


def _infer_category_and_target(group: pd.DataFrame) -> tuple[str, str]:
    sql_dml_mean = group.get("sql_has_dml", pd.Series(dtype=float)).mean() or 0
    external_mean = (
        group.get("scripts_external_count", pd.Series(dtype=float)).mean() or 0
    )
    inline_mean = group.get("scripts_inline_count", pd.Series(dtype=float)).mean() or 0
    uses_var_mean = group.get("uses_variables", pd.Series(dtype=float)).mean() or 0

    if sql_dml_mean > 0.2:
        return "Business Logic", "SQL transformation"
    if external_mean > 0.5:
        return "Source Adapter", "External script"
    if inline_mean == 0 and sql_dml_mean == 0 and external_mean == 0:
        return "Sink Adapter", "Delta write"
    if uses_var_mean > 0.5:
        return "Orchestration / Monitoring", "Workflow control"
    return "Infrastructure Only", "Workflow plumbing"


def _build_suggestion(group: pd.DataFrame, config: SuggestionConfig) -> dict:
    processor_type = group.name
    category, target = _infer_category_and_target(group)
    suggestion: dict = {
        "name": f"Auto: {processor_type.split('.')[-1]}",
        "processor_types": [processor_type],
        "conditions": [],
        "migration_category": category,
        "databricks_target": target,
        "notes": _format_notes(group),
    }

    if config.include_evidence:
        evidence_rows = group.head(config.top_n_evidence)
        suggestion["evidence"] = [
            {
                "processor_id": row.get("processor_id"),
                "name": row.get("processor_name"),
                "template": row.get("template"),
                "migration_category": row.get("migration_category"),
                "confidence": row.get("confidence"),
            }
            for _, row in evidence_rows.iterrows()
        ]

    return suggestion


def generate_suggestions(config: SuggestionConfig) -> List[dict]:
    summary_df = _load_summary(config.summary_path)
    rules = load_rules()
    covered = _covered_processor_types(rules) if config.only_new else set()

    suggestions: List[dict] = []
    for processor_type, group in summary_df.groupby("processor_type", dropna=True):
        if not processor_type:
            continue
        if config.only_new and processor_type in covered:
            continue
        suggestions.append(_build_suggestion(group, config))
    return suggestions


def write_suggestions(suggestions: Iterable[dict], output_path: Path) -> None:
    payload = {
        "generated_by": "tools.classification.suggest_rules",
        "suggestions": list(suggestions),
    }
    with output_path.open("w", encoding="utf-8") as fh:
        yaml.safe_dump(payload, fh, sort_keys=False)


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--summary",
        required=True,
        help="Path to the derived summary CSV produced by classify_all_workflows",
    )
    parser.add_argument(
        "--output",
        default="classification_rules.suggestions.yaml",
        help="File to write YAML suggestions (default: classification_rules.suggestions.yaml)",
    )
    parser.add_argument(
        "--only-new",
        action="store_true",
        help="Skip processor types already covered by existing rules",
    )
    parser.add_argument(
        "--include-evidence",
        action="store_true",
        help="Attach representative processor IDs and templates to suggestions",
    )
    parser.add_argument(
        "--top-n-evidence",
        type=int,
        default=3,
        help="Number of processor examples to include when --include-evidence is set",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_args(argv)
    config = SuggestionConfig(
        summary_path=Path(args.summary),
        output_path=Path(args.output),
        only_new=args.only_new,
        include_evidence=args.include_evidence,
        top_n_evidence=max(1, args.top_n_evidence),
    )

    suggestions = generate_suggestions(config)
    if not suggestions:
        print("No suggestions generated.")
        return 0

    write_suggestions(suggestions, config.output_path)
    print(f"Wrote {len(suggestions)} suggestion(s) to {config.output_path}")
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    raise SystemExit(main())

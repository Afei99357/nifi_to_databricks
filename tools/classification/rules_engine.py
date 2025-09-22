"""Declarative NiFi processor classifier.

Loads rules from ``classification_rules.yaml`` and applies them to processor
features produced by :mod:`tools.classification.processor_features`.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import yaml

from .processor_features import extract_processor_features

RULES_FILE = Path("classification_rules.yaml")
OVERRIDES_FILE = Path("classification_overrides.yaml")


@dataclass
class Condition:
    field: str
    equals: Optional[Any] = None
    not_equals: Optional[Any] = None
    gt: Optional[float] = None
    lt: Optional[float] = None
    contains: Optional[Any] = None
    contains_any: Optional[List[Any]] = None

    def evaluate(self, feature: Dict[str, Any]) -> bool:
        value = _get_field(feature, self.field)
        if self.equals is not None and value != self.equals:
            return False
        if self.not_equals is not None and value == self.not_equals:
            return False
        if self.gt is not None:
            try:
                if float(value or 0) <= float(self.gt):
                    return False
            except (TypeError, ValueError):
                return False
        if self.lt is not None:
            try:
                if float(value or 0) >= float(self.lt):
                    return False
            except (TypeError, ValueError):
                return False
        if self.contains is not None:
            if isinstance(value, (list, tuple, set)):
                if self.contains not in value:
                    return False
            elif isinstance(value, str):
                if str(self.contains) not in value:
                    return False
            else:
                return False
        if self.contains_any is not None:
            if not isinstance(value, (list, tuple, set)):
                return False
            if not any(item in value for item in self.contains_any):
                return False
        return True


@dataclass
class Rule:
    name: str
    migration_category: str
    databricks_target: str
    confidence: float
    notes: str = ""
    processor_types: Optional[List[str]] = None
    conditions: List[Condition] = field(default_factory=list)

    def matches(self, feature: Dict[str, Any]) -> bool:
        if self.processor_types:
            proc_type = feature.get("processor_type")
            if proc_type not in self.processor_types:
                return False
        return all(condition.evaluate(feature) for condition in self.conditions)


def _get_field(feature: Dict[str, Any], dotted_path: str) -> Any:
    parts = dotted_path.split(".")
    current: Any = feature
    for part in parts:
        if isinstance(current, dict) and part in current:
            current = current[part]
        else:
            return None
    return current


def _normalise_rule(raw_rule: Dict[str, Any]) -> Rule:
    conditions = [Condition(**cond) for cond in raw_rule.get("conditions", [])]
    processor_types = raw_rule.get("processor_types")
    if processor_types and not isinstance(processor_types, list):
        processor_types = [processor_types]
    return Rule(
        name=raw_rule.get("name", "Unnamed Rule"),
        migration_category=raw_rule.get("migration_category", "Infrastructure Only"),
        databricks_target=raw_rule.get("databricks_target", "N/A"),
        confidence=float(raw_rule.get("confidence", 0.0)),
        notes=raw_rule.get("notes", ""),
        processor_types=processor_types,
        conditions=conditions,
    )


def load_rules(path: Optional[Path] = None) -> List[Rule]:
    rules_path = path or RULES_FILE
    if not rules_path.exists():
        raise FileNotFoundError(f"Classification rules file not found: {rules_path}")
    with rules_path.open("r", encoding="utf-8") as fh:
        payload = yaml.safe_load(fh) or {}
    raw_rules = payload.get("rules") if isinstance(payload, dict) else payload
    if not raw_rules:
        raise ValueError(f"No rules found in {rules_path}")
    return [_normalise_rule(rule) for rule in raw_rules]


def load_overrides(path: Optional[Path] = None) -> Dict[str, Dict[str, Any]]:
    overrides_path = path or OVERRIDES_FILE
    if not overrides_path.exists():
        return {}
    with overrides_path.open("r", encoding="utf-8") as fh:
        payload = yaml.safe_load(fh) or {}
    if isinstance(payload, dict):
        return payload
    raise ValueError(f"Overrides file must be a mapping: {overrides_path}")


def classify_processor(
    feature: Dict[str, Any], rules: Iterable[Rule]
) -> Dict[str, Any]:
    for rule in rules:
        if rule.matches(feature):
            return {
                "migration_category": rule.migration_category,
                "databricks_target": rule.databricks_target,
                "confidence": rule.confidence,
                "rule": rule.name,
                "notes": rule.notes,
            }
    # Should not happen if a catch-all rule exists, but guard regardless.
    return {
        "migration_category": "Ambiguous",
        "databricks_target": "Needs review",
        "confidence": 0.0,
        "rule": "No Match",
        "notes": "No declarative rule matched this processor",
    }


def apply_overrides(
    processor_id: str,
    classification: Dict[str, Any],
    overrides: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    override = overrides.get(processor_id)
    if not override:
        return {**classification, "source": "rule"}
    merged = {**classification, **override}
    merged.setdefault("notes", "Override applied")
    merged["source"] = "override"
    if "confidence" not in override:
        merged["confidence"] = max(classification.get("confidence", 0.0), 0.95)
    return merged


def classify_workflow(
    xml_path: str,
    *,
    rules_path: Optional[Path] = None,
    overrides_path: Optional[Path] = None,
) -> Dict[str, Any]:
    rules = load_rules(rules_path)
    overrides = load_overrides(overrides_path)
    feature_payload = extract_processor_features(xml_path)

    classifications: List[Dict[str, Any]] = []
    summary: Dict[str, int] = {}
    ambiguous: List[Dict[str, Any]] = []

    for feature in feature_payload.get("processors", []):
        processor_id = feature.get("id")
        if not processor_id:
            continue
        base_classification = classify_processor(feature, rules)
        final_classification = apply_overrides(
            processor_id, base_classification, overrides
        )

        record = {
            "processor_id": processor_id,
            "id": processor_id,
            "name": feature.get("name"),
            "processor_type": feature.get("processor_type"),
            "type": feature.get("processor_type"),
            "short_type": feature.get("short_type"),
            "parent_group": feature.get("parent_group"),
            "migration_category": final_classification.get("migration_category"),
            "databricks_target": final_classification.get("databricks_target"),
            "confidence": final_classification.get("confidence"),
            "notes": final_classification.get("notes"),
            "classification_source": final_classification.get("source", "rule"),
            "rule": final_classification.get("rule", ""),
            "feature_evidence": {
                "sql": feature.get("sql", {}),
                "scripts": {
                    "inline_count": feature.get("scripts", {}).get("inline_count", 0),
                    "external_count": feature.get("scripts", {}).get(
                        "external_count", 0
                    ),
                    "external_hosts": feature.get("scripts", {}).get(
                        "external_hosts", []
                    ),
                },
                "tables": feature.get("tables", {}),
                "connections": feature.get("connections", {}),
                "controller_services": feature.get("controller_services", []),
                "uses_variables": feature.get("uses_variables"),
            },
            "properties": feature.get("properties"),
            "classification": final_classification.get("migration_category"),
            "data_manipulation_type": final_classification.get("migration_category"),
        }

        classifications.append(record)

        category = final_classification.get("migration_category", "Ambiguous")
        summary[category] = summary.get(category, 0) + 1
        if category == "Ambiguous" or final_classification.get("confidence", 0.0) < 0.5:
            ambiguous.append(record)

    return {
        "workflow": feature_payload.get("workflow", {}),
        "summary": summary,
        "classifications": classifications,
        "ambiguous": ambiguous,
        "rules_file": str((rules_path or RULES_FILE).resolve()),
        "overrides_file": str((overrides_path or OVERRIDES_FILE).resolve()),
    }


def to_json(result: Dict[str, Any]) -> str:
    """Serialize classification results to JSON for debugging/export."""
    return json.dumps(result, indent=2, ensure_ascii=False)


__all__ = [
    "Condition",
    "Rule",
    "apply_overrides",
    "classify_processor",
    "classify_workflow",
    "load_overrides",
    "load_rules",
    "to_json",
]

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

from .group_profiles import build_group_profiles
from .processor_features import extract_processor_features

PROMOTION_CANDIDATE_CATEGORIES = {
    "Infrastructure Only",
    "Orchestration / Monitoring",
}
PROMOTION_TARGET_CATEGORY = "Business Logic"
PROMOTION_TARGET_LABEL = "Variable setup"
PROMOTION_CONFIDENCE_FLOOR = 0.6
PROMOTION_SOURCE = "promotion"
ESSENTIAL_DOWNSTREAM_CATEGORIES = {
    "Business Logic",
    "Sink Adapter",
}

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
    processor_short_types: Optional[List[str]] = None
    conditions: List[Condition] = field(default_factory=list)

    def matches(self, feature: Dict[str, Any]) -> bool:
        # Check fully-qualified processor types
        if self.processor_types:
            proc_type = feature.get("processor_type")
            if proc_type not in self.processor_types:
                return False
        # Check short processor types (e.g., "PutHDFS" instead of "org.apache.nifi.processors.hadoop.PutHDFS")
        if self.processor_short_types:
            short_type = feature.get("short_type")
            if short_type not in self.processor_short_types:
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
    processor_short_types = raw_rule.get("processor_short_types")
    if processor_short_types and not isinstance(processor_short_types, list):
        processor_short_types = [processor_short_types]
    return Rule(
        name=raw_rule.get("name", "Unnamed Rule"),
        migration_category=raw_rule.get("migration_category", "Infrastructure Only"),
        databricks_target=raw_rule.get("databricks_target", "N/A"),
        confidence=float(raw_rule.get("confidence", 0.0)),
        notes=raw_rule.get("notes", ""),
        processor_types=processor_types,
        processor_short_types=processor_short_types,
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
    outgoing_map: Dict[str, List[str]] = {}
    variables_by_proc: Dict[str, Dict[str, Any]] = {}
    variable_consumers: Dict[str, set[str]] = {}

    for feature in feature_payload.get("processors", []):
        processor_id = feature.get("id")
        if not processor_id:
            continue
        base_classification = classify_processor(feature, rules)
        final_classification = apply_overrides(
            processor_id, base_classification, overrides
        )

        outgoing_connections = feature.get("connections", {}).get("outgoing", []) or []
        outgoing_map[processor_id] = list(outgoing_connections)

        variables_payload = feature.get("variables", {}) or {}
        variables_by_proc[processor_id] = variables_payload
        for usage in variables_payload.get("uses", []) or []:
            var_name = usage.get("variable")
            if var_name:
                variable_consumers.setdefault(var_name, set()).add(processor_id)

        scripts_feature = feature.get("scripts", {}) or {}
        scripts_payload = {
            "inline_count": scripts_feature.get("inline_count", 0),
            "external_count": scripts_feature.get("external_count", 0),
            "external_hosts": scripts_feature.get("external_hosts", []),
        }

        record = {
            "processor_id": processor_id,
            "id": processor_id,
            "name": feature.get("name"),
            "processor_type": feature.get("processor_type"),
            "type": feature.get("processor_type"),
            "short_type": feature.get("short_type"),
            "parent_group": feature.get("parent_group"),
            "parent_group_path": feature.get("parent_group_path"),
            "migration_category": final_classification.get("migration_category"),
            "databricks_target": final_classification.get("databricks_target"),
            "confidence": final_classification.get("confidence"),
            "notes": final_classification.get("notes"),
            "classification_source": final_classification.get("source", "rule"),
            "rule": final_classification.get("rule", ""),
            "feature_evidence": {
                "sql": feature.get("sql", {}),
                "scripts": scripts_payload,
                "tables": feature.get("tables", {}),
                "connections": feature.get("connections", {}),
                "controller_services": feature.get("controller_services", []),
                "uses_variables": feature.get("uses_variables"),
                "variables": variables_payload,
            },
            "properties": feature.get("properties"),
            "classification": final_classification.get("migration_category"),
            "data_manipulation_type": final_classification.get("migration_category"),
        }

        classifications.append(record)

    _apply_metadata_promotions(
        classifications,
        outgoing_map,
        variables_by_proc,
        variable_consumers,
    )

    summary, ambiguous = _summarise_classifications(classifications)
    group_profiles = build_group_profiles(classifications)

    return {
        "workflow": feature_payload.get("workflow", {}),
        "summary": summary,
        "classifications": classifications,
        "ambiguous": ambiguous,
        "group_profiles": group_profiles,
        "rules_file": str((rules_path or RULES_FILE).resolve()),
        "overrides_file": str((overrides_path or OVERRIDES_FILE).resolve()),
    }


def to_json(result: Dict[str, Any]) -> str:
    """Serialize classification results to JSON for debugging/export."""
    return json.dumps(result, indent=2, ensure_ascii=False)


def _summarise_classifications(
    classifications: Iterable[Dict[str, Any]],
) -> tuple[Dict[str, int], List[Dict[str, Any]]]:
    summary: Dict[str, int] = {}
    ambiguous: List[Dict[str, Any]] = []
    for record in classifications:
        category = record.get("migration_category") or "Ambiguous"
        summary[category] = summary.get(category, 0) + 1
        confidence = record.get("confidence") or 0.0
        if category == "Ambiguous" or confidence < 0.5:
            ambiguous.append(record)
    return summary, ambiguous


def _apply_metadata_promotions(
    classifications: List[Dict[str, Any]],
    outgoing_map: Dict[str, List[str]],
    variables_by_proc: Dict[str, Dict[str, Any]],
    variable_consumers: Dict[str, set[str]],
) -> None:
    record_by_id = {
        record.get("processor_id"): record
        for record in classifications
        if record.get("processor_id")
    }
    reachable_cache: Dict[str, set[str]] = {}

    def reachable_downstream(start: str) -> set[str]:
        if start in reachable_cache:
            return reachable_cache[start]
        visited: set[str] = set()
        queue: List[str] = list(outgoing_map.get(start, []))
        while queue:
            current = queue.pop(0)
            if current in visited:
                continue
            visited.add(current)
            queue.extend(outgoing_map.get(current, []))
        reachable_cache[start] = visited
        return visited

    for record in classifications:
        processor_id = record.get("processor_id")
        if not processor_id:
            continue
        if record.get("classification_source") == "override":
            continue
        category = record.get("migration_category")
        if category not in PROMOTION_CANDIDATE_CATEGORIES:
            continue

        variables_payload = variables_by_proc.get(processor_id, {}) or {}
        defined_entries = variables_payload.get("defines", []) or []
        defined_vars = [
            entry.get("variable") for entry in defined_entries if entry.get("variable")
        ]
        if not defined_vars:
            continue

        downstream_ids = reachable_downstream(processor_id)

        promoted_variables: set[str] = set()
        target_processors: set[str] = set()

        for var_name in defined_vars:
            consumers = variable_consumers.get(var_name, set())
            for consumer_id in consumers:
                if consumer_id == processor_id:
                    continue
                consumer_record = record_by_id.get(consumer_id)
                if not consumer_record:
                    continue
                consumer_category = consumer_record.get("migration_category")
                if consumer_category in ESSENTIAL_DOWNSTREAM_CATEGORIES:
                    promoted_variables.add(var_name)
                    target_processors.add(consumer_record.get("name") or consumer_id)

        if not promoted_variables:
            continue

        previous_category = record.get("migration_category")
        record["migration_category"] = PROMOTION_TARGET_CATEGORY
        record["classification"] = PROMOTION_TARGET_CATEGORY
        record["data_manipulation_type"] = PROMOTION_TARGET_CATEGORY
        record["databricks_target"] = PROMOTION_TARGET_LABEL
        record["classification_source"] = PROMOTION_SOURCE
        record["confidence"] = max(
            record.get("confidence") or 0.0, PROMOTION_CONFIDENCE_FLOOR
        )

        reason = (
            f"Promoted from {previous_category} after detecting variables "
            f"{', '.join(sorted(promoted_variables))} consumed by downstream processors "
            f"{', '.join(sorted(target_processors))}."
        )
        record["promotion_reason"] = reason
        if record.get("notes"):
            record["notes"] = f"{record['notes']} | {reason}"
        else:
            record["notes"] = reason


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

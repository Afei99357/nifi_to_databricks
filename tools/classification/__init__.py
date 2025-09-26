"""Classification helpers for NiFi → Databricks migration."""

from .classify_all_workflows import classify_all
from .classify_all_workflows import main as cli_main
from .group_profiles import build_group_profiles
from .processor_features import extract_processor_features
from .processor_features import to_json as features_to_json
from .rules_engine import (
    Condition,
    Rule,
    apply_overrides,
    classify_processor,
    classify_workflow,
    load_overrides,
    load_rules,
    to_json,
)
from .suggest_rules import generate_suggestions
from .suggest_rules import main as suggest_main

__all__ = [
    "Condition",
    "Rule",
    "apply_overrides",
    "classify_processor",
    "classify_workflow",
    "classify_all",
    "cli_main",
    "build_group_profiles",
    "extract_processor_features",
    "features_to_json",
    "generate_suggestions",
    "load_overrides",
    "load_rules",
    "to_json",
    "suggest_main",
]

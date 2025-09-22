import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from tools.classification import classify_processor, load_rules


def _make_feature(**overrides):
    base = {
        "processor_type": "org.apache.nifi.processors.standard.ExecuteSQL",
        "sql": {"has_dml": True, "has_metadata_only": False},
        "scripts": {"inline_count": 0, "external_count": 0},
        "tables": {"tables": [], "properties": []},
        "connections": {"incoming": [], "outgoing": []},
        "controller_services": [],
        "uses_variables": False,
    }
    base.update(overrides)
    return base


def test_execute_sql_with_dml_matches_business_logic_rule():
    rules = load_rules()
    feature = _make_feature()
    result = classify_processor(feature, rules)

    assert result["migration_category"] == "Business Logic"
    assert "SQL" in result["rule"]
    assert result["confidence"] >= 0.8


def test_unmatched_processor_falls_back_to_default_rule():
    rules = load_rules()
    feature = _make_feature(
        processor_type="org.apache.nifi.processors.custom.Unknown",
        sql={"has_dml": False, "has_metadata_only": False},
    )
    result = classify_processor(feature, rules)

    assert result["migration_category"] in {"Infrastructure Only", "Ambiguous"}
    assert result["confidence"] <= 0.5

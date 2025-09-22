import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from tools.classification import classify_processor, classify_workflow, load_rules
from tools.classification.rules_engine import Rule


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


def test_metadata_promotion_upgrades_variable_provider(monkeypatch):
    infra_type = "org.example.Promoter"
    business_type = "org.example.Consumer"

    sample_features = {
        "workflow": {
            "filename": "synthetic.xml",
            "path": "synthetic.xml",
            "processor_count": 2,
            "connection_count": 1,
        },
        "processors": [
            {
                "id": "A",
                "name": "Define Variable",
                "processor_type": infra_type,
                "short_type": "Promoter",
                "parent_group": "Root",
                "properties": {},
                "controller_services": [],
                "connections": {"incoming": [], "outgoing": ["B"]},
                "scripts": {
                    "inline_count": 0,
                    "external_count": 0,
                    "inline_scripts": [],
                    "external_scripts": [],
                    "external_hosts": [],
                },
                "sql": {
                    "has_sql": False,
                    "has_dml": False,
                    "has_metadata_only": False,
                },
                "tables": {"tables": [], "properties": []},
                "variables": {
                    "defines": [{"variable": "foo"}],
                    "uses": [],
                },
                "uses_variables": False,
            },
            {
                "id": "B",
                "name": "Consume Variable",
                "processor_type": business_type,
                "short_type": "Consumer",
                "parent_group": "Root",
                "properties": {},
                "controller_services": [],
                "connections": {"incoming": ["A"], "outgoing": []},
                "scripts": {
                    "inline_count": 0,
                    "external_count": 0,
                    "inline_scripts": [],
                    "external_scripts": [],
                    "external_hosts": [],
                },
                "sql": {
                    "has_sql": True,
                    "has_dml": True,
                    "has_metadata_only": False,
                },
                "tables": {"tables": [], "properties": []},
                "variables": {
                    "defines": [],
                    "uses": [{"variable": "foo"}],
                },
                "uses_variables": True,
            },
        ],
    }

    monkeypatch.setattr(
        "tools.classification.rules_engine.extract_processor_features",
        lambda _: sample_features,
    )
    monkeypatch.setattr(
        "tools.classification.rules_engine.load_overrides",
        lambda path=None: {},
    )

    custom_rules = [
        Rule(
            name="Consumer business logic",
            migration_category="Business Logic",
            databricks_target="SQL transformation",
            confidence=0.9,
            processor_types=[business_type],
            conditions=[],
            notes="",
        ),
        Rule(
            name="Promoter infrastructure",
            migration_category="Infrastructure Only",
            databricks_target="Workflow plumbing",
            confidence=0.2,
            processor_types=[infra_type],
            conditions=[],
            notes="",
        ),
    ]
    monkeypatch.setattr(
        "tools.classification.rules_engine.load_rules",
        lambda path=None: custom_rules,
    )

    result = classify_workflow("synthetic.xml")
    promoted = {rec["processor_id"]: rec for rec in result["classifications"]}

    promoter = promoted["A"]
    consumer = promoted["B"]

    assert promoter["migration_category"] == "Business Logic"
    assert promoter["classification_source"] == "promotion"
    assert promoter["databricks_target"] == "Variable setup"
    assert promoter.get("promotion_reason")
    assert promoter["confidence"] >= 0.6

    assert consumer["migration_category"] == "Business Logic"
    assert result["summary"].get("Business Logic") == 2

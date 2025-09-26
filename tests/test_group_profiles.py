import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from tools.classification.group_profiles import build_group_profiles


def test_group_profiles_aggregate_metrics():
    records = [
        {
            "processor_id": "A",
            "name": "Transform inbound data",
            "parent_group": "GroupA",
            "parent_group_path": "Root/GroupA",
            "migration_category": "Business Logic",
            "confidence": 0.9,
            "feature_evidence": {
                "scripts": {"inline_count": 1, "external_count": 0},
                "variables": {
                    "defines": [{"variable": "foo"}],
                    "uses": [],
                },
                "controller_services": ["svc1"],
                "connections": {"incoming": [], "outgoing": ["B"]},
            },
        },
        {
            "processor_id": "B",
            "name": "Ship to downstream system",
            "parent_group": "GroupB",
            "parent_group_path": "Root/GroupB",
            "migration_category": "Infrastructure Only",
            "confidence": 0.4,
            "feature_evidence": {
                "scripts": {"inline_count": 0, "external_count": 1},
                "variables": {
                    "defines": [],
                    "uses": [{"variable": "foo"}],
                },
                "controller_services": [],
                "connections": {"incoming": ["A"], "outgoing": []},
            },
        },
        {
            "processor_id": "C",
            "name": "Trigger follow-up",
            "parent_group": "GroupA",
            "parent_group_path": "Root/GroupA",
            "migration_category": "Orchestration / Monitoring",
            "confidence": 0.7,
            "feature_evidence": {
                "scripts": {"inline_count": 0, "external_count": 0},
                "variables": {
                    "defines": [],
                    "uses": [{"variable": "foo"}],
                },
                "controller_services": [],
                "connections": {"incoming": ["B"], "outgoing": []},
            },
        },
    ]

    profiles = build_group_profiles(records)
    profiles_by_path = {profile["group_path"]: profile for profile in profiles}

    assert set(profiles_by_path) == {"Root/GroupA", "Root/GroupB"}

    group_a = profiles_by_path["Root/GroupA"]
    assert group_a["processor_count"] == 2
    assert group_a["migration_category_counts"]["Business Logic"] == 1
    assert group_a["migration_category_counts"]["Orchestration / Monitoring"] == 1
    assert group_a["needs_migration_count"] == 2
    assert group_a["ambiguous_count"] == 0
    assert group_a["inline_script_total"] == 1
    assert group_a["external_script_total"] == 0
    assert group_a["variables_defined"] == ["foo"]
    assert group_a["variables_used"] == ["foo"]
    assert group_a["controller_services"] == ["svc1"]
    assert group_a["incoming_groups"] == ["Root/GroupB"]
    assert group_a["outgoing_groups"] == ["Root/GroupB"]

    group_b = profiles_by_path["Root/GroupB"]
    assert group_b["processor_count"] == 1
    assert group_b["needs_migration_count"] == 0
    assert group_b["ambiguous_count"] == 1  # confidence below 0.5 counts as ambiguous
    assert group_b["external_script_total"] == 1
    assert group_b["has_external_scripts"] is True
    assert group_b["external_script_processors"] == [
        {"id": "B", "name": "Ship to downstream system"}
    ]
    assert group_b["incoming_groups"] == ["Root/GroupA"]
    assert group_b["outgoing_groups"] == []


def test_group_profiles_falls_back_to_group_name_when_path_missing():
    records = [
        {
            "processor_id": "X",
            "name": "Root task",
            "parent_group": "Root",
            "migration_category": "Business Logic",
            "confidence": 0.8,
            "feature_evidence": {
                "scripts": {"inline_count": 0, "external_count": 0},
                "variables": {"defines": [], "uses": []},
                "controller_services": [],
                "connections": {"incoming": [], "outgoing": []},
            },
        }
    ]

    profiles = build_group_profiles(records)
    assert len(profiles) == 1
    profile = profiles[0]
    assert profile["group_path"] == "Root"
    assert profile["group"] == "Root"
    assert profile["processor_count"] == 1

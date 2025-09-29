import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from tools.conversion.processor_batches import (
    batch_snapshots,
    build_batches_from_records,
    build_script_lookup,
    estimate_prompt_chars,
    snapshot_from_record,
)


def make_record(processor_id: str, **overrides):
    base = {
        "processor_id": processor_id,
        "template": "sample.xml",
        "name": f"Processor {processor_id}",
        "short_type": "InvokeScriptedProcessor",
        "migration_category": "Business Logic",
        "parent_group_path": "Root/GroupA",
        "feature_evidence": {
            "sql": {"has_sql": False, "has_dml": False, "has_metadata_only": False},
            "scripts": {
                "inline_count": 1,
                "external_count": 0,
            },
            "tables": {"tables": [], "properties": []},
            "variables": {"defines": [], "uses": []},
            "connections": {"incoming": [], "outgoing": []},
            "controller_services": [],
            "uses_variables": False,
        },
        "classification_source": "rule",
        "rule": "Example rule",
        "databricks_target": "spark_batch",
        "confidence": 0.7,
    }
    base.update(overrides)
    return base


def test_snapshot_from_record_normalises_fields():
    record = make_record("abc", feature_evidence={"sql": {"has_sql": True}})
    snapshot = snapshot_from_record(record)
    assert snapshot is not None
    assert snapshot.processor_id == "abc"
    assert snapshot.feature_evidence["sql"]["has_sql"] is True
    assert snapshot.feature_evidence["scripts"]["inline_count"] == 0
    assert snapshot.feature_evidence["connections"] == {"incoming": [], "outgoing": []}


def test_batch_snapshots_respects_limits():
    snapshots = [snapshot_from_record(make_record(str(i))) for i in range(6)]
    snapshots = [snap for snap in snapshots if snap]
    batches = batch_snapshots(snapshots, max_processors=3, max_chars=10_000)
    assert [len(batch) for batch in batches] == [3, 3]


def test_batch_snapshots_respects_char_budget():
    heavy_record = make_record("heavy")
    light_record = make_record("light")
    snapshots = [
        snapshot_from_record(
            heavy_record,
            scripts_detail={
                "inline_scripts": [{"content": "x" * 4000}],
                "external_scripts": [],
            },
        ),
        snapshot_from_record(light_record),
    ]
    snapshots = [snap for snap in snapshots if snap]
    batches = batch_snapshots(snapshots, max_processors=10, max_chars=1000)
    assert len(batches) == 2


def test_build_script_lookup_enriches_external_scripts():
    script_results = [
        {
            "processor_id": "42",
            "inline_scripts": [
                {
                    "property_name": "Script Body",
                    "script_type": "groovy",
                    "content": "println 'hello'",
                }
            ],
            "external_scripts": [
                {
                    "path": "/opt/job/run.sh",
                    "type": "bash",
                    "property_source": "Command",
                }
            ],
        }
    ]
    lookup = build_script_lookup(script_results)
    assert "42" in lookup
    entry = lookup["42"]
    assert entry["inline_scripts"][0]["script_type"] == "groovy"
    assert entry["external_scripts"][0]["manual_review_note"].startswith(
        "External file"
    )


def test_build_batches_from_records_sorted_and_serialised():
    records = [
        make_record("2", template="b.xml", parent_group_path="Root/B"),
        make_record("1", template="a.xml", parent_group_path="Root/A"),
        make_record("3", template="a.xml", parent_group_path="Root/A"),
    ]

    batches = build_batches_from_records(records, max_processors=2, max_chars=10000)
    assert len(batches) == 2
    first_batch = batches[0]
    assert first_batch["processor_ids"] == ["1", "3"]
    assert first_batch["templates"] == ["a.xml"]
    payload = json.dumps(first_batch, ensure_ascii=False)
    assert "processor_ids" in payload


def test_build_batches_from_records_includes_script_lookup():
    records = [make_record("55")]
    script_lookup = {
        "55": {
            "inline_scripts": [
                {
                    "property_name": "Script Body",
                    "script_type": "python",
                    "content": "print('hi')",
                    "line_count": 1,
                }
            ],
            "external_scripts": [],
            "external_hosts": [],
        }
    }

    batches = build_batches_from_records(
        records,
        max_processors=4,
        max_chars=10000,
        script_lookup=script_lookup,
    )
    assert len(batches) == 1
    processor_payload = batches[0]["processors"][0]
    assert processor_payload["scripts"]["inline_scripts"][0]["content"].startswith(
        "print"
    )


def test_estimate_prompt_chars_grows_with_content():
    base = snapshot_from_record(make_record("base"))
    assert base is not None
    bigger = snapshot_from_record(
        make_record("big"),
        scripts_detail={
            "inline_scripts": [
                {
                    "property_name": "Script Body",
                    "script_type": "groovy",
                    "content": "a" * 500,
                }
            ],
            "external_scripts": [],
        },
    )
    assert bigger is not None
    assert estimate_prompt_chars(bigger) > estimate_prompt_chars(base)

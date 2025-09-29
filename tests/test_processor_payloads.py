import sys
from pathlib import Path
from typing import Any, Dict, List, cast

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from tools.classification.processor_payloads import build_payloads


def make_record(processor_id: str):
    return {
        "processor_id": processor_id,
        "template": "example.xml",
        "name": f"Processor {processor_id}",
        "short_type": "ExecuteScript",
        "migration_category": "Business Logic",
        "parent_group": "GroupA",
        "parent_group_path": "Root/GroupA",
        "feature_evidence": {
            "sql": {"has_sql": False, "has_dml": False, "has_metadata_only": False},
            "scripts": {"inline_count": 1, "external_count": 1},
            "tables": {"tables": [], "properties": []},
            "variables": {"defines": [], "uses": []},
            "connections": {"incoming": [], "outgoing": []},
            "controller_services": [],
            "uses_variables": False,
        },
        "classification_source": "rule",
        "rule": "Sample rule",
        "databricks_target": "spark_batch",
        "confidence": 0.8,
        "scripts_detail": {
            "inline_scripts": [
                {
                    "property_name": "Script Body",
                    "script_type": "python",
                    "content": "print('hello world')",
                    "line_count": 1,
                    "content_preview": "print('hello world')",
                }
            ],
            "external_scripts": [
                {
                    "path": "/opt/jobs/do_work.sh",
                    "type": "bash",
                    "property_source": "Command",
                    "manual_review_note": "External file – review content separately before migration.",
                }
            ],
            "external_hosts": ["nifi-node-1"],
        },
    }


def test_build_payloads_includes_scripts_summary():
    record = make_record("101")
    payloads = build_payloads([record])
    assert len(payloads) == 1
    payload = payloads[0]
    scripts = cast(Dict[str, Any], payload.scripts)
    inline = cast(List[Dict[str, Any]], scripts["inline"])
    external = cast(List[Dict[str, Any]], scripts["external"])
    assert inline[0]["content"] == "print('hello world')"
    assert external[0]["path"] == "/opt/jobs/do_work.sh"
    payload_dict = payload.to_payload()
    scripts_dict = cast(Dict[str, Any], payload_dict["scripts"])
    inline_dicts = cast(List[Dict[str, Any]], scripts_dict["inline"])
    assert inline_dicts[0]["language"] == "python"

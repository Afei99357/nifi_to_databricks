import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from tools.conversion.snippet_store import (
    SNIPPET_STORE_FILE,
    load_snippet_store,
    save_snippet_store,
    update_snippet_store,
)


def test_load_store_returns_defaults(tmp_path, monkeypatch):
    monkeypatch.setattr("tools.conversion.snippet_store.SNIPPET_STORE_DIR", tmp_path)
    monkeypatch.setattr(
        "tools.conversion.snippet_store.SNIPPET_STORE_FILE",
        tmp_path / "snippets.json",
    )
    store = load_snippet_store()
    assert store["snippets"] == {}
    assert "metadata" in store


def test_update_and_save_store(tmp_path, monkeypatch):
    monkeypatch.setattr("tools.conversion.snippet_store.SNIPPET_STORE_DIR", tmp_path)
    monkeypatch.setattr(
        "tools.conversion.snippet_store.SNIPPET_STORE_FILE",
        tmp_path / "snippets.json",
    )
    store = load_snippet_store()
    records = [
        {
            "processor_id": "abc",
            "template": "flow.xml",
            "name": "Test",
            "short_type": "ExecuteScript",
            "parent_group": "Group",
            "parent_group_path": "Root/Group",
            "migration_category": "Business Logic",
            "databricks_target": "spark",
            "recommended_target": "spark_batch",
            "migration_needed": True,
            "implementation_hint": "Do transformation",
            "blockers": "",
            "next_step": "generate_notebook",
            "code_language": "pyspark",
            "databricks_code": "print('hello')",
            "confidence": 0.9,
            "classification_source": "rule",
            "rule": "Some rule",
        }
    ]
    update_snippet_store(
        store,
        records,
        endpoint="databricks-meta-llama",
        max_tokens=4096,
        batch_index=1,
    )
    save_snippet_store(store)
    reloaded = load_snippet_store()
    assert "abc" in reloaded["snippets"]
    entry = reloaded["snippets"]["abc"]
    assert entry["databricks_code"] == "print('hello')"
    assert entry["endpoint"] == "databricks-meta-llama"
    assert entry["batch_index"] == 1

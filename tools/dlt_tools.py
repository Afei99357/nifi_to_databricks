# tools/dlt_tools.py
# DLT/Lakehouse expectations and pipeline config helpers.

from __future__ import annotations

import json

# Removed langchain_core.tools import - no longer using # Removed @tool decorator - direct function call approach decorator

__all__ = ["generate_dlt_expectations", "generate_dlt_pipeline_config"]


# Removed @tool decorator - direct function call approach
def generate_dlt_expectations(table_name: str, rules_json: str) -> str:
    """
    Return SQL to create a DLT/Lakeflow dataset with expectations from simple rules.

    Example rules_json:
      {"not_null_id": "id IS NOT NULL", "valid_price": "price >= 0"}
    """
    try:
        rules = json.loads(rules_json) if rules_json else {}
        ex_lines = [f"EXPECT {name} : {expr}" for name, expr in rules.items()]
        ex_block = ("\n  ".join(ex_lines)) if ex_lines else ""
        sql = (
            f"CREATE OR REFRESH STREAMING TABLE {table_name}\n"
            f"  {ex_block}\n"
            "AS SELECT * FROM STREAM(LIVE.source_table);"
        )
        return sql
    except Exception as e:
        return f"Invalid rules: {e}"


# Removed @tool decorator - direct function call approach
def generate_dlt_pipeline_config(
    pipeline_name: str, catalog: str, db_schema: str, notebook_path: str
) -> str:
    """
    Return minimal JSON config for a DLT/Lakeflow pipeline.
    """
    cfg = {
        "name": pipeline_name,
        "storage": f"/pipelines/{pipeline_name}",
        "target": f"{catalog}.{db_schema}",
        "development": True,
        "continuous": True,
        "libraries": [{"notebook": {"path": notebook_path}}],
    }
    return json.dumps(cfg, indent=2)

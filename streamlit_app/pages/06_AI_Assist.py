#!/usr/bin/env python3

"""AI-assisted migration planner leveraging Databricks LLM endpoints."""

from __future__ import annotations

import json
import os
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List

import json_repair
import pandas as pd
import streamlit as st

# Ensure repository root on path for shared utilities
CURRENT_DIR = Path(__file__).resolve()
REPO_ROOT = CURRENT_DIR.parents[2]
sys.path.insert(0, str(REPO_ROOT))

from model_serving_utils import query_endpoint  # type: ignore
from tools.classification.processor_payloads import (
    build_payloads,
    format_payloads_for_prompt,
)
from tools.conversion import (
    DEFAULT_MAX_CHARS,
    DEFAULT_MAX_PROCESSORS,
    build_batches_from_records,
    build_script_lookup,
    load_snippet_store,
    save_snippet_store,
    update_snippet_store,
)

TRIAGE_SYSTEM_PROMPT = """You are a Databricks migration engineer generating production-ready code from NiFi processors. Output STRICT JSON (array) using the schema below.

Rules:
- Classification guidance: Processors are pre-classified with a migration_category:
  - "Infrastructure Only": ALWAYS mark as recommended_target="retire" with empty databricks_code. These are schedulers, logging, delays, retries. IMPORTANT: For scheduling processors (GenerateFlowFile, Timer, Cron), extract the scheduling information (CRON expression, timer period, processor name with time) into implementation_hint field (e.g., "Runs daily at 3:30 AM via CRON: 0 30 03 ? * * *" or "Triggers every 5 minutes").
  - "Orchestration / Monitoring": Usually mark as "retire" unless they contain essential routing/branching logic. Document routing rules and retry logic in implementation_hint.
  - "Business Logic", "Source Adapter", "Sink Adapter": Generate migration code.
  - "Ambiguous": Evaluate and decide based on processor details.
- Scope and mapping: Map NiFi processors to Databricks patterns:
  - Sources/Sinks: files (HDFS/S3/ADLS), JDBC, Kafka, HTTP → Auto Loader, COPY INTO, spark.read/write, Structured Streaming.
  - SQL processors: inline SQL → spark.sql(...) or DBSQL compatible statements.
  - Script runners (ExecuteStreamCommand/InvokeScriptedProcessor): try native replacement first; otherwise emit workflow_task_shell + a TODO.
  - Attribute/route/orchestration: keep only if they affect data/branching; otherwise mark as "retire".
- Code style: Emit plain Python (no notebook magics or %sql). Use spark.sql(...) for SQL. Keep snippets ≤ 200 lines each, runnable in a notebook cell.
- Parameters: Accept runtime inputs via a top config block or widgets (dates, identifiers, catalog/schema/tables/paths, connection names).
- Idempotency and writes: Prefer MERGE for upserts; use INSERT OVERWRITE only for full partition refresh. No ORDER BY in inserts.
- Validation and logging: Add basic row-count checks and log meaningful messages. Raise on failures (don't swallow exceptions).
- Performance: Repartition before writes if needed; apply OPTIMIZE/ANALYZE only when beneficial and targeted.
- Secrets: Never inline credentials. Use dbutils.secrets.get(scope, key). If unknown, add a TODO line.
- Be conservative: If something is unclear, mark "unknown" and add a minimal TODO—do not invent schemas, endpoints, or business rules.
- Response format only: Return ONLY JSON using this schema:
[
  {
    "processor_id": "string",
    "migration_needed": true,
    "recommended_target": "auto_loader|copy_into|spark_batch|spark_structured_streaming|dbsql|workflow_task_shell|uc_table_ddl|retire|manual_investigation",
    "implementation_hint": "string",
    "blockers": ["..."],
    "rationale": "string",
    "next_step": "generate_notebook|manual_review|confirm_native|retire",
    "confidence": "high|medium|low",
    "code_language": "pyspark",
    "databricks_code": "string"
  }
]
"""

COMPOSE_SYSTEM_PROMPT = """You are a Databricks solutions engineer. Merge multiple processor snippets into one runnable Databricks notebook.

Context:
- Each processor has:
  - has_code: true if there's databricks_code, false if retired/infrastructure-only
  - recommended_target: shows intended Databricks pattern (or "retire" if not migrated)
  - upstream_processors and downstream_processors: NiFi data flow relationships
  - migration_category, implementation_hint, rationale, blockers: context about the processor
- Processors with has_code=false were marked as "retire" or infrastructure-only (schedulers, logging, routing)
- For retired processors, implementation_hint and rationale contain important scheduling/orchestration details
- Use relationships to determine execution order and maintain data flow

Rules:
- Handle processors with has_code=false:
  - Don't include their (empty) code in the notebook
  - DO extract and document their metadata in a "Migration Notes" markdown section:
    - Check implementation_hint and rationale for scheduling information (e.g., "Trigger daily - 08:00" → document as "Run this notebook daily at 08:00 via Databricks Jobs")
    - Extract retry logic, error handling, routing rules from implementation_hint and rationale
    - Document logging and monitoring patterns
    - Note any blockers or concerns
  - List each retired processor with its name, type, and reason for retirement
- Handle processors with has_code=true:
  - Include their databricks_code in the notebook
  - Use upstream_processors and downstream_processors to determine execution order
  - Respect data flow dependencies
- Notebook structure:
  1. Title/overview markdown cell
  2. "Migration Notes" markdown cell (document retired processors, scheduling, infrastructure patterns)
  3. Imports + config (widgets or dict)
  4. Functions by stage (ingest, transform, write, validate, cleanup)
  5. main() function
  - Use plain Python (no %magic commands). Use spark.sql(...) for SQL.
- Deduplicate: Hoist shared imports/config/secrets. Apply consistent logging and error handling.
- Summary: Describe what was kept/trimmed, extracted scheduling/infrastructure info, open TODOs, and assumptions.
- Response format: Return ONLY JSON with these fields:
  - group_name: string
  - summary: string (markdown)
  - notebook_cells: array of cell objects, each with:
    - cell_type: "markdown" or "code"
    - source: string (cell content)
  - next_actions: array of strings

Example notebook_cells structure:
[
  {"cell_type": "markdown", "source": "# Workflow Migration\\n\\nMigrated from NiFi workflow"},
  {"cell_type": "markdown", "source": "## Migration Notes\\n\\n**Scheduling**: Run daily at 08:00 (from 'Trigger daily - 08:00' processor)\\n\\n**Retired Processors**:\\n- LogMessage (infrastructure-only logging)\\n- RouteOnAttribute (orchestration handled by Databricks Jobs)"},
  {"cell_type": "code", "source": "import pandas as pd\\nfrom pyspark.sql import SparkSession"},
  {"cell_type": "markdown", "source": "## Configuration"},
  {"cell_type": "code", "source": "catalog = 'main'\\nschema = 'default'"}
]
"""


def _build_processor_id_mapping(df: pd.DataFrame) -> Dict[str, Dict[str, str]]:
    """Build mapping from processor ID to name and type."""
    return {
        str(row.get("processor_id")): {
            "name": row.get("name"),
            "short_type": row.get("short_type"),
        }
        for _, row in df.iterrows()
    }


def _resolve_processor_relationships(
    incoming_ids: List[str],
    outgoing_ids: List[str],
    id_to_info: Dict[str, Dict[str, str]],
) -> tuple[List[Dict[str, str]], List[Dict[str, str]]]:
    """Resolve processor IDs to names and types."""
    incoming_info = [
        {
            "processor_id": pid,
            "name": id_to_info[pid]["name"],
            "short_type": id_to_info[pid]["short_type"],
        }
        for pid in incoming_ids
        if pid in id_to_info
    ]

    outgoing_info = [
        {
            "processor_id": pid,
            "name": id_to_info[pid]["name"],
            "short_type": id_to_info[pid]["short_type"],
        }
        for pid in outgoing_ids
        if pid in id_to_info
    ]

    return incoming_info, outgoing_info


def _build_compose_records(compose_df: pd.DataFrame) -> List[Dict[str, Any]]:
    """Build composition records with resolved relationships.

    Includes ALL processors (even those without code) so the composition LLM can:
    - Document retired/infrastructure processors
    - Extract scheduling and orchestration metadata
    - Explain what was removed and why
    """
    id_to_info = _build_processor_id_mapping(compose_df)
    compose_records = []

    for _, row in compose_df.iterrows():
        code = str(row.get("databricks_code") or "").strip()
        has_code = bool(code)

        incoming_ids = row.get("incoming_processor_ids", []) or []
        outgoing_ids = row.get("outgoing_processor_ids", []) or []
        incoming_info, outgoing_info = _resolve_processor_relationships(
            incoming_ids, outgoing_ids, id_to_info
        )

        compose_records.append(
            {
                "processor_id": row.get("processor_id"),
                "name": row.get("name"),
                "short_type": row.get("short_type"),
                "migration_category": row.get("migration_category"),
                "databricks_target": row.get("databricks_target"),
                "recommended_target": row.get("recommended_target"),
                "implementation_hint": row.get("implementation_hint"),
                "databricks_code": code if has_code else "",
                "has_code": has_code,
                "blockers": row.get("blockers"),
                "next_step": row.get("next_step"),
                "rationale": row.get("rationale"),
                "upstream_processors": incoming_info,
                "downstream_processors": outgoing_info,
            }
        )

    return compose_records


def _parse_composition_response(raw_content: str) -> Dict[str, Any] | None:
    """Parse and clean LLM composition response."""
    cleaned = raw_content.strip().strip("`")
    if cleaned.startswith("json"):
        cleaned = cleaned[4:].lstrip()

    if not cleaned:
        return None

    try:
        payload = json_repair.loads(cleaned)
    except Exception:
        return None

    # Handle list responses by taking first element
    if isinstance(payload, list):
        return payload[0] if payload else None

    return payload if isinstance(payload, dict) else None


def _display_notebook_cells(notebook_cells: List[Dict[str, str]]) -> str:
    """Display notebook cells and return notebook JSON string."""
    for idx, cell in enumerate(notebook_cells):
        cell_type = cell.get("cell_type", "code")
        source = cell.get("source", "")

        if cell_type == "markdown":
            st.markdown(f"**Cell {idx + 1} (Markdown)**")
            st.markdown(source)
        else:
            st.markdown(f"**Cell {idx + 1} (Code)**")
            st.code(source, language="python")

    # Format cells for Jupyter notebook structure
    formatted_cells = []
    for cell in notebook_cells:
        cell_type = cell.get("cell_type", "code")
        source = cell.get("source", "")

        # Split source into lines and add newline characters
        # Jupyter format requires each line (except last) to end with \n
        lines = source.split("\n")
        if lines:
            # Add \n to all lines except the last one
            source_lines = [line + "\n" for line in lines[:-1]]
            # Last line doesn't get \n (unless original source ended with \n)
            if source.endswith("\n"):
                source_lines.append(lines[-1] + "\n")
            else:
                source_lines.append(lines[-1])
        else:
            source_lines = [""]

        formatted_cell = {
            "cell_type": cell_type,
            "metadata": {},
            "source": source_lines,
        }

        # Add required fields for code cells
        if cell_type == "code":
            formatted_cell["execution_count"] = None
            formatted_cell["outputs"] = []

        formatted_cells.append(formatted_cell)

    # Create proper Jupyter notebook structure
    notebook_json = {
        "cells": formatted_cells,
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3",
            },
            "language_info": {
                "name": "python",
                "version": "3.10.0",
                "mimetype": "text/x-python",
                "codemirror_mode": {"name": "ipython", "version": 3},
                "pygments_lexer": "ipython3",
                "nbconvert_exporter": "python",
                "file_extension": ".py",
            },
        },
        "nbformat": 4,
        "nbformat_minor": 4,
    }
    return json.dumps(notebook_json, ensure_ascii=False, indent=2)


def _display_composition_result(
    result_payload: Dict[str, Any], group_label: str
) -> None:
    """Display composition result with notebook cells or fallback code."""
    notebook_cells = result_payload.get("notebook_cells", [])
    notebook_code = result_payload.get("notebook_code", "")
    summary = result_payload.get("summary")
    next_actions = result_payload.get("next_actions", [])

    st.subheader(f"Composed notebook · {result_payload.get('group_name', group_label)}")

    if summary:
        st.markdown(summary)

    if notebook_cells:
        notebook_str = _display_notebook_cells(notebook_cells)
        st.download_button(
            "Download Databricks notebook (.ipynb)",
            data=notebook_str,
            file_name="composed_notebook.ipynb",
            mime="application/x-ipynb+json",
            use_container_width=True,
        )
    elif notebook_code:
        st.code(notebook_code, language="python")
        st.download_button(
            "Download composed code (.py)",
            data=notebook_code,
            file_name="composed_notebook.py",
            mime="text/x-python",
            use_container_width=True,
        )

    if next_actions:
        st.markdown("**Next actions**")
        for item in next_actions:
            st.write(f"- {item}")


def _records_to_dataframe(records: List[dict]) -> pd.DataFrame:
    """Convert classification records to dataframe."""
    rows = []
    for record in records:
        processor_id = record.get("processor_id") or record.get("id")
        if not processor_id:
            continue

        rows.append(
            {
                "processor_id": str(processor_id),
                "template": str(record.get("template") or ""),
                "name": str(record.get("name") or ""),
                "short_type": str(
                    record.get("short_type") or record.get("processor_type") or ""
                ),
                "migration_category": str(
                    record.get("migration_category") or "Ambiguous"
                ),
                "databricks_target": str(record.get("databricks_target") or ""),
                "classification_source": str(record.get("classification_source") or ""),
                "rule": str(record.get("rule") or ""),
                "confidence": float(record.get("confidence") or 0.0),
                "parent_group": str(
                    record.get("parent_group") or record.get("parentGroupName") or ""
                ),
                "parent_group_path": str(
                    record.get("parent_group_path")
                    or record.get("parentGroupPath")
                    or record.get("parent_group")
                    or record.get("parentGroupName")
                    or ""
                ),
            }
        )

    if not rows:
        return pd.DataFrame(
            columns=[
                "processor_id",
                "template",
                "name",
                "short_type",
                "migration_category",
                "databricks_target",
                "classification_source",
                "rule",
                "confidence",
                "parent_group",
                "parent_group_path",
            ]
        )

    df = pd.DataFrame(rows)
    return df.drop_duplicates(subset=["processor_id"]).reset_index(drop=True)


def _collect_session_classifications() -> (
    tuple[List[Dict[str, object]], Dict[str, Dict[str, object]]]
):
    """Collect classification results from session state."""
    records: List[Dict[str, object]] = []
    templates: Dict[str, Dict[str, object]] = {}

    for key, payload in st.session_state.items():
        if not isinstance(key, str) or not key.startswith("classification_results_"):
            continue
        if not isinstance(payload, dict):
            continue

        template_name = payload.get("workflow", {}).get("filename") or key[24:]
        templates[template_name] = payload

        for record in payload.get("classifications", []) or []:
            cloned = dict(record)
            cloned["template"] = template_name
            records.append(cloned)

    return records, templates


def _collect_script_lookup() -> Dict[str, Dict[str, object]]:
    """Collect script details from session state."""
    script_entries: List[Dict[str, object]] = []
    for key, payload in st.session_state.items():
        if not isinstance(key, str) or not key.startswith("script_results_"):
            continue
        if isinstance(payload, list):
            script_entries.extend([e for e in payload if isinstance(e, dict)])

    return build_script_lookup(script_entries) if script_entries else {}


def main() -> None:
    st.set_page_config(page_title="AI Migration Planner", page_icon="🧭", layout="wide")
    st.title("🧭 AI Migration Planner")
    st.write(
        "Prioritise NiFi processors for Databricks migration using the existing classification evidence."
    )

    # === Endpoint selection ===
    default_endpoint = os.getenv("SERVING_ENDPOINT", "databricks-claude-sonnet-4")
    endpoint_choices = [
        "databricks-meta-llama-3-3-70b-instruct",
        "databricks-claude-sonnet-4",
        "Custom…",
    ]

    default_index = (
        endpoint_choices.index(default_endpoint)
        if default_endpoint in endpoint_choices[:-1]
        else len(endpoint_choices) - 1
    )
    preset_value = "" if default_index < len(endpoint_choices) - 1 else default_endpoint

    chosen_option = st.selectbox(
        "AI models",
        endpoint_choices,
        index=default_index,
        help="Select the Databricks serving endpoint used for the AI Migration Planner.",
    )
    endpoint_name = (
        st.text_input("Custom endpoint name", value=preset_value)
        if chosen_option == "Custom…"
        else chosen_option
    )

    max_tokens = 60000

    # === Load data ===
    records, template_payloads = _collect_session_classifications()
    script_lookup = _collect_script_lookup()

    # Attach script details to records
    for record in records:
        processor_id = str(record.get("processor_id") or record.get("id") or "")
        if processor_id in script_lookup:
            record["scripts_detail"] = script_lookup[processor_id]

    if not records:
        st.warning(
            "No processor classifications available. Run Start Analysis on the Dashboard first."
        )
        if st.button("🔙 Back to Dashboard", use_container_width=True):
            st.switch_page("Dashboard.py")
        return

    snippet_store = load_snippet_store()
    template_names = sorted(template_payloads.keys())

    st.subheader("Analysis scope")
    st.caption(
        f"Templates: {', '.join(template_names) if template_names else '(unknown)'}"
    )

    df = _records_to_dataframe(records)
    st.caption(f"{len(df)} processors across {len(template_names)} template(s)")

    record_lookup = {
        str(rec.get("processor_id")): rec for rec in records if rec.get("processor_id")
    }

    # === Filter processors ===
    st.subheader("1. Filter processors")

    col1, col2, col3 = st.columns(3)
    with col1:
        categories = sorted(df["migration_category"].dropna().unique())
        category_filter = st.multiselect(
            "Migration categories",
            options=categories,
            default=categories,
            help="Deselect categories to exclude them from the AI Migration Planner.",
        )
    with col2:
        source_options = sorted(df["classification_source"].dropna().unique())
        source_filter = st.multiselect(
            "Classification sources", options=source_options, default=source_options
        )
    with col3:
        processors_per_call = st.number_input(
            "Processors per LLM call",
            min_value=1,
            max_value=40,
            value=min(DEFAULT_MAX_PROCESSORS, len(df)) or 1,
            help="Upper bound on processors included in a single model request.",
        )

    search_name = st.text_input("Search by processor name or short type", "")

    # Apply filters
    filtered_df = df.copy()
    if category_filter:
        filtered_df = filtered_df[
            filtered_df["migration_category"].isin(category_filter)
        ]
    if source_filter:
        filtered_df = filtered_df[
            filtered_df["classification_source"].isin(source_filter)
        ]
    if search_name:
        query = search_name.lower()
        filtered_df = filtered_df[
            filtered_df["name"].str.lower().str.contains(query, na=False)
            | filtered_df["short_type"].str.lower().str.contains(query, na=False)
        ]

    if filtered_df.empty:
        st.warning("Filters removed all processors. Adjust filters to proceed.")
        return

    filtered_df = filtered_df.sort_values(
        ["migration_category", "template", "name"]
    ).reset_index(drop=True)
    st.caption(f"Showing {len(filtered_df)} processors after filtering.")

    # === Checkbox selection ===
    # Initialize selection state with all current processors
    if "selected_for_llm" not in st.session_state:
        st.session_state.selected_for_llm = set(filtered_df["processor_id"].tolist())

    # Keep only processors that still exist after filtering
    current_ids = set(filtered_df["processor_id"].tolist())
    st.session_state.selected_for_llm = st.session_state.selected_for_llm.intersection(
        current_ids
    )

    col_select_all, col_select_none = st.columns(2)
    with col_select_all:
        if st.button("Select All", use_container_width=True):
            st.session_state.selected_for_llm = current_ids.copy()
            st.rerun()
    with col_select_none:
        if st.button("Deselect All", use_container_width=True):
            st.session_state.selected_for_llm = set()
            st.rerun()

    # Build display dataframe with current selection state
    display_df = (
        filtered_df[
            [
                "processor_id",
                "template",
                "name",
                "short_type",
                "migration_category",
                "databricks_target",
                "classification_source",
            ]
        ]
        .head(200)
        .copy()
    )

    # Add selected column based on current session state
    display_df.insert(
        0,
        "selected",
        display_df["processor_id"].isin(st.session_state.selected_for_llm),
    )

    edited_df = st.data_editor(
        display_df,
        column_config={
            "selected": st.column_config.CheckboxColumn(
                "Include in LLM",
                help="Select to include this processor in the AI Migration Planner",
                default=False,
            )
        },
        disabled=[
            "processor_id",
            "template",
            "name",
            "short_type",
            "migration_category",
            "databricks_target",
            "classification_source",
        ],
        hide_index=True,
        use_container_width=True,
        key="processor_selection_editor",
    )

    # Update selection state from edited dataframe
    for _, row in edited_df.iterrows():
        pid = row["processor_id"]
        if row["selected"]:
            st.session_state.selected_for_llm.add(pid)
        else:
            st.session_state.selected_for_llm.discard(pid)

    st.caption(
        f"✓ {len(st.session_state.selected_for_llm)} processor(s) selected for LLM call"
    )

    # === Additional context ===
    st.subheader("2. Provide additional context (optional)")
    additional_notes = st.text_area(
        "Notes for the model",
        placeholder="Add migration constraints, priority groups, or architectural decisions to apply across this batch.",
    )

    # === Run AI Migration Planner ===
    st.subheader("3. Run AI Migration Planner")
    if st.button("Run AI Migration Planner on selection", use_container_width=True):
        selected_processor_ids = list(st.session_state.selected_for_llm)
        if not selected_processor_ids:
            st.warning(
                "No processors selected for the AI Migration Planner. Use checkboxes to select processors."
            )
            return

        selected_df = filtered_df[
            filtered_df["processor_id"].isin(selected_processor_ids)
        ].copy()
        selected_records = [
            record_lookup[pid]
            for pid in selected_df["processor_id"]
            if pid in record_lookup
        ]

        batches = build_batches_from_records(
            selected_records,
            max_processors=int(processors_per_call),
            max_chars=DEFAULT_MAX_CHARS,
            script_lookup=script_lookup,
        )

        # Show batch plan
        batch_summaries = [
            {
                "batch": p.get("batch_index"),
                "processors": len(p.get("processor_ids", [])),
                "est_chars": p.get("prompt_char_count", 0),
            }
            for p in batches
        ]
        st.caption("Batch plan (processor count vs. estimated prompt characters).")
        st.dataframe(
            pd.DataFrame(batch_summaries).sort_values("batch").reset_index(drop=True),
            use_container_width=True,
        )

        selected_templates = sorted(selected_df["template"].dropna().unique().tolist())
        all_results: List[pd.DataFrame] = []
        snippet_store_modified = False

        # Process each batch
        for batch_index, batch_payload in enumerate(batches, start=1):
            batch_ids = batch_payload["processor_ids"]
            batch_records = [
                record_lookup[pid] for pid in batch_ids if pid in record_lookup
            ]
            batch_df = selected_df[selected_df["processor_id"].isin(batch_ids)]

            st.caption(
                f"Batch {batch_index}: {len(batch_ids)} processors, ≈{batch_payload.get('prompt_char_count', 0):,} chars"
            )

            # Build payload
            payloads = build_payloads(batch_records)
            payload_json = format_payloads_for_prompt(payloads)
            user_payload_dict = json.loads(payload_json)
            user_payload_dict["selection_summary"] = {
                "workflow_files": selected_templates,
                "category_counts": dict(
                    Counter(batch_df["migration_category"].tolist())
                ),
            }
            user_payload = json.dumps(user_payload_dict, ensure_ascii=False, indent=2)

            user_message = f"INPUT_PROCESSORS:\n{user_payload}\nIf additional notes are provided, apply them across all processors."
            if additional_notes.strip():
                user_message += f"\nADDITIONAL_NOTES:\n{additional_notes.strip()}"

            with st.expander(
                f"Preview payload sent to LLM (batch {batch_index})", expanded=False
            ):
                st.code(user_payload, language="json")

            messages = [
                {"role": "system", "content": TRIAGE_SYSTEM_PROMPT.strip()},
                {"role": "user", "content": user_message},
            ]

            # Call LLM
            with st.spinner(
                f"Calling serving endpoint (batch {batch_index}/{len(batches)})..."
            ):
                reply = query_endpoint(endpoint_name, messages, int(max_tokens))

            raw_content = reply.get("content", "")
            cleaned_content = raw_content.strip()
            if cleaned_content.startswith("```") and cleaned_content.endswith("```"):
                cleaned_content = cleaned_content[3:-3].strip()
            if cleaned_content.startswith("json"):
                cleaned_content = cleaned_content[4:].lstrip()

            if not cleaned_content:
                st.warning(f"Response was empty for batch {batch_index}.")
                continue

            # Parse response
            parsed = json_repair.loads(cleaned_content)
            if isinstance(parsed, dict):
                results = parsed.get("ai_migration_planner_result") or parsed.get(
                    "triage_result"
                )
                parsed = results if isinstance(results, list) else [parsed]

            parsed = [item for item in parsed if isinstance(item, dict)]
            if not parsed:
                st.warning(f"Batch {batch_index} returned no result objects.")
                continue

            # Convert to dataframe and merge
            result_df = pd.DataFrame(parsed)
            result_df["blockers"] = result_df.get(
                "blockers", pd.Series([""] * len(result_df))
            ).apply(lambda v: ", ".join(v) if isinstance(v, list) else str(v or ""))
            result_df["databricks_code"] = result_df.get(
                "databricks_code", pd.Series([""] * len(result_df))
            ).fillna("")
            result_df["code_language"] = result_df.get(
                "code_language", pd.Series(["unknown"] * len(result_df))
            ).fillna("unknown")

            merged_df = batch_df.merge(result_df, on="processor_id", how="left")
            merged_df["batch_index"] = batch_index
            all_results.append(merged_df)

            # Update snippet store
            update_snippet_store(
                snippet_store,
                merged_df.to_dict("records"),
                endpoint=endpoint_name,
                max_tokens=int(max_tokens),
                batch_index=batch_index,
            )
            snippet_store_modified = True

        if not all_results:
            st.info("No results were produced.")
            return

        combined_df = pd.concat(all_results, ignore_index=True).fillna("")

        # Normalize confidence columns from merge
        if "confidence_y" in combined_df.columns:
            combined_df["confidence"] = combined_df["confidence_y"]
        elif (
            "confidence_x" in combined_df.columns
            and "confidence" not in combined_df.columns
        ):
            combined_df["confidence"] = combined_df["confidence_x"]
        combined_df = combined_df.drop(
            columns=["confidence_x", "confidence_y"], errors="ignore"
        )

        st.session_state["ai_migration_planner_results_records"] = combined_df.to_dict(
            "records"
        )

        if snippet_store_modified:
            save_snippet_store(snippet_store)
            st.success(f"Stored snippets for {len(combined_df)} processor(s).")
            st.info(
                "Review cached snippets below to inspect code or compose grouped notebooks."
            )
            snippet_store = load_snippet_store()

    # === Display persisted results ===
    stored_records = st.session_state.get("ai_migration_planner_results_records")
    if stored_records:
        stored_df = pd.DataFrame(stored_records).fillna("")
        summary_cols = [
            "processor_id",
            "template",
            "name",
            "parent_group_path",
            "migration_category",
            "recommended_target",
            "migration_needed",
            "next_step",
            "blockers",
            "confidence",
            "batch_index",
        ]
        available_cols = [col for col in summary_cols if col in stored_df.columns]

        st.subheader("AI Migration Planner results")
        st.dataframe(
            stored_df[available_cols].set_index(pd.Index(range(1, len(stored_df) + 1))),
            use_container_width=True,
        )
        st.download_button(
            "Download AI Migration Planner results CSV",
            data=stored_df.to_csv(index=False).encode("utf-8"),
            file_name="ai_migration_planner_results.csv",
            mime="text/csv",
            use_container_width=True,
        )

    # === Cached snippet review & composition ===
    snippet_entries = list(snippet_store.get("snippets", {}).values())
    if snippet_entries:
        st.subheader("Cached processor snippets")
        snippet_df = (
            pd.DataFrame(snippet_entries)
            .fillna("")
            .sort_values(["parent_group_path", "processor_id"], kind="stable")
            .reset_index(drop=True)
        )

        group_options = ["All"] + sorted(
            {str(v) or "(unknown)" for v in snippet_df.get("parent_group_path", "")}
        )
        selected_snippet_group = st.selectbox(
            "Snippet group",
            group_options,
            index=0,
            help="Filter cached snippets by NiFi group path.",
            key="snippet_group_select",
        )

        snippet_view = (
            snippet_df
            if selected_snippet_group == "All"
            else snippet_df[
                snippet_df["parent_group_path"].fillna("") == selected_snippet_group
            ]
        )

        display_cols = [
            "processor_id",
            "template",
            "name",
            "short_type",
            "parent_group_path",
            "migration_category",
            "code_language",
            "batch_index",
            "cached_at",
        ]
        available_cols = [col for col in display_cols if col in snippet_view.columns]
        st.dataframe(snippet_view[available_cols], use_container_width=True)

        for _, row in snippet_view.iterrows():
            code = str(row.get("databricks_code", "") or "").strip()
            if code:
                processor_label = (
                    f"{row.get('processor_id')} · {row.get('name', '')}".strip()
                )
                with st.expander(f"Snippet · {processor_label}"):
                    st.code(code, language=row.get("code_language", "text"))

        st.download_button(
            "Download cached snippets JSON",
            data=json.dumps(snippet_entries, ensure_ascii=False, indent=2),
            file_name="processor_snippets.json",
            mime="application/json",
            use_container_width=True,
        )

        # === Composition ===
        compose_options = [o for o in group_options if o != "(unknown)" or o == "All"]
        compose_group = st.selectbox(
            "Compose group",
            compose_options,
            help="Select which group to merge into a single notebook.",
            key="compose_group_select",
        )
        compose_notes = st.text_area(
            "Additional notes for composition",
            key="compose_notes",
            placeholder="Optional context or constraints for the composed notebook.",
        )

        # Display cached composition result if available
        cached_composition = st.session_state.get("composed_notebook_result")
        if cached_composition:
            st.info(
                "📝 A composed notebook is available below. Generate a new one to replace it."
            )

        if st.button("Compose notebook for group", key="compose_button"):
            compose_df = (
                snippet_df
                if compose_group == "All"
                else snippet_df[
                    snippet_df["parent_group_path"].fillna("") == compose_group
                ]
            )
            group_label = "All processors" if compose_group == "All" else compose_group

            compose_records = _build_compose_records(compose_df)
            if not compose_records:
                st.warning("No processors available for the selected group.")
                return

            # Count how many have code vs metadata only
            with_code = sum(1 for r in compose_records if r.get("has_code"))
            without_code = len(compose_records) - with_code
            if without_code > 0:
                st.info(
                    f"Composition includes {with_code} processor(s) with code and "
                    f"{without_code} processor(s) with metadata only (retired/infrastructure). "
                    f"The LLM will document the retired processors in a 'Migration Notes' section."
                )

            # Simplify retired processor records to reduce payload size
            optimized_records = []
            for record in compose_records:
                if not record.get("has_code"):
                    # Send essential metadata for retired processors
                    # Include implementation_hint and rationale so 2nd LLM can document
                    # scheduling, retry logic, routing rules, etc.
                    optimized_records.append(
                        {
                            "processor_id": record["processor_id"],
                            "name": record["name"],
                            "short_type": record["short_type"],
                            "migration_category": record["migration_category"],
                            "databricks_target": record["databricks_target"],
                            "recommended_target": record.get(
                                "recommended_target", "retire"
                            ),
                            "implementation_hint": record.get(
                                "implementation_hint", ""
                            ),
                            "rationale": record.get("rationale", ""),
                            "blockers": record.get("blockers", ""),
                            "has_code": False,
                            "databricks_code": "",
                        }
                    )
                else:
                    # Send full record for processors with code
                    optimized_records.append(record)

            group_payload = {
                "group_name": group_label,
                "processor_count": len(optimized_records),
                "processors": optimized_records,
            }
            if compose_notes.strip():
                group_payload["notes"] = compose_notes.strip()

            # Estimate payload size and warn if too large
            payload_str = json.dumps(group_payload, ensure_ascii=False)
            payload_chars = len(payload_str)

            # Rough estimate: 1 token ≈ 3-4 characters
            estimated_input_tokens = payload_chars // 3
            system_prompt_tokens = len(COMPOSE_SYSTEM_PROMPT) // 3

            total_input_tokens = estimated_input_tokens + system_prompt_tokens

            # Context limits: Claude 3.5 Sonnet = 200K, Llama 3.3 = 128K
            if total_input_tokens > 150000:
                st.error(
                    f"⚠️ **Payload too large**: ~{total_input_tokens:,} tokens "
                    f"({payload_chars:,} chars payload + {system_prompt_tokens:,} chars system prompt). "
                    f"This exceeds typical context limits (128K-200K tokens). "
                    f"**Please select a smaller group or sub-group.**"
                )
                st.caption(
                    "💡 Tip: Use the 'Compose group' dropdown to select a specific NiFi process group instead of 'All'."
                )
                return
            elif total_input_tokens > 100000:
                st.warning(
                    f"⚠️ **Large payload**: ~{total_input_tokens:,} tokens. "
                    f"This may approach context limits for some models (Llama 3.3: 128K). "
                    f"If composition fails, select a smaller group."
                )
            elif total_input_tokens > 50000:
                st.info(
                    f"ℹ️ Moderate payload: ~{total_input_tokens:,} tokens. Should work fine."
                )

            messages = [
                {"role": "system", "content": COMPOSE_SYSTEM_PROMPT.strip()},
                {
                    "role": "user",
                    "content": f"GROUP_INPUT:\n{json.dumps(group_payload, ensure_ascii=False, indent=2)}",
                },
            ]

            with st.spinner("Composing notebook from cached snippets..."):
                reply = query_endpoint(endpoint_name, messages, int(max_tokens))

            raw_content = reply.get("content", "")
            st.subheader("Composition response")
            st.code(raw_content or "(no content)")

            result_payload = _parse_composition_response(raw_content)
            if result_payload:
                # Cache the composition result in session state
                st.session_state["composed_notebook_result"] = {
                    "result_payload": result_payload,
                    "group_label": group_label,
                }
                _display_composition_result(result_payload, group_label)
            else:
                st.warning("Unable to parse composition output as JSON.")

        # Always display cached composition if available (even after page refresh)
        if cached_composition:
            st.markdown("---")
            st.subheader("📥 Cached Composed Notebook")
            _display_composition_result(
                cached_composition["result_payload"],
                cached_composition["group_label"],
            )


if __name__ == "__main__":
    main()

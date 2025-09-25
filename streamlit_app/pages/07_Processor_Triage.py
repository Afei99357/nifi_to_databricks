#!/usr/bin/env python3

"""Processor triage page leveraging Databricks LLM endpoints."""

from __future__ import annotations

import json
import math
import os
import sys
from collections import Counter
from pathlib import Path
from typing import List

import json_repair
import pandas as pd
import streamlit as st

# Ensure repository root on path for shared utilities
CURRENT_DIR = Path(__file__).resolve()
REPO_ROOT = CURRENT_DIR.parents[2]
sys.path.insert(0, str(REPO_ROOT))

from model_serving_utils import is_endpoint_supported, query_endpoint  # type: ignore
from tools.classification.dossiers import (
    build_dossiers,
    format_dossiers_for_prompt,
    load_classification_records,
)

DEFAULT_RESULTS_DIR = REPO_ROOT / "derived_classification_results"
TRIAGE_SYSTEM_PROMPT = (
    "You are a NiFi to Databricks migration strategist.\n"
    "You will receive JSON describing a batch of processors, with evidence synthesized from the NiFi flow.\n"
    "Decide which processors require Databricks implementation, choose an appropriate target pattern, and highlight blockers.\n\n"
    "Rules:\n"
    "- Base decisions only on supplied evidence; if information is missing, state the gap explicitly.\n"
    "- Treat `Infrastructure Only` processors as retirement candidates unless evidence shows required orchestration.\n"
    "- For shared scripts or controller services, call out cross-cutting dependencies instead of repeating identical guidance.\n"
    "- Limit blockers to concrete, actionable items (e.g., external script, controller service, missing schema).\n"
    "- Keep rationales factual, <= 30 words.\n"
    "- Preferred targets: auto_loader, copy_into, spark_batch, spark_structured_streaming, dbsql, workflow_task_shell, uc_table_ddl. Use retire for decommission and manual_investigation when unclear.\n"
    "- Output ONLY JSON following the schema below.\n\n"
    "Output schema (JSON array):\n"
    "[\n"
    "  {\n"
    '    "processor_id": "string",\n'
    '    "migration_needed": true,\n'
    '    "recommended_target": "auto_loader|copy_into|spark_batch|spark_structured_streaming|dbsql|workflow_task_shell|uc_table_ddl|retire|manual_investigation",\n'
    '    "implementation_hint": "string",\n'
    '    "blockers": ["..."],\n'
    '    "rationale": "string",\n'
    '    "next_step": "generate_notebook|manual_review|confirm_native|retire",\n'
    '    "confidence": "high|medium|low"\n'
    "  }\n"
    "]\n"
)


def _list_classification_files(base_dir: Path) -> List[Path]:
    if not base_dir.exists():
        return []
    return sorted(p for p in base_dir.glob("*.json"))


def _records_to_dataframe(records: List[dict]) -> pd.DataFrame:
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
            ]
        )
    df = pd.DataFrame(rows)
    df = df.drop_duplicates(subset=["processor_id"]).reset_index(drop=True)
    return df


def _prepare_user_payload(
    dossiers_json: str, workflows: List[str], filtered_df: pd.DataFrame
) -> str:
    payload = json.loads(dossiers_json)
    category_counts = Counter(filtered_df["migration_category"].tolist())
    payload["selection_summary"] = {
        "workflow_files": workflows,
        "category_counts": dict(category_counts),
    }
    return json.dumps(payload, ensure_ascii=False, indent=2)


def _normalise_blockers(value):
    if value is None:
        return ""
    if isinstance(value, float) and math.isnan(value):
        return ""
    if isinstance(value, list):
        return ", ".join(str(item) for item in value)
    return value


def main() -> None:
    st.set_page_config(page_title="Processor Triage", page_icon="🧭", layout="wide")
    st.title("🧭 Processor Triage")
    st.write(
        "Prioritise NiFi processors for Databricks migration using the existing classification evidence."
    )

    default_endpoint = os.getenv("SERVING_ENDPOINT", "")
    endpoint_choices = [
        "databricks-meta-llama-3-3-70b-instruct",
        "databricks-claude-sonnet-4",
        "Custom…",
    ]
    if default_endpoint in endpoint_choices[:-1]:
        default_index = endpoint_choices.index(default_endpoint)
        preset_value = ""
    else:
        default_index = len(endpoint_choices) - 1
        preset_value = default_endpoint

    chosen_option = st.selectbox(
        "AI models",
        endpoint_choices,
        index=default_index,
        help="Select the Databricks serving endpoint used for triage.",
    )
    if chosen_option == "Custom…":
        endpoint_name = st.text_input("Custom endpoint name", value=preset_value)
    else:
        endpoint_name = chosen_option

    max_tokens = st.slider(
        "Max tokens", min_value=256, max_value=8192, value=2048, step=256
    )

    st.divider()

    st.subheader("1. Choose classification outputs")
    available_files = _list_classification_files(DEFAULT_RESULTS_DIR)
    if not available_files:
        st.error(
            f"No classification JSON files found under `{DEFAULT_RESULTS_DIR}`. Run the batch classification pipeline first."
        )
        return

    file_labels = [path.name for path in available_files]
    selected_labels = st.multiselect(
        "Workflows",
        options=file_labels,
        help="Pick one or more classification JSON files to include in this triage batch.",
    )
    if not selected_labels:
        st.info("Select at least one workflow to continue.")
        return

    selected_paths = [path for path in available_files if path.name in selected_labels]
    records = load_classification_records(selected_paths)
    if not records:
        st.warning("No processor records found in the selected files.")
        return

    df = _records_to_dataframe(records)

    st.subheader("2. Filter processors")
    col1, col2, col3 = st.columns(3)
    with col1:
        categories = sorted(cat for cat in df["migration_category"].dropna().unique())
        category_filter = st.multiselect(
            "Migration categories",
            options=categories,
            default=categories,
        )
    with col2:
        source_options = sorted(
            src for src in df["classification_source"].dropna().unique()
        )
        source_filter = st.multiselect(
            "Classification sources",
            options=source_options,
            default=source_options,
        )
    with col3:
        max_processors = st.number_input(
            "Batch size",
            min_value=1,
            max_value=40,
            value=min(20, len(df)),
            help="Limit processors per LLM call to stay within context window.",
        )

    search_name = st.text_input("Search by processor name or short type", "")
    exclude_infra = st.checkbox("Ignore Infrastructure Only", value=False)

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
    if exclude_infra:
        filtered_df = filtered_df[
            filtered_df["migration_category"] != "Infrastructure Only"
        ]

    if filtered_df.empty:
        st.warning("Filters removed all processors. Adjust filters to proceed.")
        return

    filtered_df = filtered_df.sort_values(
        [
            "migration_category",
            "template",
            "name",
        ]
    ).reset_index(drop=True)

    st.caption(f"Showing {len(filtered_df)} processors after filtering.")
    st.dataframe(
        filtered_df.head(200).set_index(
            pd.Index(range(1, min(len(filtered_df), 200) + 1))
        ),
        use_container_width=True,
    )

    st.subheader("3. Provide additional context (optional)")
    additional_notes = st.text_area(
        "Notes for the model",
        placeholder="Add migration constraints, priority groups, or architectural decisions to apply across this batch.",
    )

    st.subheader("4. Run triage")
    if st.button("Run triage on selection", use_container_width=True):
        if not endpoint_name:
            st.error("Specify a serving endpoint name to continue.")
            return
        if not is_endpoint_supported(endpoint_name):
            st.error(f"Endpoint `{endpoint_name}` is not chat-completions compatible.")
            return

        selected_df = filtered_df.head(int(max_processors))
        selected_ids = selected_df["processor_id"].tolist()

        id_to_record = {
            str(rec.get("processor_id") or rec.get("id")): rec for rec in records
        }
        selected_records = [
            id_to_record[pid] for pid in selected_ids if pid in id_to_record
        ]
        dossiers = build_dossiers(selected_records)
        dossier_json = format_dossiers_for_prompt(dossiers)
        user_payload = _prepare_user_payload(dossier_json, selected_labels, selected_df)

        user_message = (
            "INPUT_DOSSIERS:\n"
            f"{user_payload}\n"
            "If additional notes are provided, apply them across all processors."
        )
        if additional_notes.strip():
            user_message += f"\nADDITIONAL_NOTES:\n{additional_notes.strip()}"

        messages = [
            {"role": "system", "content": TRIAGE_SYSTEM_PROMPT.strip()},
            {"role": "user", "content": user_message},
        ]

        with st.spinner("Calling serving endpoint..."):
            try:
                reply = query_endpoint(endpoint_name, messages, int(max_tokens))
            except Exception as exc:  # pragma: no cover
                st.error(f"LLM call failed: {exc}")
                return

        raw_content = reply.get("content", "")

        def _clean_response(text: str) -> str:
            cleaned = text.strip()
            if cleaned.startswith("```") and cleaned.endswith("```"):
                cleaned = cleaned[3:-3].strip()
            if cleaned.startswith("json"):
                cleaned = cleaned[4:].lstrip()
            return cleaned

        cleaned_content = _clean_response(raw_content)

        st.subheader("Assistant response")
        st.code(raw_content or "(no content)")

        if not cleaned_content:
            st.warning("Response was empty.")
            return

        try:
            parsed = json_repair.loads(cleaned_content)
        except Exception:
            st.error(
                "Response was not valid JSON; adjust the prompt or reduce batch size."
            )
            return

        if isinstance(parsed, dict):
            results = parsed.get("triage_result")
            if isinstance(results, list):
                parsed = results
            else:
                parsed = [parsed]

        if not isinstance(parsed, list):
            st.error("Model response was not a JSON array; review the output above.")
            return

        parsed = [item for item in parsed if isinstance(item, dict)]
        if not parsed:
            st.error("JSON array did not contain objects.")
            return

        result_df = pd.DataFrame(parsed)
        if "blockers" in result_df.columns:
            result_df["blockers"] = result_df["blockers"].apply(_normalise_blockers)
        else:
            result_df["blockers"] = ""
        merged_df = selected_df.merge(result_df, on="processor_id", how="left")

        st.subheader("Triage results")
        st.dataframe(merged_df, use_container_width=True)

        download_payload = json.dumps(parsed, ensure_ascii=False, indent=2)
        st.download_button(
            "Download triage JSON",
            data=download_payload,
            file_name="processor_triage_results.json",
            mime="application/json",
            use_container_width=True,
        )


if __name__ == "__main__":
    main()

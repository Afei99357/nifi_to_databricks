#!/usr/bin/env python3

"""AI-assisted migration planner leveraging Databricks LLM endpoints."""

from __future__ import annotations

import json
import math
import os
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List

import json_repair
import pandas as pd
import streamlit as st
from requests import HTTPError

# Ensure repository root on path for shared utilities
CURRENT_DIR = Path(__file__).resolve()
REPO_ROOT = CURRENT_DIR.parents[2]
sys.path.insert(0, str(REPO_ROOT))

from model_serving_utils import is_endpoint_supported, query_endpoint  # type: ignore
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

TRIAGE_SYSTEM_PROMPT = """You are a NiFi to Databricks migration strategist.
You will receive JSON describing a batch of processors, with evidence synthesized from the NiFi flow.
Decide which processors require Databricks implementation, choose an appropriate target pattern, highlight blockers, and draft runnable Databricks code for the recommended solution.

Rules:
- Base decisions only on supplied evidence; if information is missing, state the gap explicitly.
- Treat `Infrastructure Only` processors as retirement candidates unless evidence shows required orchestration.
- For shared scripts or controller services, call out cross-cutting dependencies instead of repeating identical guidance.
- Limit blockers to concrete, actionable items (e.g., external script, controller service, missing schema).
- Keep rationales factual, <= 30 words.
- Preferred targets: auto_loader, copy_into, spark_batch, spark_structured_streaming, dbsql, workflow_task_shell, uc_table_ddl. Use retire for decommission and manual_investigation when unclear.
- When migration is required, populate `databricks_code` with <= 200 lines of runnable code tailored to the recommended target (PySpark for Spark jobs, SQL for dbsql/copy_into/uc_table_ddl, shell/bash for workflow_task_shell).
- Include essential imports, configuration, and comments so the code can run inside a Databricks notebook or Jobs task.
- Return an empty string for `databricks_code` when migration is not needed.
- Output ONLY JSON following the schema below.

Output schema (JSON array):
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
    "code_language": "pyspark|sql|shell|python|unknown",
    "databricks_code": "string"
  }
]
"""

COMPOSE_SYSTEM_PROMPT = """You are a Databricks solutions engineer.
You receive multiple NiFi processor snippets that have already been converted into Databricks-ready fragments.
Merge them into a single coherent notebook or workflow task description.

Rules:
- Before composing, drop processors that the Databricks migration no longer needs (logging-only, infrastructure-only, route/notify steps that have native replacements).
- Capture any removals in the summary so readers know which processors were omitted and why.
- Preserve the logical order supplied for the remaining processors.
- Deduplicate imports / session setup.
- Include inline TODO comments if additional context or manual work is required (especially when snippets reference external files).
- Emit a plain Python script format (no `%md`, `%sql`, or `# MAGIC` directives); use Python comments and embedded `spark.sql(...)` calls for SQL cells.
- Return JSON with fields: group_name, summary, notebook_code, next_actions (list of strings).
"""


ENDPOINT_OUTPUT_LIMITS = {
    "databricks-meta-llama-3-3-70b-instruct": 8192,
}

SESSION_RESULTS_KEY = "ai_migration_planner_results_records"
LEGACY_RESULTS_KEY = "triage_result"


def _records_to_dataframe(records: List[dict]) -> pd.DataFrame:
    rows = []
    for record in records:
        processor_id = record.get("processor_id") or record.get("id")
        if not processor_id:
            continue
        parent_group = record.get("parent_group") or record.get("parentGroupName")
        parent_group_path = (
            record.get("parent_group_path")
            or record.get("parentGroupPath")
            or parent_group
        )
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
                "parent_group": str(parent_group or ""),
                "parent_group_path": str(parent_group_path or ""),
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
    df = df.drop_duplicates(subset=["processor_id"]).reset_index(drop=True)
    return df


def _prepare_user_payload(
    payload_json: str, workflows: List[str], filtered_df: pd.DataFrame
) -> str:
    payload = json.loads(payload_json)
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


def _collect_session_classifications() -> (
    tuple[List[Dict[str, object]], Dict[str, Dict[str, object]]]
):
    records: List[Dict[str, object]] = []
    templates: Dict[str, Dict[str, object]] = {}

    prefix = "classification_results_"
    for key, payload in st.session_state.items():
        if not isinstance(key, str) or not key.startswith(prefix):
            continue
        if not isinstance(payload, dict):
            continue
        template_name = (
            payload.get("workflow", {}).get("filename") or key[len(prefix) :]
        )
        templates[template_name] = payload
        for record in payload.get("classifications", []) or []:
            cloned = dict(record)
            cloned["template"] = template_name
            records.append(cloned)

    return records, templates


def _collect_script_lookup() -> Dict[str, Dict[str, object]]:
    script_entries: List[Dict[str, object]] = []
    prefix = "script_results_"
    for key, payload in st.session_state.items():
        if not isinstance(key, str) or not key.startswith(prefix):
            continue
        if isinstance(payload, list):
            for entry in payload:
                if isinstance(entry, dict):
                    script_entries.append(entry)
    if not script_entries:
        return {}
    return build_script_lookup(script_entries)


def main() -> None:
    st.set_page_config(page_title="AI Migration Planner", page_icon="🧭", layout="wide")
    st.title("🧭 AI Migration Planner")
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
        help="Select the Databricks serving endpoint used for the AI Migration Planner.",
    )
    if chosen_option == "Custom…":
        endpoint_name = st.text_input("Custom endpoint name", value=preset_value)
    else:
        endpoint_name = chosen_option

    char_budget = DEFAULT_MAX_CHARS
    max_tokens = min(60000, ENDPOINT_OUTPUT_LIMITS.get(endpoint_name, 60000))

    records, template_payloads = _collect_session_classifications()
    script_lookup = _collect_script_lookup()
    if script_lookup:
        for record in records:
            processor_id = str(record.get("processor_id") or record.get("id") or "")
            if processor_id and processor_id in script_lookup:
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
        "Templates: " + ", ".join(template_names)
        if template_names
        else "(template names unavailable)"
    )

    df = _records_to_dataframe(records)
    if df.empty:
        st.warning("Classification results are empty.")
        return

    st.caption(f"{len(df)} processors across {len(template_names)} template(s)")

    record_lookup = {
        str(rec.get("processor_id")): rec for rec in records if rec.get("processor_id")
    }

    st.subheader("1. Filter processors")

    col1, col2, col3 = st.columns(3)
    with col1:
        categories = sorted(cat for cat in df["migration_category"].dropna().unique())
        category_filter = st.multiselect(
            "Migration categories",
            options=categories,
            default=categories,
            help="Deselect categories to exclude them from the AI Migration Planner.",
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
        processors_per_call = st.number_input(
            "Processors per LLM call",
            min_value=1,
            max_value=40,
            value=min(DEFAULT_MAX_PROCESSORS, len(df)) or 1,
            help="Upper bound on processors included in a single model request.",
        )

    search_name = st.text_input("Search by processor name or short type", "")

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
    st.dataframe(
        filtered_df.head(200).set_index(
            pd.Index(range(1, min(len(filtered_df), 200) + 1))
        ),
        use_container_width=True,
    )

    st.subheader("2. Provide additional context (optional)")
    additional_notes = st.text_area(
        "Notes for the model",
        placeholder="Add migration constraints, priority groups, or architectural decisions to apply across this batch.",
    )

    st.subheader("3. Run AI Migration Planner")
    if st.button("Run AI Migration Planner on selection", use_container_width=True):
        if not endpoint_name:
            st.error("Specify a serving endpoint name to continue.")
            return
        if not is_endpoint_supported(endpoint_name):
            st.error(f"Endpoint `{endpoint_name}` is not chat-completions compatible.")
            return

        selected_df = filtered_df.copy()
        if selected_df.empty:
            st.warning(
                "No processors selected for the AI Migration Planner after filtering."
            )
            return
        if "processor_id" not in selected_df.columns:
            st.error("Processor identifiers are required for the AI Migration Planner.")
            return

        selected_ids = selected_df["processor_id"].astype(str).tolist()
        selected_records = [
            record_lookup.get(pid) for pid in selected_ids if pid in record_lookup
        ]
        selected_records = [rec for rec in selected_records if rec]
        if not selected_records:
            st.error("Unable to locate processor records for the current selection.")
            return

        batches = build_batches_from_records(
            selected_records,
            max_processors=int(processors_per_call),
            max_chars=int(char_budget),
            script_lookup=script_lookup,
        )
        if not batches:
            st.warning("No processors available for batching.")
            return

        batch_summaries = []
        for payload in batches:
            batch_summaries.append(
                {
                    "batch": payload.get("batch_index"),
                    "processors": len(payload.get("processor_ids", [])),
                    "est_chars": payload.get("prompt_char_count", 0),
                }
            )

        if batch_summaries:
            summary_df = pd.DataFrame(batch_summaries)
            summary_df = summary_df.sort_values("batch").reset_index(drop=True)
            st.caption(
                "Batch plan (processor count vs. estimated prompt characters)."
            )
            st.dataframe(
                summary_df,
                use_container_width=True,
            )

        selected_templates = (
            sorted(selected_df["template"].dropna().unique().tolist())
            if "template" in selected_df.columns
            else template_names
        )

        all_results: List[pd.DataFrame] = []
        raw_responses: List[Dict[str, Any]] = []
        saved_processor_ids: List[str] = []
        snippet_store_modified = False

        for batch_index, batch_payload in enumerate(batches, start=1):
            batch_ids = batch_payload["processor_ids"]
            batch_records = [
                record_lookup.get(pid) for pid in batch_ids if pid in record_lookup
            ]
            batch_records = [rec for rec in batch_records if rec]
            if not batch_records:
                continue

            batch_df = selected_df[selected_df["processor_id"].isin(batch_ids)]
            st.caption(
                f"Batch {batch_index}: {len(batch_ids)} processors, "
                f"≈{batch_payload.get('prompt_char_count', 0):,} chars"
            )
            payloads = build_payloads(batch_records)
            payload_json = format_payloads_for_prompt(payloads)
            user_payload = _prepare_user_payload(
                payload_json,
                selected_templates,
                batch_df,
            )

            user_message = (
                f"INPUT_PROCESSORS:\n{user_payload}\n"
                "If additional notes are provided, apply them across all processors."
            )
            if additional_notes.strip():
                user_message += f"\nADDITIONAL_NOTES:\n{additional_notes.strip()}"

            with st.expander(
                f"Preview payload sent to LLM (batch {batch_index})",
                expanded=False,
            ):
                st.code(user_payload, language="json")

            messages = [
                {"role": "system", "content": TRIAGE_SYSTEM_PROMPT.strip()},
                {"role": "user", "content": user_message},
            ]

            with st.spinner(
                f"Calling serving endpoint (batch {batch_index}/{len(batches)})..."
            ):
                try:
                    reply = query_endpoint(endpoint_name, messages, int(max_tokens))
                except HTTPError as exc:  # pragma: no cover
                    st.error("LLM call failed on batch " f"{batch_index}: {exc}")
                    response = getattr(exc, "response", None)
                    detail = ""
                    if response is not None:
                        detail = getattr(response, "text", "") or str(response)
                    if detail:
                        st.caption("Endpoint response")
                        st.code(detail.strip())
                    return
                except Exception as exc:  # pragma: no cover
                    st.error(f"LLM call failed on batch {batch_index}: {exc}")
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

            if not cleaned_content:
                st.warning(f"Response was empty for batch {batch_index}.")
                continue

            try:
                parsed = json_repair.loads(cleaned_content)
            except Exception:
                st.error(
                    "Response was not valid JSON; adjust the prompt or reduce batch size."
                )
                return

            if isinstance(parsed, dict):
                results = parsed.get("ai_migration_planner_result") or parsed.get(
                    LEGACY_RESULTS_KEY
                )
                if isinstance(results, list):
                    parsed = results
                else:
                    parsed = [parsed]

            if not isinstance(parsed, list):
                st.error(
                    "Model response was not a JSON array; review the output above."
                )
                return

            parsed = [item for item in parsed if isinstance(item, dict)]
            if not parsed:
                st.warning(f"Batch {batch_index} returned no result objects.")
                continue

            result_df = pd.DataFrame(parsed)
            raw_responses.extend(parsed)
            if "blockers" in result_df.columns:
                result_df["blockers"] = result_df["blockers"].apply(_normalise_blockers)
            else:
                result_df["blockers"] = ""

            if "databricks_code" not in result_df.columns:
                result_df["databricks_code"] = ""
            result_df["databricks_code"] = result_df["databricks_code"].fillna("")

            if "code_language" not in result_df.columns:
                result_df["code_language"] = "unknown"
            result_df["code_language"] = result_df["code_language"].fillna("unknown")

            merged_df = batch_df.merge(result_df, on="processor_id", how="left")
            merged_df["batch_index"] = batch_index
            all_results.append(merged_df)

            records_payload = merged_df.to_dict("records")
            update_snippet_store(
                snippet_store,
                records_payload,
                endpoint=endpoint_name,
                max_tokens=int(max_tokens),
                batch_index=batch_index,
            )
            snippet_store_modified = True
            saved_processor_ids.extend(
                [
                    str(row.get("processor_id"))
                    for row in records_payload
                    if str(row.get("databricks_code") or "").strip()
                ]
            )

        if not all_results:
            st.info("No results were produced.")
            return

        combined_df = pd.concat(all_results, ignore_index=True).fillna("")

        # Normalise confidence columns from merge
        if "confidence" not in combined_df.columns:
            if "confidence_y" in combined_df.columns:
                combined_df["confidence"] = combined_df["confidence_y"]
            elif "confidence_x" in combined_df.columns:
                combined_df["confidence"] = combined_df["confidence_x"]
        combined_df = combined_df.fillna("")

        for redundant in ("confidence_x", "confidence_y"):
            if redundant in combined_df.columns:
                combined_df = combined_df.drop(columns=[redundant])

        st.session_state[SESSION_RESULTS_KEY] = combined_df.to_dict("records")

        if snippet_store_modified:
            save_snippet_store(snippet_store)
            saved_unique = len({pid for pid in saved_processor_ids if pid})
            if saved_unique:
                st.success(f"Stored snippets for {saved_unique} processor(s).")
                st.info(
                    "Review cached snippets below to inspect code or compose grouped notebooks."
                )
            snippet_store = load_snippet_store()

    # Display persisted AI Migration Planner results when available
    stored_records = st.session_state.get(SESSION_RESULTS_KEY)
    if stored_records:
        stored_df = pd.DataFrame(stored_records).fillna("")
        if not stored_df.empty:
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
                stored_df[available_cols].set_index(
                    pd.Index(range(1, len(stored_df[available_cols]) + 1))
                ),
                use_container_width=True,
            )

            st.download_button(
                "Download AI Migration Planner results CSV",
                data=stored_df.to_csv(index=False).encode("utf-8"),
                file_name="ai_migration_planner_results.csv",
                mime="text/csv",
                use_container_width=True,
            )

    # --- Cached snippet review & composition ---
    snippet_entries = list(snippet_store.get("snippets", {}).values())
    if snippet_entries:
        st.subheader("Cached processor snippets")
        snippet_df = pd.DataFrame(snippet_entries)
        if not snippet_df.empty:
            snippet_df = snippet_df.fillna("")
            snippet_df = snippet_df.sort_values(
                ["parent_group_path", "processor_id"],
                kind="stable",
            ).reset_index(drop=True)

            group_options = ["All"] + sorted(
                {
                    str(value) or "(unknown)"
                    for value in snippet_df.get("parent_group_path", "")
                }
            )
            selected_snippet_group = st.selectbox(
                "Snippet group",
                group_options,
                index=0,
                help="Filter cached snippets by NiFi group path.",
                key="snippet_group_select",
            )

            if selected_snippet_group == "All":
                snippet_view = snippet_df
            else:
                snippet_view = snippet_df[
                    snippet_df["parent_group_path"].fillna("") == selected_snippet_group
                ]

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
            available_cols = [
                col for col in display_cols if col in snippet_view.columns
            ]
            st.dataframe(snippet_view[available_cols], use_container_width=True)

            for _, row in snippet_view.iterrows():
                code = str(row.get("databricks_code", "") or "")
                if not code.strip():
                    continue
                processor_label = (
                    f"{row.get('processor_id')} · {row.get('name', '')}".strip()
                )
                with st.expander(f"Snippet · {processor_label}"):
                    st.code(code, language=row.get("code_language", "text"))

            download_snippets = json.dumps(
                snippet_entries, ensure_ascii=False, indent=2
            )
            st.download_button(
                "Download cached snippets JSON",
                data=download_snippets,
                file_name="processor_snippets.json",
                mime="application/json",
                use_container_width=True,
            )

            compose_options = [
                option
                for option in group_options
                if option != "(unknown)" or option == "All"
            ]
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

            if st.button("Compose notebook for group", key="compose_button"):
                if not endpoint_name:
                    st.error("Specify a serving endpoint name before composing.")
                else:
                    if compose_group == "All":
                        compose_df = snippet_df
                        group_label = "All processors"
                    else:
                        compose_df = snippet_df[
                            snippet_df["parent_group_path"].fillna("") == compose_group
                        ]
                        group_label = compose_group

                    compose_records = [
                        {
                            "processor_id": row.get("processor_id"),
                            "name": row.get("name"),
                            "short_type": row.get("short_type"),
                            "migration_category": row.get("migration_category"),
                            "implementation_hint": row.get("implementation_hint"),
                            "databricks_code": row.get("databricks_code"),
                            "blockers": row.get("blockers"),
                            "next_step": row.get("next_step"),
                        }
                        for _, row in compose_df.iterrows()
                        if str(row.get("databricks_code") or "").strip()
                    ]

                    if not compose_records:
                        st.warning(
                            "No cached snippets with code available for the selected group."
                        )
                    else:
                        group_payload = {
                            "group_name": group_label,
                            "processor_count": len(compose_records),
                            "processors": compose_records,
                        }
                        if compose_notes.strip():
                            group_payload["notes"] = compose_notes.strip()

                        messages = [
                            {
                                "role": "system",
                                "content": COMPOSE_SYSTEM_PROMPT.strip(),
                            },
                            {
                                "role": "user",
                                "content": "GROUP_INPUT:\n"
                                + json.dumps(
                                    group_payload, ensure_ascii=False, indent=2
                                ),
                            },
                        ]

                        with st.spinner("Composing notebook from cached snippets..."):
                            try:
                                reply = query_endpoint(
                                    endpoint_name,
                                    messages,
                                    int(max_tokens),
                                )
                            except Exception as exc:  # pragma: no cover
                                st.error(f"LLM composition failed: {exc}")
                            else:
                                raw_content = reply.get("content", "")
                                st.subheader("Composition response")
                                st.code(raw_content or "(no content)")

                                cleaned = raw_content.strip().strip("`")
                                if cleaned.startswith("json"):
                                    cleaned = cleaned[4:].lstrip()
                                if cleaned:
                                    try:
                                        payload = json_repair.loads(cleaned)
                                    except Exception:
                                        st.warning(
                                            "Unable to parse composition output as JSON."
                                        )
                                    else:
                                        if isinstance(payload, list) and payload:
                                            payload = payload[0]
                                        if isinstance(payload, dict):
                                            notebook_code = payload.get(
                                                "notebook_code", ""
                                            )
                                            summary = payload.get("summary")
                                            next_actions = payload.get(
                                                "next_actions", []
                                            )

                                            st.subheader(
                                                f"Composed notebook · {payload.get('group_name', group_label)}"
                                            )
                                            if summary:
                                                st.markdown(summary)
                                            if notebook_code:
                                                st.code(
                                                    notebook_code,
                                                    language="python",
                                                )
                                                st.download_button(
                                                    "Download composed notebook",
                                                    data=notebook_code,
                                                    file_name="composed_notebook.py",
                                                    mime="text/x-python",
                                                    use_container_width=True,
                                                )
                                            if (
                                                isinstance(next_actions, list)
                                                and next_actions
                                            ):
                                                st.markdown("**Next actions**")
                                                for item in next_actions:
                                                    st.write(f"- {item}")
                                        else:
                                            st.warning(
                                                "Composition output was not a JSON object."
                                            )


if __name__ == "__main__":
    main()

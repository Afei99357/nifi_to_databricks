#!/usr/bin/env python3

"""AI Analyzer page for Databricks serving endpoint testing."""

from __future__ import annotations

import json
import os

import json_repair
import pandas as pd
import streamlit as st

from model_serving_utils import is_endpoint_supported, query_endpoint  # type: ignore

DEFAULT_SYSTEM_PROMPT = (
    "You are a static code and workflow migration analyst.\n"
    "Your task is to analyze external scripts (bash, Python, JARs, inline SQL, etc.) that are called inside Apache NiFi workflows.\n"
    "These scripts were historically used to orchestrate Hadoop-based data pipelines.\n"
    "We are migrating these pipelines to Databricks + Unity Catalog.\n\n"
    "Rules:\n"
    "- Output ONLY a single JSON object that strictly follows the provided schema.\n"
    '- Be conservative; if not sure, always return "unknown".\n'
    "- Never invent file paths, schemas, or endpoints.\n"
    '- Use evidence from the code and metadata provided. If absent, return "unknown".\n'
    "- Purposes must come from: movement, transformation, orchestration, logging, permissions, security, db_io, network_io, compression.\n"
    "- Identify side effects (chmod, delete, network calls, db writes, temp files, etc.).\n"
    "- Rate PII risk ONLY if you see likely personal data fields (emails, SSNs, names, etc.).\n"
    "- Idempotency: \n"
    '  - "idempotent" if rerunning does not change results (e.g., overwriting file, logging). \n'
    '  - "non_idempotent" if it appends, deletes, renames, or modifies external state.\n'
    "- migration_action values:\n"
    '  - "retire" → logging/permissions-only, no business transformation.\n'
    '  - "replace_with_native" → can be expressed with Databricks native features (Auto Loader, COPY INTO, Spark, DBSQL).\n'
    '  - "reimplement_minimal" → small custom transform to rewrite in Spark or SQL.\n'
    '  - "preserve_as_is" → complex binary/JAR, external dependency, or too risky to rewrite now.\n'
    "- Return only factual analysis. No prose outside of JSON.\n\n"
    "JSON_SCHEMA:\n"
    "{\n"
    '  "language": "bash|python|java|scala|sql|unknown",\n'
    '  "purpose_primary": "movement|transformation|orchestration|logging|permissions|security|db_io|network_io|compression|unknown",\n'
    '  "purpose_secondary": ["..."],\n'
    '  "inputs": [\n'
    '    {"type":"file|dir|db|topic|http|stdin","pattern_or_path":"...","format":"csv|json|parquet|binary|unknown"}\n'
    "  ],\n"
    '  "outputs": [\n'
    '    {"type":"file|dir|db|topic|http|stdout","pattern_or_path":"...","format":"csv|json|parquet|table|unknown"}\n'
    "  ],\n"
    '  "side_effects": ["chmod","delete_files","network_calls","db_writes","temp_files","exec_other_programs","unknown"],\n'
    '  "data_transformations": ["filter","map","schema_change:add|drop|rename","merge","encrypt|decrypt","compress|decompress","none","unknown"],\n'
    '  "external_dependencies": ["impala-shell","hdfs","aws","gsutil","sed","awk","spark-submit","jar:...","pip:...","unknown"],\n'
    '  "pii_risk": "low|medium|high|unknown",\n'
    '  "idempotency": "idempotent|non_idempotent|unknown",\n'
    '  "scheduling_trigger": "nifi_called|cron|manual|unknown",\n'
    '  "danger_flags": ["world_writable","shell_injection_risk","hardcoded_credentials","deletes_recursively","downloads_and_executes","unknown"],\n'
    '  "extracted_signals": { "shebang":"...", "imports":["..."], "binaries_called":["..."], "regex_hits":["..."] },\n'
    '  "summary": "1-2 sentences, factual only.",\n'
    '  "databricks_replacement": "auto_loader|copy_into|spark_batch|spark_structured_streaming|dbsql|workflow_task_shell|uc_table_ddl|unknown",\n'
    '  "replacement_notes": "string",\n'
    '  "migration_action": "retire|replace_with_native|reimplement_minimal|preserve_as_is",\n'
    '  "action_rationale": "string",\n'
    '  "uc_assets_touched": ["..."],\n'
    '  "runtime_environment": ["bash","python","java","hdfs","impala-shell","openssl","unknown"],\n'
    '  "blocking_dependencies": ["..."],\n'
    '  "security_requirements": ["kerberos","keytab","kms","service_principal","unknown"],\n'
    '  "effort_estimate": "low|medium|high|unknown",\n'
    '  "test_strategy": "golden_sample_files|delta_table_diff|row_count_check|checksum|unknown"\n'
    "}"
)
DEFAULT_USER_PROMPT = "Paste code or instructions here..."


def main() -> None:
    st.set_page_config(page_title="AI Analyzer", page_icon="🧪", layout="centered")
    st.title("🧪 AI Analyzer")
    st.write(
        "Use this page to send ad-hoc prompts to the Databricks serving endpoint. "
        "Set the `SERVING_ENDPOINT` environment variable or enter the endpoint name below."
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
        options=endpoint_choices,
        index=default_index,
        help="Select a Databricks serving endpoint or choose Custom to type another name.",
    )

    if chosen_option == "Custom…":
        endpoint_name = st.text_input("Custom endpoint name", value=preset_value)
    else:
        endpoint_name = chosen_option
    max_tokens = st.slider(
        "Max tokens", min_value=64, max_value=4096, value=512, step=64
    )

    system_prompt = st.text_area("System prompt", DEFAULT_SYSTEM_PROMPT, height=160)
    user_prompt = st.text_area("Code snippet / prompt", DEFAULT_USER_PROMPT, height=240)

    if st.button("Send request", use_container_width=True):
        if not endpoint_name:
            st.error("Enter the serving endpoint name or export `SERVING_ENDPOINT`.")
            return

        if not is_endpoint_supported(endpoint_name):
            st.error(
                f"Endpoint `{endpoint_name}` is not chat-completions compatible. "
                "Supported task types: agent/v1/chat, agent/v2/chat, llm/v1/chat."
            )
            return

        messages = [
            {"role": "system", "content": system_prompt.strip()},
            {"role": "user", "content": user_prompt.strip()},
        ]

        with st.spinner("Calling serving endpoint..."):
            try:
                reply = query_endpoint(endpoint_name, messages, max_tokens)
            except Exception as exc:  # pragma: no cover - UI feedback
                st.error(f"LLM call failed: {exc}")
                return

        raw_content = reply.get("content", "")

        def _clean_response(text: str) -> str:
            stripped = text.strip()
            if stripped.startswith("```") and stripped.endswith("```"):
                stripped = stripped[3:-3].strip()
            if stripped.startswith("json"):
                stripped = stripped[4:].lstrip()
            return stripped

        cleaned_content = _clean_response(raw_content)

        st.subheader("Assistant response")
        st.code(raw_content or "(no content)")

        if cleaned_content:
            try:
                parsed = json_repair.loads(cleaned_content)
            except Exception:
                st.warning("Response was not valid JSON; unable to tabulate.")
            else:
                if isinstance(parsed, dict):
                    st.subheader("Structured view")
                    rows = [
                        {
                            "Field": key,
                            "Value": (
                                json.dumps(value, ensure_ascii=False)
                                if isinstance(value, (dict, list))
                                else value
                            ),
                        }
                        for key, value in parsed.items()
                        if key != "confidence"
                    ]
                    df = pd.DataFrame(rows)
                    st.dataframe(df, hide_index=True, use_container_width=True)
                else:
                    st.info("Parsed JSON was not an object; showing raw output only.")


if __name__ == "__main__":
    main()

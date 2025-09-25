#!/usr/bin/env python3

"""AI Analyzer page for Databricks serving endpoint testing."""

from __future__ import annotations

import hashlib
import json
import os
from typing import Any

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
    "- Return only factual analysis. No prose outside of JSON.\n"
    "- Produce one JSON object PER FILE provided. Wrap all objects in a JSON array.\n"
    "- For each element, include the exact filename supplied in metadata using the `filename` field.\n\n"
    '- At the end, fill the field "migration_decision_summary" with ONE short sentence (max 25 words) that clearly states whether this script should be migrated to Databricks based on the information you extract here and why.\n'
    "- Keep it factual and action-oriented, not speculative.\n\n"
    "JSON_SCHEMA (array of objects):\n"
    "[\n"
    "  {\n"
    '    "filename": "string",\n'
    '    "language": "bash|python|java|scala|sql|unknown",\n'
    '    "purpose_primary": "movement|transformation|orchestration|logging|permissions|security|db_io|network_io|compression|unknown",\n'
    '    "purpose_secondary": ["..."],\n'
    '    "inputs": [\n'
    '      {"type":"file|dir|db|topic|http|stdin","pattern_or_path":"...","format":"csv|json|parquet|binary|unknown"}\n'
    "    ],\n"
    '    "outputs": [\n'
    '      {"type":"file|dir|db|topic|http|stdout","pattern_or_path":"...","format":"csv|json|parquet|table|unknown"}\n'
    "    ],\n"
    '    "side_effects": ["chmod","delete_files","network_calls","db_writes","temp_files","exec_other_programs","unknown"],\n'
    '    "data_transformations": ["filter","map","schema_change:add|drop|rename","merge","encrypt|decrypt","compress|decompress","none","unknown"],\n'
    '    "external_dependencies": ["impala-shell","hdfs","aws","gsutil","sed","awk","spark-submit","jar:...","pip:...","unknown"],\n'
    '    "pii_risk": "low|medium|high|unknown",\n'
    '    "idempotency": "idempotent|non_idempotent|unknown",\n'
    '    "scheduling_trigger": "nifi_called|cron|manual|unknown",\n'
    '    "danger_flags": ["world_writable","shell_injection_risk","hardcoded_credentials","deletes_recursively","downloads_and_executes","unknown"],\n'
    '    "extracted_signals": { "shebang":"...", "imports":["..."], "binaries_called":["..."], "regex_hits":["..."] },\n'
    '    "summary": "1-2 sentences, factual only.",\n'
    '    "databricks_replacement": "auto_loader|copy_into|spark_batch|spark_structured_streaming|dbsql|workflow_task_shell|uc_table_ddl|unknown",\n'
    '    "replacement_notes": "string",\n'
    '    "migration_action": "retire|replace_with_native|reimplement_minimal|preserve_as_is",\n'
    '    "action_rationale": "string",\n'
    '    "uc_assets_touched": ["..."],\n'
    '    "runtime_environment": ["bash","python","java","hdfs","impala-shell","openssl","unknown"],\n'
    '    "blocking_dependencies": ["..."],\n'
    '    "security_requirements": ["kerberos","keytab","kms","service_principal","unknown"],\n'
    '    "effort_estimate": "low|medium|high|unknown",\n'
    '    "test_strategy": "golden_sample_files|delta_table_diff|row_count_check|checksum|unknown",\n'
    '    "migration_decision_summary": "string"\n'
    "  }\n"
    "]"
)
DEFAULT_USER_PROMPT = ""

MAX_PREVIEW_CHARS = 4000


def _decode_uploaded_file(upload) -> tuple[str, bytes]:
    """Decode an uploaded file into text, replacing invalid bytes."""

    raw_bytes = upload.getvalue()
    try:
        text = raw_bytes.decode("utf-8")
    except UnicodeDecodeError:
        text = raw_bytes.decode("latin-1", errors="replace")
    return text, raw_bytes


def _summarise_content(content: str, max_chars: int = MAX_PREVIEW_CHARS) -> str:
    """Trim long content while keeping the start and end for context."""

    if len(content) <= max_chars:
        return content

    head = max_chars // 2
    tail = max_chars - head
    return (
        content[:head]
        + "\n... [TRUNCATED FOR LENGTH] ...\n"
        + content[-tail:]
    )


def _normalise_cell_value(value: Any) -> Any:
    """Render nested structures as JSON strings for display."""

    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    return value


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

    st.caption(
        "Upload one or more code files to analyze, or paste a snippet below when you do not have files handy."
    )
    uploaded_files = st.file_uploader(
        "Code files",
        type=["py", "sh", "sql", "scala", "java", "bash", "txt", "ps1", "jar"],
        accept_multiple_files=True,
        help="Files are read as text; binary formats will be truncated and decoded best-effort.",
    )

    additional_context = st.text_area(
        "Optional additional context or manual code snippet",
        DEFAULT_USER_PROMPT,
        placeholder="Paste code or guidance when no files are uploaded, or provide extra instructions.",
        height=220,
    )

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

        file_entries = []

        if uploaded_files:
            for file_obj in uploaded_files:
                text, raw_bytes = _decode_uploaded_file(file_obj)
                preview = _summarise_content(text)
                sha = hashlib.sha256(raw_bytes).hexdigest()
                entry = {
                    "filename": file_obj.name,
                    "size": len(raw_bytes),
                    "sha256": sha,
                    "preview": preview,
                }
                file_entries.append(entry)

        if not file_entries:
            manual_content = additional_context.strip()
            if not manual_content:
                st.error(
                    "Upload at least one file or provide a code snippet / instructions to analyze."
                )
                return
            file_entries.append(
                {
                    "filename": "manual_input",
                    "size": len(manual_content.encode("utf-8")),
                    "sha256": hashlib.sha256(manual_content.encode("utf-8")).hexdigest(),
                    "preview": _summarise_content(manual_content),
                }
            )

        prompt_sections = []
        for index, entry in enumerate(file_entries, start=1):
            prompt_sections.append(
                (
                    f"FILE {index}\n"
                    f"FILENAME: {entry['filename']}\n"
                    f"SIZE_BYTES: {entry['size']}\n"
                    f"SHA256: {entry['sha256']}\n"
                    "CONTENT_START\n"
                    f"{entry['preview']}\n"
                    "CONTENT_END"
                )
            )

        supplementary = additional_context.strip()
        if supplementary and uploaded_files:
            prompt_sections.append(
                "GLOBAL_NOTES\n" + supplementary
            )

        user_prompt = "\n\n".join(prompt_sections)

        messages = [
            {"role": "system", "content": DEFAULT_SYSTEM_PROMPT.strip()},
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
                data_rows = []
                if isinstance(parsed, dict):
                    data_rows = [parsed]
                elif isinstance(parsed, list):
                    data_rows = [item for item in parsed if isinstance(item, dict)]

                if not data_rows:
                    st.info("Parsed JSON did not contain objects; showing raw output only.")
                else:
                    st.subheader("Structured view")
                    df = pd.DataFrame(
                        [
                            {
                                "filename": row.get("filename", ""),
                                **{
                                    key: _normalise_cell_value(row[key])
                                    for key in row
                                    if key not in {"filename", "confidence"}
                                },
                            }
                            for row in data_rows
                        ]
                    )
                    st.dataframe(df, use_container_width=True)

                    download_payload = json.dumps(data_rows, ensure_ascii=False, indent=2)
                    st.download_button(
                        "Download JSON",
                        data=download_payload,
                        file_name="ai_analyzer_results.json",
                        mime="application/json",
                        use_container_width=True,
                    )


if __name__ == "__main__":
    main()

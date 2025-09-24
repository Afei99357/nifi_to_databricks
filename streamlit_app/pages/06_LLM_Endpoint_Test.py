#!/usr/bin/env python3

"""Simple page to validate Databricks serving endpoint connectivity."""

from __future__ import annotations

import os

import streamlit as st

from model_serving_utils import is_endpoint_supported, query_endpoint  # type: ignore

DEFAULT_SYSTEM_PROMPT = (
    "You are a static code analyst. You must produce STRICT JSON exactly matching the provided schema.\n"
    "Rules:\n"
    '- Be conservative; if not sure, use "unknown".\n'
    "- Never invent file paths, schemas, or endpoints.\n"
    "- Prefer classifications: movement (file/topic/db moves), transformation (content changes), orchestration (calling other tools), logging, permissions, security, db_io, network_io, compression.\n"
    "- Identify side effects (chmod, delete, network calls, db writes).\n"
    "- Rate PII risk only if you see likely personal data fields or explicit handling.\n"
    "- Idempotency: changes state repeatedly? file renames/moves/append/db writes often non-idempotent.\n"
    "- confidence ∈ [0,1].\n"
    "Output ONLY the JSON object. No extra text."
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
        "Serving endpoint",
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
    user_prompt = st.text_area("User prompt", DEFAULT_USER_PROMPT, height=240)

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

        st.subheader("Assistant response")
        st.code(reply.get("content", "(no content)"))


if __name__ == "__main__":
    main()

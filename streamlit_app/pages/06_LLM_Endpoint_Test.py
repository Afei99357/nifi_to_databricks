#!/usr/bin/env python3

"""Simple page to validate Databricks serving endpoint connectivity."""

from __future__ import annotations

import os

import streamlit as st

from model_serving_utils import is_endpoint_supported, query_endpoint  # type: ignore

DEFAULT_SYSTEM_PROMPT = "You are a static code analyst. Return STRICT JSON only."
DEFAULT_USER_PROMPT = "Paste code or instructions here..."


def main() -> None:
    st.set_page_config(
        page_title="LLM Endpoint Test", page_icon="🧪", layout="centered"
    )
    st.title("🧪 Databricks LLM Endpoint Test")
    st.write(
        "Use this page to send ad-hoc prompts to the Databricks serving endpoint. "
        "Set the `SERVING_ENDPOINT` environment variable or enter the endpoint name below."
    )

    default_endpoint = os.getenv("SERVING_ENDPOINT", "")
    endpoint_name = st.text_input("Serving endpoint", value=default_endpoint)
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

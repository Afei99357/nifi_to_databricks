"""Utilities for calling Databricks model serving endpoints."""

from __future__ import annotations

from databricks.sdk import WorkspaceClient
from mlflow.deployments import get_deploy_client


def _get_endpoint_task_type(endpoint_name: str) -> str:
    """Get the task type of a serving endpoint."""

    workspace = WorkspaceClient()
    endpoint = workspace.serving_endpoints.get(endpoint_name)
    return endpoint.task


def is_endpoint_supported(endpoint_name: str) -> bool:
    """Return True if the endpoint exposes a chat-compatible task type."""

    task_type = _get_endpoint_task_type(endpoint_name)
    supported_task_types = ["agent/v1/chat", "agent/v2/chat", "llm/v1/chat"]
    return task_type in supported_task_types


def _validate_endpoint_task_type(endpoint_name: str) -> None:
    if not is_endpoint_supported(endpoint_name):
        raise Exception(
            "Detected unsupported endpoint type for this basic chatbot template. "
            "This template only supports chat completions-compatible endpoints. "
            "For a richer chatbot template with support for all conversational endpoints on Databricks, "
            "see https://docs.databricks.com/aws/en/generative-ai/agent-framework/chat-app"
        )


def _query_endpoint(
    endpoint_name: str, messages: list[dict[str, str]], max_tokens: int
) -> list[dict[str, str]]:
    """Call a chat or agent serving endpoint and normalise its response."""

    _validate_endpoint_task_type(endpoint_name)

    response = get_deploy_client("databricks").predict(
        endpoint=endpoint_name,
        inputs={"messages": messages, "max_tokens": max_tokens},
    )

    if "messages" in response:
        return response["messages"]

    if "choices" in response:
        choice_message = response["choices"][0]["message"]
        content = choice_message.get("content")

        if isinstance(content, list):
            combined = "".join(
                part.get("text", "") for part in content if part.get("type") == "text"
            )
            return [
                {"role": choice_message.get("role", "assistant"), "content": combined}
            ]

        if isinstance(content, str):
            return [choice_message]

    raise Exception(
        "This app can only run against:"
        "1) Databricks foundation model or external model endpoints with the chat task type"
        " (see https://docs.databricks.com/aws/en/machine-learning/model-serving/score-foundation-models#chat-completion-model-query)"
        "2) Databricks agent serving endpoints implementing the conversational agent schema"
        " (see https://docs.databricks.com/aws/en/generative-ai/agent-framework/author-agent)"
    )


def query_endpoint(
    endpoint_name: str, messages: list[dict[str, str]], max_tokens: int
) -> dict[str, str]:
    """Return the last message from the serving endpoint response."""

    return _query_endpoint(endpoint_name, messages, max_tokens)[-1]


__all__ = ["is_endpoint_supported", "query_endpoint"]

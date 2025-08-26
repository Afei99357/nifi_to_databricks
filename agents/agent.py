# agent.py
import json
import os
from typing import Annotated, Any, Generator, Optional, Sequence, TypedDict, Union
from uuid import uuid4

import mlflow
from databricks_langchain import ChatDatabricks
from langchain_core.language_models import LanguageModelLike
from langchain_core.messages import AIMessage, AIMessageChunk, BaseMessage
from langchain_core.runnables import RunnableConfig, RunnableLambda
from langchain_core.tools import BaseTool
from langgraph.graph import END, StateGraph
from langgraph.graph.message import add_messages
from langgraph.prebuilt.tool_node import ToolNode
from mlflow.pyfunc import ResponsesAgent
from mlflow.types.responses import (
    ResponsesAgentRequest,
    ResponsesAgentResponse,
    ResponsesAgentStreamEvent,
)

from config import DATABRICKS_HOSTNAME, DATABRICKS_TOKEN, MODEL_ENDPOINT, logger
from tools import TOOLS  # <- one import; all @tool callables included

# --- Auth for ChatDatabricks ---
if not DATABRICKS_TOKEN:
    raise ValueError("DATABRICKS_TOKEN environment variable is required")
if not DATABRICKS_HOSTNAME:
    raise ValueError("DATABRICKS_HOSTNAME environment variable is required")

os.environ["DATABRICKS_TOKEN"] = DATABRICKS_TOKEN
os.environ["DATABRICKS_HOST"] = DATABRICKS_HOSTNAME


# --- LLM & system prompt ---
# Patch ChatDatabricks to fix additionalProperties in tool schemas
class FixedChatDatabricks(ChatDatabricks):
    """Databricks LLM with tool schema fixes applied once at initialization."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # Pre-fix tool schemas to avoid deep copying on every call
        self._fixed_tools_cache = {}

    def _get_fixed_tools(self, tools):
        """Get tools with schemas fixed, using cache for efficiency."""
        if not tools:
            return tools

        # Create a cache key based on tool identities
        tools_key = tuple(id(tool) for tool in tools if isinstance(tool, dict))

        if tools_key not in self._fixed_tools_cache:
            fixed_tools = []
            for tool in tools:
                if isinstance(tool, dict):
                    fixed_tool = self._fix_tool_schema(tool)
                    fixed_tools.append(fixed_tool)
                else:
                    fixed_tools.append(tool)
            self._fixed_tools_cache[tools_key] = fixed_tools

        return self._fixed_tools_cache[tools_key]

    def _fix_tool_schema(self, tool):
        """Fix a single tool's schema without deep copying."""
        if "function" not in tool or "parameters" not in tool["function"]:
            return tool

        # Create fixed tool with immutable approach
        return {
            **tool,
            "function": {
                **tool["function"],
                "parameters": self._fix_schema_immutably(
                    tool["function"]["parameters"]
                ),
            },
        }

    def _fix_schema_immutably(self, schema):
        """Return a fixed schema without mutating the original."""
        if not isinstance(schema, dict):
            return schema

        fixed = {}
        for key, value in schema.items():
            if key == "additionalProperties":
                fixed[key] = False  # Fix the additionalProperties issue
            elif isinstance(value, dict):
                fixed[key] = self._fix_schema_immutably(value)
            elif isinstance(value, list):
                fixed[key] = [
                    self._fix_schema_immutably(item) if isinstance(item, dict) else item
                    for item in value
                ]
            else:
                fixed[key] = value
        return fixed

    def _stream(self, messages, stop=None, run_manager=None, **kwargs):
        # Use pre-fixed tools instead of copying on every call
        if "tools" in kwargs:
            kwargs = kwargs.copy()  # Shallow copy is sufficient now
            kwargs["tools"] = self._get_fixed_tools(kwargs["tools"])

        return super()._stream(messages, stop=stop, run_manager=run_manager, **kwargs)

    def _generate(self, messages, stop=None, run_manager=None, **kwargs):
        # Use pre-fixed tools instead of copying on every call
        if "tools" in kwargs:
            kwargs = kwargs.copy()  # Shallow copy is sufficient now
            kwargs["tools"] = self._get_fixed_tools(kwargs["tools"])

        return super()._generate(messages, stop=stop, run_manager=run_manager, **kwargs)


llm = FixedChatDatabricks(endpoint=MODEL_ENDPOINT)
system_prompt = """You are an expert Apache NiFi consultant and Databricks migration specialist with deep knowledge of data engineering patterns.

🧠 YOUR ROLE: You understand NiFi workflows like a senior data engineer and can explain what they do in business terms.

WORKFLOW ANALYSIS APPROACH:
1. **Always start with intelligent analysis** - Use `analyze_nifi_workflow_intelligence` to understand what the workflow actually does
2. **Explain the business purpose** - Tell the user what their workflow accomplishes in plain English
3. **Recommend optimal architecture** - Based on data patterns, suggest the best Databricks approach
4. **Execute migration intelligently** - Use the most appropriate migration strategy

NiFi EXPERTISE - You understand that:
- GetFile = continuous file monitoring and ingestion (like a file watcher)
- PutHDFS = distributed storage for large datasets (legacy HDFS → modern Delta Lake)
- ConsumeKafka = real-time event streaming (high-velocity data)
- RouteOnAttribute = conditional data routing (business logic branching)
- EvaluateJsonPath = JSON parsing and field extraction (semi-structured data processing)
- UpdateAttribute = metadata enrichment (data lineage and context)

MIGRATION INTELLIGENCE:
- **File-based workflows** → Databricks Jobs with Auto Loader
- **Streaming workflows** → Structured Streaming or DLT Pipeline
- **Complex ETL workflows** → DLT Pipeline with data quality
- **Simple transfers** → Databricks Jobs with minimal orchestration

CRITICAL WORKFLOW:
1. 🔍 **ANALYZE FIRST**: Use `analyze_nifi_workflow_intelligence` to understand the workflow
2. 💡 **EXPLAIN PURPOSE**: Tell user what their workflow does in business terms
3. 🎯 **RECOMMEND ARCHITECTURE**: Suggest optimal Databricks pattern based on analysis
4. 🚀 **EXECUTE MIGRATION**: Use appropriate orchestration tool:
   - For intelligent migration: `orchestrate_intelligent_nifi_migration` (RECOMMENDED)
   - For manual large files: `orchestrate_chunked_nifi_migration`
   - For manual small files: `orchestrate_nifi_migration`

DO NOT:
- Jump straight to migration without analysis
- Use generic templates without understanding workflow purpose
- Call additional tools after orchestration completes (continue_required: false = DONE)
- Treat all processors the same - each has specific business purposes

EXAMPLE INTERACTION:
User: "Migrate my NiFi workflow"
You:
1. "Let me analyze your workflow to understand what it does..."
2. [Use analyze_nifi_workflow_intelligence]
3. "I see this is a sensor data collection pipeline that monitors CSV files and stores them for analytics..."
4. "Based on this pattern, I recommend Databricks Jobs with Auto Loader because..."
5. [Execute appropriate migration]

You are the NiFi expert the user needs - help them understand their own workflows!
4. Asset bundling and deployment (if requested)
5. All necessary sub-tasks internally

DO NOT chain multiple tools - each orchestration tool is self-contained and final.
"""

# -------------------------
# LangGraph agent scaffolding
# -------------------------


def _fix_additional_properties(schema: dict) -> None:
    """Recursively remove or set additionalProperties to False in JSON schema."""
    if not isinstance(schema, dict):
        return

    # Set additionalProperties to False (don't remove it completely)
    if "additionalProperties" in schema:
        schema["additionalProperties"] = False

    # Recursively fix nested schemas
    for key, value in schema.items():
        if isinstance(value, dict):
            _fix_additional_properties(value)
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    _fix_additional_properties(item)


class AgentState(TypedDict):
    messages: Annotated[Sequence[BaseMessage], add_messages]
    custom_inputs: Optional[dict[str, Any]]
    custom_outputs: Optional[dict[str, Any]]
    rounds: Optional[int]


def create_tool_calling_agent(
    model: LanguageModelLike,
    tools: Union[ToolNode, Sequence[BaseTool]],
    system_prompt: Optional[str] = None,
) -> Any:
    model = model.bind_tools(tools)

    def should_continue(state: AgentState):
        # Determine whether the agent requested a tool call in the last model message
        try:
            last_message = state["messages"][-1]
        except Exception:
            logger.debug("Agent state has no messages; ending")
            return "end"
        # Check if the model asked for tool calls and log details
        wants_tool = isinstance(last_message, AIMessage) and bool(
            last_message.tool_calls
        )

        # If model requested tool calls, log which tools and arguments
        if wants_tool:
            try:
                for tc in last_message.tool_calls:
                    tname = getattr(
                        tc, "name", tc.get("name") if isinstance(tc, dict) else str(tc)
                    )
                    targs = getattr(
                        tc, "args", tc.get("args") if isinstance(tc, dict) else None
                    )
                    # Show abbreviated args for cleaner output
                    args_preview = (
                        str(targs)[:100] + "..."
                        if len(str(targs)) > 100
                        else str(targs)
                    )
                    logger.info(f"Model requested tool call: {tname} args={targs}")
                    print(f"🔧 [TOOL REQUEST] {tname}({args_preview})")
            except Exception:
                logger.debug("Could not introspect tool_calls for logging")

        # Inspect recent tool outputs for an explicit continue flag
        tool_signaled_continue = False
        signaled_tools = []
        try:
            # Walk messages in reverse to find latest tool outputs
            for msg in reversed(state.get("messages", [])):
                # Extract role & content in a robust way
                role = getattr(msg, "type", None) or getattr(msg, "role", None)
                content = None
                try:
                    content = getattr(msg, "content", None)
                except Exception:
                    content = None

                if (
                    role == "tool"
                    or role == "function_call_output"
                    or (isinstance(role, str) and role.lower() == "tool")
                ):
                    # Try to parse content as JSON to find continue_required
                    if isinstance(content, (dict, list)):
                        parsed = content
                    else:
                        try:
                            parsed = json.loads(content) if content else None
                        except Exception:
                            parsed = None

                    if (
                        isinstance(parsed, dict)
                        and parsed.get("continue_required") is not None
                    ):
                        if parsed.get("continue_required"):
                            tool_signaled_continue = True
                        # collect tool name if present
                        signaled_tools.append(
                            parsed.get("tool_name")
                            or parsed.get("name")
                            or "<unknown_tool>"
                        )
                        break

                    # Also support simple string flags like 'continue_required: true'
                    if isinstance(content, str) and "continue_required" in content:
                        # crude detection
                        if "true" in content.lower():
                            tool_signaled_continue = True
                            signaled_tools.append("<tool_with_string_flag>")
                            break
        except Exception as e:
            logger.debug(f"Error while inspecting tool outputs for continue flag: {e}")

        rounds = state.get("rounds", 0) or 0

        # For first request: allow one tool call
        if wants_tool and rounds == 0:
            state["rounds"] = 1
            logger.info("Agent invoking tool (expecting single-round completion)")
            print("🔄 [AGENT] Model requested tool call")
            return "continue"

        # After first tool execution: always end (single-round expectation)
        if rounds > 0:
            if wants_tool or tool_signaled_continue:
                logger.warning(
                    "Tool requested continuation but single-round agent expects completion"
                )
                print(
                    "⚠️  [AGENT] Tool requested continuation, but single-round mode active - ending"
                )
            else:
                logger.info("Tool completed successfully")
                print("✅ [AGENT COMPLETE] Migration finished successfully")
            return "end"

        # No tool requested initially: end immediately
        logger.debug("Agent not requesting tool calls; ending")
        print("✅ [AGENT COMPLETE] No tool calls requested")
        return "end"

    pre = (
        RunnableLambda(
            lambda s: [{"role": "system", "content": system_prompt}] + s["messages"]
        )
        if system_prompt
        else RunnableLambda(lambda s: s["messages"])
    )
    runnable = pre | model

    def call_model(state: AgentState, config: RunnableConfig):
        logger.debug(f"Calling model with {len(state.get('messages', []))} messages")
        resp = runnable.invoke(state, config)
        logger.debug("Model returned response; forwarding to graph")
        return {"messages": [resp]}

    workflow = StateGraph(AgentState)
    workflow.add_node("agent", RunnableLambda(call_model))
    workflow.add_node("tools", ToolNode(tools))
    workflow.set_entry_point("agent")
    workflow.add_conditional_edges(
        "agent", should_continue, {"continue": "tools", "end": END}
    )
    workflow.add_edge("tools", "agent")
    return workflow.compile()


class LangGraphResponsesAgent(ResponsesAgent):
    def __init__(self, agent):
        self.agent = agent

    def _responses_to_cc(self, message: dict[str, Any]) -> list[dict[str, Any]]:
        t = message.get("type")
        if t == "function_call":
            return [
                {
                    "role": "assistant",
                    "content": "tool call",
                    "tool_calls": [
                        {
                            "id": message["call_id"],
                            "type": "function",
                            "function": {
                                "arguments": message["arguments"],
                                "name": message["name"],
                            },
                        }
                    ],
                }
            ]
        if t == "message" and isinstance(message["content"], list):
            return [
                {"role": message["role"], "content": c["text"]}
                for c in message["content"]
            ]
        if t == "reasoning":
            return [{"role": "assistant", "content": json.dumps(message["summary"])}]
        if t == "function_call_output":
            return [
                {
                    "role": "tool",
                    "content": message["output"],
                    "tool_call_id": message["call_id"],
                }
            ]
        allowed = {
            k: v
            for k, v in message.items()
            if k in ["role", "content", "name", "tool_calls", "tool_call_id"]
        }
        return [allowed] if allowed else []

    def _prep_msgs_for_cc_llm(self, responses_input) -> list[dict[str, Any]]:
        out = []
        for msg in responses_input:
            out.extend(self._responses_to_cc(msg.model_dump()))
        return out

    def _langchain_to_responses(
        self, messages: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        for m in messages:
            m = m.model_dump()
            role = m["type"]
            if role == "ai":
                if tool_calls := m.get("tool_calls"):
                    return [
                        self.create_function_call_item(
                            id=m.get("id"),
                            call_id=tc["id"],
                            name=tc["name"],
                            arguments=json.dumps(tc["args"]),
                        )
                        for tc in tool_calls
                    ]
                return [self.create_text_output_item(text=m["content"], id=m.get("id"))]
            if role == "tool":
                return [
                    self.create_function_call_output_item(
                        call_id=m["tool_call_id"], output=m["content"]
                    )
                ]
            if role == "user":
                return [m]

    def predict(self, request: ResponsesAgentRequest) -> ResponsesAgentResponse:
        outputs = [
            e.item
            for e in self.predict_stream(request)
            if e.type == "response.output_item.done"
        ]
        return ResponsesAgentResponse(
            output=outputs, custom_outputs=getattr(request, "custom_inputs", {})
        )

    def predict_stream(self, request: ResponsesAgentRequest):
        if isinstance(request, dict):
            request = ResponsesAgentRequest(
                input=request.get("input", []),
                custom_inputs=request.get("custom_inputs", {}),
            )

        cc_msgs = []
        for msg in request.input:
            cc_msgs.extend(
                self._responses_to_cc(msg.model_dump())
                if hasattr(msg, "model_dump")
                else [msg]
            )

        for event in self.agent.stream(
            {"messages": cc_msgs}, stream_mode=["updates", "messages"]
        ):
            if event[0] == "updates":
                for node in event[1].values():
                    for item in self._langchain_to_responses(node["messages"]):
                        yield ResponsesAgentStreamEvent(
                            type="response.output_item.done", item=item
                        )
            elif event[0] == "messages":
                try:
                    chunk = event[1][0]
                    if isinstance(chunk, AIMessageChunk) and (content := chunk.content):
                        yield ResponsesAgentStreamEvent(
                            **self.create_text_delta(delta=content, item_id=chunk.id)
                        )
                except Exception as e:
                    logger.warning(f"Streaming error: {e}")


# Boot and (optionally) register
try:
    mlflow.langchain.autolog()
    logger.info("MLflow autolog enabled")
except Exception as e:
    logger.warning(f"MLflow autolog not available: {e}")

agent = create_tool_calling_agent(llm, TOOLS, system_prompt)
AGENT = LangGraphResponsesAgent(agent)

try:
    mlflow.models.set_model(AGENT)
    logger.info("Agent registered with MLflow")
except Exception as e:
    logger.warning(f"Could not register with MLflow: {e}")

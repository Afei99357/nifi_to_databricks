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
llm = ChatDatabricks(endpoint=MODEL_ENDPOINT)
system_prompt = """You are an expert Apache NiFi consultant and Databricks migration specialist with deep knowledge of data engineering patterns.

🧠 YOUR ROLE: You understand NiFi workflows like a senior data engineer and can explain what they do in business terms.

WORKFLOW ANALYSIS APPROACH:
1. **Always start with intelligent analysis** - Use `analyze_nifi_workflow_intelligence` to understand what the workflow actually does
2. **Explain the business purpose** - Tell the user what their workflow accomplishes in plain English
3. **Recommend optimal architecture** - Based on data patterns, suggest the best Databricks approach
4. **Execute migration** - Use available migration tools based on workflow complexity

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
4. 🚀 **EXECUTE MIGRATION**: Use available migration tools:
   - For complex/large workflows: `orchestrate_chunked_nifi_migration`
   - For building migration plans: `build_migration_plan`
   - For processing individual chunks: `process_nifi_chunk`

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

        # Single-round agent: allow one tool call, then always end
        if wants_tool and not state.get("tool_executed"):
            state["tool_executed"] = True
            logger.info("Agent invoking tool (single-round mode)")
            print("🔄 [AGENT] Model requested tool call")
            return "continue"

        # After tool execution or no tool requested: always end
        logger.info("Agent completed successfully")
        print("✅ [AGENT COMPLETE] Migration finished successfully")
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


# Cretae agent object
mlflow.langchain.autolog()
agent = create_tool_calling_agent(llm, TOOLS, system_prompt)
AGENT = LangGraphResponsesAgent(agent)
mlflow.models.set_model(AGENT)

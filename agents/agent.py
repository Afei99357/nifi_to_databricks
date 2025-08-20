# agent.py
import os
import json
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
from mlflow.types.responses import ResponsesAgentRequest, ResponsesAgentResponse, ResponsesAgentStreamEvent

from config import logger, DATABRICKS_HOSTNAME, DATABRICKS_TOKEN, MODEL_ENDPOINT
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
    def _stream(self, messages, stop=None, run_manager=None, **kwargs):
        # Fix tools in the request data that's about to be sent to the API
        if 'tools' in kwargs:
            kwargs = kwargs.copy()  # Don't modify the original
            tools_copy = []
            for tool in kwargs['tools']:
                if isinstance(tool, dict):
                    tool_copy = self._deep_copy_and_fix_tool(tool)
                    tools_copy.append(tool_copy)
                else:
                    tools_copy.append(tool)
            kwargs['tools'] = tools_copy
        
        return super()._stream(messages, stop=stop, run_manager=run_manager, **kwargs)
    
    def _generate(self, messages, stop=None, run_manager=None, **kwargs):
        # Fix tools in the request data for non-streaming generation
        if 'tools' in kwargs:
            kwargs = kwargs.copy()  # Don't modify the original
            tools_copy = []
            for tool in kwargs['tools']:
                if isinstance(tool, dict):
                    tool_copy = self._deep_copy_and_fix_tool(tool)
                    tools_copy.append(tool_copy)
                else:
                    tools_copy.append(tool)
            kwargs['tools'] = tools_copy
        
        return super()._generate(messages, stop=stop, run_manager=run_manager, **kwargs)
    
    def _deep_copy_and_fix_tool(self, tool):
        """Deep copy a tool dict and fix its schema"""
        import copy
        tool_copy = copy.deepcopy(tool)
        if 'function' in tool_copy and 'parameters' in tool_copy['function']:
            _fix_additional_properties(tool_copy['function']['parameters'])
        return tool_copy

llm = FixedChatDatabricks(endpoint=MODEL_ENDPOINT)
system_prompt = """You are an expert in Apache NiFi and Databricks migration.

CRITICAL INSTRUCTION: When a user requests a migration, use the appropriate orchestration tool and STOP:
- For intelligent migration: Use ONLY `orchestrate_intelligent_nifi_migration` - this handles everything internally
- For manual large files: Use ONLY `orchestrate_chunked_nifi_migration` - this is complete by itself  
- For manual small files: Use ONLY `orchestrate_nifi_migration` - this is complete by itself
- DO NOT call additional tools after orchestration tools complete successfully
- DO NOT try to "help" or "continue" the migration - the orchestration tools are comprehensive
- When you see "continue_required": false in a tool result, that means the migration is DONE

Core Migration Patterns:
- Use Auto Loader for GetFile/ListFile
- Use Delta Lake for PutHDFS/PutFile  
- Use Structured Streaming and Databricks Jobs
- Always provide executable PySpark and explain the migration patterns

Handling Large NiFi Files:
- For large NiFi XML files (>50 processors or complex workflows), use `orchestrate_chunked_nifi_migration` instead of `orchestrate_nifi_migration`
- The chunked approach prevents context limit issues by processing NiFi workflows in manageable chunks while preserving graph relationships
- Cross-chunk dependencies are automatically handled in the final Databricks job configuration

Migration Strategy:
1. For intelligent/automatic migration: `orchestrate_intelligent_nifi_migration` (RECOMMENDED - analyzes and chooses best approach)
2. For manual large workflows: `orchestrate_chunked_nifi_migration` 
3. For manual small workflows: `orchestrate_nifi_migration`

Each orchestration tool is COMPLETE and handles:
1. XML parsing and analysis
2. Code generation for all processors
3. Job configuration and dependencies  
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
    max_rounds: int = 10,
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
        wants_tool = isinstance(last_message, AIMessage) and bool(last_message.tool_calls)

        # If model requested tool calls, log which tools and arguments
        if wants_tool:
            try:
                for tc in last_message.tool_calls:
                    tname = getattr(tc, "name", tc.get("name") if isinstance(tc, dict) else str(tc))
                    targs = getattr(tc, "args", tc.get("args") if isinstance(tc, dict) else None)
                    # Show abbreviated args for cleaner output
                    args_preview = str(targs)[:100] + "..." if len(str(targs)) > 100 else str(targs)
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

                if role == "tool" or role == "function_call_output" or (isinstance(role, str) and role.lower() == "tool"):
                    # Try to parse content as JSON to find continue_required
                    if isinstance(content, (dict, list)):
                        parsed = content
                    else:
                        try:
                            parsed = json.loads(content) if content else None
                        except Exception:
                            parsed = None

                    if isinstance(parsed, dict) and parsed.get("continue_required") is not None:
                        if parsed.get("continue_required"):
                            tool_signaled_continue = True
                        # collect tool name if present
                        signaled_tools.append(parsed.get("tool_name") or parsed.get("name") or "<unknown_tool>")
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
        logger.debug(f"Agent continuation check: wants_tool={wants_tool}, tool_signaled_continue={tool_signaled_continue}, rounds={rounds}, max_rounds={max_rounds}")

        # If a tool explicitly requested continuation, honor it (subject to max_rounds)
        if tool_signaled_continue and rounds < max_rounds:
            state["rounds"] = rounds + 1
            logger.info(f"Tool(s) {signaled_tools} requested continuation — invoking tools round {state['rounds']} of {max_rounds}")
            print(f"🔄 [AGENT ROUND {state['rounds']}/{max_rounds}] Tool signaled continue: {signaled_tools}")
            return "continue"

        # Otherwise, continue only when model explicitly requests tool calls and rounds not exceeded
        if wants_tool and rounds < max_rounds:
            state["rounds"] = rounds + 1
            logger.info(f"Agent invoking tool round {state['rounds']} of {max_rounds}")
            print(f"🔄 [AGENT ROUND {state['rounds']}/{max_rounds}] Model requested tool call")
            return "continue"

        # Otherwise end the workflow
        if (wants_tool or tool_signaled_continue) and rounds >= max_rounds:
            logger.warning(f"Max agent-tool rounds reached ({rounds}/{max_rounds}); stopping further tool calls")
            print(f"🛑 [AGENT COMPLETE] Max rounds reached; stopping")
        else:
            logger.debug("Agent not requesting further tool calls; ending")
            print(f"✅ [AGENT COMPLETE] Migration finished successfully")
        return "end"

    pre = RunnableLambda(lambda s: [{"role": "system", "content": system_prompt}] + s["messages"]) if system_prompt \
         else RunnableLambda(lambda s: s["messages"])
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
    workflow.add_conditional_edges("agent", should_continue, {"continue": "tools", "end": END})
    workflow.add_edge("tools", "agent")
    return workflow.compile()

class LangGraphResponsesAgent(ResponsesAgent):
    def __init__(self, agent):
        self.agent = agent

    def _responses_to_cc(self, message: dict[str, Any]) -> list[dict[str, Any]]:
        t = message.get("type")
        if t == "function_call":
            return [{"role": "assistant", "content": "tool call", "tool_calls": [{
                "id": message["call_id"], "type": "function",
                "function": {"arguments": message["arguments"], "name": message["name"]}
            }]}]
        if t == "message" and isinstance(message["content"], list):
            return [{"role": message["role"], "content": c["text"]} for c in message["content"]]
        if t == "reasoning":
            return [{"role": "assistant", "content": json.dumps(message["summary"])}]
        if t == "function_call_output":
            return [{"role": "tool", "content": message["output"], "tool_call_id": message["call_id"]}]
        allowed = {k: v for k, v in message.items() if k in ["role", "content", "name", "tool_calls", "tool_call_id"]}
        return [allowed] if allowed else []

    def _prep_msgs_for_cc_llm(self, responses_input) -> list[dict[str, Any]]:
        out = []
        for msg in responses_input:
            out.extend(self._responses_to_cc(msg.model_dump()))
        return out

    def _langchain_to_responses(self, messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
        for m in messages:
            m = m.model_dump()
            role = m["type"]
            if role == "ai":
                if tool_calls := m.get("tool_calls"):
                    return [self.create_function_call_item(
                        id=m.get("id"), call_id=tc["id"], name=tc["name"], arguments=json.dumps(tc["args"])
                    ) for tc in tool_calls]
                return [self.create_text_output_item(text=m["content"], id=m.get("id"))]
            if role == "tool":
                return [self.create_function_call_output_item(call_id=m["tool_call_id"], output=m["content"])]
            if role == "user":
                return [m]

    def predict(self, request: ResponsesAgentRequest) -> ResponsesAgentResponse:
        outputs = [e.item for e in self.predict_stream(request) if e.type == "response.output_item.done"]
        return ResponsesAgentResponse(output=outputs, custom_outputs=getattr(request, "custom_inputs", {}))

    def predict_stream(self, request: ResponsesAgentRequest):
        if isinstance(request, dict):
            request = ResponsesAgentRequest(input=request.get("input", []), custom_inputs=request.get("custom_inputs", {}))

        cc_msgs = []
        for msg in request.input:
            cc_msgs.extend(self._responses_to_cc(msg.model_dump()) if hasattr(msg, "model_dump") else [msg])

        for event in self.agent.stream({"messages": cc_msgs}, stream_mode=["updates", "messages"]):
            if event[0] == "updates":
                for node in event[1].values():
                    for item in self._langchain_to_responses(node["messages"]):
                        yield ResponsesAgentStreamEvent(type="response.output_item.done", item=item)
            elif event[0] == "messages":
                try:
                    chunk = event[1][0]
                    if isinstance(chunk, AIMessageChunk) and (content := chunk.content):
                        yield ResponsesAgentStreamEvent(**self.create_text_delta(delta=content, item_id=chunk.id))
                except Exception as e:
                    logger.warning(f"Streaming error: {e}")

# Boot and (optionally) register
try:
    mlflow.langchain.autolog()
    logger.info("MLflow autolog enabled")
except Exception as e:
    logger.warning(f"MLflow autolog not available: {e}")

# Allow dynamic configuration of agent max rounds via env var AGENT_MAX_ROUNDS
try:
    _env_max = os.environ.get("AGENT_MAX_ROUNDS")
    if _env_max is not None:
        try:
            _max_rounds = int(_env_max)
        except Exception:
            logger.warning(f"Invalid AGENT_MAX_ROUNDS='{_env_max}', falling back to default=10")
            _max_rounds = 10
    else:
        _max_rounds = 10
except Exception:
    _max_rounds = 10

logger.info(f"Creating agent with max_rounds={_max_rounds}")
agent = create_tool_calling_agent(llm, TOOLS, system_prompt, max_rounds=_max_rounds)
AGENT = LangGraphResponsesAgent(agent)

try:
    mlflow.models.set_model(AGENT)
    logger.info("Agent registered with MLflow")
except Exception as e:
    logger.warning(f"Could not register with MLflow: {e}")
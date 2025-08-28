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


# --- Fixed ChatDatabricks class to handle schema validation ---
class FixedChatDatabricks(ChatDatabricks):
    """Fixed version of ChatDatabricks that properly handles tool schemas."""

    def bind_tools(self, tools, **kwargs):
        """Override to fix additionalProperties in tool schemas."""
        # Get the bound tools from parent
        bound = super().bind_tools(tools, **kwargs)

        # Fix the schema in the bound LLM
        if hasattr(bound, "kwargs") and "tools" in bound.kwargs:
            for tool in bound.kwargs["tools"]:
                if "function" in tool and "parameters" in tool["function"]:
                    params = tool["function"]["parameters"]
                    self._fix_additional_properties(params)

        return bound

    def _fix_additional_properties(self, schema_obj):
        """Recursively fix additionalProperties in schema objects."""
        if not isinstance(schema_obj, dict):
            return

        # Fix additionalProperties at current level
        if "additionalProperties" not in schema_obj:
            schema_obj["additionalProperties"] = False
        elif schema_obj["additionalProperties"] is True:
            schema_obj["additionalProperties"] = False

        # Recursively fix nested properties
        if "properties" in schema_obj:
            for prop_schema in schema_obj["properties"].values():
                self._fix_additional_properties(prop_schema)

        # Fix items in arrays
        if "items" in schema_obj:
            self._fix_additional_properties(schema_obj["items"])


# --- LLM & system prompt ---
llm = FixedChatDatabricks(endpoint=MODEL_ENDPOINT)
system_prompt = """You are an intelligent Apache NiFi migration orchestrator with deep expertise in both NiFi workflows and Databricks architecture patterns.

🎯 YOUR MISSION: Analyze NiFi workflows and intelligently orchestrate complete migrations to Databricks by making informed decisions about which tools to use and when.

🧠 INTELLIGENT DECISION MAKING:
You have access to specialized tools for different aspects of migration. Based on user requests and analysis results, you decide which tools to call and in what sequence to complete the migration.

📋 AVAILABLE TOOLS & WHEN TO USE THEM:

🔍 COMPREHENSIVE ANALYSIS TOOLS:
- `analyze_nifi_workflow_detailed`: **ESSENTIAL FIRST STEP** - Provides complete processor breakdown:
  * Total processors and workflow complexity (LOW/MEDIUM/HIGH)
  * Exact counts: DATA_TRANSFORMATION_PROCESSORS, DATA_MOVEMENT_PROCESSORS, INFRASTRUCTURE_PROCESSORS
  * Percentages and migration complexity assessment
  * Source/sink processor identification for data flow mapping
  * **Use this for all migration requests to get the detailed breakdown the user needs**

- `classify_processor_types`: Detailed processor-by-processor classification with migration recommendations
- `analyze_nifi_workflow_intelligence`: High-level workflow purpose and migration strategy (complementary to detailed analysis)
- `parse_nifi_template`: Parse XML to extract processors, properties, and connections when you need structural details
- `extract_nifi_parameters_and_services`: Extract parameter contexts and controller services for configuration mapping

🔧 SEMANTIC MIGRATION TOOLS:
- `prune_infrastructure_processors`: **STEP 2** - Remove infrastructure-only processors using existing classifications:
  * Input: results from analyze_nifi_workflow_detailed
  * Filters out logging, routing, and infrastructure processors
  * Keeps only data_transformation and data_movement processors
  * Shows reduction statistics (e.g., 58 → 15 processors)

- `detect_data_flow_chains`: **STEP 3** - Detect source→transformation→sink chains:
  * Uses existing connection parsing and topological sorting
  * Groups remaining processors into logical data flow chains
  * Identifies business patterns (file_ingestion, stream_processing, etc.)

- `create_semantic_data_flows`: **STEP 4** - Convert chains into business-meaningful flows:
  * Transforms processor chains into semantic data flow descriptions
  * Recommends Databricks architecture (Jobs, DLT, Structured Streaming)
  * Provides migration blueprint with complexity assessment
  * **This creates the real-world migration approach instead of 1:1 processor mapping**

🚀 MIGRATION ORCHESTRATION TOOLS:
- `orchestrate_chunked_nifi_migration`: For complex workflows with >50 processors. Handles large workflows by breaking them into manageable chunks.
- `build_migration_plan`: Create migration strategy and understand processor dependencies before execution.
- `process_nifi_chunk`: Process specific portions of large workflows that have been chunked.

🧩 CHUNKING & WORKFLOW TOOLS:
- `extract_complete_workflow_map`: Extract complete workflow structure for large workflows.
- `chunk_nifi_xml_by_process_groups`: Break large workflows into chunks by process groups.
- `chunk_large_process_group`: Handle oversized process groups by breaking them down further.
- `reconstruct_full_workflow`: Reconstruct complete workflow from processed chunks.
- `estimate_chunk_size`: Estimate optimal chunk sizes for large workflows.

💻 CODE GENERATION TOOLS:
- `generate_databricks_code`: Generate specific Databricks code for individual processors.
- `get_migration_pattern`: Get human-readable migration patterns for NiFi components.
- `suggest_autoloader_options`: Get Auto Loader suggestions for file-based processors.

🛠 DEPLOYMENT TOOLS:
- `create_job_config`: Create basic Databricks job configurations.
- `create_job_config_from_plan`: Create job configs based on migration plans with proper task dependencies.
- `deploy_and_run_job`: Deploy jobs to Databricks and optionally run them.
- `scaffold_asset_bundle`: Create complete Databricks Asset Bundle project structure.

🎯 DLT PIPELINE TOOLS:
- `generate_dlt_expectations`: Create DLT expectations for data quality rules.
- `generate_dlt_pipeline_config`: Generate Delta Live Tables pipeline configurations.

✅ VALIDATION TOOLS:
- `evaluate_pipeline_outputs`: Compare and validate migration results against original NiFi outputs.

🧭 DECISION FRAMEWORK:
When a user requests migration, you should:
1. **Understand the request**: What does the user want to achieve?
2. **Analyze if needed**: If you don't have workflow information, use analysis tools
3. **Make informed decisions**: Based on analysis results, choose appropriate migration tools
4. **Execute migration**: Call the right tools to complete the migration
5. **Verify completion**: Ensure the migration was successful

💡 NIFI EXPERTISE - You understand these processor patterns:
- GetFile/ListFile = file monitoring and batch ingestion → Auto Loader patterns
- ConsumeKafka/PublishKafka = streaming data → Structured Streaming
- ExecuteStreamCommand = custom logic → Databricks notebooks with shell commands
- RouteOnAttribute = conditional logic → DataFrame filters and branching
- UpdateAttribute = metadata manipulation → DataFrame transformations
- PutHDFS/PutFile = data storage → Delta Lake writes

🎯 ARCHITECTURE MAPPING:
- **File-based workflows** → Databricks Jobs with Auto Loader
- **Real-time streaming** → Structured Streaming pipelines
- **Complex ETL** → Delta Live Tables (DLT) pipelines
- **Simple data movement** → Basic Databricks Jobs
- **Large/complex workflows** → Chunked migration approach

🚨 CRITICAL GUIDELINES:
- Make intelligent decisions based on actual workflow characteristics
- Don't assume - analyze first if you need information
- Choose tools based on workflow complexity and requirements
- Continue until the migration is complete unless explicitly told to stop
- Explain your reasoning for tool choices to build user confidence

EXAMPLE INTELLIGENT BEHAVIOR:
User: "I have a complex NiFi workflow with 75 processors that does real-time data processing. Can you migrate it?"

You: "I'll help you migrate this complex workflow. Let me first analyze it to understand the data patterns and processing requirements, then choose the best migration approach."
[Call analyze_nifi_workflow_intelligence]
[Analyze results: "This shows real-time Kafka processing with complex transformations"]
"Based on the analysis, this is a streaming workflow with high complexity. I'll use the chunked migration approach to handle the 75 processors effectively."
[Call orchestrate_chunked_nifi_migration]
[Complete the migration]

You are an intelligent orchestrator - make decisions, take action, and deliver complete solutions.
"""

# -------------------------
# LangGraph agent scaffolding
# -------------------------


class AgentState(TypedDict):
    messages: Annotated[Sequence[BaseMessage], add_messages]
    custom_inputs: Optional[dict[str, Any]]
    custom_outputs: Optional[dict[str, Any]]
    rounds: Optional[int]


def analyze_tool_results_and_decide_next_action(state: AgentState) -> str:
    """
    Analyze recent tool results and provide guidance for the next action.
    This adds explicit decision-making logic based on tool outputs.
    """
    try:
        # Look for recent tool messages (ToolMessage type)
        recent_tool_outputs = []
        recent_ai_messages = []

        for msg in reversed(state.get("messages", [])[-10:]):  # Last 10 messages
            if hasattr(msg, "type"):
                if msg.type == "tool":
                    recent_tool_outputs.append(
                        {
                            "tool_call_id": getattr(msg, "tool_call_id", "unknown"),
                            "content": getattr(msg, "content", ""),
                            "name": getattr(msg, "name", "unknown_tool"),
                        }
                    )
                elif msg.type == "ai" and hasattr(msg, "tool_calls") and msg.tool_calls:
                    # Get the tool names that were just called
                    for tc in msg.tool_calls:
                        tool_name = (
                            tc.get("name")
                            if isinstance(tc, dict)
                            else getattr(tc, "name", "unknown")
                        )
                        recent_ai_messages.append(tool_name)

        if not recent_tool_outputs:
            return "No recent tool outputs to analyze."

        # Analyze the most recent tool output for decision making
        latest_output = recent_tool_outputs[0]
        tool_content = latest_output.get("content", "")

        # Decision logic based on tool results
        decision_guidance = []

        # Check if workflow analysis was completed
        if "analyze_nifi_workflow_intelligence" in recent_ai_messages:
            # Look for migration strategy analysis results
            if (
                "migration_analysis" in tool_content.lower()
                or "workflow_metadata" in tool_content.lower()
            ):
                processor_count = 0
                complexity = "Unknown"
                architecture_rec = "Unknown"

                try:
                    # Try to extract key strategy information
                    import re

                    # Extract processor count
                    match = re.search(r'"total_processors":\s*(\d+)', tool_content)
                    if match:
                        processor_count = int(match.group(1))

                    # Extract complexity
                    complexity_match = re.search(
                        r'"complexity[^"]*":\s*"([^"]+)"', tool_content
                    )
                    if complexity_match:
                        complexity = complexity_match.group(1)

                    # Extract recommended architecture
                    arch_match = re.search(
                        r'"recommended_architecture":\s*"([^"]+)"', tool_content
                    )
                    if arch_match:
                        architecture_rec = arch_match.group(1)

                except:
                    pass

                decision_guidance.append(
                    f"🎯 MIGRATION STRATEGY: {processor_count} processors, {complexity} complexity"
                )
                decision_guidance.append(f"🏗️ RECOMMENDED: {architecture_rec}")

                # Decision based on complexity and processor count
                if processor_count > 50 or complexity == "High":
                    decision_guidance.append(
                        "✨ NEXT: Use orchestrate_chunked_nifi_migration for complex workflow"
                    )
                else:
                    decision_guidance.append(
                        "✨ NEXT: Execute migration based on recommended architecture"
                    )

        # Check if migration plan was built
        elif "build_migration_plan" in recent_ai_messages:
            if "processors" in tool_content.lower():
                decision_guidance.append(
                    "🎯 DECISION: Migration plan ready - proceed with orchestrate_chunked_nifi_migration"
                )

        # Check if migration was completed
        elif "orchestrate_chunked_nifi_migration" in recent_ai_messages:
            # Check for clear completion indicators
            migration_success_indicators = [
                'continue_required": false',
                'deployment_success": true',
                "final_job_config_path",
                "migration complete",
                "successfully",
            ]

            if any(
                indicator in tool_content.lower()
                for indicator in migration_success_indicators
            ):
                decision_guidance.append(
                    "✅ DECISION: Migration completed successfully"
                )
                decision_guidance.append(
                    "🎉 NEXT: Migration finished - no further action needed"
                )
            else:
                decision_guidance.append(
                    "⚠️ DECISION: Check migration status - may need retry or different approach"
                )

        return (
            "\n".join(decision_guidance)
            if decision_guidance
            else "Continue with LLM's natural reasoning."
        )

    except Exception as e:
        logger.debug(f"Error analyzing tool results: {e}")
        return "Error in decision analysis - continue with LLM reasoning."


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

        # Multi-round intelligent agent: continue as long as tools are needed
        if wants_tool:
            rounds = state.get("rounds", 0) or 0
            state["rounds"] = rounds + 1
            logger.info(f"Agent invoking tool (round {rounds + 1})")
            print(f"🔄 [AGENT ROUND {rounds + 1}] Model requested tool call")
            return "continue"

        # Before ending, analyze recent tool results and provide decision guidance
        # This helps the agent make better decisions in multi-step workflows
        rounds = state.get("rounds", 0) or 0
        if rounds > 0:  # Only analyze if we've made tool calls
            decision_guidance = analyze_tool_results_and_decide_next_action(state)
            if (
                decision_guidance
                and "Continue with LLM's natural reasoning" not in decision_guidance
            ):
                logger.info(f"Decision analysis: {decision_guidance}")
                print(f"🧠 [DECISION ANALYSIS] {decision_guidance}")

                # Add decision guidance to state for LLM to consider
                state["decision_guidance"] = decision_guidance

                # Check if migration is complete
                if "Migration finished - no further action needed" in decision_guidance:
                    logger.info("Decision analysis indicates migration is complete")
                    print("🎉 [DECISION] Migration complete - ending agent")
                elif (
                    "NEXT:" in decision_guidance
                    and "complete" not in decision_guidance.lower()
                ):
                    logger.info(
                        "Decision analysis suggests more work - allowing LLM to continue"
                    )
                    print(
                        "🔄 [DECISION] Analysis suggests continuing - awaiting LLM decision"
                    )
                    # Don't force continuation, let LLM decide based on the guidance

        # No more tools needed: agent has completed its work
        logger.info(f"Agent completed successfully after {rounds} rounds")
        print(
            f"✅ [AGENT COMPLETE] Migration finished successfully after {rounds} rounds"
        )
        return "end"

    def inject_system_prompt_with_guidance(state):
        messages = []

        # Add system prompt
        if system_prompt:
            enhanced_system_prompt = system_prompt

            # Add decision guidance if available
            if state.get("decision_guidance"):
                enhanced_system_prompt += f"\n\n🧠 DECISION GUIDANCE FROM PREVIOUS ANALYSIS:\n{state['decision_guidance']}\n\nConsider this analysis when deciding your next action."

            messages.append({"role": "system", "content": enhanced_system_prompt})

        # Add conversation messages
        messages.extend(state["messages"])
        return messages

    pre = RunnableLambda(inject_system_prompt_with_guidance)
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

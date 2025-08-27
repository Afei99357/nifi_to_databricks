"""
Utility functions for handling MLflow agent responses
"""

import json
import os
from datetime import datetime
from typing import Any, Dict, List

from utils.workflow_summary import print_workflow_summary_from_data


def save_agent_summary_to_markdown(response, output_path: str = None) -> str:
    """
    Extract and save the formatted NiFi analysis summary to a markdown file.

    Args:
        response: MLflow agent response object
        output_path: Optional path to save markdown. If None, auto-generates filename.

    Returns:
        str: Path to saved markdown file
    """
    # Auto-generate filename if not provided
    if output_path is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = f"agent_analysis_summary_{timestamp}.md"

    # Extract formatted summary from response
    formatted_summary = None
    workflow_filename = "Unknown Workflow"

    for output in response.output:
        if hasattr(output, "output") and output.type == "function_call_output":
            try:
                content = output.output
                # Look for formatted summary in the output
                if "🔍 NIFI WORKFLOW ANALYSIS SUMMARY" in content:
                    start_marker = "🔍 NIFI WORKFLOW ANALYSIS SUMMARY"
                    end_marker = (
                        "============================================================"
                    )

                    start_idx = content.find(start_marker)
                    if start_idx != -1:
                        # Find the end of the summary section
                        end_idx = content.find(
                            end_marker, start_idx + len(start_marker)
                        )
                        if end_idx != -1:
                            end_idx += len(end_marker)
                            formatted_summary = content[start_idx:end_idx]
                        else:
                            formatted_summary = content[start_idx:]
                        break
            except Exception:
                continue

    if formatted_summary is None:
        print("⚠️ No formatted NiFi analysis summary found in response")
        return None

    # Create directory if needed
    os.makedirs(
        os.path.dirname(output_path) if os.path.dirname(output_path) else ".",
        exist_ok=True,
    )

    # Save to markdown file
    with open(output_path, "w") as f:
        f.write("# NiFi Workflow Analysis - Agent Summary\n\n")
        f.write(
            "**Analysis Date:** " + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "\n"
        )
        f.write("**Type:** LLM Agent Intelligence Analysis\n\n")
        f.write("## Formatted Analysis Summary\n\n")
        f.write("```\n")
        f.write(formatted_summary)
        f.write("\n```\n")
        f.write("\n\n---\n")
        f.write(
            "*Formatted summary extracted from NiFi to Databricks Migration Tool - LLM Agent Analysis*\n"
        )

    print(f"📄 Formatted analysis summary saved to: {os.path.abspath(output_path)}")
    return output_path


def display_agent_response(response) -> None:
    """
    Display MLflow agent response in a clean, readable format.
    """
    print("🤖 AGENT RESPONSE SUMMARY")
    print("=" * 50)

    # First, show all tools that were called
    tools_called = []
    for output in response.output:
        if hasattr(output, "name") and output.type == "function_call":
            tools_called.append(output.name)

    if tools_called:
        print(f"\n🔧 TOOLS CALLED: {', '.join(tools_called)}")
        print("-" * 30)

    for i, output in enumerate(response.output):
        print(f"\n📋 Output {i+1}: {output.type}")

        if hasattr(output, "output") and output.type == "function_call_output":
            # Tool result
            try:
                tool_result = json.loads(output.output)
                if "processors_analysis" in tool_result:
                    # This is a workflow intelligence result - show summary directly
                    print("🔍 NIFI WORKFLOW INTELLIGENCE DETECTED")
                    print(
                        f"📊 Total Processors Analyzed: {tool_result.get('total_processors', 'Unknown')}"
                    )
                    print("\n")
                    # Process the JSON data directly without temp files
                    print_workflow_summary_from_data(tool_result)

                else:
                    print(f"📊 Tool Result: {json.dumps(tool_result, indent=2)}")
            except:
                print(f"📊 Raw Output: {output.output}")

        elif hasattr(output, "content") and output.type == "message":
            # Agent message
            print("💬 Agent Message:")
            for content in output.content:
                if isinstance(content, dict) and content.get("type") == "output_text":
                    text = content.get("text", "")
                    # Truncate long text
                    if len(text) > 500:
                        text = text[:500] + "..."
                    print(f"   {text}")

        elif hasattr(output, "name") and output.type == "function_call":
            # Function call
            print(f"🔧 Function Call: {output.name}")
            if hasattr(output, "arguments"):
                try:
                    args = json.loads(output.arguments)
                    print(f"   Arguments: {args}")
                except:
                    print(f"   Arguments: {output.arguments}")

    print("=" * 50)

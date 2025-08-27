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
    Capture and save the complete display_agent_response output to a markdown file.

    Args:
        response: MLflow agent response object
        output_path: Optional path to save markdown. If None, auto-generates filename.

    Returns:
        str: Path to saved markdown file
    """
    # Auto-generate filename if not provided
    if output_path is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = f"agent_response_summary_{timestamp}.md"

    # Create directory if needed
    os.makedirs(
        os.path.dirname(output_path) if os.path.dirname(output_path) else ".",
        exist_ok=True,
    )

    # Capture all the output that display_agent_response() would generate
    import io
    import sys
    from contextlib import redirect_stdout

    # Capture the printed output from display_agent_response
    captured_output = io.StringIO()
    with redirect_stdout(captured_output):
        display_agent_response(response)

    full_output = captured_output.getvalue()

    # Save to markdown file
    with open(output_path, "w") as f:
        f.write("# NiFi Workflow Analysis - Agent Response Summary\n\n")
        f.write(
            "**Analysis Date:** " + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "\n"
        )
        f.write("**Type:** Complete Agent Response Output\n\n")
        f.write("## Agent Response Details\n\n")
        f.write("```\n")
        f.write(full_output)
        f.write("\n```\n")
        f.write("\n\n---\n")
        f.write(
            "*Complete agent response output captured from NiFi to Databricks Migration Tool*\n"
        )

    print(f"📄 Complete agent response saved to: {os.path.abspath(output_path)}")
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

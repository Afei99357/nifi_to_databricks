"""
Utility functions for handling MLflow agent responses
"""

import json
import os
from datetime import datetime
from typing import Any, Dict, List

from utils.workflow_summary import print_workflow_summary_from_data


def save_agent_response_to_json(response, output_path: str = None) -> str:
    """
    Convert MLflow agent response to a clean JSON file.

    Args:
        response: MLflow agent response object
        output_path: Optional path to save JSON. If None, auto-generates filename.

    Returns:
        str: Path to saved JSON file
    """
    # Auto-generate filename if not provided
    if output_path is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = f"agent_response_{timestamp}.json"

    outputs: List[Dict[str, Any]] = []
    result = {
        "timestamp": datetime.now().isoformat(),
        "response_summary": {
            "total_outputs": len(response.output),
            "output_types": [output.type for output in response.output],
        },
        "outputs": outputs,
    }

    # Extract each output
    for i, output in enumerate(response.output):
        output_data = {
            "index": i,
            "type": output.type,
        }

        if hasattr(output, "output") and output.type == "function_call_output":
            # Tool result - parse JSON if possible
            try:
                tool_result = json.loads(output.output)
                output_data["tool_result"] = tool_result
                output_data["raw_output"] = output.output
            except:
                output_data["raw_output"] = output.output

        elif hasattr(output, "content") and output.type == "message":
            # Agent message - extract text
            output_data["content"] = []
            for content in output.content:
                if isinstance(content, dict) and content.get("type") == "output_text":
                    output_data["content"].append(
                        {"type": "text", "text": content.get("text", "")}
                    )
                else:
                    output_data["content"].append(content)

        elif hasattr(output, "name") and output.type == "function_call":
            # Function call info
            output_data["function_name"] = output.name
            if hasattr(output, "arguments"):
                try:
                    output_data["arguments"] = json.loads(output.arguments)
                except:
                    output_data["arguments"] = output.arguments

        outputs.append(output_data)

    # Save to JSON file
    os.makedirs(
        os.path.dirname(output_path) if os.path.dirname(output_path) else ".",
        exist_ok=True,
    )
    with open(output_path, "w") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    print(f"✅ Agent response saved to: {os.path.abspath(output_path)}")

    # Also print the JSON content in the logs
    print("📄 JSON Content:")
    print("=" * 40)
    print(json.dumps(result, indent=2, ensure_ascii=False))
    print("=" * 40)

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

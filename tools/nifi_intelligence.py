# tools/nifi_intelligence.py
# NiFi workflow intelligence tools for agent use

import json
import os
import xml.etree.ElementTree as ET
from typing import Any, Dict, List

from databricks_langchain import ChatDatabricks
from langchain_core.tools import tool

# Import utility functions
from utils.nifi_analysis_utils import analyze_processors_batch


@tool
def analyze_nifi_workflow_intelligence(xml_content: str) -> str:
    """
    Analyze a NiFi workflow and provide high-level migration strategy recommendations.

    This function acts as a migration strategist, focusing on:
    - Understanding the overall business purpose of the workflow
    - Identifying data flow patterns (batch, streaming, hybrid)
    - Recommending optimal Databricks architecture patterns
    - Providing concrete migration tasks and implementation approach

    Args:
        xml_content: NiFi XML template content OR file path to NiFi XML file

    Returns:
        JSON with business purpose analysis, architecture recommendations, and migration strategy
    """
    try:
        # Check if xml_content is a file path
        if xml_content.startswith("/") and xml_content.endswith(".xml"):
            try:
                with open(xml_content, "r") as f:
                    xml_content = f.read()
            except Exception as e:
                return json.dumps(
                    {"error": f"Failed to read file {xml_content}: {str(e)}"}
                )

        if not xml_content.strip():
            return json.dumps({"error": "Empty XML content provided"})

        root = ET.fromstring(xml_content)

        # Extract high-level workflow characteristics for strategy analysis
        processors = root.findall(".//processors")
        connections = root.findall(".//connections")

        # Count processor types to understand workflow patterns
        processor_types = {}
        data_sources = []
        data_sinks = []
        processing_patterns = []

        for processor in processors:
            proc_type = (processor.findtext("type") or "").strip()
            proc_name = (processor.findtext("name") or "Unknown").strip()

            # Count processor types
            processor_types[proc_type] = processor_types.get(proc_type, 0) + 1

            # Identify data sources
            if proc_type in [
                "GetFile",
                "ListFile",
                "ConsumeKafka",
                "GetHTTP",
                "GenerateFlowFile",
            ]:
                data_sources.append({"type": proc_type, "name": proc_name})

            # Identify data sinks
            elif proc_type in [
                "PutFile",
                "PutHDFS",
                "PublishKafka",
                "PutSQL",
                "PutSFTP",
            ]:
                data_sinks.append({"type": proc_type, "name": proc_name})

            # Identify key processing patterns
            elif proc_type in [
                "ExecuteStreamCommand",
                "ExecuteSQL",
                "RouteOnAttribute",
                "UpdateAttribute",
            ]:
                processing_patterns.append({"type": proc_type, "name": proc_name})

        total_processors = len(processors)

        # Determine workflow complexity
        complexity = "Low"
        if total_processors > 50:
            complexity = "High"
        elif total_processors > 20:
            complexity = "Medium"

        # Use LLM to analyze workflow patterns and provide migration strategy
        model_endpoint = os.getenv(
            "MODEL_ENDPOINT", "databricks-meta-llama-3-3-70b-instruct"
        )
        llm = ChatDatabricks(endpoint=model_endpoint, temperature=0.1)

        analysis_prompt = f"""
You are a NiFi-to-Databricks migration strategist. Analyze this workflow and provide high-level migration recommendations.

WORKFLOW OVERVIEW:
- Total Processors: {total_processors}
- Complexity: {complexity}
- Data Sources: {data_sources}
- Data Sinks: {data_sinks}
- Key Processing: {processing_patterns}
- Most Common Processor Types: {dict(sorted(processor_types.items(), key=lambda x: x[1], reverse=True)[:5])}

Provide your analysis in this JSON format:
{{
  "business_purpose": "What this workflow does in business terms (1-2 sentences)",
  "data_flow_pattern": "batch|streaming|hybrid",
  "complexity_assessment": "{complexity}",
  "recommended_architecture": "Best Databricks approach (Jobs|DLT|Structured Streaming|Hybrid)",
  "migration_strategy": {{
    "approach": "High-level migration approach",
    "key_components": ["List of main Databricks components needed"],
    "implementation_tasks": [
      "Task 1: High-level implementation step",
      "Task 2: Another key step",
      "Task 3: Final step"
    ]
  }},
  "estimated_effort": "Low|Medium|High",
  "key_considerations": ["Important factors for migration success"]
}}

Focus on STRATEGY and ARCHITECTURE, not individual processor conversion.
"""

        response = llm.invoke(analysis_prompt)
        strategy_analysis = response.content

        # Try to parse as JSON, fallback to text if needed
        try:
            strategy_json = json.loads(strategy_analysis)
        except:
            strategy_json = {
                "business_purpose": "Analysis pending - see raw response",
                "raw_llm_response": strategy_analysis,
            }

        # Add workflow metadata
        result = {
            "workflow_metadata": {
                "total_processors": total_processors,
                "complexity": complexity,
                "data_sources_count": len(data_sources),
                "data_sinks_count": len(data_sinks),
                "processing_patterns_count": len(processing_patterns),
            },
            "migration_analysis": strategy_json,
            "analysis_timestamp": "generated_by_migration_strategist",
        }

        return json.dumps(result, indent=2)

    except Exception as e:
        return json.dumps({"error": f"Migration strategy analysis failed: {str(e)}"})

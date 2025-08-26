# tools/nifi_intelligence.py
# Intelligent NiFi workflow analysis and understanding system

import json
import os
import xml.etree.ElementTree as ET
from typing import Any, Dict, List, Optional

from databricks_langchain import ChatDatabricks
from langchain_core.tools import tool

# Removed hardcoded knowledge base - using pure LLM intelligence instead


def analyze_processor_properties(
    processor_type: str, properties: Dict[str, Any]
) -> Dict[str, Any]:
    """Use pure LLM intelligence to analyze what this processor actually does with data."""

    model_endpoint = os.environ.get(
        "MODEL_ENDPOINT", "databricks-meta-llama-3-3-70b-instruct"
    )

    try:
        llm = ChatDatabricks(
            endpoint=model_endpoint, temperature=0.1
        )  # Low temp for consistent analysis

        analysis_prompt = f"""You are a NiFi data engineering expert. Analyze this processor to understand what it ACTUALLY does with data.

PROCESSOR TYPE: {processor_type}
CONFIGURATION: {json.dumps(properties, indent=2)}

Focus on DATA MANIPULATION vs INFRASTRUCTURE:
- Does this processor TRANSFORM the actual data content? (change structure, extract fields, convert formats)
- Does this processor just MOVE data from A to B? (file ingestion, storage, network transfer)
- Does this processor do INFRASTRUCTURE work? (logging, routing, delays, authentication)

Return ONLY a JSON object:
{{
  "data_manipulation_type": "data_transformation | data_movement | infrastructure_only | external_processing",
  "actual_data_processing": "Detailed description of what happens to the data content",
  "transforms_data_content": true/false,
  "business_purpose": "What this accomplishes in business terms",
  "data_impact_level": "high | medium | low | none",
  "key_operations": ["list", "of", "key", "operations"]
}}

Examples:
- GetFile: moves files, data_movement, no content transformation
- EvaluateJsonPath: extracts JSON fields, data_transformation, changes data structure
- LogMessage: pure logging, infrastructure_only, no data impact
- UpdateAttribute: metadata only, infrastructure_only, doesn't change core data
- ExecuteStreamCommand with SQL: transforms data, external_processing, high impact
- RouteOnAttribute: routes data, infrastructure_only, no content change

Be specific about what happens to the actual data content, not just metadata or routing."""

        response = llm.invoke(analysis_prompt)

        # Parse LLM response
        try:
            analysis_result = json.loads(response.content.strip())
        except json.JSONDecodeError:
            # Try to extract JSON from response if it's wrapped in text
            content = response.content.strip()
            if "{" in content and "}" in content:
                start = content.find("{")
                end = content.rfind("}") + 1
                analysis_result = json.loads(content[start:end])
            else:
                raise ValueError("Could not parse JSON from LLM response")

        # Add processor info and metadata
        analysis_result.update(
            {
                "processor_type": processor_type,
                "properties": properties,
                "analysis_method": "llm_intelligent",
            }
        )

        return analysis_result

    except Exception as e:
        # Fallback to basic analysis if LLM fails
        return {
            "processor_type": processor_type,
            "properties": properties,
            "data_manipulation_type": "unknown",
            "actual_data_processing": f"LLM analysis failed: {str(e)}. Manual analysis needed.",
            "transforms_data_content": False,
            "business_purpose": f"Unknown processor: {processor_type}",
            "data_impact_level": "unknown",
            "key_operations": ["analysis_failed"],
            "analysis_method": "fallback_basic",
            "error": str(e),
        }


@tool
def analyze_nifi_workflow_intelligence(xml_content: str) -> str:
    """
    Intelligently analyze a NiFi workflow to understand its real-world purpose and data patterns.

    This function acts as a NiFi expert, analyzing workflows to understand:
    - What the workflow actually does in business terms
    - Data flow patterns and characteristics
    - Processing intent and architecture patterns
    - Optimal Databricks migration strategy

    Args:
        xml_content: NiFi XML template content

    Returns:
        JSON with comprehensive workflow intelligence analysis
    """
    try:
        root = ET.fromstring(xml_content)

        # Extract all processors with their properties
        processors_analysis = []
        connections_analysis = []

        # Analyze processors
        for processor in root.findall(".//processors"):
            proc_id = (processor.findtext("id") or "").strip()
            proc_name = (processor.findtext("name") or "Unknown").strip()
            proc_type = (processor.findtext("type") or "").strip()

            # Extract properties
            properties = {}
            config = processor.find("config")
            if config is not None:
                props_elem = config.find("properties")
                if props_elem is not None:
                    for entry in props_elem.findall("entry"):
                        key_elem = entry.find("key")
                        value_elem = entry.find("value")
                        if key_elem is not None and value_elem is not None:
                            key = key_elem.text or ""
                            value = value_elem.text or ""
                            if value:  # Only store non-empty values
                                properties[key] = value

            # Deep analysis of this processor
            processor_analysis = analyze_processor_properties(proc_type, properties)
            processor_analysis.update(
                {"id": proc_id, "name": proc_name, "properties": properties}
            )

            processors_analysis.append(processor_analysis)

        # Analyze connections to understand data flow
        for connection in root.findall(".//connections"):
            src_id = (connection.findtext(".//source/id") or "").strip()
            dst_id = (connection.findtext(".//destination/id") or "").strip()
            relationship = (connection.findtext("selectedRelationships") or "").strip()

            connections_analysis.append(
                {
                    "source_id": src_id,
                    "destination_id": dst_id,
                    "relationship": relationship,
                }
            )

        # Build workflow understanding
        workflow_intelligence = analyze_workflow_patterns(
            processors_analysis, connections_analysis
        )

        result = {
            "workflow_intelligence": workflow_intelligence,
            "processors_analysis": processors_analysis,
            "connections_analysis": connections_analysis,
            "total_processors": len(processors_analysis),
            "analysis_timestamp": "generated_by_nifi_intelligence_engine",
        }

        return json.dumps(result, indent=2)

    except Exception as e:
        return json.dumps({"error": f"Workflow intelligence analysis failed: {str(e)}"})


def analyze_workflow_patterns(
    processors: List[Dict], connections: List[Dict]
) -> Dict[str, Any]:
    """Use LLM to analyze the overall workflow patterns and business purpose."""

    model_endpoint = os.environ.get(
        "MODEL_ENDPOINT", "databricks-meta-llama-3-3-70b-instruct"
    )

    try:
        llm = ChatDatabricks(endpoint=model_endpoint, temperature=0.1)

        # Prepare workflow summary for LLM
        processor_summary = []
        for proc in processors:
            proc_class = (
                proc.get("processor_type", "").split(".")[-1]
                if "." in proc.get("processor_type", "")
                else proc.get("processor_type", "Unknown")
            )
            data_manipulation = proc.get("data_manipulation_type", "unknown")
            transforms_data = proc.get("transforms_data_content", False)
            business_purpose = proc.get("business_purpose", "unknown")

            processor_summary.append(
                {
                    "name": proc.get("name", "unnamed"),
                    "type": proc_class,
                    "data_manipulation_type": data_manipulation,
                    "transforms_data_content": transforms_data,
                    "business_purpose": business_purpose,
                }
            )

        # Analyze data flow connections
        data_flow = []
        for conn in connections:
            src_proc = next(
                (p for p in processors if p.get("id") == conn["source_id"]), None
            )
            dst_proc = next(
                (p for p in processors if p.get("id") == conn["destination_id"]), None
            )

            if src_proc and dst_proc:
                data_flow.append(
                    {
                        "from": src_proc.get("name", "unknown"),
                        "to": dst_proc.get("name", "unknown"),
                        "relationship": conn.get("relationship", "unknown"),
                    }
                )

        workflow_prompt = f"""You are a data engineering expert analyzing a complete NiFi workflow to understand what it ACTUALLY accomplishes with data.

PROCESSORS IN WORKFLOW:
{json.dumps(processor_summary, indent=2)}

DATA FLOW CONNECTIONS:
{json.dumps(data_flow, indent=2)}

Analyze this workflow focusing on:
1. What is the REAL business purpose? (not just "processes data")
2. Which processors actually TRANSFORM data vs just move/route it?
3. What is the end-to-end data journey?
4. Is this primarily data transformation, data movement, or infrastructure?

Return ONLY a JSON object:
{{
  "business_purpose": "Specific description of what this workflow accomplishes in business terms",
  "data_transformation_summary": "What actual data transformations happen (if any)",
  "infrastructure_vs_processing": "infrastructure_heavy | processing_heavy | balanced",
  "core_data_processors": ["list", "of", "processors", "that", "actually", "transform", "data"],
  "infrastructure_processors": ["list", "of", "processors", "that", "are", "just", "infrastructure"],
  "data_flow_pattern": "simple_transfer | complex_etl | error_handling_heavy | streaming_pipeline | batch_processing",
  "workflow_complexity": "simple | moderate | complex",
  "key_insights": ["insight1", "insight2", "insight3"]
}}

Focus on distinguishing between:
- Processors that change data content (EvaluateJsonPath, ConvertRecord, ExecuteStreamCommand with SQL)
- Processors that just move data (GetFile, PutHDFS, Kafka producers/consumers)
- Processors that are pure infrastructure (LogMessage, UpdateAttribute, RouteOnAttribute, ControlRate)

Be specific about what the workflow actually does for the business, not generic descriptions."""

        response = llm.invoke(workflow_prompt)

        # Parse LLM response
        try:
            workflow_analysis = json.loads(response.content.strip())
        except json.JSONDecodeError:
            # Try to extract JSON from response
            content = response.content.strip()
            if "{" in content and "}" in content:
                start = content.find("{")
                end = content.rfind("}") + 1
                workflow_analysis = json.loads(content[start:end])
            else:
                raise ValueError("Could not parse JSON from LLM response")

        # Add metadata
        workflow_analysis.update(
            {
                "total_processors": len(processors),
                "total_connections": len(connections),
                "analysis_method": "llm_intelligent",
            }
        )

        return workflow_analysis

    except Exception as e:
        # Fallback analysis
        return {
            "business_purpose": f"LLM workflow analysis failed: {str(e)}. Manual analysis needed.",
            "data_transformation_summary": "Analysis failed",
            "infrastructure_vs_processing": "unknown",
            "core_data_processors": [],
            "infrastructure_processors": [],
            "data_flow_pattern": "unknown",
            "workflow_complexity": "unknown",
            "key_insights": ["LLM analysis failed"],
            "total_processors": len(processors),
            "total_connections": len(connections),
            "analysis_method": "fallback_basic",
            "error": str(e),
        }


# All hardcoded analysis functions removed - will be replaced with pure LLM intelligence

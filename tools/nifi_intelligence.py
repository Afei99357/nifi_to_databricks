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
    Intelligently analyze a NiFi workflow to understand its real-world purpose and data patterns.

    This function acts as a NiFi expert, analyzing workflows to understand:
    - What the workflow actually does in business terms
    - Data flow patterns and characteristics
    - Processing intent and architecture patterns
    - Optimal Databricks migration strategy

    Args:
        xml_content: NiFi XML template content OR file path to NiFi XML file

    Returns:
        JSON with comprehensive workflow intelligence analysis
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

        # If it looks like a file path but doesn't exist, return error
        elif xml_content.startswith("/") or xml_content.endswith(".xml"):
            return json.dumps(
                {
                    "error": f"File path appears invalid or file doesn't exist: {xml_content}"
                }
            )

        if not xml_content.strip():
            return json.dumps({"error": "Empty XML content provided"})

        root = ET.fromstring(xml_content)

        # Extract all processors with their properties (no individual analysis yet)
        processors_data = []
        connections_analysis = []

        # Extract processor data for batch analysis
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

            processors_data.append(
                {
                    "id": proc_id,
                    "name": proc_name,
                    "processor_type": proc_type,
                    "properties": properties,
                }
            )

        # 🚀 BATCH ANALYSIS - Analyze ALL processors in a single LLM call!
        processors_analysis = analyze_processors_batch(processors_data)

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

        result = {
            "processors_analysis": processors_analysis,
            "connections_analysis": connections_analysis,
            "total_processors": len(processors_analysis),
            "analysis_timestamp": "generated_by_nifi_intelligence_engine",
        }

        return json.dumps(result, indent=2)

    except Exception as e:
        return json.dumps({"error": f"Workflow intelligence analysis failed: {str(e)}"})

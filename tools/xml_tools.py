# tools/xml_tools.py
# XML parsing tools exposed to the agent.

from __future__ import annotations

import json
import xml.etree.ElementTree as ET
from typing import Any, Dict, List

# Removed langchain_core.tools import - no longer using # Removed @tool decorator - direct function call approach decorator

__all__ = [
    "parse_nifi_template",
]


# Removed @tool decorator - direct function call approach
def parse_nifi_template(xml_content: str) -> str:
    """
    Parse a NiFi XML template and extract processors, properties, and connections.
    Returns a JSON string:
      {
        "processors": [{"name": "...", "type": "...", "properties": {...}}, ...],
        "connections": [{"source": "...", "destination": "...", "relationships": [...]}, ...],
        "processor_count": N,
        "connection_count": M
      }
    """
    try:
        root = ET.fromstring(xml_content)

        processors: List[Dict[str, Any]] = []
        connections: List[Dict[str, Any]] = []

        # processors
        for processor in root.findall(".//processors"):
            proc_info = {
                "name": (processor.findtext("name") or "Unknown").strip(),
                "type": (processor.findtext("type") or "Unknown").strip(),
                "properties": {},
            }

            props_node = processor.find(".//properties")
            if props_node is not None:
                for entry in props_node.findall("entry"):
                    k = entry.findtext("key")
                    v = entry.findtext("value")
                    if k is not None:
                        proc_info["properties"][k] = v

            processors.append(proc_info)

        # connections
        for connection in root.findall(".//connections"):
            source = connection.findtext(".//source/id")
            destination = connection.findtext(".//destination/id")
            rels = [
                (rel.text or "").strip()
                for rel in connection.findall(".//selectedRelationships")
                if rel is not None and rel.text
            ]
            conn_info = {
                "source": (source or "Unknown").strip(),
                "destination": (destination or "Unknown").strip(),
                "relationships": rels,
            }
            connections.append(conn_info)

        result = {
            "processors": processors,
            "connections": connections,
            "processor_count": len(processors),
            "connection_count": len(connections),
            "continue_required": False,
            "tool_name": "parse_nifi_template",
        }
        return json.dumps(result, indent=2)
    except ET.ParseError as e:
        return f"Error parsing XML: {str(e)}"
    except Exception as e:
        return f"Unexpected error: {str(e)}"


# Removed @tool decorator - direct function call approach

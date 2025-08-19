# tools/xml_tools.py
# XML parsing tools exposed to the agent.

from __future__ import annotations

import json
import xml.etree.ElementTree as ET
from typing import Any, Dict, List

from langchain_core.tools import tool

__all__ = ["parse_nifi_template", "extract_nifi_parameters_and_services"]


@tool
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
        }
        return json.dumps(result, indent=2)
    except ET.ParseError as e:
        return f"Error parsing XML: {str(e)}"
    except Exception as e:
        return f"Unexpected error: {str(e)}"


@tool
def extract_nifi_parameters_and_services(xml_content: str) -> str:
    """
    Return NiFi Parameter Contexts and Controller Services with suggested Databricks mappings.
    Returns a JSON string:
      {
        "parameter_contexts": [{"name": "...", "parameters":[{"name": "...","value":"...","sensitive":bool}, ...]}],
        "controller_services": [{"id":"...","name":"...","type":"...","properties": {...}}, ...],
        "suggested_mappings": [{"nifi":"...","databricks_equivalent":"...","how":"..."}]
      }
    """
    try:
        root = ET.fromstring(xml_content)
        out = {"parameter_contexts": [], "controller_services": [], "suggested_mappings": []}

        # Parameter Contexts
        for pc in root.findall(".//parameterContexts/parameterContext"):
            name = (pc.findtext("component/name") or "unnamed").strip()
            params = []
            for p in pc.findall(".//component/parameters/parameter"):
                params.append(
                    {
                        "name": p.findtext("parameter/name"),
                        "value": p.findtext("parameter/value"),
                        "sensitive": (p.findtext("parameter/sensitive") == "true"),
                    }
                )
            out["parameter_contexts"].append({"name": name, "parameters": params})

        # Controller Services
        for cs in root.findall(".//controllerServices/controllerService"):
            c = cs.find("component")
            out["controller_services"].append(
                {
                    "id": cs.findtext("id"),
                    "name": c.findtext("name") if c is not None else None,
                    "type": c.findtext("type") if c is not None else None,
                    "properties": {
                        e.findtext("name"): e.findtext("value")
                        for e in (c.findall(".//properties/entry") if c is not None else [])
                    },
                }
            )

        # Simple mapping rules → Databricks
        for cs in out["controller_services"]:
            t = (cs.get("type") or "").lower()
            if "dbcp" in t or "jdbc" in t:
                out["suggested_mappings"].append(
                    {
                        "nifi": cs.get("name"),
                        "databricks_equivalent": "JDBC via spark.read/write + Databricks Secrets",
                        "how": "Store URL/user/password in a secret scope; attach JDBC drivers to the cluster.",
                    }
                )
            if "sslcontextservice" in t:
                out["suggested_mappings"].append(
                    {
                        "nifi": cs.get("name"),
                        "databricks_equivalent": "Secure endpoints + secrets-backed cert paths",
                        "how": "Upload certs to a secured location; reference via secrets or init scripts.",
                    }
                )

        return json.dumps(out, indent=2)
    except Exception as e:
        return f"Failed to parse NiFi XML: {e}"

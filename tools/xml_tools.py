# tools/xml_tools.py
# Consolidated XML parsing tools for NiFi template analysis

from __future__ import annotations

import json
import xml.etree.ElementTree as ET
from typing import Any, Dict, List
from databricks.sdk.runtime import *

__all__ = [
    "parse_nifi_template",
    "parse_nifi_template_impl",
    "extract_nifi_parameters_and_services_impl",
]


def _trim(s: str | None) -> str:
    """Trim helper that returns '' for None."""
    return (s or "").strip()


def parse_nifi_template_impl(xml_content: str) -> Dict[str, Any]:
    """
    Parse a NiFi XML template and extract processors, properties, and connections.
    Returns Python dict for programmatic use.

    Parameters:
        xml_content: The raw NiFi XML content

    Returns:
        Dict with processors, connections, counts, and process groups
    """
    root = ET.fromstring(xml_content)

    processors: List[Dict[str, Any]] = []
    connections: List[Dict[str, Any]] = []

    # Build process group mapping for enhanced task naming
    process_groups = {}
    for group in root.findall(".//processGroups"):
        group_id = group.findtext("id")
        group_name = group.findtext("name") or "UnnamedGroup"
        if group_id:
            process_groups[group_id] = group_name

    # Extract processors with process group context
    for processor in root.findall(".//processors"):
        parent_group_id = processor.findtext("parentGroupId")
        parent_group_name = (
            process_groups.get(parent_group_id, "Root") if parent_group_id else "Root"
        )

        proc_info = {
            "name": _trim(processor.findtext("name") or "Unknown"),
            "type": _trim(processor.findtext("type") or "Unknown"),
            "id":  _trim(processor.findtext("id") or "Unknown"),
            "properties": {},
            "parentGroupId": parent_group_id,
            "parentGroupName": parent_group_name,
        }

        props_node = processor.find(".//properties")
        if props_node is not None:
            for entry in props_node.findall("entry"):
                k = entry.findtext("key")
                v = entry.findtext("value")
                if k is not None:
                    proc_info["properties"][k] = v

        processors.append(proc_info)

    # Extract connections
    for connection in root.findall(".//connections"):
        source = _trim(connection.findtext(".//source/id") or "Unknown")
        destination = _trim(connection.findtext(".//destination/id") or "Unknown")
        rels = [
            _trim(rel.text or "")
            for rel in connection.findall(".//selectedRelationships")
            if rel is not None and rel.text
        ]
        connections.append(
            {
                "source": source,
                "destination": destination,
                "relationships": rels,
            }
        )

    return {
        "processors": processors,
        "connections": connections,
        "processor_count": len(processors),
        "connection_count": len(connections),
        "process_groups": process_groups,
    }


def parse_nifi_template(xml_content: str) -> str:
    """
    Parse a NiFi XML template and return JSON string.
    Legacy interface for existing tool compatibility.
    """
    try:
        result = parse_nifi_template_impl(xml_content)
        result.update(
            {
                "continue_required": False,
                "tool_name": "parse_nifi_template",
            }
        )
        return json.dumps(result, indent=2)
    except ET.ParseError as e:
        return f"Error parsing XML: {str(e)}"
    except Exception as e:
        return f"Unexpected error: {str(e)}"


def extract_nifi_parameters_and_services_impl(xml_content: str) -> Dict[str, Any]:
    """
    Extract Parameter Contexts and Controller Services from NiFi XML template.

    Parameters:
        xml_content: The raw NiFi XML content

    Returns:
        Dict with parameter_contexts, controller_services, and suggested_mappings
    """
    root = ET.fromstring(xml_content)

    result: Dict[str, Any] = {
        "parameter_contexts": [],
        "controller_services": [],
        "suggested_mappings": [],
    }

    # Parameter Contexts
    for pc in root.findall(".//parameterContexts/parameterContext"):
        name = _trim(pc.findtext("component/name") or "unnamed")
        params = []
        for p in pc.findall(".//component/parameters/parameter"):
            params.append(
                {
                    "name": p.findtext("parameter/name"),
                    "value": p.findtext("parameter/value"),
                    "sensitive": (p.findtext("parameter/sensitive") == "true"),
                }
            )
        result["parameter_contexts"].append({"name": name, "parameters": params})

    # Controller Services
    for cs in root.findall(".//controllerServices/controllerService"):
        c = cs.find("component")
        entries = c.findall(".//properties/entry") if c is not None else []
        props = {e.findtext("name"): e.findtext("value") for e in entries}
        result["controller_services"].append(
            {
                "id": cs.findtext("id"),
                "name": c.findtext("name") if c is not None else None,
                "type": c.findtext("type") if c is not None else None,
                "properties": props,
            }
        )

    # Generate Databricks mapping suggestions
    for cs in result["controller_services"]:
        service_type = (cs.get("type") or "").lower()
        if "dbcp" in service_type or "jdbc" in service_type:
            result["suggested_mappings"].append(
                {
                    "nifi": cs.get("name"),
                    "databricks_equivalent": "JDBC via spark.read/write + Databricks Secrets",
                    "how": "Store URL/user/password in a secret scope; attach JDBC drivers to cluster.",
                }
            )
        elif "sslcontextservice" in service_type:
            result["suggested_mappings"].append(
                {
                    "nifi": cs.get("name"),
                    "databricks_equivalent": "Secure endpoints + secrets-backed cert paths",
                    "how": "Upload certs to secured location; reference via secrets or init scripts.",
                }
            )

    return result


def list_xml_files(xml_volumes_path):
    """
    Return a list of XML file paths from the 
    specified catalog, schema, and volume.
    """

    xml_paths_df = (
        spark.read.format("binaryFile")
            .option("recursiveFileLookup", "true")
            .option("pathGlobFilter", "*.xml")       # only *.xml files
            .load(xml_volumes_path)
            .select("path")                          # keep just the path
            .distinct()
    )

    xml_paths_df.show(truncate=False)
    # If you need them as a Python list:
    xml_paths = [r.path for r in xml_paths_df.collect()]

    return xml_paths

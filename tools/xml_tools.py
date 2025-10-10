# tools/xml_tools.py
# Consolidated XML parsing tools for NiFi template analysis

from __future__ import annotations

import xml.etree.ElementTree as ET
from typing import Any, Dict, List

__all__ = [
    "parse_nifi_template_impl",
    "extract_processor_info",
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
    input_ports: List[Dict[str, Any]] = []
    output_ports: List[Dict[str, Any]] = []

    # Build process group mapping (name + parent) for hierarchy resolution
    process_groups: Dict[str, Dict[str, Any]] = {}
    for group in root.findall(".//processGroups"):
        group_id = _trim(group.findtext("id"))
        if not group_id:
            continue
        parent_id = _trim(group.findtext("parentGroupId")) or None
        raw_name = (
            group.findtext("name")
            or group.findtext("component/name")
            or group.findtext("component/component/name")
            or "UnnamedGroup"
        )
        group_name = _trim(raw_name) or "UnnamedGroup"
        process_groups[group_id] = {"name": group_name, "parent_id": parent_id}

    group_paths: Dict[str, str] = {}

    def _resolve_group_path(group_id: str | None, depth: int = 0) -> str:
        if not group_id:
            return ""
        if group_id in group_paths:
            return group_paths[group_id]
        info = process_groups.get(group_id)
        if info is None or depth > 100:
            return ""
        parent_path = _resolve_group_path(info.get("parent_id"), depth + 1)
        path = f"{parent_path}/{info['name']}" if parent_path else info["name"]
        group_paths[group_id] = path
        return path

    for gid in list(process_groups.keys()):
        _resolve_group_path(gid)

    # Extract processors with process group context
    for processor in root.findall(".//processors"):
        parent_group_id = processor.findtext("parentGroupId")
        parent_group_info = process_groups.get(parent_group_id or "")
        parent_group_name = (
            parent_group_info.get("name") if parent_group_info else "Root"
        )
        parent_group_path = (
            group_paths.get(parent_group_id or "", "") or parent_group_name
        )

        proc_info = {
            "id": _trim(processor.findtext("id") or "Unknown"),
            "name": _trim(processor.findtext("name") or "Unknown"),
            "type": _trim(processor.findtext("type") or "Unknown"),
            "comments": _trim(processor.findtext(".//config/comments") or ""),
            "properties": {},
            "parentGroupId": parent_group_id,
            "parentGroupName": parent_group_name,
            "parentGroupPath": parent_group_path,
        }

        props_node = processor.find(".//properties")
        if props_node is not None:
            for entry in props_node.findall("entry"):
                k = entry.findtext("key")
                v = entry.findtext("value")
                if k is not None:
                    proc_info["properties"][k] = v

        processors.append(proc_info)

    # Extract input ports
    for port in root.findall(".//inputPorts"):
        port_id = _trim(port.findtext("id") or "Unknown")
        parent_group_id = port.findtext("parentGroupId")
        input_ports.append(
            {
                "id": port_id,
                "name": _trim(port.findtext("name") or "Unknown"),
                "parentGroupId": parent_group_id,
            }
        )

    # Extract output ports
    for port in root.findall(".//outputPorts"):
        port_id = _trim(port.findtext("id") or "Unknown")
        parent_group_id = port.findtext("parentGroupId")
        output_ports.append(
            {
                "id": port_id,
                "name": _trim(port.findtext("name") or "Unknown"),
                "parentGroupId": parent_group_id,
            }
        )

    # Extract connections
    for connection in root.findall(".//connections"):
        source = _trim(connection.findtext(".//source/id") or "Unknown")
        destination = _trim(connection.findtext(".//destination/id") or "Unknown")
        source_type = _trim(connection.findtext(".//source/type") or "PROCESSOR")
        dest_type = _trim(connection.findtext(".//destination/type") or "PROCESSOR")
        rels = [
            _trim(rel.text or "")
            for rel in connection.findall(".//selectedRelationships")
            if rel is not None and rel.text
        ]
        connections.append(
            {
                "source": source,
                "destination": destination,
                "source_type": source_type,
                "destination_type": dest_type,
                "relationships": rels,
            }
        )

    return {
        "processors": processors,
        "connections": connections,
        "input_ports": input_ports,
        "output_ports": output_ports,
        "processor_count": len(processors),
        "connection_count": len(connections),
        "process_groups": process_groups,
        "process_group_paths": group_paths,
    }


def extract_processor_info(xml_content: str) -> List[Dict[str, str]]:
    """
    Extract basic processor information for Dashboard display.

    Parameters:
        xml_content: The raw NiFi XML content

    Returns:
        List of dicts with id, name, type, comments for each processor
    """
    try:
        root = ET.fromstring(xml_content)
        processors = []

        for processor in root.findall(".//processors"):
            proc_info = {
                "id": _trim(processor.findtext("id") or "Unknown"),
                "name": _trim(processor.findtext("name") or "Unknown"),
                "type": _trim(processor.findtext("type") or "Unknown"),
                "comments": _trim(processor.findtext(".//config/comments") or ""),
            }
            processors.append(proc_info)

        return processors

    except ET.ParseError:
        # Return empty list if XML parsing fails
        return []
    except Exception:
        # Return empty list for any other errors
        return []

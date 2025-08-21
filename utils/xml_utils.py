# utils/xml_utils.py
# ----------------------------------------------------------------------
# Pure NiFi XML parsing implementations used by tool wrappers.
# These functions return Python dicts and raise on invalid XML.
# ----------------------------------------------------------------------

from __future__ import annotations

import xml.etree.ElementTree as ET
from typing import Any, Dict, List

__all__ = [
    "parse_nifi_template_impl",
    "extract_nifi_parameters_and_services_impl",
]


def _t(s: str | None) -> str:
    """Trim helper that returns '' for None."""
    return (s or "").strip()


def parse_nifi_template_impl(xml_content: str) -> Dict[str, Any]:
    """
    Parse a NiFi XML template and extract processors, properties, and connections.

    Parameters
    ----------
    xml_content : str
        The raw NiFi XML content.

    Returns
    -------
    Dict[str, Any]
        {
          "processors": [
            {"name": str, "type": str, "properties": {k: v, ...}}, ...
          ],
          "connections": [
            {"source": str, "destination": str, "relationships": [str, ...]}, ...
          ],
          "processor_count": int,
          "connection_count": int
        }

    Notes
    -----
    - This is a pure function with no LangChain/JSON/@tool dependencies.
    - Any XML parsing errors will raise ET.ParseError.
    """
    root = ET.fromstring(xml_content)

    processors: List[Dict[str, Any]] = []
    # Many NiFi exports have one <processors> element per processor entry.
    # Keep the same selector your agent already used for compatibility.
    for processor in root.findall(".//processors"):
        info = {
            "name": _t(processor.findtext("name") or "Unknown"),
            "type": _t(processor.findtext("type") or "Unknown"),
            "properties": {},
        }

        props_node = processor.find(".//properties")
        if props_node is not None:
            for entry in props_node.findall("entry"):
                k = entry.findtext("key")
                v = entry.findtext("value")
                if k is not None:
                    info["properties"][k] = v

        processors.append(info)

    connections: List[Dict[str, Any]] = []
    for connection in root.findall(".//connections"):
        source = _t(connection.findtext(".//source/id") or "Unknown")
        dest = _t(connection.findtext(".//destination/id") or "Unknown")
        # selectedRelationships may appear multiple times; collect text values
        rels = [
            _t(rel.text)
            for rel in connection.findall(".//selectedRelationships")
            if rel is not None and rel.text
        ]
        connections.append(
            {"source": source, "destination": dest, "relationships": rels}
        )

    return {
        "processors": processors,
        "connections": connections,
        "processor_count": len(processors),
        "connection_count": len(connections),
    }


def extract_nifi_parameters_and_services_impl(xml_content: str) -> Dict[str, Any]:
    """
    Extract Parameter Contexts and Controller Services from a NiFi XML template,
    and provide simple Databricks mapping suggestions.

    Parameters
    ----------
    xml_content : str
        The raw NiFi XML content.

    Returns
    -------
    Dict[str, Any]
        {
          "parameter_contexts": [
            {"name": str, "parameters": [{"name": str, "value": str|None, "sensitive": bool}, ...]}, ...
          ],
          "controller_services": [
            {"id": str|None, "name": str|None, "type": str|None, "properties": {k: v, ...}}, ...
          ],
          "suggested_mappings": [
            {"nifi": str|None, "databricks_equivalent": str, "how": str}, ...
          ]
        }

    Notes
    -----
    - Pure implementation. Tool wrappers (in tools/xml_tools.py) turn this into JSON.
    """
    root = ET.fromstring(xml_content)

    out: Dict[str, Any] = {
        "parameter_contexts": [],
        "controller_services": [],
        "suggested_mappings": [],
    }

    # Parameter Contexts
    for pc in root.findall(".//parameterContexts/parameterContext"):
        name = _t(pc.findtext("component/name") or "unnamed")
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
        entries = c.findall(".//properties/entry") if c is not None else []
        props = {e.findtext("name"): e.findtext("value") for e in entries}
        out["controller_services"].append(
            {
                "id": cs.findtext("id"),
                "name": c.findtext("name") if c is not None else None,
                "type": c.findtext("type") if c is not None else None,
                "properties": props,
            }
        )

    # Simple suggestion rules → Databricks equivalents
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

    return out

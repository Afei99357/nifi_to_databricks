# tools/xml_tools.py
# XML parsing tools exposed to the agent.

from __future__ import annotations

import json
import xml.etree.ElementTree as ET
from typing import Any, Dict, List

from langchain_core.tools import tool

__all__ = [
    "parse_nifi_template",
    "extract_nifi_parameters_and_services",
    "analyze_nifi_architecture_requirements",
    "recommend_databricks_architecture",
]


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
            "continue_required": False,
            "tool_name": "parse_nifi_template",
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
        out = {
            "parameter_contexts": [],
            "controller_services": [],
            "suggested_mappings": [],
        }

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
                        for e in (
                            c.findall(".//properties/entry") if c is not None else []
                        )
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

        out["continue_required"] = False
        out["tool_name"] = "extract_nifi_parameters_and_services"
        return json.dumps(out, indent=2)
    except Exception as e:
        return f"Failed to parse NiFi XML: {e}"


@tool
def analyze_nifi_architecture_requirements(xml_content: str) -> str:
    """
    Analyze NiFi XML to detect processor types and determine architecture requirements.

    Returns feature flags and processor analysis to guide Databricks architecture decisions:
    {
        "feature_flags": {
            "has_streaming": bool,
            "has_batch": bool,
            "has_transforms": bool,
            "has_external_sinks": bool,
            "has_routing": bool,
            "has_json_processing": bool
        },
        "processor_analysis": {
            "sources": [...],
            "transforms": [...],
            "sinks": [...],
            "total_count": N
        },
        "complexity_level": "simple|moderate|complex"
    }
    """
    try:
        root = ET.fromstring(xml_content)

        # Processor classification
        streaming_sources = {
            "ListenHTTP",
            "ConsumeKafka",
            "ListenTCP",
            "ListenUDP",
            "ListenSyslog",
            "ConsumeJMS",
            "ConsumeMQTT",
            "ConsumeAMQP",
            "GetTwitter",
            "ListenRELP",
        }

        batch_sources = {
            "GetFile",
            "ListFile",
            "FetchFile",
            "GetFTP",
            "GetSFTP",
            "FetchS3Object",
            "ListS3",
            "GetHDFS",
            "QueryDatabaseTable",
        }

        transform_processors = {
            "EvaluateJsonPath",
            "UpdateAttribute",
            "ReplaceText",
            "TransformXml",
            "ConvertRecord",
            "SplitText",
            "SplitJson",
            "MergeContent",
            "CompressContent",
            "EncryptContent",
            "HashContent",
            "ValidateRecord",
            "LookupRecord",
        }

        routing_processors = {
            "RouteOnAttribute",
            "RouteOnContent",
            "RouteText",
            "RouteJSON",
            "DistributeLoad",
            "ControlRate",
            "PriorizeAttribute",
        }

        json_processors = {
            "EvaluateJsonPath",
            "SplitJson",
            "ConvertJSONToSQL",
            "JoltTransformJSON",
        }

        external_sinks = {
            "PublishKafka",
            "InvokeHTTP",
            "PutEmail",
            "PutSFTP",
            "PutFTP",
            "PublishJMS",
            "PublishMQTT",
            "PutElasticsearch",
            "PutSlack",
        }

        file_sinks = {"PutHDFS", "PutFile", "PutS3Object", "MergeContent"}

        # Initialize feature flags
        feature_flags = {
            "has_streaming": False,
            "has_batch": False,
            "has_transforms": False,
            "has_external_sinks": False,
            "has_routing": False,
            "has_json_processing": False,
        }

        # Initialize processor analysis
        processor_analysis = {
            "sources": [],
            "transforms": [],
            "sinks": [],
            "total_count": 0,
        }

        # Analyze all processors
        for processor in root.findall(".//processors"):
            proc_type = (processor.findtext("type") or "").strip()
            proc_name = (processor.findtext("name") or "Unknown").strip()

            # Extract just the class name from full path
            class_name = proc_type.split(".")[-1] if "." in proc_type else proc_type

            processor_analysis["total_count"] += 1

            # Classify processor and set feature flags
            if class_name in streaming_sources:
                feature_flags["has_streaming"] = True
                processor_analysis["sources"].append(
                    {
                        "name": proc_name,
                        "type": class_name,
                        "category": "streaming_source",
                    }
                )

            elif class_name in batch_sources:
                feature_flags["has_batch"] = True
                processor_analysis["sources"].append(
                    {"name": proc_name, "type": class_name, "category": "batch_source"}
                )

            elif class_name in transform_processors:
                feature_flags["has_transforms"] = True
                processor_analysis["transforms"].append(
                    {"name": proc_name, "type": class_name, "category": "transform"}
                )

            elif class_name in routing_processors:
                feature_flags["has_routing"] = True
                processor_analysis["transforms"].append(
                    {"name": proc_name, "type": class_name, "category": "routing"}
                )

            elif class_name in external_sinks:
                feature_flags["has_external_sinks"] = True
                processor_analysis["sinks"].append(
                    {"name": proc_name, "type": class_name, "category": "external_sink"}
                )

            elif class_name in file_sinks:
                processor_analysis["sinks"].append(
                    {"name": proc_name, "type": class_name, "category": "file_sink"}
                )

            # Check for JSON processing
            if class_name in json_processors:
                feature_flags["has_json_processing"] = True

        # Determine complexity level
        complexity_factors = [
            feature_flags["has_streaming"]
            and feature_flags["has_batch"],  # Mixed sources
            feature_flags["has_routing"],  # Conditional logic
            feature_flags["has_json_processing"],  # Complex transformations
            len(processor_analysis["sinks"]) > 2,  # Multiple outputs
            processor_analysis["total_count"] > 10,  # Large workflow
        ]

        complexity_score = sum(complexity_factors)
        if complexity_score >= 3:
            complexity_level = "complex"
        elif complexity_score >= 1:
            complexity_level = "moderate"
        else:
            complexity_level = "simple"

        result = {
            "feature_flags": feature_flags,
            "processor_analysis": processor_analysis,
            "complexity_level": complexity_level,
            "continue_required": False,
            "tool_name": "analyze_nifi_architecture_requirements",
        }

        return json.dumps(result, indent=2)

    except Exception as e:
        return f"Failed to analyze NiFi architecture requirements: {e}"


@tool
def recommend_databricks_architecture(xml_content: str) -> str:
    """
    Recommend the best Databricks architecture based on NiFi XML analysis.

    Applies decision rules to determine whether to generate:
    - Databricks Job (batch orchestration)
    - DLT Pipeline (streaming ETL with transformations)
    - Structured Streaming (custom streaming logic)

    Returns recommendation with reasoning:
    {
        "recommendation": "databricks_job|dlt_pipeline|structured_streaming",
        "confidence": "high|medium|low",
        "reasoning": [...],
        "architecture_details": {...},
        "alternative_options": [...]
    }
    """
    try:
        # First analyze the architecture requirements
        analysis_json = analyze_nifi_architecture_requirements(xml_content)
        analysis = json.loads(analysis_json)

        feature_flags = analysis["feature_flags"]
        processor_analysis = analysis["processor_analysis"]
        complexity_level = analysis["complexity_level"]

        # Apply decision rules
        reasoning = []
        confidence = "high"
        alternative_options = []

        # Decision Rule 1: Pure batch processing
        if (
            feature_flags["has_batch"]
            and not feature_flags["has_streaming"]
            and not feature_flags["has_routing"]
        ):

            recommendation = "databricks_job"
            reasoning.append("Only batch file sources detected (GetFile, ListFile)")
            reasoning.append("No streaming sources or complex routing logic")
            reasoning.append("Simple batch orchestration is sufficient")

            architecture_details = {
                "job_type": "scheduled_batch",
                "source_pattern": "Auto Loader for file ingestion",
                "sink_pattern": "Delta Lake writes",
                "scheduling": "Triggered or scheduled execution",
            }

            alternative_options.append(
                {
                    "option": "dlt_pipeline",
                    "reason": "If you want declarative SQL-based transformations",
                }
            )

        # Decision Rule 2: Streaming sources present
        elif feature_flags["has_streaming"]:

            # Sub-rule: Streaming + transformations/routing = DLT
            if (
                feature_flags["has_transforms"]
                or feature_flags["has_routing"]
                or feature_flags["has_json_processing"]
            ):

                recommendation = "dlt_pipeline"
                reasoning.append(
                    "Streaming sources detected (ListenHTTP, ConsumeKafka, etc.)"
                )
                reasoning.append("Complex transformations and/or routing logic present")
                reasoning.append("DLT provides best streaming ETL capabilities")

                architecture_details = {
                    "pipeline_type": "streaming_etl",
                    "source_pattern": "Structured Streaming sources",
                    "transform_pattern": "Declarative SQL/PySpark transformations",
                    "sink_pattern": "Delta Live Tables with data quality",
                }

                alternative_options.append(
                    {
                        "option": "structured_streaming",
                        "reason": "If you need custom streaming logic not suited for DLT",
                    }
                )

            # Sub-rule: Simple streaming without complex transforms
            else:
                recommendation = "structured_streaming"
                reasoning.append(
                    "Streaming sources present but minimal transformations"
                )
                reasoning.append("Custom Structured Streaming may be more appropriate")

                architecture_details = {
                    "pipeline_type": "custom_streaming",
                    "source_pattern": "readStream() operations",
                    "sink_pattern": "writeStream() to Delta tables",
                }

                alternative_options.append(
                    {
                        "option": "dlt_pipeline",
                        "reason": "If transformations grow more complex over time",
                    }
                )

        # Decision Rule 3: Mixed batch + streaming
        elif feature_flags["has_batch"] and feature_flags["has_streaming"]:

            recommendation = "dlt_pipeline"
            reasoning.append("Mixed batch and streaming sources detected")
            reasoning.append(
                "DLT can handle both batch and streaming in unified pipeline"
            )
            reasoning.append("Complexity level: " + complexity_level)

            architecture_details = {
                "pipeline_type": "unified_batch_streaming",
                "batch_sources": "Auto Loader for files",
                "streaming_sources": "Structured Streaming",
                "unified_processing": "DLT tables handle both patterns",
            }

            alternative_options.append(
                {
                    "option": "databricks_job",
                    "reason": "Multi-task job with separate batch and streaming tasks",
                }
            )

        # Decision Rule 4: Heavy transformations/routing (regardless of sources)
        elif (
            feature_flags["has_transforms"]
            and feature_flags["has_routing"]
            and complexity_level in ["moderate", "complex"]
        ):

            recommendation = "dlt_pipeline"
            reasoning.append("Heavy transformation and routing logic detected")
            reasoning.append("Multiple conditional branches and data transformations")
            reasoning.append(
                "DLT provides best declarative transformation capabilities"
            )

            architecture_details = {
                "pipeline_type": "transformation_heavy",
                "transform_pattern": "Multi-table DLT pipeline with bronze/silver/gold layers",
                "routing_pattern": "Conditional SQL logic and multiple output tables",
            }

        # Decision Rule 5: Default fallback
        else:
            recommendation = "databricks_job"
            reasoning.append("Standard ETL pattern detected")
            reasoning.append("Databricks Job provides good orchestration capabilities")
            confidence = "medium"

            architecture_details = {
                "job_type": "multi_task_etl",
                "orchestration": "Task dependencies based on processor connections",
            }

        # Adjust confidence based on complexity
        if complexity_level == "complex" and recommendation != "dlt_pipeline":
            confidence = "medium"
            reasoning.append(
                "High complexity may benefit from DLT pipeline consideration"
            )

        result = {
            "recommendation": recommendation,
            "confidence": confidence,
            "reasoning": reasoning,
            "architecture_details": architecture_details,
            "alternative_options": alternative_options,
            "analysis_summary": {
                "total_processors": processor_analysis["total_count"],
                "complexity": complexity_level,
                "key_features": [k for k, v in feature_flags.items() if v],
            },
            "continue_required": False,
            "tool_name": "recommend_databricks_architecture",
        }

        return json.dumps(result, indent=2)

    except Exception as e:
        return f"Failed to recommend Databricks architecture: {e}"

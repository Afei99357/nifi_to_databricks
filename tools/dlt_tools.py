# tools/dlt_tools.py
# DLT/Lakehouse expectations and pipeline config helpers.

from __future__ import annotations

import json
import xml.etree.ElementTree as ET
from typing import Any, Dict, List

from langchain_core.tools import tool

__all__ = [
    "generate_dlt_expectations",
    "generate_dlt_pipeline_config",
    "create_dlt_sql_from_processors",
    "generate_dlt_pipeline_from_nifi",
    "generate_chunked_dlt_pipeline_from_nifi",
]


@tool
def generate_dlt_expectations(table_name: str, rules_json: str) -> str:
    """
    Return SQL to create a DLT/Lakeflow dataset with expectations from simple rules.

    Example rules_json:
      {"not_null_id": "id IS NOT NULL", "valid_price": "price >= 0"}
    """
    try:
        rules = json.loads(rules_json) if rules_json else {}
        ex_lines = [f"EXPECT {name} : {expr}" for name, expr in rules.items()]
        ex_block = ("\n  ".join(ex_lines)) if ex_lines else ""
        sql = (
            f"CREATE OR REFRESH STREAMING TABLE {table_name}\n"
            f"  {ex_block}\n"
            "AS SELECT * FROM STREAM(LIVE.source_table);"
        )
        return sql
    except Exception as e:
        return f"Invalid rules: {e}"


@tool
def generate_dlt_pipeline_config(
    pipeline_name: str, catalog: str, db_schema: str, notebook_path: str
) -> str:
    """
    Return minimal JSON config for a DLT/Lakeflow pipeline.
    """
    cfg = {
        "name": pipeline_name,
        "storage": f"/pipelines/{pipeline_name}",
        "target": f"{catalog}.{db_schema}",
        "development": True,
        "continuous": True,
        "libraries": [{"notebook": {"path": notebook_path}}],
    }
    return json.dumps(cfg, indent=2)


@tool
def create_dlt_sql_from_processors(
    processors_json: str, project_name: str = "nifi_migration"
) -> str:
    """
    Generate DLT pipeline SQL from NiFi processors, creating a streaming workflow similar to NiFi.

    This creates interconnected tables where each DLT table represents a NiFi processor output,
    automatically handling lineage and dependencies like NiFi queues.

    Args:
        processors_json: JSON string with processor information
        project_name: Project name for table prefixes

    Returns:
        Complete DLT SQL notebook content
    """
    try:
        processors = json.loads(processors_json)

        # Processor type mappings to DLT patterns
        streaming_sources = {
            "ListenHTTP",
            "ConsumeKafka",
            "ListenTCP",
            "ListenUDP",
            "ListenSyslog",
            "ConsumeJMS",
            "ConsumeMQTT",
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
        }

        transform_processors = {
            "EvaluateJsonPath",
            "UpdateAttribute",
            "ReplaceText",
            "ConvertRecord",
            "SplitText",
            "SplitJson",
            "RouteOnAttribute",
        }

        sql_sections = []

        # Add header
        sql_sections.append(
            f"""-- Databricks notebook source
-- DLT Pipeline generated from NiFi workflow
-- Project: {project_name}
-- Generated: Auto-migration from NiFi XML

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *
"""
        )

        # Process each processor into a DLT table
        for i, proc in enumerate(processors):
            proc_type = proc.get("type", "Unknown")
            proc_name = proc.get("name", f"processor_{i}")
            properties = proc.get("properties", {})

            # Extract class name from full Java path
            class_name = proc_type.split(".")[-1] if "." in proc_type else proc_type

            # Create safe table name
            table_name = (
                f"{project_name}_{proc_name}".lower()
                .replace("-", "_")
                .replace(" ", "_")
            )

            if class_name in streaming_sources:
                sql_sections.append(
                    _generate_streaming_source_sql(table_name, class_name, properties)
                )
            elif class_name in batch_sources:
                sql_sections.append(
                    _generate_batch_source_sql(table_name, class_name, properties)
                )
            elif class_name in transform_processors:
                # Find upstream table (simplified - assumes sequential processing)
                upstream_table = (
                    f"{project_name}_processor_{i-1}"
                    if i > 0
                    else f"{project_name}_bronze"
                )
                sql_sections.append(
                    _generate_transform_sql(
                        table_name, class_name, properties, upstream_table
                    )
                )
            else:
                # Generic processor
                upstream_table = (
                    f"{project_name}_processor_{i-1}"
                    if i > 0
                    else f"{project_name}_bronze"
                )
                sql_sections.append(
                    _generate_generic_sql(
                        table_name, class_name, properties, upstream_table
                    )
                )

        return "\n\n".join(sql_sections)

    except Exception as e:
        return f"Error generating DLT SQL: {str(e)}"


def _generate_streaming_source_sql(
    table_name: str, processor_type: str, properties: Dict[str, Any]
) -> str:
    """Generate DLT SQL for streaming sources."""

    if processor_type == "ListenHTTP":
        # HTTP streaming source - simulate with cloud files for demo
        return f"""@dlt.table(
    name="{table_name}",
    comment="HTTP streaming source from NiFi {processor_type}"
)
def {table_name}():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.includeExistingFiles", "true")
        .load("/Volumes/main/default/streaming_input/")
        .select(
            current_timestamp().alias("ingestion_time"),
            "*"
        )
    )"""

    elif processor_type == "ConsumeKafka":
        bootstrap_servers = properties.get("kafka.bootstrap.servers", "localhost:9092")
        topic = properties.get("topic", "default_topic")

        return f"""@dlt.table(
    name="{table_name}",
    comment="Kafka consumer from NiFi {processor_type}"
)
def {table_name}():
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "{bootstrap_servers}")
        .option("subscribe", "{topic}")
        .option("startingOffsets", "latest")
        .load()
        .select(
            col("timestamp").alias("kafka_timestamp"),
            col("offset").alias("kafka_offset"),
            col("partition").alias("kafka_partition"),
            col("value").cast("string").alias("message_body")
        )
    )"""

    else:
        # Generic streaming source
        return f"""@dlt.table(
    name="{table_name}",
    comment="Streaming source from NiFi {processor_type}"
)
def {table_name}():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load("/path/to/streaming/data")
        .select("*", current_timestamp().alias("ingestion_time"))
    )"""


def _generate_batch_source_sql(
    table_name: str, processor_type: str, properties: Dict[str, Any]
) -> str:
    """Generate DLT SQL for batch sources."""

    if processor_type in ["GetFile", "ListFile"]:
        file_filter = properties.get("File Filter", "*")
        input_dir = properties.get("Input Directory", "/path/to/files")

        return f"""@dlt.table(
    name="{table_name}",
    comment="Auto Loader batch source from NiFi {processor_type}"
)
def {table_name}():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")  # Adjust format as needed
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("{input_dir}")
        .select(
            input_file_name().alias("source_file"),
            current_timestamp().alias("processing_time"),
            "*"
        )
    )"""

    else:
        return f"""@dlt.table(
    name="{table_name}",
    comment="Batch source from NiFi {processor_type}"
)
def {table_name}():
    return (
        spark.read
        .format("delta")  # Default to Delta
        .load("/path/to/batch/data")
        .select("*", current_timestamp().alias("processing_time"))
    )"""


def _generate_transform_sql(
    table_name: str,
    processor_type: str,
    properties: Dict[str, Any],
    upstream_table: str,
) -> str:
    """Generate DLT SQL for transformation processors."""

    if processor_type == "EvaluateJsonPath":
        json_paths = {k: v for k, v in properties.items() if k.startswith("json.path.")}

        select_exprs = ["*"]  # Keep original columns
        for attr_name, json_path in json_paths.items():
            # Convert NiFi JsonPath to Spark get_json_object
            spark_path = json_path.replace("$.", "$.")  # Simple conversion
            select_exprs.append(
                f'get_json_object(message_body, "{spark_path}") as {attr_name.replace("json.path.", "")}'
            )

        return f"""@dlt.table(
    name="{table_name}",
    comment="JSON path evaluation from NiFi {processor_type}"
)
def {table_name}():
    return (
        dlt.read_stream("{upstream_table}")
        .select({", ".join(select_exprs)})
    )"""

    elif processor_type == "RouteOnAttribute":
        # Create branching logic - simplified to single route for demo
        route_conditions = [
            f"{k}: {v}"
            for k, v in properties.items()
            if not k.startswith("Routing Strategy")
        ]

        return f"""@dlt.table(
    name="{table_name}",
    comment="Routing logic from NiFi {processor_type}"
)
def {table_name}():
    # Route conditions: {', '.join(route_conditions)}
    return (
        dlt.read_stream("{upstream_table}")
        .filter(col("some_field").isNotNull())  # Add specific routing logic here
        .select("*", lit("matched_route").alias("route_result"))
    )"""

    elif processor_type == "UpdateAttribute":
        # Add attributes as new columns
        attrs = {
            k: v for k, v in properties.items() if not k.startswith("Delete Attributes")
        }

        attr_exprs = ["*"]
        for attr_name, attr_value in attrs.items():
            attr_exprs.append(f'lit("{attr_value}").alias("{attr_name}")')

        return f"""@dlt.table(
    name="{table_name}",
    comment="Attribute updates from NiFi {processor_type}"
)
def {table_name}():
    return (
        dlt.read_stream("{upstream_table}")
        .select({", ".join(attr_exprs)})
    )"""

    else:
        return f"""@dlt.table(
    name="{table_name}",
    comment="Transform from NiFi {processor_type}"
)
def {table_name}():
    return (
        dlt.read_stream("{upstream_table}")
        .select("*")  # Add transformation logic here
    )"""


def _generate_generic_sql(
    table_name: str,
    processor_type: str,
    properties: Dict[str, Any],
    upstream_table: str,
) -> str:
    """Generate generic DLT SQL for unknown processors."""

    return f"""@dlt.table(
    name="{table_name}",
    comment="Generic processor from NiFi {processor_type}"
)
def {table_name}():
    # TODO: Implement specific logic for {processor_type}
    # Properties: {json.dumps(properties, indent=2)}
    return (
        dlt.read_stream("{upstream_table}")
        .select("*", current_timestamp().alias("processed_at"))
    )"""


@tool
def generate_dlt_pipeline_from_nifi(
    xml_content: str,
    project_name: str,
    catalog: str = "main",
    schema_name: str = "default",
    notebook_path: str = "/Workspace/Users/me@company.com/dlt_pipeline",
) -> str:
    """
    Generate complete DLT pipeline configuration and SQL from NiFi XML.

    Creates both the DLT pipeline configuration and the SQL notebook content
    that implements the NiFi workflow as interconnected streaming tables.

    Args:
        xml_content: NiFi XML template content
        project_name: Project name for generated assets
        catalog: Unity Catalog name
        schema_name: Schema name within catalog
        notebook_path: Path where DLT notebook will be created

    Returns:
        JSON with pipeline config and SQL notebook content
    """
    try:
        # Parse NiFi XML to extract processors
        root = ET.fromstring(xml_content)
        processors = []

        for processor in root.findall(".//processors"):
            proc_info = {
                "name": (processor.findtext("name") or "Unknown").strip(),
                "type": (processor.findtext("type") or "Unknown").strip(),
                "properties": {},
            }

            # Extract properties
            props_node = processor.find(".//properties")
            if props_node is not None:
                for entry in props_node.findall("entry"):
                    k = entry.findtext("key")
                    v = entry.findtext("value")
                    if k is not None:
                        proc_info["properties"][k] = v

            processors.append(proc_info)

        # Generate DLT SQL content
        sql_content = create_dlt_sql_from_processors.func(
            processors_json=json.dumps(processors), project_name=project_name
        )

        # Generate pipeline configuration
        pipeline_config = {
            "name": f"{project_name}_dlt_pipeline",
            "storage": f"/pipelines/{project_name}_dlt_pipeline",
            "target": f"{catalog}.{schema_name}",
            "development": True,
            "continuous": True,
            "libraries": [{"notebook": {"path": notebook_path}}],
            "configuration": {
                "spark.sql.adaptive.enabled": "true",
                "spark.sql.adaptive.coalescePartitions.enabled": "true",
            },
        }

        result = {
            "pipeline_config": pipeline_config,
            "sql_notebook_content": sql_content,
            "notebook_path": notebook_path,
            "processor_count": len(processors),
            "migration_type": "dlt_pipeline",
            "continue_required": False,
            "tool_name": "generate_dlt_pipeline_from_nifi",
        }

        return json.dumps(result, indent=2)

    except Exception as e:
        return json.dumps({"error": f"Failed to generate DLT pipeline: {str(e)}"})


@tool
def generate_chunked_dlt_pipeline_from_nifi(
    xml_content: str,
    project_name: str,
    max_processors_per_chunk: int = 25,
    catalog: str = "main",
    schema_name: str = "default",
    notebook_path: str = "/Workspace/Users/me@company.com/dlt_pipeline",
) -> str:
    """
    Generate DLT pipeline from large NiFi XML using chunking approach.

    This handles large workflows by:
    1. Chunking processors while preserving relationships
    2. Generating DLT SQL for each chunk with proper cross-chunk references
    3. Merging chunks into single cohesive DLT pipeline
    4. Maintaining bronze → silver → gold lineage across chunks

    Args:
        xml_content: NiFi XML template content
        project_name: Project name for generated assets
        max_processors_per_chunk: Maximum processors per chunk
        catalog: Unity Catalog name
        schema_name: Schema name within catalog
        notebook_path: Path where DLT notebook will be created

    Returns:
        JSON with chunked DLT pipeline config and SQL notebook content
    """
    try:
        from tools.chunking_tools import chunk_nifi_xml_by_process_groups

        # First, chunk the NiFi XML
        chunking_result_str = chunk_nifi_xml_by_process_groups.func(
            xml_content=xml_content, max_processors_per_chunk=max_processors_per_chunk
        )

        chunking_result = json.loads(chunking_result_str)
        if "error" in chunking_result:
            return json.dumps({"error": f"Chunking failed: {chunking_result['error']}"})

        chunks = chunking_result.get("chunks", [])
        global_connections = chunking_result.get("global_connections", [])

        if not chunks:
            return json.dumps({"error": "No chunks generated from NiFi XML"})

        # Generate DLT SQL for each chunk
        chunk_sql_sections = []
        chunk_table_map = (
            {}
        )  # Maps processor_id → table_name for cross-chunk references
        all_processors = []

        for i, chunk in enumerate(chunks):
            chunk_processors = chunk.get("processors", [])
            if not chunk_processors:
                continue

            # Track all processors for global table naming
            for proc in chunk_processors:
                proc_id = proc.get("id", f"proc_{len(all_processors)}")
                proc_name = proc.get("name", f"processor_{len(all_processors)}")
                table_name = _create_safe_table_name(project_name, proc_name, i)
                chunk_table_map[proc_id] = table_name
                all_processors.append(
                    {**proc, "table_name": table_name, "chunk_index": i}
                )

        # Generate SQL sections for each chunk with cross-chunk awareness
        for i, chunk in enumerate(chunks):
            chunk_processors = chunk.get("processors", [])
            if not chunk_processors:
                continue

            chunk_sql = _generate_chunk_dlt_sql(
                chunk_processors=chunk_processors,
                chunk_index=i,
                project_name=project_name,
                chunk_table_map=chunk_table_map,
                global_connections=global_connections,
            )
            chunk_sql_sections.append(chunk_sql)

        # Combine all chunks into single DLT notebook
        full_sql_content = _merge_chunk_dlt_sql(
            chunk_sql_sections=chunk_sql_sections,
            project_name=project_name,
            total_chunks=len(chunks),
            total_processors=len(all_processors),
        )

        # Generate pipeline configuration
        pipeline_config = {
            "name": f"{project_name}_dlt_pipeline",
            "storage": f"/pipelines/{project_name}_dlt_pipeline",
            "target": f"{catalog}.{schema_name}",
            "development": True,
            "continuous": True,
            "libraries": [{"notebook": {"path": notebook_path}}],
            "configuration": {
                "spark.sql.adaptive.enabled": "true",
                "spark.sql.adaptive.coalescePartitions.enabled": "true",
                "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            },
        }

        result = {
            "pipeline_config": pipeline_config,
            "sql_notebook_content": full_sql_content,
            "notebook_path": notebook_path,
            "processor_count": len(all_processors),
            "chunk_count": len(chunks),
            "migration_type": "chunked_dlt_pipeline",
            "chunking_result": chunking_result,
            "continue_required": False,
            "tool_name": "generate_chunked_dlt_pipeline_from_nifi",
        }

        return json.dumps(result, indent=2)

    except Exception as e:
        return json.dumps(
            {"error": f"Failed to generate chunked DLT pipeline: {str(e)}"}
        )


def _create_safe_table_name(project_name: str, proc_name: str, chunk_index: int) -> str:
    """Create safe table name with chunk prefix."""
    safe_proc_name = (
        proc_name.lower().replace("-", "_").replace(" ", "_").replace(".", "_")
    )
    return f"{project_name}_chunk_{chunk_index:02d}_{safe_proc_name}"


def _generate_chunk_dlt_sql(
    chunk_processors: List[Dict[str, Any]],
    chunk_index: int,
    project_name: str,
    chunk_table_map: Dict[str, str],
    global_connections: List[Dict[str, str]],
) -> str:
    """Generate DLT SQL for a single chunk with cross-chunk references."""

    # Processor type mappings (same as before)
    streaming_sources = {
        "ListenHTTP",
        "ConsumeKafka",
        "ListenTCP",
        "ListenUDP",
        "ListenSyslog",
        "ConsumeJMS",
        "ConsumeMQTT",
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
    }

    transform_processors = {
        "EvaluateJsonPath",
        "UpdateAttribute",
        "ReplaceText",
        "ConvertRecord",
        "SplitText",
        "SplitJson",
        "RouteOnAttribute",
    }

    sql_sections = []
    sql_sections.append(f"# Chunk {chunk_index} - {len(chunk_processors)} processors")

    for proc in chunk_processors:
        proc_type = proc.get("type", "Unknown")
        proc_name = proc.get("name", f"processor_{chunk_index}")
        proc_id = proc.get("id", "")
        properties = proc.get("properties", {})

        # Extract class name from full Java path
        class_name = proc_type.split(".")[-1] if "." in proc_type else proc_type

        # Get table name from global map
        table_name = chunk_table_map.get(
            proc_id, f"{project_name}_chunk_{chunk_index:02d}_{proc_name}"
        )

        # Find upstream processor from global connections
        upstream_table = _find_upstream_table(
            proc_id, global_connections, chunk_table_map
        )

        if class_name in streaming_sources:
            sql_sections.append(
                _generate_streaming_source_sql(table_name, class_name, properties)
            )
        elif class_name in batch_sources:
            sql_sections.append(
                _generate_batch_source_sql(table_name, class_name, properties)
            )
        elif class_name in transform_processors:
            sql_sections.append(
                _generate_transform_sql(
                    table_name, class_name, properties, upstream_table
                )
            )
        else:
            sql_sections.append(
                _generate_generic_sql(
                    table_name, class_name, properties, upstream_table
                )
            )

    return "\n\n".join(sql_sections)


def _find_upstream_table(
    proc_id: str,
    global_connections: List[Dict[str, str]],
    chunk_table_map: Dict[str, str],
) -> str:
    """Find upstream table name for cross-chunk references."""
    for conn in global_connections:
        if conn.get("destination") == proc_id:
            source_id = conn.get("source", "")
            upstream_table = chunk_table_map.get(source_id)
            if upstream_table:
                return upstream_table

    # Default fallback
    return "bronze_source"


def _merge_chunk_dlt_sql(
    chunk_sql_sections: List[str],
    project_name: str,
    total_chunks: int,
    total_processors: int,
) -> str:
    """Merge all chunk SQL into single DLT notebook."""

    header = f"""-- Databricks notebook source
-- Chunked DLT Pipeline generated from NiFi workflow
-- Project: {project_name}
-- Total Chunks: {total_chunks}
-- Total Processors: {total_processors}
-- Generated: Auto-migration from large NiFi XML

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# This DLT pipeline was generated from a large NiFi workflow using chunking
# Each chunk maintains proper lineage with cross-chunk table references
# Bronze → Silver → Gold pattern preserved across all chunks
"""

    # Combine all sections
    all_sections = [header] + chunk_sql_sections

    return "\n\n".join(all_sections)

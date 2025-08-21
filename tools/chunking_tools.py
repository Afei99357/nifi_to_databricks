# tools/chunking_tools.py
# Large NiFi XML file chunking utilities for handling context limitations.

from __future__ import annotations

import json
import os
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set, Tuple

from langchain_core.tools import tool

__all__ = [
    "chunk_nifi_xml_by_process_groups",
    "chunk_large_process_group",
    "reconstruct_full_workflow",
    "estimate_chunk_size",
    "extract_complete_workflow_map",
]


@dataclass
class ProcessorChunk:
    """Represents a chunk of processors with their connections."""

    chunk_id: str
    processors: List[Dict[str, Any]]
    internal_connections: List[Dict[str, Any]]  # connections within this chunk
    external_connections: List[Dict[str, Any]]  # connections to other chunks
    process_group_id: Optional[str] = None


@dataclass
class CrossChunkLink:
    """Represents a connection between different chunks."""

    source_chunk_id: str
    destination_chunk_id: str
    source_processor_id: str
    destination_processor_id: str
    relationships: List[str]


def _estimate_processor_size(processor: Dict[str, Any]) -> int:
    """Estimate the token/character size of a processor for chunking decisions."""
    base_size = 50  # base overhead
    name_size = len(processor.get("name", ""))
    type_size = len(processor.get("type", ""))
    props_size = sum(
        len(str(k)) + len(str(v)) for k, v in processor.get("properties", {}).items()
    )
    return base_size + name_size + type_size + props_size


@tool
def estimate_chunk_size(
    processors: List[Dict[str, Any]], connections: List[Dict[str, Any]]
) -> str:
    """Estimate the total size of a chunk for context planning."""
    total_size = 0
    total_size += sum(_estimate_processor_size(p) for p in processors)
    total_size += sum(len(str(c)) for c in connections)

    return json.dumps(
        {
            "estimated_size": total_size,
            "processor_count": len(processors),
            "connection_count": len(connections),
            "recommendation": (
                "small"
                if total_size < 5000
                else "medium" if total_size < 15000 else "large"
            ),
        }
    )


@tool
def extract_complete_workflow_map(xml_content: str) -> str:
    """
    Extract and save the complete NiFi workflow structure including all processors,
    connections, process groups, and controller services.

    This creates a comprehensive map that can be used to restore proper task
    connectivity after chunked processing.

    Returns:
        JSON with complete workflow structure
    """
    try:
        # Handle both file path and XML content
        if xml_content.strip().startswith("<?xml") or xml_content.strip().startswith(
            "<"
        ):
            root = ET.fromstring(xml_content)
        else:
            if os.path.exists(xml_content):
                with open(xml_content, "r") as f:
                    xml_text = f.read()
                root = ET.fromstring(xml_text)
            else:
                root = ET.fromstring(xml_content)

        # Extract all components
        all_processors, all_connections = _extract_all_processors_and_connections(root)
        process_groups = _group_by_process_groups(root)

        # Extract controller services
        controller_services = {}
        for controller in root.findall(".//controllerServices"):
            cs_id = (controller.findtext("id") or "").strip()
            if cs_id:
                controller_services[cs_id] = {
                    "id": cs_id,
                    "name": (controller.findtext("name") or "Unknown").strip(),
                    "type": (controller.findtext("type") or "Unknown").strip(),
                    "properties": {},
                    "parent_group_id": (
                        controller.findtext("parentGroupId") or ""
                    ).strip(),
                }

                # Extract properties
                props_node = controller.find(".//properties")
                if props_node is not None:
                    for entry in props_node.findall("entry"):
                        key = entry.findtext("key")
                        value = entry.findtext("value")
                        if key is not None:
                            controller_services[cs_id]["properties"][key] = value

        # Extract funnels (flow control elements)
        funnels = {}
        for funnel in root.findall(".//funnels"):
            funnel_id = (funnel.findtext("id") or "").strip()
            if funnel_id:
                funnels[funnel_id] = {
                    "id": funnel_id,
                    "name": (
                        funnel.findtext("name") or f"Funnel_{funnel_id[:8]}"
                    ).strip(),
                    "type": "Funnel",
                    "parent_group_id": (funnel.findtext("parentGroupId") or "").strip(),
                    "inputs": [],  # Will be populated from connections
                    "outputs": [],  # Will be populated from connections
                }

        # Analyze funnel connections and create bypass mappings
        funnel_bypasses = (
            {}
        )  # Maps: {downstream_processor: [list_of_upstream_processors]}

        for conn in all_connections:
            src_id = conn["source"]
            dst_id = conn["destination"]

            # Track funnel inputs
            if dst_id in funnels:
                funnels[dst_id]["inputs"].append(src_id)

            # Track funnel outputs
            if src_id in funnels:
                funnels[src_id]["outputs"].append(dst_id)

        # Create bypass mappings for each funnel
        for funnel_id, funnel_info in funnels.items():
            for output_processor in funnel_info["outputs"]:
                if output_processor not in funnel_bypasses:
                    funnel_bypasses[output_processor] = []
                # All inputs to the funnel become inputs to the output processor
                funnel_bypasses[output_processor].extend(funnel_info["inputs"])

        # Build processor dependency graph (excluding funnels)
        processor_dependencies = {}
        processor_dependents = {}

        for conn in all_connections:
            src_id = conn["source"]
            dst_id = conn["destination"]

            # Skip connections involving funnels - they'll be handled by bypass mappings
            if src_id in funnels or dst_id in funnels:
                continue

            # Track dependencies (what each processor depends on)
            if dst_id not in processor_dependencies:
                processor_dependencies[dst_id] = []
            processor_dependencies[dst_id].append(src_id)

            # Track dependents (what depends on each processor)
            if src_id not in processor_dependents:
                processor_dependents[src_id] = []
            processor_dependents[src_id].append(dst_id)

        # Apply funnel bypass mappings to processor dependencies
        for downstream_proc, upstream_procs in funnel_bypasses.items():
            if downstream_proc not in processor_dependencies:
                processor_dependencies[downstream_proc] = []

            # Add all upstream processors that connect through funnels
            for upstream_proc in upstream_procs:
                if upstream_proc not in funnels:  # Don't add funnels themselves
                    processor_dependencies[downstream_proc].append(upstream_proc)

                    # Also update the upstream processor's dependents
                    if upstream_proc not in processor_dependents:
                        processor_dependents[upstream_proc] = []
                    if downstream_proc not in processor_dependents[upstream_proc]:
                        processor_dependents[upstream_proc].append(downstream_proc)

        # Create complete workflow map
        workflow_map = {
            "processors": all_processors,
            "connections": all_connections,
            "process_groups": process_groups,
            "controller_services": controller_services,
            "funnels": funnels,
            "funnel_bypasses": funnel_bypasses,
            "processor_dependencies": processor_dependencies,
            "processor_dependents": processor_dependents,
            "summary": {
                "total_processors": len(all_processors),
                "total_connections": len(all_connections),
                "total_process_groups": len(process_groups),
                "total_controller_services": len(controller_services),
                "total_funnels": len(funnels),
                "funnels_bypassed": len(funnel_bypasses),
            },
        }

        return json.dumps(workflow_map, indent=2)

    except Exception as e:
        return json.dumps({"error": f"Error extracting workflow map: {str(e)}"})


def _extract_all_processors_and_connections(
    root: ET.Element,
) -> Tuple[Dict[str, Dict], List[Dict]]:
    """Extract all processors and connections from NiFi XML root."""
    processors = {}
    connections = []

    # First, identify all funnels to exclude them from processors
    funnel_ids = set()
    for funnel in root.findall(".//funnels"):
        funnel_id = (funnel.findtext("id") or "").strip()
        if funnel_id:
            funnel_ids.add(funnel_id)

    # Extract processors with their IDs (excluding funnels)
    for processor in root.findall(".//processors"):
        proc_id = (processor.findtext("id") or "").strip()
        if not proc_id or proc_id in funnel_ids:
            continue  # Skip funnels and empty IDs

        proc_info = {
            "id": proc_id,
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

        processors[proc_id] = proc_info

    # Extract connections
    for connection in root.findall(".//connections"):
        source_id = (connection.findtext(".//source/id") or "").strip()
        dest_id = (connection.findtext(".//destination/id") or "").strip()
        if not source_id or not dest_id:
            continue

        rels = [
            (rel.text or "").strip()
            for rel in connection.findall(".//selectedRelationships")
            if rel is not None and rel.text
        ]

        conn_info = {
            "source": source_id,
            "destination": dest_id,
            "relationships": rels,
        }
        connections.append(conn_info)

    return processors, connections


def _group_by_process_groups(root: ET.Element) -> Dict[str, List[str]]:
    """Group processor IDs by their process group."""
    process_groups = {}

    # Handle root-level processors (no explicit process group)
    root_processors = []
    for processor in root.findall(".//processors"):
        proc_id = (processor.findtext("id") or "").strip()
        if proc_id:
            # Check if this processor is inside a processGroup
            parent_group = processor.find("./../../..")
            if parent_group is not None and parent_group.tag == "processGroup":
                group_id = (parent_group.findtext("id") or "").strip()
                if group_id not in process_groups:
                    process_groups[group_id] = []
                process_groups[group_id].append(proc_id)
            else:
                root_processors.append(proc_id)

    # Add root processors as a separate group
    if root_processors:
        process_groups["__root__"] = root_processors

    # Find explicit process groups
    for pg in root.findall(".//processGroup"):
        group_id = (pg.findtext("id") or "").strip()
        if group_id and group_id not in process_groups:
            group_processors = []
            for processor in pg.findall(".//processors"):
                proc_id = (processor.findtext("id") or "").strip()
                if proc_id:
                    group_processors.append(proc_id)
            if group_processors:
                process_groups[group_id] = group_processors

    return process_groups


@tool
def chunk_nifi_xml_by_process_groups(
    xml_content: str, max_processors_per_chunk: int = 25
) -> str:
    """
    Chunk NiFi XML by process groups, preserving graph relationships.

    Returns JSON with chunks and cross-chunk links:
    {
        "chunks": [{"chunk_id": "...", "processors": [...], "internal_connections": [...],
                   "external_connections": [...]}],
        "cross_chunk_links": [{"source_chunk_id": "...", "destination_chunk_id": "...",
                              "source_processor_id": "...", "destination_processor_id": "...",
                              "relationships": [...]}],
        "summary": {"total_processors": N, "total_connections": M, "chunk_count": K}
    }
    """
    try:
        root = ET.fromstring(xml_content)

        # Extract all processors and connections
        all_processors, all_connections = _extract_all_processors_and_connections(root)

        # Group processors by process groups
        process_groups = _group_by_process_groups(root)

        chunks = []
        cross_chunk_links = []
        processor_to_chunk_map = {}  # Maps processor_id to chunk_id

        # Create chunks from process groups
        for group_id, processor_ids in process_groups.items():
            if len(processor_ids) <= max_processors_per_chunk:
                # Single chunk for this process group
                chunk_id = f"chunk_{group_id}"
                chunk_processors = [
                    all_processors[pid]
                    for pid in processor_ids
                    if pid in all_processors
                ]

                # Update processor to chunk mapping
                for pid in processor_ids:
                    processor_to_chunk_map[pid] = chunk_id

                # Will determine connections later
                chunks.append(
                    ProcessorChunk(
                        chunk_id=chunk_id,
                        processors=chunk_processors,
                        internal_connections=[],
                        external_connections=[],
                        process_group_id=group_id if group_id != "__root__" else None,
                    )
                )
            else:
                # Split large process group into multiple chunks
                sub_chunks = _split_large_group(
                    processor_ids, all_processors, max_processors_per_chunk, group_id
                )
                for sub_chunk in sub_chunks:
                    chunks.append(sub_chunk)
                    # Update processor to chunk mapping
                    for proc in sub_chunk.processors:
                        processor_to_chunk_map[proc["id"]] = sub_chunk.chunk_id

        # Now classify connections as internal or external
        for conn in all_connections:
            src_id = conn["source"]
            dst_id = conn["destination"]

            src_chunk = processor_to_chunk_map.get(src_id)
            dst_chunk = processor_to_chunk_map.get(dst_id)

            if src_chunk and dst_chunk:
                if src_chunk == dst_chunk:
                    # Internal connection
                    for chunk in chunks:
                        if chunk.chunk_id == src_chunk:
                            chunk.internal_connections.append(conn)
                            break
                else:
                    # External connection - add to both chunks and create cross-chunk link
                    for chunk in chunks:
                        if chunk.chunk_id == src_chunk:
                            chunk.external_connections.append(conn)
                        elif chunk.chunk_id == dst_chunk:
                            chunk.external_connections.append(conn)

                    cross_chunk_links.append(
                        CrossChunkLink(
                            source_chunk_id=src_chunk,
                            destination_chunk_id=dst_chunk,
                            source_processor_id=src_id,
                            destination_processor_id=dst_id,
                            relationships=conn["relationships"],
                        )
                    )

        # Convert to JSON serializable format
        chunks_json = []
        for chunk in chunks:
            chunks_json.append(
                {
                    "chunk_id": chunk.chunk_id,
                    "processors": chunk.processors,
                    "internal_connections": chunk.internal_connections,
                    "external_connections": chunk.external_connections,
                    "process_group_id": chunk.process_group_id,
                    "processor_count": len(chunk.processors),
                    "internal_connection_count": len(chunk.internal_connections),
                    "external_connection_count": len(chunk.external_connections),
                }
            )

        cross_chunk_links_json = []
        for link in cross_chunk_links:
            cross_chunk_links_json.append(
                {
                    "source_chunk_id": link.source_chunk_id,
                    "destination_chunk_id": link.destination_chunk_id,
                    "source_processor_id": link.source_processor_id,
                    "destination_processor_id": link.destination_processor_id,
                    "relationships": link.relationships,
                }
            )

        result = {
            "chunks": chunks_json,
            "cross_chunk_links": cross_chunk_links_json,
            "summary": {
                "total_processors": len(all_processors),
                "total_connections": len(all_connections),
                "chunk_count": len(chunks),
                "cross_chunk_links_count": len(cross_chunk_links),
            },
        }

        return json.dumps(result, indent=2)

    except ET.ParseError as e:
        return json.dumps({"error": f"XML parsing error: {str(e)}"})
    except Exception as e:
        return json.dumps({"error": f"Unexpected error: {str(e)}"})


def _split_large_group(
    processor_ids: List[str],
    all_processors: Dict[str, Dict],
    max_processors_per_chunk: int,
    group_id: str,
) -> List[ProcessorChunk]:
    """Split a large process group into smaller chunks while trying to preserve locality."""
    chunks = []

    # Simple batching strategy - could be enhanced with graph-aware splitting
    for i in range(0, len(processor_ids), max_processors_per_chunk):
        batch = processor_ids[i : i + max_processors_per_chunk]
        chunk_id = f"chunk_{group_id}_{i // max_processors_per_chunk}"

        chunk_processors = [
            all_processors[pid] for pid in batch if pid in all_processors
        ]

        chunks.append(
            ProcessorChunk(
                chunk_id=chunk_id,
                processors=chunk_processors,
                internal_connections=[],  # Will be populated later
                external_connections=[],  # Will be populated later
                process_group_id=group_id if group_id != "__root__" else None,
            )
        )

    return chunks


@tool
def chunk_large_process_group(
    processors: List[Dict[str, Any]],
    connections: List[Dict[str, Any]],
    max_processors_per_chunk: int = 25,
) -> str:
    """
    Split a large list of processors into smaller chunks while preserving connections.
    Useful for very large process groups that need further subdivision.
    """
    try:
        chunks = []
        processor_ids = [p["id"] for p in processors]

        # Simple batching approach
        for i in range(0, len(processors), max_processors_per_chunk):
            batch_processors = processors[i : i + max_processors_per_chunk]
            batch_ids = set(p["id"] for p in batch_processors)

            # Find internal connections (both endpoints in this batch)
            internal_connections = [
                conn
                for conn in connections
                if conn["source"] in batch_ids and conn["destination"] in batch_ids
            ]

            # Find external connections (one endpoint in this batch)
            external_connections = [
                conn
                for conn in connections
                if (conn["source"] in batch_ids) != (conn["destination"] in batch_ids)
            ]

            chunk = {
                "chunk_id": f"chunk_{i // max_processors_per_chunk}",
                "processors": batch_processors,
                "internal_connections": internal_connections,
                "external_connections": external_connections,
                "processor_count": len(batch_processors),
            }
            chunks.append(chunk)

        result = {
            "chunks": chunks,
            "summary": {
                "original_processor_count": len(processors),
                "original_connection_count": len(connections),
                "chunk_count": len(chunks),
            },
        }

        return json.dumps(result, indent=2)

    except Exception as e:
        return json.dumps({"error": f"Error chunking processors: {str(e)}"})


@tool
def reconstruct_full_workflow(
    chunk_results_json: str, cross_chunk_links_json: str, workflow_map_json: str = None
) -> str:
    """
    Reconstruct a complete Databricks workflow from individual chunk processing results.

    Args:
        chunk_results_json: JSON string list of chunk result dicts
        cross_chunk_links_json: JSON string list describing cross-chunk links
        workflow_map_json: Optional JSON string with complete workflow map for connectivity

    Returns:
        JSON with reconstructed workflow including task dependencies and orchestration
    """
    try:
        # Parse input JSON strings
        try:
            chunk_results = json.loads(chunk_results_json) if chunk_results_json else []
        except Exception:
            chunk_results = []
        try:
            cross_chunk_links = (
                json.loads(cross_chunk_links_json) if cross_chunk_links_json else []
            )
        except Exception:
            cross_chunk_links = []
        try:
            workflow_map = json.loads(workflow_map_json) if workflow_map_json else None
        except Exception:
            workflow_map = None
        # Collect all tasks from chunks, eliminating duplicates by processor ID
        all_tasks = []
        seen_task_ids = set()  # Track unique task/processor IDs
        seen_task_names = (
            set()
        )  # Track unique task names to avoid Databricks key conflicts
        chunk_task_mapping = {}  # Maps chunk_id to list of task names

        for chunk_result in chunk_results:
            chunk_id = chunk_result.get("chunk_id", "unknown")
            tasks = chunk_result.get("tasks", [])

            chunk_task_mapping[chunk_id] = []
            for task in tasks:
                # Use processor ID or task ID to identify unique tasks
                task_id = task.get(
                    "processor_id", task.get("id", task.get("name", "unknown"))
                )
                task_name = task.get("name", task.get("id", "unknown"))

                # If task already processed by another chunk, skip it
                if task_id in seen_task_ids:
                    chunk_task_mapping[chunk_id].append(task_name)
                    continue

                # Ensure task name is unique for Databricks job keys
                original_name = task_name
                counter = 1
                while task_name in seen_task_names:
                    task_name = f"{original_name}_{counter}"
                    counter += 1

                # Update task with unique name if changed
                if task_name != original_name:
                    task = task.copy()  # Don't modify original
                    task["name"] = task_name

                seen_task_ids.add(task_id)
                seen_task_names.add(task_name)
                all_tasks.append(task)
                chunk_task_mapping[chunk_id].append(task_name)

        # Build task dependencies using workflow map if available
        task_dependencies = {}

        # Create a processor ID to task name mapping for precise dependency tracking
        processor_to_task = {}
        for task in all_tasks:
            proc_id = task.get("processor_id", task.get("id"))
            if proc_id:
                processor_to_task[proc_id] = task.get("name")

        # Use complete workflow map if provided for more accurate dependencies
        if workflow_map and "processor_dependencies" in workflow_map:
            processor_dependencies = workflow_map["processor_dependencies"]

            # Map processor dependencies to task dependencies
            for task in all_tasks:
                proc_id = task.get("processor_id", task.get("id"))
                task_name = task.get("name")

                if proc_id in processor_dependencies:
                    # Get all processors this one depends on
                    dependent_proc_ids = processor_dependencies[proc_id]

                    task_deps = []
                    for dep_proc_id in dependent_proc_ids:
                        dep_task_name = processor_to_task.get(dep_proc_id)
                        if dep_task_name and dep_task_name != task_name:
                            task_deps.append(dep_task_name)

                    if task_deps:
                        task_dependencies[task_name] = task_deps
        else:
            # Fallback to cross-chunk links approach
            for link in cross_chunk_links:
                src_processor_id = link.get("source_processor_id")
                dst_processor_id = link.get("destination_processor_id")

                # Find the actual task names for the connected processors
                src_task_name = processor_to_task.get(src_processor_id)
                dst_task_name = processor_to_task.get(dst_processor_id)

                # Only create dependency if both tasks exist and are different
                if src_task_name and dst_task_name and src_task_name != dst_task_name:
                    if dst_task_name not in task_dependencies:
                        task_dependencies[dst_task_name] = []
                    if src_task_name not in task_dependencies[dst_task_name]:
                        task_dependencies[dst_task_name].append(src_task_name)

        # Additional validation: remove any circular dependencies
        def has_circular_dependency(task, deps, visited=None):
            if visited is None:
                visited = set()
            if task in visited:
                return True
            visited.add(task)
            for dep in deps:
                if dep in task_dependencies:
                    if has_circular_dependency(
                        dep, task_dependencies[dep], visited.copy()
                    ):
                        return True
            return False

        # Remove circular dependencies by removing problematic edges
        cleaned_dependencies = {}
        for task, deps in task_dependencies.items():
            clean_deps = []
            for dep in deps:
                temp_deps = clean_deps + [dep]
                if not has_circular_dependency(task, temp_deps):
                    clean_deps.append(dep)
            if clean_deps:
                cleaned_dependencies[task] = clean_deps

        task_dependencies = cleaned_dependencies

        # Create final workflow structure
        workflow = {
            "tasks": all_tasks,
            "task_dependencies": task_dependencies,
            "chunk_count": len(chunk_results),
            "cross_chunk_links_count": len(cross_chunk_links),
            "orchestration_strategy": "databricks_multi_task_job",
        }

        # Generate Databricks job configuration with proper task dependencies
        job_config = _generate_multi_task_job_config(workflow)
        workflow["databricks_job_config"] = job_config

        return json.dumps(workflow, indent=2)

    except Exception as e:
        return json.dumps({"error": f"Error reconstructing workflow: {str(e)}"})


def _generate_multi_task_job_config(workflow: Dict[str, Any]) -> Dict[str, Any]:
    """Generate Databricks multi-task job configuration from workflow."""
    tasks = []

    for task in workflow["tasks"]:
        task_name = task.get("name", task.get("id", "unknown"))
        dependencies = workflow["task_dependencies"].get(task_name, [])

        databricks_task = {
            "task_key": task_name,
            "notebook_task": {
                "notebook_path": task.get(
                    "notebook_path", "/Workspace/generated_tasks/" + task_name
                ),
                "source": "WORKSPACE",
            },
            "depends_on": (
                [{"task_key": dep} for dep in dependencies] if dependencies else []
            ),
        }

        # Add cluster configuration if specified in task
        if task.get("cluster_config"):
            databricks_task["new_cluster"] = task["cluster_config"]

        tasks.append(databricks_task)

    return {
        "name": "reconstructed_nifi_workflow",
        "tasks": tasks,
        "max_concurrent_runs": 1,
        "timeout_seconds": 3600,
    }

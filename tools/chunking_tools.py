# tools/chunking_tools.py
# Large NiFi XML file chunking utilities for handling context limitations.

from __future__ import annotations

import json
import xml.etree.ElementTree as ET
from typing import Any, Dict, List, Set, Tuple, Optional
from dataclasses import dataclass
from langchain_core.tools import tool

__all__ = [
    "chunk_nifi_xml_by_process_groups",
    "chunk_large_process_group", 
    "reconstruct_full_workflow",
    "estimate_chunk_size"
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
    props_size = sum(len(str(k)) + len(str(v)) for k, v in processor.get("properties", {}).items())
    return base_size + name_size + type_size + props_size

@tool
def estimate_chunk_size(processors: List[Dict[str, Any]], connections: List[Dict[str, Any]]) -> str:
    """Estimate the total size of a chunk for context planning."""
    total_size = 0
    total_size += sum(_estimate_processor_size(p) for p in processors)
    total_size += sum(len(str(c)) for c in connections)
    
    return json.dumps({
        "estimated_size": total_size,
        "processor_count": len(processors),
        "connection_count": len(connections),
        "recommendation": "small" if total_size < 5000 else "medium" if total_size < 15000 else "large"
    })

def _extract_all_processors_and_connections(root: ET.Element) -> Tuple[Dict[str, Dict], List[Dict]]:
    """Extract all processors and connections from NiFi XML root."""
    processors = {}
    connections = []
    
    # Extract processors with their IDs
    for processor in root.findall(".//processors"):
        proc_id = (processor.findtext("id") or "").strip()
        if not proc_id:
            continue
            
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
def chunk_nifi_xml_by_process_groups(xml_content: str, max_processors_per_chunk: int = 25) -> str:
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
                chunk_processors = [all_processors[pid] for pid in processor_ids if pid in all_processors]
                
                # Update processor to chunk mapping
                for pid in processor_ids:
                    processor_to_chunk_map[pid] = chunk_id
                
                # Will determine connections later
                chunks.append(ProcessorChunk(
                    chunk_id=chunk_id,
                    processors=chunk_processors,
                    internal_connections=[],
                    external_connections=[],
                    process_group_id=group_id if group_id != "__root__" else None
                ))
            else:
                # Split large process group into multiple chunks
                sub_chunks = _split_large_group(processor_ids, all_processors, max_processors_per_chunk, group_id)
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
                    
                    cross_chunk_links.append(CrossChunkLink(
                        source_chunk_id=src_chunk,
                        destination_chunk_id=dst_chunk,
                        source_processor_id=src_id,
                        destination_processor_id=dst_id,
                        relationships=conn["relationships"]
                    ))
        
        # Convert to JSON serializable format
        chunks_json = []
        for chunk in chunks:
            chunks_json.append({
                "chunk_id": chunk.chunk_id,
                "processors": chunk.processors,
                "internal_connections": chunk.internal_connections,
                "external_connections": chunk.external_connections,
                "process_group_id": chunk.process_group_id,
                "processor_count": len(chunk.processors),
                "internal_connection_count": len(chunk.internal_connections),
                "external_connection_count": len(chunk.external_connections)
            })
        
        cross_chunk_links_json = []
        for link in cross_chunk_links:
            cross_chunk_links_json.append({
                "source_chunk_id": link.source_chunk_id,
                "destination_chunk_id": link.destination_chunk_id,
                "source_processor_id": link.source_processor_id,
                "destination_processor_id": link.destination_processor_id,
                "relationships": link.relationships
            })
        
        result = {
            "chunks": chunks_json,
            "cross_chunk_links": cross_chunk_links_json,
            "summary": {
                "total_processors": len(all_processors),
                "total_connections": len(all_connections),
                "chunk_count": len(chunks),
                "cross_chunk_links_count": len(cross_chunk_links)
            }
        }
        
        return json.dumps(result, indent=2)
        
    except ET.ParseError as e:
        return json.dumps({"error": f"XML parsing error: {str(e)}"})
    except Exception as e:
        return json.dumps({"error": f"Unexpected error: {str(e)}"})

def _split_large_group(processor_ids: List[str], all_processors: Dict[str, Dict], 
                      max_processors_per_chunk: int, group_id: str) -> List[ProcessorChunk]:
    """Split a large process group into smaller chunks while trying to preserve locality."""
    chunks = []
    
    # Simple batching strategy - could be enhanced with graph-aware splitting
    for i in range(0, len(processor_ids), max_processors_per_chunk):
        batch = processor_ids[i:i + max_processors_per_chunk]
        chunk_id = f"chunk_{group_id}_{i // max_processors_per_chunk}"
        
        chunk_processors = [all_processors[pid] for pid in batch if pid in all_processors]
        
        chunks.append(ProcessorChunk(
            chunk_id=chunk_id,
            processors=chunk_processors,
            internal_connections=[],  # Will be populated later
            external_connections=[],  # Will be populated later
            process_group_id=group_id if group_id != "__root__" else None
        ))
    
    return chunks

@tool
def chunk_large_process_group(processors: List[Dict[str, Any]], connections: List[Dict[str, Any]], 
                            max_processors_per_chunk: int = 25) -> str:
    """
    Split a large list of processors into smaller chunks while preserving connections.
    Useful for very large process groups that need further subdivision.
    """
    try:
        chunks = []
        processor_ids = [p["id"] for p in processors]
        
        # Simple batching approach
        for i in range(0, len(processors), max_processors_per_chunk):
            batch_processors = processors[i:i + max_processors_per_chunk]
            batch_ids = set(p["id"] for p in batch_processors)
            
            # Find internal connections (both endpoints in this batch)
            internal_connections = [
                conn for conn in connections 
                if conn["source"] in batch_ids and conn["destination"] in batch_ids
            ]
            
            # Find external connections (one endpoint in this batch)
            external_connections = [
                conn for conn in connections
                if (conn["source"] in batch_ids) != (conn["destination"] in batch_ids)
            ]
            
            chunk = {
                "chunk_id": f"chunk_{i // max_processors_per_chunk}",
                "processors": batch_processors,
                "internal_connections": internal_connections,
                "external_connections": external_connections,
                "processor_count": len(batch_processors)
            }
            chunks.append(chunk)
        
        result = {
            "chunks": chunks,
            "summary": {
                "original_processor_count": len(processors),
                "original_connection_count": len(connections),
                "chunk_count": len(chunks)
            }
        }
        
        return json.dumps(result, indent=2)
        
    except Exception as e:
        return json.dumps({"error": f"Error chunking processors: {str(e)}"})

@tool
def reconstruct_full_workflow(chunk_results: List[Dict[str, Any]], cross_chunk_links: List[Dict[str, Any]]) -> str:
    """
    Reconstruct a complete Databricks workflow from individual chunk processing results.
    
    Args:
        chunk_results: List of processing results from agent for each chunk
        cross_chunk_links: Cross-chunk connection information from chunking
        
    Returns:
        JSON with reconstructed workflow including task dependencies and orchestration
    """
    try:
        # Collect all tasks from chunks, eliminating duplicates by processor ID
        all_tasks = []
        seen_task_ids = set()  # Track unique task/processor IDs
        seen_task_names = set()  # Track unique task names to avoid Databricks key conflicts
        chunk_task_mapping = {}  # Maps chunk_id to list of task names
        
        for chunk_result in chunk_results:
            chunk_id = chunk_result.get("chunk_id", "unknown")
            tasks = chunk_result.get("tasks", [])
            
            chunk_task_mapping[chunk_id] = []
            for task in tasks:
                # Use processor ID or task ID to identify unique tasks
                task_id = task.get("processor_id", task.get("id", task.get("name", "unknown")))
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
        
        # Build task dependencies based on cross-chunk links
        task_dependencies = {}
        for link in cross_chunk_links:
            src_chunk = link["source_chunk_id"]
            dst_chunk = link["destination_chunk_id"]
            
            # All tasks in destination chunk depend on all tasks in source chunk
            src_tasks = chunk_task_mapping.get(src_chunk, [])
            dst_tasks = chunk_task_mapping.get(dst_chunk, [])
            
            for dst_task in dst_tasks:
                if dst_task not in task_dependencies:
                    task_dependencies[dst_task] = []
                task_dependencies[dst_task].extend(src_tasks)
        
        # Remove duplicates and self-dependencies
        for task, deps in task_dependencies.items():
            task_dependencies[task] = list(set(dep for dep in deps if dep != task))
        
        # Create final workflow structure
        workflow = {
            "tasks": all_tasks,
            "task_dependencies": task_dependencies,
            "chunk_count": len(chunk_results),
            "cross_chunk_links_count": len(cross_chunk_links),
            "orchestration_strategy": "databricks_multi_task_job"
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
                "notebook_path": task.get("notebook_path", "/Workspace/generated_tasks/" + task_name),
                "source": "WORKSPACE"
            },
            "depends_on": [{"task_key": dep} for dep in dependencies] if dependencies else []
        }
        
        # Add cluster configuration if specified in task
        if task.get("cluster_config"):
            databricks_task["new_cluster"] = task["cluster_config"]
        
        tasks.append(databricks_task)
    
    return {
        "name": "reconstructed_nifi_workflow",
        "tasks": tasks,
        "max_concurrent_runs": 1,
        "timeout_seconds": 3600
    }
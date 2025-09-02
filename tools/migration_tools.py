# tools/migration_tools.py
# NiFi → Databricks conversion orchestrators and flow utilities.

from __future__ import annotations

import json
import os
import re
import textwrap
import xml.etree.ElementTree as ET
from collections import defaultdict, deque
from pathlib import Path
from typing import Any, Dict, List

from databricks_langchain import ChatDatabricks
from json_repair import repair_json

from config import DATABRICKS_HOSTNAME, logger

# Registry removed - generating fresh each time
from utils import read_text as _read_text
from utils import safe_name as _safe_name
from utils import write_text as _write_text

# Removed langchain_core.tools import - no longer using # Removed @tool decorator - direct function call approach decorator


def _generate_task_id(
    proc_name: str, proc_type: str, fallback_prefix: str, idx: int
) -> str:
    """
    Generate descriptive task ID from processor name.

    Args:
        proc_name: NiFi processor display name
        proc_type: Processor type (for fallback)
        fallback_prefix: Prefix for fallback (chunk_id, subset_id, etc.)
        idx: Index for uniqueness

    Returns:
        Descriptive task ID like "BQ_Tracing_Comp_Add_Queries" or "ExecuteStreamCommand_0"
    """
    if proc_name and proc_name != "Unknown":
        # Use processor name, sanitized for task naming
        task_id = _safe_name(proc_name)
        # Ensure it's not too long for Databricks task names
        if len(task_id) > 80:
            task_id = task_id[:80]
        return task_id
    else:
        # Fallback to type + index
        proc_class = proc_type.split(".")[-1] if "." in proc_type else proc_type
        return f"{proc_class}_{idx}"


def _unescape_code(code: str) -> str:
    """
    Unescape literal escape sequences from LLM-generated code.
    Converts \\n to actual newlines, \\t to tabs, etc.
    """
    if not code:
        return code

    # Handle common escape sequences
    unescaped = code.encode().decode("unicode_escape")
    return unescaped


# Chunking tools removed - no longer needed with intelligent batching
from tools.generator_tools import generate_databricks_code
from tools.job_tools import (
    check_job_run_status,
    deploy_and_run_job,
    scaffold_asset_bundle,
)
from utils import extract_nifi_parameters_and_services_impl

# Default max processors per chunk (batch size) via env var
MAX_PROCS_PER_CHUNK_DEFAULT = int(os.environ.get("MAX_PROCESSORS_PER_CHUNK", "20"))


def _generate_batch_processor_code(
    processors: List[Dict[str, Any]], chunk_id: str, project: str
) -> List[Dict[str, Any]]:
    """
    Generate Databricks code for multiple processors in a single LLM call to reduce API requests.
    Uses built-in patterns when available, only sends remaining processors to LLM.
    """
    try:
        print(
            f"🧠 [LLM BATCH] Generating code for {len(processors)} processors in {chunk_id}"
        )

        # Import the builtin pattern function
        from tools.generator_tools import _get_builtin_pattern

        # Separate processors into those with built-in patterns and those needing LLM generation
        builtin_tasks = []
        llm_needed_processors = []
        processor_specs = []
        processor_types = []

        for idx, processor in enumerate(processors):
            # Get processor identification - prefer original type, fallback to classification info
            proc_type = processor.get(
                "type", processor.get("processor_type", "Unknown")
            )
            proc_name = processor.get("name", f"processor_{idx}")
            props = processor.get("properties", {})

            # Include business context from analysis for better code generation
            classification = processor.get("classification", "unknown")
            reasoning = processor.get("reasoning", "")
            business_purpose = processor.get("business_purpose", "")

            # Build workflow context for this processor
            workflow_context = {
                "processor_index": idx,
                "processor_name": proc_name,
                "previous_processors": processors[
                    :idx
                ],  # All processors before this one
                "chunk_id": chunk_id,
                "project": project,
            }

            # Check if there's a built-in pattern for this processor
            pattern = _get_builtin_pattern(proc_type, props, workflow_context)

            if pattern["code"]:  # Built-in pattern available
                # Use built-in pattern with Unity Catalog conversion
                class_name = proc_type.split(".")[-1] if "." in proc_type else proc_type
                processor_types.append(f"{class_name} (built-in)")

                task = {
                    "id": processor.get(
                        "id", _generate_task_id(proc_name, proc_type, chunk_id, idx)
                    ),
                    "name": _safe_name(proc_name),
                    "type": proc_type,
                    "code": pattern["code"],
                    "properties": props,
                    "chunk_id": chunk_id,
                    "processor_index": idx,
                }
                builtin_tasks.append(task)
            else:  # No built-in pattern, needs LLM generation
                class_name = proc_type.split(".")[-1] if "." in proc_type else proc_type
                processor_types.append(f"{class_name} (LLM)")

                llm_needed_processors.append(processor)
                processor_specs.append(
                    {
                        "index": idx,
                        "type": proc_type,
                        "name": proc_name,
                        "properties": props,
                        "id": processor.get(
                            "id", _generate_task_id(proc_name, proc_type, chunk_id, idx)
                        ),
                        "classification": classification,
                        "business_purpose": business_purpose or reasoning,
                        "analysis_reasoning": reasoning,
                        "workflow_context": workflow_context,
                    }
                )

        print(f"🔍 [LLM BATCH] Processor types: {', '.join(processor_types)}")

        # If all processors have built-in patterns, return early
        if not llm_needed_processors:
            print(
                f"✨ [LLM BATCH] All {len(processors)} processors used built-in patterns"
            )
            return builtin_tasks

        # Get batch size from environment (same as processor classification)
        max_batch_size = int(os.environ.get("MAX_PROCESSORS_PER_CHUNK", "20"))

        # Ensure max_batch_size is valid
        if max_batch_size <= 0:
            print(
                f"⚠️ Invalid MAX_PROCESSORS_PER_CHUNK: {max_batch_size}, using default of 20"
            )
            max_batch_size = 20

        print(f"⚙️ [LLM BATCH] Using batch size: {max_batch_size}")

        # Process LLM processors in batches to avoid JSON parsing issues
        if len(llm_needed_processors) <= max_batch_size:
            # Single batch - current behavior
            print(
                f"🚀 [LLM BATCH] Sending {len(llm_needed_processors)} processors to LLM..."
            )
            llm_tasks = _process_single_llm_batch(
                llm_needed_processors, processor_specs, chunk_id, project
            )
            builtin_tasks.extend(llm_tasks)
        else:
            # Multiple batches - new chunked approach
            print(
                f"📦 [LLM CHUNKING] Splitting {len(llm_needed_processors)} processors into batches of {max_batch_size}..."
            )

            for i in range(0, len(llm_needed_processors), max_batch_size):
                batch_processors = llm_needed_processors[i : i + max_batch_size]
                batch_specs = processor_specs[i : i + max_batch_size]
                batch_num = (i // max_batch_size) + 1
                total_batches = (
                    len(llm_needed_processors) + max_batch_size - 1
                ) // max_batch_size

                print(
                    f"🚀 [LLM BATCH {batch_num}/{total_batches}] Sending {len(batch_processors)} processors to LLM..."
                )

                batch_tasks = _process_single_llm_batch(
                    batch_processors,
                    batch_specs,
                    f"{chunk_id}_batch_{batch_num}",
                    project,
                )
                builtin_tasks.extend(batch_tasks)

                print(
                    f"✅ [LLM BATCH {batch_num}/{total_batches}] Completed {len(batch_tasks)} tasks"
                )

        return builtin_tasks

    except Exception as e:
        logger.error(f"Batch code generation failed: {e}")
        # Fall back to processing each batch individually
        return []


def _process_single_llm_batch(
    processors: List[Dict[str, Any]],
    processor_specs: List[Dict[str, Any]],
    chunk_id: str,
    project: str,
) -> List[Dict[str, Any]]:
    """Process a single batch of processors with LLM generation"""
    try:

        # Get the model endpoint from environment
        model_endpoint = os.environ.get(
            "MODEL_ENDPOINT", "databricks-meta-llama-3-3-70b-instruct"
        )

        # Create LLM with very low temperature for consistent JSON output
        llm_json = ChatDatabricks(endpoint=model_endpoint, temperature=0.05)

        # Enhanced prompt that forces JSON-only output
        json_enforced_prompt = f"""RESPOND WITH ONLY VALID JSON. NO MARKDOWN. NO EXPLANATIONS. NO TEXT BEFORE OR AFTER THE JSON.

You are a NiFi to Databricks migration expert. Generate PySpark code for the processors listed below.

PROCESSORS TO CONVERT:
{json.dumps(processor_specs, indent=2)}

IMPORTANT: This is a BATCH ETL workflow (not streaming). Generate batch processing code.

REQUIREMENTS:
1. Return processor index as string key, PySpark code as string value
2. Use Databricks BATCH patterns (Delta Lake, regular DataFrame operations)
3. Include comments in the code explaining the logic
4. For GetFile/ListFile: use spark.read (NOT streaming/Auto Loader) for one-time file processing
5. For PutFile/PutHDFS: use Delta Lake batch writes
6. For ExecuteStreamCommand: use subprocess or shell commands
7. For JSON processors: use PySpark JSON functions
8. CRITICAL - CONTEXT-AWARE DATAFRAMES:
   - Check workflow_context.previous_processors to understand data flow
   - For source processors: Create df_[clean_processor_name] as output
   - For processing processors: Use previous processor's DataFrame as input
   - NEVER use undefined 'df' - always create or reference specific DataFrame variables
9. DATA PASSING: Save results to intermediate Delta tables for next job task to read
10. LEGACY HDFS PATH CONVERSION (CRITICAL):
    - If you see paths starting with /user/, /hdfs/, /tmp/, /data/, /var/ in PutFile/PutHDFS
    - These are LEGACY HDFS paths that must be converted to Unity Catalog
    - Generate code like: target_table = 'main.default.table_name'  # TODO: Update catalog.schema.table
    - Use .saveAsTable(target_table) instead of .save('/legacy/path')
    - Add TODO comments explaining the conversion from legacy path

CRITICAL: Your response must be ONLY a JSON object. Start with {{ and end with }}.

CRITICAL JSON ESCAPE RULES (MUST FOLLOW EXACTLY):
- Use \\n for newlines (double backslash + n)
- Use \\" for quotes (backslash + quote)
- Use \\\\ for literal backslashes (four backslashes)
- For SQL queries: Replace ALL internal quotes with \\"
- For multi-line SQL: Use \\n instead of actual newlines
- NEVER use triple quotes in JSON values
- NEVER use unescaped $ signs - use \\$ instead
- Replace \\t with \\\\t and \\r with \\\\r
- For paths like /etc/security: use /etc/security (no escaping needed)
- For log messages with quotes: use \\" for each quote
- DO NOT include multiple JSON objects - return ONE object only

EXAMPLES OF CORRECT JSON ESCAPING:
{{"0": "# LogMessage → Warning Log\\nprint(\\"Warning: Analysis failed\\")\\nlog_msg = \\"Failed to open file\\"", "1": "# ExecuteStreamCommand → Kinit\\ncommand = \\"/bin/kinit -kt /etc/security/keytab/file\\"\\nresult = subprocess.run(command, shell=True)"}}

VALIDATE: Your JSON must parse correctly. Test before responding.

GENERATE JSON FOR ALL {len(processor_specs)} PROCESSORS:"""

        response = llm_json.invoke(json_enforced_prompt)
        print(f"✅ [LLM BATCH] Received response, parsing generated code...")

        # Log the first 200 chars of response for debugging
        def clean_json_escape_sequences(content: str) -> str:
            """Fix common escape sequence issues in JSON strings"""
            # Extract JSON object boundaries first
            start = content.find("{")
            end = content.rfind("}") + 1
            if start >= 0 and end > start:
                content = content[start:end]

            # Fix specific escape sequence patterns that cause issues
            # 1. Fix unescaped single quotes in string values
            content = re.sub(r"(:\\s*\"[^\"]*)'([^\"]*\")", r"\1\\'\\2", content)

            # 2. Fix unescaped dollar signs in ${...} patterns
            content = re.sub(r"(?<!\\)\$\{", r"\\${", content)

            # 3. Fix path separators that aren't properly escaped
            content = re.sub(r'(?<!\\)\\(?![\\"/nrt])', r"\\\\", content)

            # 4. Fix newline sequences that should be escaped
            content = (
                content.replace("\n", "\\n").replace("\t", "\\t").replace("\r", "\\r")
            )

            return content

        response_preview = response.content[:200].replace("\n", "\\n")
        print(f"🔍 [LLM BATCH] Response preview: {response_preview}...")

        try:
            generated_code_map = json.loads(response.content.strip())
            print(
                f"🎯 [LLM BATCH] Successfully parsed {len(generated_code_map)} code snippets"
            )
        except json.JSONDecodeError as e:
            print(f"⚠️  [LLM BATCH] JSON parsing failed: {e}")
            content = response.content.strip()
            generated_code_map = None

            # Try various recovery strategies in order
            recovery_strategies = [
                ("json-repair", lambda c: repair_json(c)),
                (
                    "markdown extraction",
                    lambda c: (
                        repair_json(c.split("```json")[1].split("```")[0].strip())
                        if "```json" in c
                        else None
                    ),
                ),
                (
                    "boundary detection",
                    lambda c: (
                        repair_json(c[c.find("{") : c.rfind("}") + 1])
                        if c.find("{") >= 0 and c.rfind("}") > c.find("{")
                        else None
                    ),
                ),
                ("escape sequence cleaning", clean_json_escape_sequences),
            ]

            for strategy_name, strategy_func in recovery_strategies:
                if generated_code_map is not None:
                    break
                try:
                    repaired_content = strategy_func(content)
                    if repaired_content:
                        generated_code_map = json.loads(repaired_content)
                        print(f"🔧 [LLM BATCH] Recovered JSON using {strategy_name}")
                except (json.JSONDecodeError, ValueError, IndexError):
                    continue

            # If all recovery fails, fall back to individual generation
            if generated_code_map is None:
                print(
                    f"❌ [LLM BATCH] All JSON recovery attempts failed, falling back to individual generation"
                )
                raise e

        # Build tasks from the batch response
        generated_tasks = []
        for spec in processor_specs:
            idx = spec["index"]
            code = generated_code_map.get(
                str(idx),
                f"# {spec['type']} → Code generation failed\\n# TODO: Implement manually",
            )

            task = {
                "id": spec["id"],
                "name": _safe_name(spec["name"]),
                "type": spec["type"],
                "code": code,
                "properties": spec["properties"],
                "chunk_id": chunk_id,
                "processor_index": idx,
            }
            generated_tasks.append(task)

        print(f"✨ [LLM BATCH] Generated {len(generated_tasks)} tasks for {chunk_id}")
        return generated_tasks

    except Exception as e:
        logger.error(f"Single batch code generation failed: {e}")
        # Sub-batch fallback to reduce per-processor LLM calls
        sub_batch_size = int(os.environ.get("LLM_SUB_BATCH_SIZE", "10"))

        if len(processors) > sub_batch_size:
            print(
                f"🔄 [LLM SUB-BATCH] Splitting {len(processors)} into sub-batches of {sub_batch_size}..."
            )
            generated_tasks: List[Dict[str, Any]] = []
            for start in range(0, len(processors), sub_batch_size):
                subset_processors = processors[start : start + sub_batch_size]
                subset_specs = processor_specs[start : start + sub_batch_size]
                subset_id = f"{chunk_id}__sb{start // sub_batch_size}"
                try:
                    # Recursive call to _process_single_llm_batch for sub-batch
                    sub_tasks = _process_single_llm_batch(
                        subset_processors, subset_specs, subset_id, project
                    )
                    generated_tasks.extend(sub_tasks)
                except Exception:
                    # If sub-batch still fails, fall back to per-processor for this subset only
                    for idx, processor in enumerate(subset_processors):
                        proc_type = processor.get("type", "Unknown")
                        proc_name = processor.get("name", f"processor_{start+idx}")
                        props = processor.get("properties", {})
                        try:
                            code = generate_databricks_code(
                                processor_type=proc_type,
                                properties=json.dumps(props),
                            )
                        except Exception:
                            code = f"""# {proc_type} → Fallback Template
# Properties: {json.dumps(props, indent=2)}

df = spark.read.format('delta').load('/path/to/input')
# TODO: Implement {proc_type} logic
df.write.format('delta').mode('append').save('/path/to/output')
"""
                        task = {
                            "id": subset_specs[idx]["id"],
                            "name": _safe_name(proc_name),
                            "type": proc_type,
                            "code": code,
                            "properties": props,
                            "chunk_id": subset_id,
                            "processor_index": start + idx,
                        }
                        generated_tasks.append(task)
            return generated_tasks
        else:
            # Final fallback: individual generation for small sets
            print(
                f"🔄 [LLM INDIVIDUAL] Falling back to individual generation for {len(processors)} processors..."
            )
            generated_tasks = []
            for idx, processor in enumerate(processors):
                proc_spec = processor_specs[idx]
                proc_type = processor.get("type", "Unknown")
                proc_name = processor.get("name", f"processor_{idx}")
                props = processor.get("properties", {})

                try:
                    code = generate_databricks_code(
                        processor_type=proc_type,
                        properties=json.dumps(props),
                    )
                except Exception:
                    code = f"""# {proc_type} → Fallback Template
# Properties: {json.dumps(props, indent=2)}

df = spark.read.format('delta').load('/path/to/input')
# TODO: Implement {proc_type} logic
df.write.format('delta').mode('append').save('/path/to/output')
"""
                task = {
                    "id": proc_spec["id"],
                    "name": _safe_name(proc_name),
                    "type": proc_type,
                    "code": code,
                    "properties": props,
                    "chunk_id": chunk_id,
                    "processor_index": idx,
                }
                generated_tasks.append(task)
            return generated_tasks


def _default_notebook_path(project: str) -> str:
    user = os.environ.get("WORKSPACE_USER") or os.environ.get("USER_EMAIL") or "Shared"
    base = f"/Users/{user}" if "@" in user else "/Shared"
    proj_name = _safe_name(project)
    return f"{base}/{proj_name}/main"


def _get_job_url(job_id: str) -> str:
    """Generate correct Databricks job URL using workspace hostname."""
    if DATABRICKS_HOSTNAME:
        # Remove trailing slash and ensure proper format
        base_url = DATABRICKS_HOSTNAME.rstrip("/")
        return f"{base_url}/#job/{job_id}"
    else:
        # Try alternative environment variable names or debug
        alt_hostname = os.environ.get("DATABRICKS_HOST")
        if alt_hostname:
            base_url = alt_hostname.rstrip("/")
            return f"{base_url}/#job/{job_id}"
        else:
            # Log for debugging
            print("⚠️  [DEBUG] DATABRICKS_HOSTNAME not found, using fallback URL")
            # Fallback to generic URL if hostname not configured
            return f"https://databricks.com/#job/{job_id}"


__all__ = [
    "build_migration_plan",
    "orchestrate_focused_nifi_migration",
    "process_nifi_chunk",
]


# Removed @tool decorator - direct function call approach
def build_migration_plan(xml_content: str) -> str:
    """
    Produce a topologically sorted DAG of NiFi processors based on Connections.

    Args:
        xml_content: Either XML content as string OR file path to XML file

    Returns JSON:
      {
        "tasks": [{"id": "...", "name": "...", "type": "..."}, ...],
        "edges": [["src_id","dst_id"], ...],
        "note": "..."
      }
    """
    try:
        # Check if input is a file path or XML content
        if xml_content.strip().startswith("<?xml") or xml_content.strip().startswith(
            "<"
        ):
            # Input is XML content
            root = ET.fromstring(xml_content)
        else:
            # Input is likely a file path
            if os.path.exists(xml_content):
                with open(xml_content, "r") as f:
                    xml_text = f.read()
                root = ET.fromstring(xml_text)
            else:
                # Try parsing as XML content anyway
                root = ET.fromstring(xml_content)

        # id → meta
        procs: Dict[str, Dict[str, Any]] = {}
        for pr in root.findall(".//processors"):
            pid = (pr.findtext("id") or "").strip()
            procs[pid] = {
                "id": pid,
                "name": (pr.findtext("name") or pid).strip(),
                "type": (pr.findtext("type") or "Unknown").strip(),
            }

        edges: List[List[str]] = []
        for conn in root.findall(".//connections"):
            src = (conn.findtext(".//source/id") or "").strip()
            dst = (conn.findtext(".//destination/id") or "").strip()
            if src and dst:
                edges.append([src, dst])

        # Kahn's algorithm for topo order
        indeg = defaultdict(int)
        graph: Dict[str, List[str]] = defaultdict(list)
        for s, d in edges:
            graph[s].append(d)
            indeg[d] += 1
            if s not in indeg:
                indeg[s] += 0

        q = deque([n for n in indeg if indeg[n] == 0])
        ordered: List[Dict[str, Any]] = []
        while q:
            n = q.popleft()
            if n in procs:
                ordered.append(procs[n])
            for v in graph[n]:
                indeg[v] -= 1
                if indeg[v] == 0:
                    q.append(v)

        plan = {
            "tasks": ordered,
            "edges": edges,
            "note": "Use this order to compose Jobs tasks or DLT dependencies",
        }
        return json.dumps(plan, indent=2)
    except Exception as e:
        return f"Failed building plan: {e}"


# Removed @tool decorator - direct function call approach
def process_nifi_chunk(chunk_data: str, project: str, chunk_index: int = 0) -> str:
    """
    Process a single NiFi chunk and generate Databricks code for its processors.

    Args:
        chunk_data: JSON string containing chunk information (processors, connections)
        project: Project name for context
        chunk_index: Index of this chunk for naming

    Returns:
        JSON with generated code and task information for this chunk
    """
    try:
        chunk = json.loads(chunk_data)
        processors = chunk.get("processors", [])
        internal_connections = chunk.get("internal_connections", [])
        external_connections = chunk.get("external_connections", [])
        chunk_id = chunk.get("chunk_id", f"chunk_{chunk_index}")

        generated_tasks = []

        # Generate code for all processors in a single batched LLM call
        generated_tasks = _generate_batch_processor_code(processors, chunk_id, project)

        # Analyze connections for task ordering within chunk
        task_dependencies = {}
        processor_id_to_task = {task["id"]: task["name"] for task in generated_tasks}

        for conn in internal_connections:
            src_id = conn["source"]
            dst_id = conn["destination"]

            if src_id in processor_id_to_task and dst_id in processor_id_to_task:
                dst_task = processor_id_to_task[dst_id]
                src_task = processor_id_to_task[src_id]

                if dst_task not in task_dependencies:
                    task_dependencies[dst_task] = []
                task_dependencies[dst_task].append(src_task)

        result = {
            "chunk_id": chunk_id,
            "tasks": generated_tasks,
            "internal_task_dependencies": task_dependencies,
            "external_connections": external_connections,
            "processor_count": len(processors),
            "task_count": len(generated_tasks),
            "project": project,
            "continue_required": False,
            "tool_name": "process_nifi_chunk",
        }

        return json.dumps(result, indent=2)

    except Exception as e:
        return json.dumps(
            {
                "error": f"Error processing chunk: {str(e)}",
                "chunk_data": chunk_data[:200],
            }
        )


def orchestrate_focused_nifi_migration(
    xml_path: str,
    pruned_processors: List[Dict[str, Any]],
    semantic_flows: Dict[str, Any],
    out_dir: str,
    project: str,
    job: str,
    notebook_path: str = "",
    max_processors_per_chunk: int = MAX_PROCS_PER_CHUNK_DEFAULT,
    existing_cluster_id: str = "",
    run_now: bool = False,
) -> str:
    """
    Focused migration that only processes essential data processors.

    This migration approach:
    1. SKIPS infrastructure processors entirely (logging, routing, flow control)
    2. Uses SIMPLE TEMPLATES for data movement processors (read/write operations)
    3. Uses LLM INTELLIGENCE only for actual data transformation processors

    Args:
        xml_path: Original XML path for metadata
        pruned_processors: List of essential processors after pruning
        semantic_flows: Semantic data flow analysis results
        out_dir: Output directory for generated artifacts
        project: Project name
        job: Job name
        notebook_path: Target notebook path in Databricks workspace
        max_processors_per_chunk: Maximum processors per chunk (default: 20)
        existing_cluster_id: Existing cluster ID to use
        run_now: Whether to deploy and run the job

    Returns:
        JSON summary with focused migration results
    """
    try:
        print(
            f"🎯 [FOCUSED MIGRATION] Processing {len(pruned_processors)} essential processors only"
        )

        # Categorize processors for different treatment
        data_transformation_procs = []
        data_movement_procs = []
        external_processing_procs = []

        for proc in pruned_processors:
            # Handle both classification key names for compatibility
            classification = proc.get(
                "classification", proc.get("data_manipulation_type", "unknown")
            )
            if classification == "data_transformation":
                data_transformation_procs.append(proc)
            elif classification == "data_movement":
                data_movement_procs.append(proc)
            elif classification == "external_processing":
                external_processing_procs.append(proc)

        print(f"📊 [PROCESSOR BREAKDOWN]")
        print(
            f"   • Data Transformation: {len(data_transformation_procs)} (LLM generation)"
        )
        print(
            f"   • External Processing: {len(external_processing_procs)} (LLM generation)"
        )
        print(f"   • Data Movement: {len(data_movement_procs)} (Simple templates)")
        print(f"   • Infrastructure: 0 (Skipped entirely)")

        # Setup output directory
        root = Path(out_dir)
        proj_name = _safe_name(project)
        out = root / proj_name
        (out / "src/steps").mkdir(parents=True, exist_ok=True)
        (out / "jobs").mkdir(parents=True, exist_ok=True)
        (out / "conf").mkdir(parents=True, exist_ok=True)
        (out / "notebooks").mkdir(parents=True, exist_ok=True)

        if not notebook_path:
            notebook_path = _default_notebook_path(project)

        # Generate tasks for each processor category
        all_tasks = []

        # 1. Generate LLM-powered tasks for data transformation processors
        if data_transformation_procs or external_processing_procs:
            llm_processors = data_transformation_procs + external_processing_procs
            print(
                f"🧠 [LLM GENERATION] Processing {len(llm_processors)} data processors with intelligence..."
            )

            # Use batched processing for efficient LLM generation
            llm_tasks = _generate_batch_processor_code(
                llm_processors, "focused_migration", project
            )
            all_tasks.extend(llm_tasks)

        # 2. Generate simple template tasks for data movement processors
        if data_movement_procs:
            print(
                f"📦 [TEMPLATE GENERATION] Processing {len(data_movement_procs)} data movement processors..."
            )
            template_tasks = _process_processors_with_templates(
                data_movement_procs, project
            )
            all_tasks.extend(template_tasks)

        # 3. Create job configuration with only essential tasks
        print(
            f"🔗 [JOB CREATION] Creating job with {len(all_tasks)} essential tasks..."
        )

        # Build simple dependencies based on semantic flows
        task_dependencies = _build_focused_dependencies(all_tasks, semantic_flows)

        job_config = {
            "name": job,
            "tasks": [
                {
                    "task_key": task["id"],
                    "notebook_task": {
                        "notebook_path": f"{notebook_path}/src/steps/{task['id']}.py"
                    },
                    "depends_on": task_dependencies.get(task["id"], []),
                    "new_cluster": {
                        "spark_version": "15.4.x-scala2.12",
                        "node_type_id": "i3.xlarge",
                        "num_workers": 2,
                    },
                }
                for task in all_tasks
            ],
        }

        # Save artifacts
        _write_text(out / "jobs" / "job.focused.json", json.dumps(job_config, indent=2))

        # Save focused analysis
        focused_analysis = {
            "original_xml": xml_path,
            "processors_processed": len(pruned_processors),
            "breakdown": {
                "data_transformation": len(data_transformation_procs),
                "external_processing": len(external_processing_procs),
                "data_movement": len(data_movement_procs),
                "infrastructure_skipped": "All infrastructure processors skipped",
            },
            "semantic_flows": semantic_flows,
            "migration_approach": "focused_essential_only",
        }

        _write_text(
            out / "conf" / "focused_migration_analysis.json",
            json.dumps(focused_analysis, indent=2),
        )

        # Generate notebooks for each task
        for task in all_tasks:
            task_code = task.get(
                "code", f"# {task['name']}\n# TODO: Implement task logic"
            )
            notebook_content = f'# Databricks notebook source\n"""\n{task["name"]}\nTask ID: {task["id"]}\n"""\n\n{task_code}'
            _write_text(out / "src" / "steps" / f"{task['id']}.py", notebook_content)

        # Create orchestrator notebook
        orchestrator = _generate_focused_orchestrator(all_tasks, project)
        _write_text(out / "notebooks" / "orchestrator.py", orchestrator)

        # Generate bundle YAML
        bundle_yaml = scaffold_asset_bundle(project, job, notebook_path)
        _write_text(out / "databricks.yml", bundle_yaml)

        # Deploy if requested
        deploy_result = "Not deployed"
        if run_now and existing_cluster_id:
            try:
                print(f"🚀 [DEPLOY] Deploying focused migration job...")
                deploy_result = deploy_and_run_job(
                    json.dumps(job_config), run_now=run_now
                )
                print(f"✅ [DEPLOY SUCCESS] {deploy_result}")
            except Exception as e:
                print(f"❌ [DEPLOY FAILED] {e}")
                deploy_result = f"Error: {e}"

        result = {
            "migration_type": "focused_essential_only",
            "processors_migrated": len(all_tasks),
            "processors_skipped": f"All infrastructure processors skipped",
            "breakdown": focused_analysis["breakdown"],
            "output_directory": str(out),
            "job_config_path": str(out / "jobs" / "job.focused.json"),
            "deploy_result": deploy_result,
            "semantic_flows_applied": True,
            "cost_optimization": f"Reduced LLM calls by focusing on {len(data_transformation_procs + external_processing_procs)} essential processors only",
        }

        return json.dumps(result, indent=2)

    except Exception as e:
        return json.dumps({"error": f"Focused migration failed: {str(e)}"})


def _process_processors_with_templates(
    processors: List[Dict[str, Any]], project: str
) -> List[Dict[str, Any]]:
    """Generate tasks using simple templates for data movement processors."""
    tasks = []

    for i, proc in enumerate(processors):
        proc_type = proc.get("processor_type", "Unknown")
        proc_name = proc.get("name", f"processor_{i}")

        # Simple template based on processor type
        if "GetFile" in proc_type or "ListFile" in proc_type:
            code = f"""# {proc_type} → Auto Loader Template
# Simple file reading operation

df = spark.readStream.format("cloudFiles") \\
    .option("cloudFiles.format", "json") \\
    .load("/path/to/input")

df.writeStream \\
    .format("delta") \\
    .outputMode("append") \\
    .option("checkpointLocation", "/path/to/checkpoint") \\
    .start("/path/to/output")
"""
        elif "PutFile" in proc_type or "PutHDFS" in proc_type:
            code = f"""# {proc_type} → Delta Write Template
# Simple file writing operation

df = spark.read.format("delta").load("/path/to/input")

df.write \\
    .format("delta") \\
    .mode("append") \\
    .save("/path/to/output")
"""
        else:
            code = f"""# {proc_type} → Data Movement Template
# Simple data transfer operation

df = spark.read.format("delta").load("/path/to/input")
# {proc_name}: Move data without transformation
df.write.format("delta").mode("append").save("/path/to/output")
"""

        task = {
            "id": proc.get("id", f"move_proc_{i}"),
            "name": proc_name,
            "type": proc_type,
            "code": code,
            "classification": "data_movement",
            "business_purpose": "Data movement without transformation",
        }
        tasks.append(task)

    return tasks


def _build_focused_dependencies(
    tasks: List[Dict[str, Any]], semantic_flows: Dict[str, Any]
) -> Dict[str, List[str]]:
    """Build simple task dependencies based on semantic flows."""
    dependencies = {}

    # For now, create simple sequential dependencies
    # TODO: Use semantic_flows to create smarter dependencies
    for i, task in enumerate(tasks):
        if i > 0:
            dependencies[task["id"]] = [{"task_key": tasks[i - 1]["id"]}]
        else:
            dependencies[task["id"]] = []

    return dependencies


def _generate_focused_orchestrator(tasks: List[Dict[str, Any]], project: str) -> str:
    """Generate orchestrator notebook for focused migration."""
    task_list = "\\n".join(
        [f"# - {task['name']} ({task['classification']})" for task in tasks]
    )

    return f'''# Databricks notebook source
"""
Focused NiFi Migration Orchestrator
Project: {project}
Generated: Automated migration focusing only on essential data processors

Tasks ({len(tasks)} total):
{task_list}
"""

# COMMAND ----------

print("🎯 Starting focused NiFi migration execution...")
print(f"Processing {len(tasks)} essential data processors only")
print("Infrastructure processors were skipped entirely")

# COMMAND ----------

# Task execution logic would go here
# Each task runs as a separate Databricks job task

print("✅ Focused migration orchestration complete")
'''

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


from tools.chunking_tools import (
    chunk_nifi_xml_by_process_groups,
    extract_complete_workflow_map,
    reconstruct_full_workflow,
)
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
            proc_type = processor.get("type", "Unknown")
            proc_name = processor.get("name", f"processor_{idx}")
            props = processor.get("properties", {})

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
                    "id": processor.get("id", f"{chunk_id}_task_{idx}"),
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
                        "id": processor.get("id", f"{chunk_id}_task_{idx}"),
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

        # Continue with LLM generation for remaining processors
        print(
            f"🚀 [LLM BATCH] Sending {len(llm_needed_processors)} processors to LLM..."
        )

        # Get the model endpoint from environment
        model_endpoint = os.environ.get(
            "MODEL_ENDPOINT", "databricks-meta-llama-3-3-70b-instruct"
        )
        llm = ChatDatabricks(endpoint=model_endpoint)

        # Create batched prompt for all processors
        batch_prompt = f"""You are a NiFi to Databricks migration expert. Generate PySpark code for multiple NiFi processors in a single response.

For each processor below, generate the equivalent PySpark/Databricks code that performs the same function.

PROCESSORS TO CONVERT:
{json.dumps(processor_specs, indent=2)}

REQUIREMENTS:
1. Return ONLY a valid JSON object with processor index as key and generated code as value
2. Use Databricks BATCH patterns (Delta Lake, regular DataFrame operations - NO streaming)
3. Handle the specific properties for each processor appropriately
4. Include comments explaining the logic
5. Make the code functional and ready to use
6. For GetFile/ListFile: use spark.read (NOT Auto Loader) for batch file processing
7. For PutFile/PutHDFS: use Delta Lake writes
8. For ConsumeKafka: use Structured Streaming (only exception)
9. For JSON processors: use PySpark JSON functions
10. CONTEXT-AWARE DATAFRAMES: Use the workflow_context to determine proper DataFrame variable names:
    - For source processors (GetFile, ListFile): Create df_[processor_name] as output
    - For processing processors: Read from previous processor's output DataFrame
    - Never use undefined 'df' variable - always reference the actual DataFrame from previous steps
11. LEGACY HDFS PATH CONVERSION (CRITICAL):
    - Detect legacy paths in PutFile/PutHDFS: /user/, /hdfs/, /tmp/, /data/, /var/
    - Convert to Unity Catalog: target_table = 'main.default.converted_table'
    - Use .saveAsTable() instead of .save('/legacy/path')
    - Add conversion comments: "# TODO: Converted from legacy HDFS path"

CRITICAL JSON FORMATTING RULES:
- Return ONLY the JSON object, no markdown, no explanations
- Escape all backslashes as double backslashes (\\\\)
- Escape all newlines as \\\\n
- Escape all quotes as \\\\"
- Do not include any text outside the JSON object

RESPONSE FORMAT (EXACT):
{{
  "0": "# ProcessorType1 → Databricks equivalent\\\\nfrom pyspark.sql.functions import *\\\\n\\\\n# Your PySpark code here",
  "1": "# ProcessorType2 → Databricks equivalent\\\\nfrom pyspark.sql.functions import *\\\\n\\\\n# Your PySpark code here"
}}

Generate the code for all {len(processor_specs)} processors as a valid JSON object:"""

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

            # Prepare pattern for bulk save later
            processor_class = (
                spec["type"].split(".")[-1] if "." in spec["type"] else spec["type"]
            )
            pattern_obj = {
                "category": "llm_generated",
                "databricks_equivalent": "LLM Generated Solution",
                "description": f"Auto-generated pattern for {processor_class} based on properties analysis",
                "code_template": code,
                "best_practices": [
                    "Review and customize the generated code",
                    "Test thoroughly before production use",
                    "Consider processor-specific optimizations",
                ],
                "generated_from_properties": spec["properties"],
                "generation_source": "llm_hybrid_approach",
            }
            # Registry removed - no pattern buffering

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

        # Registry removed - generating fresh each time

        # Combine built-in patterns with LLM-generated tasks
        all_tasks = builtin_tasks + generated_tasks

        print(
            f"✨ [LLM BATCH] Generated {len(all_tasks)} processor tasks for {chunk_id} "
            f"({len(builtin_tasks)} built-in, {len(generated_tasks)} LLM)"
        )
        logger.info(
            f"Generated code for {len(all_tasks)} processors: {len(builtin_tasks)} built-in patterns, {len(generated_tasks)} LLM-generated"
        )
        return all_tasks

    except Exception as e:
        logger.error(f"Batch code generation failed: {e}")
        # Sub-batch fallback to reduce per-processor LLM calls
        sub_batch_size = int(os.environ.get("LLM_SUB_BATCH_SIZE", "10"))

        if len(processors) > sub_batch_size:
            generated_tasks: List[Dict[str, Any]] = []
            for start in range(0, len(processors), sub_batch_size):
                subset = processors[start : start + sub_batch_size]
                subset_id = f"{chunk_id}__sb{start // sub_batch_size}"
                try:
                    # Reuse batch pathway for each sub-batch
                    sub_tasks = _generate_batch_processor_code(
                        subset, subset_id, project
                    )
                    generated_tasks.extend(sub_tasks)
                except Exception:
                    # If sub-batch still fails, fall back to per-processor for this subset only
                    for idx, processor in enumerate(subset):
                        proc_type = processor.get("type", "Unknown")
                        proc_name = processor.get("name", f"processor_{start+idx}")
                        props = processor.get("properties", {})
                        try:
                            code = generate_databricks_code.func(
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
                            "id": processor.get("id", f"{subset_id}_task_{idx}"),
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
            generated_tasks = []
            for idx, processor in enumerate(processors):
                proc_type = processor.get("type", "Unknown")
                proc_name = processor.get("name", f"processor_{idx}")
                props = processor.get("properties", {})

                try:
                    code = generate_databricks_code.func(
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
                    "id": processor.get("id", f"{chunk_id}_task_{idx}"),
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
    "orchestrate_chunked_nifi_migration",
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


# Removed @tool decorator - direct function call approach
def orchestrate_chunked_nifi_migration(
    xml_path: str,
    out_dir: str,
    project: str,
    job: str,
    notebook_path: str = "",
    max_processors_per_chunk: int = MAX_PROCS_PER_CHUNK_DEFAULT,
    existing_cluster_id: str = "",
    run_now: bool = False,
) -> str:
    """
    End-to-end chunked migration for large NiFi XML files.

    This tool handles large NiFi XML files by:
    1. Chunking the XML by process groups and processor batches
    2. Processing each chunk individually (avoids context limits)
    3. Reconstructing the full workflow with proper task dependencies
    4. Generating a complete Databricks multi-task job configuration

    Args:
        xml_path: Path to the NiFi XML file (local or DBFS)
        out_dir: Output directory for generated artifacts
        project: Project name
        job: Job name
        notebook_path: Target notebook path in Databricks workspace
        max_processors_per_chunk: Maximum processors per chunk (default: 20)
        existing_cluster_id: Existing cluster ID to use
        deploy: Whether to deploy the job to Databricks

    Returns:
        JSON summary with chunking statistics, processing results, and final workflow
    """
    try:
        # Resolve effective max processors per chunk with env override
        env_max_str = os.environ.get("MAX_PROCESSORS_PER_CHUNK")
        effective_max = int(env_max_str) if env_max_str else max_processors_per_chunk
        # --- Setup output directory ---
        root = Path(out_dir)
        proj_name = _safe_name(project)
        out = root / proj_name
        (out / "src/steps").mkdir(parents=True, exist_ok=True)
        (out / "jobs").mkdir(parents=True, exist_ok=True)
        (out / "conf").mkdir(parents=True, exist_ok=True)
        (out / "notebooks").mkdir(parents=True, exist_ok=True)
        (out / "chunks").mkdir(parents=True, exist_ok=True)  # For chunk artifacts

        # --- Default notebook path if not provided ---
        if not notebook_path:
            notebook_path = _default_notebook_path(project)

        # --- Read and extract complete workflow map ---
        xml_text = _read_text(xml_path)

        # Step 0: Extract complete workflow structure for connectivity restoration
        workflow_map_result = json.loads(extract_complete_workflow_map.func(xml_text))
        if "error" in workflow_map_result:
            return json.dumps(
                {
                    "error": f"Workflow map extraction failed: {workflow_map_result['error']}"
                }
            )

        # Save complete workflow map for reference
        _write_text(
            out / "conf/complete_workflow_map.json",
            json.dumps(workflow_map_result, indent=2),
        )

        # Step 1: Chunk the XML by process groups
        chunking_result = json.loads(
            chunk_nifi_xml_by_process_groups.func(
                xml_content=xml_text, max_processors_per_chunk=effective_max
            )
        )

        if "error" in chunking_result:
            return json.dumps({"error": f"Chunking failed: {chunking_result['error']}"})

        chunks = chunking_result["chunks"]
        cross_chunk_links = chunking_result["cross_chunk_links"]
        summary = chunking_result["summary"]

        # Save chunking results
        _write_text(
            out / "conf/chunking_result.json", json.dumps(chunking_result, indent=2)
        )

        # Step 2: Process each chunk individually
        chunk_results = []
        all_step_files = []
        task_counter = 0  # Global task counter for consistent file naming

        print(
            f"📋 [MIGRATION] Processing {len(chunks)} chunks with {summary['total_processors']} total processors"
        )
        print(f"🎯 [MIGRATION] Target: max {effective_max} processors per chunk")

        for i, chunk in enumerate(chunks):
            chunk_processor_count = len(chunk.get("processors", []))
            print(
                f"\n📦 [CHUNK {i+1}/{len(chunks)}] Processing {chunk_processor_count} processors..."
            )
            chunk_data = json.dumps(chunk)

            # Process this chunk
            chunk_result = json.loads(
                process_nifi_chunk.func(
                    chunk_data=chunk_data, project=project, chunk_index=i
                )
            )

            if "error" in chunk_result:
                print(f"❌ [CHUNK {i+1}/{len(chunks)}] Error: {chunk_result['error']}")
                logger.warning(f"Error processing chunk {i}: {chunk_result['error']}")
                continue

            print(
                f"✅ [CHUNK {i+1}/{len(chunks)}] Generated {chunk_result.get('task_count', 0)} tasks"
            )
            chunk_results.append(chunk_result)

            # Write individual task files for this chunk
            for task in chunk_result["tasks"]:
                task_name = task["name"]
                code = task["code"]
                # Unescape the code to convert \n to actual newlines, etc.
                unescaped_code = _unescape_code(code)
                step_path = out / f"src/steps/{task_counter:02d}_{task_name}.py"
                _write_text(step_path, unescaped_code)
                all_step_files.append(str(step_path))
                task_counter += 1  # Increment for next task

            # Save chunk processing result
            _write_text(
                out / f"chunks/chunk_{i}_result.json",
                json.dumps(chunk_result, indent=2),
            )

        # Step 3: Reconstruct the full workflow with complete connectivity map
        workflow_result = json.loads(
            reconstruct_full_workflow.func(
                chunk_results_json=json.dumps(chunk_results),
                cross_chunk_links_json=json.dumps(cross_chunk_links),
                workflow_map_json=json.dumps(workflow_map_result),
                base_notebook_path=notebook_path,
            )
        )

        if "error" in workflow_result:
            return json.dumps(
                {"error": f"Workflow reconstruction failed: {workflow_result['error']}"}
            )

        # Save reconstructed workflow
        _write_text(
            out / "conf/reconstructed_workflow.json",
            json.dumps(workflow_result, indent=2),
        )

        # Step 4: Generate final Databricks job configuration
        final_job_config = workflow_result.get("databricks_job_config", {})
        final_job_config.update(
            {
                "name": job,
                "email_notifications": (
                    {
                        "on_failure": [os.environ.get("NOTIFICATION_EMAIL", "")],
                    }
                    if os.environ.get("NOTIFICATION_EMAIL")
                    else {}
                ),
            }
        )

        # Enhance job config with cluster settings
        if existing_cluster_id:
            for task in final_job_config.get("tasks", []):
                task["existing_cluster_id"] = existing_cluster_id
        else:
            cluster_config = {
                "spark_version": "16.4.x-scala2.12",
                "node_type_id": "Standard_DS3_v2",
                "num_workers": 0,
                "autotermination_minutes": 60,
            }
            for task in final_job_config.get("tasks", []):
                task["new_cluster"] = cluster_config

        _write_text(
            out / "jobs/job.chunked.json", json.dumps(final_job_config, indent=2)
        )

        # Step 5: Registry removed - no pattern persistence needed

        # Step 6: Generate project artifacts
        # Bundle + README
        bundle_yaml = scaffold_asset_bundle.func(project, job, notebook_path)
        _write_text(out / "databricks.yml", bundle_yaml)

        readme = [
            f"# {project} (Chunked Migration)",
            "",
            "Generated from large NiFi flow using chunked processing.",
            "",
            f"## Migration Statistics",
            f"- Original processors: {summary['total_processors']}",
            f"- Original connections: {summary['total_connections']}",
            f"- Chunks created: {summary['chunk_count']}",
            f"- Cross-chunk links: {summary['cross_chunk_links_count']}",
            "",
            "## Contents",
            "- `src/steps/` individual processor translations (grouped by chunks)",
            "- `chunks/` individual chunk processing results",
            "- `conf/` chunking analysis and reconstructed workflow",
            "- `jobs/job.chunked.json` final multi-task job configuration",
            "- `databricks.yml` Databricks Asset Bundle",
            "",
            "## ⚠️  Important Migration Notes",
            "**MANUAL REVIEW REQUIRED**: Legacy HDFS paths have been converted to Unity Catalog format.",
            "- Check all `src/steps/*.py` files for TODO comments",
            "- Update table references from legacy paths to `catalog.schema.table` format",
            "- Verify your Unity Catalog permissions and table locations",
            "- Example: `/user/nifi/data` → `main.default.your_table_name`",
            "",
            "## Next steps",
            "1. **CRITICAL**: Review TODO comments in generated Python files",
            "2. Update Unity Catalog table references in `src/steps/*.py`",
            "3. Review the chunked migration results in `conf/`",
            "4. Test individual chunks if needed using files in `chunks/`",
            "5. Deploy the final job using `jobs/job.chunked.json`",
            "6. Monitor cross-chunk dependencies for correct execution order",
        ]
        _write_text(out / "README.md", "\n".join(readme))

        # Step 7: Save parameter contexts and controller services
        params_js = extract_nifi_parameters_and_services_impl(xml_text)
        _write_text(
            out / "conf/parameter_contexts.json", json.dumps(params_js, indent=2)
        )

        # Step 8: Generate orchestrator notebook (enhanced for chunked processing)
        orchestrator = textwrap.dedent(
            f"""\
        # Databricks notebook source
        # Orchestrator notebook for chunked NiFi migration
        dbutils.widgets.text("STEP_MODULE", "")
        dbutils.widgets.text("CHUNK_ID", "")
        step_mod = dbutils.widgets.get("STEP_MODULE")
        chunk_id = dbutils.widgets.get("CHUNK_ID")

        import importlib.util, sys, os, glob, json

        def run_module(rel_path: str):
            root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
            full_path = os.path.join(root, rel_path)
            if not os.path.exists(full_path):
                raise FileNotFoundError(f"Module not found: {{full_path}}")
            spec = importlib.util.spec_from_file_location("step_mod", full_path)
            mod = importlib.util.module_from_spec(spec)
            sys.modules["step_mod"] = mod
            spec.loader.exec_module(mod)

        def run_chunk_steps(chunk_prefix: str):
            root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
            pattern = os.path.join(root, "src", "steps", f"{{chunk_prefix}}_*.py")
            steps = sorted(glob.glob(pattern))
            print(f"Running steps for chunk {{chunk_prefix}}: {{len(steps)}} files")
            for s in steps:
                rel = os.path.relpath(s, root)
                print(f" -> {{rel}}")
                run_module(rel)

        if step_mod:
            print(f"Running single step: {{step_mod}}")
            run_module(step_mod)
        elif chunk_id:
            print(f"Running all steps for chunk: {{chunk_id}}")
            run_chunk_steps(chunk_id)
        else:
            root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
            steps = sorted(glob.glob(os.path.join(root, "src", "steps", "*.py")))
            print("No STEP_MODULE or CHUNK_ID provided; running all steps sequentially:")
            print(f"Total steps: {{len(steps)}}")
            for s in steps:
                rel = os.path.relpath(s, root)
                print(f" -> {{rel}}")
                run_module(rel)
        """
        )
        _write_text(out / "notebooks/main", orchestrator)

        # --- 9) Always create the job in Databricks
        print(f"\n🚀 [DEPLOY] Creating Databricks job...")

        try:
            deploy_result = deploy_and_run_job.func(
                json.dumps(final_job_config), run_now=run_now
            )

            # Parse deployment result
            if deploy_result and "job_id" in str(deploy_result):
                try:
                    result_data = json.loads(deploy_result)
                    job_id = result_data.get("job_id")
                    run_id = result_data.get("run_id")

                    print(f"✅ [DEPLOY SUCCESS] Job created (job_id: {job_id})")

                    if run_now and run_id:
                        print(
                            f"🚀 [JOB TRIGGERED] Execution started (run_id: {run_id})"
                        )
                        print(f"⏳ [STATUS CHECK] Verifying job startup...")

                        # Poll for actual job status
                        status = check_job_run_status(
                            job_id, run_id, max_wait_seconds=45
                        )

                        if status["status"] == "RUNNING":
                            print(f"✅ [JOB RUNNING] Job is actively running!")
                            print(
                                f"📊 [MONITOR] View progress: {status['run_page_url']}"
                            )
                        elif status["status"] == "FAILED":
                            print(f"❌ [JOB FAILED] {status['state_message']}")
                            print(f"🔗 [DEBUG] Check details: {status['run_page_url']}")
                        elif status["status"] == "TIMEOUT":
                            print(f"⏰ [JOB PENDING] {status['state_message']}")
                            print(
                                f"🔗 [MONITOR] Check status: {status['run_page_url']}"
                            )
                        elif status["status"] == "SUCCESS":
                            print(f"🎉 [JOB COMPLETE] Job finished successfully!")
                            print(f"📊 [RESULTS] View output: {status['run_page_url']}")
                    elif run_now and not run_id:
                        print(f"⚠️  [JOB CREATED] Job created but run trigger failed")
                        print(f"🔗 [MANUAL RUN] Start manually: {_get_job_url(job_id)}")
                    else:
                        print(f"🎯 [JOB READY] Job ready for manual execution")
                        print(f"🔗 [RUN MANUALLY] Start job: {_get_job_url(job_id)}")

                    deployment_success = True
                    deployment_error = None

                except json.JSONDecodeError:
                    # Fallback for non-JSON responses
                    print(f"✅ [DEPLOY SUCCESS] {deploy_result}")
                    deployment_success = True
                    deployment_error = None
            else:
                print(f"❌ [DEPLOY FAILED] {deploy_result}")
                deployment_success = False
                deployment_error = str(deploy_result)

        except Exception as e:
            print(f"❌ [DEPLOY FAILED] Exception: {e}")
            deployment_success = False
            deployment_error = str(e)
            deploy_result = f"Error: {e}"

        # Final result summary
        total_tasks = sum(len(cr["tasks"]) for cr in chunk_results)

        # Overall success always depends on deployment now
        migration_success = deployment_success

        if migration_success:
            print(f"\n🎉 [MIGRATION COMPLETE] All steps succeeded!")
        else:
            print(f"\n❌ [MIGRATION FAILED] Job deployment failed!")

        print(
            f"📊 [SUMMARY] Processed {summary['total_processors']} processors → {total_tasks} tasks"
        )
        print(
            f"📊 [SUMMARY] Used {len(chunk_results)} chunks (max {max_processors_per_chunk} processors/chunk)"
        )
        print(f"📊 [SUMMARY] Generated {len(all_step_files)} step files")
        print(f"📊 [SUMMARY] Output directory: {out}")

        # Check for legacy HDFS path conversions and warn user
        legacy_paths_found = False
        for step_file in all_step_files:
            with open(step_file, "r") as f:
                content = f.read()
                if "TODO: UPDATE TABLE REFERENCE" in content:
                    legacy_paths_found = True
                    break

        if legacy_paths_found:
            print("⚠️  [MIGRATION WARNING] Legacy HDFS paths detected and converted!")
            print(
                "⚠️  [ACTION REQUIRED] Review generated Python files for TODO comments"
            )
            print(
                "⚠️  [ACTION REQUIRED] Update table references to Unity Catalog format"
            )
            print("⚠️  [EXAMPLE] /user/nifi/data → main.default.your_table_name")

        if deployment_error:
            print(f"❌ [DEPLOY ERROR] {deployment_error}")

        result = {
            "migration_type": "chunked",
            "success": migration_success,
            "output_dir": str(out),
            "chunking_summary": summary,
            "chunks_processed": len(chunk_results),
            "total_tasks_generated": total_tasks,
            "step_files_written": all_step_files,
            "cross_chunk_links_count": len(cross_chunk_links),
            "final_job_config_path": str(out / "jobs/job.chunked.json"),
            "notebook_path": notebook_path,
            "deployment_success": deployment_success,
            "deploy_result": deploy_result,
            "deployment_error": deployment_error,
            "continue_required": False,
            "tool_name": "orchestrate_chunked_nifi_migration",
        }

        return json.dumps(result, indent=2)

    except Exception as e:
        return json.dumps({"error": f"Chunked migration failed: {str(e)}"})

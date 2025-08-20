# tools/migration_tools.py
# NiFi → Databricks conversion orchestrators and flow utilities.

from __future__ import annotations

import json
import os
import base64
import textwrap
from pathlib import Path
import requests
import xml.etree.ElementTree as ET
from typing import Any, Dict, List

from langchain_core.tools import tool

from config import logger
from utils import safe_name as _safe_name, write_text as _write_text, read_text as _read_text

def _unescape_code(code: str) -> str:
    """
    Unescape literal escape sequences from LLM-generated code.
    Converts \\n to actual newlines, \\t to tabs, etc.
    """
    if not code:
        return code
    
    # Handle common escape sequences
    unescaped = code.encode().decode('unicode_escape')
    return unescaped
from utils import (
    parse_nifi_template_impl,
    extract_nifi_parameters_and_services_impl,
)
from tools.xml_tools import parse_nifi_template, extract_nifi_parameters_and_services
from tools.job_tools import (
    create_job_config,
    create_job_config_from_plan,
    deploy_and_run_job,
    scaffold_asset_bundle,
)
from tools.pattern_tools import (
    generate_databricks_code,
    suggest_autoloader_options,
)
from tools.dlt_tools import generate_dlt_pipeline_config
from tools.chunking_tools import (
    chunk_nifi_xml_by_process_groups,
    reconstruct_full_workflow,
    estimate_chunk_size,
    extract_complete_workflow_map
)

def _generate_batch_processor_code(processors: List[Dict[str, Any]], chunk_id: str, project: str) -> List[Dict[str, Any]]:
    """
    Generate Databricks code for multiple processors in a single LLM call to reduce API requests.
    """
    try:
        from databricks_langchain import ChatDatabricks
        import os
        
        print(f"🧠 [LLM BATCH] Generating code for {len(processors)} processors in {chunk_id}")
        
        # Get the model endpoint from environment
        model_endpoint = os.environ.get("MODEL_ENDPOINT", "databricks-meta-llama-3-3-70b-instruct")
        llm = ChatDatabricks(endpoint=model_endpoint)
        
        # Prepare batch request for all processors
        processor_specs = []
        processor_types = []
        for idx, processor in enumerate(processors):
            proc_type = processor.get("type", "Unknown")
            proc_name = processor.get("name", f"processor_{idx}")
            props = processor.get("properties", {})
            
            # Extract just the class name for display
            class_name = proc_type.split(".")[-1] if "." in proc_type else proc_type
            processor_types.append(class_name)
            
            processor_specs.append({
                "index": idx,
                "type": proc_type,
                "name": proc_name,
                "properties": props,
                "id": processor.get("id", f"{chunk_id}_task_{idx}")
            })
        
        print(f"🔍 [LLM BATCH] Processor types: {', '.join(processor_types)}")
        print(f"🚀 [LLM BATCH] Sending batch request to {model_endpoint}...")
        
        # Create batched prompt for all processors
        batch_prompt = f"""You are a NiFi to Databricks migration expert. Generate PySpark code for multiple NiFi processors in a single response.

For each processor below, generate the equivalent PySpark/Databricks code that performs the same function.

PROCESSORS TO CONVERT:
{json.dumps(processor_specs, indent=2)}

REQUIREMENTS:
1. Return ONLY a valid JSON object with processor index as key and generated code as value
2. Use proper Databricks patterns (Delta Lake, DataFrame operations, Auto Loader, Structured Streaming)
3. Handle the specific properties for each processor appropriately
4. Include comments explaining the logic
5. Make the code functional and ready to use
6. For GetFile/ListFile: use Auto Loader with cloudFiles format
7. For PutFile/PutHDFS: use Delta Lake writes
8. For ConsumeKafka: use Structured Streaming
9. For JSON processors: use PySpark JSON functions

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

        # Call LLM once for all processors
        response = llm.invoke(batch_prompt)
        print(f"✅ [LLM BATCH] Received response, parsing generated code...")
        
        # Log the first 200 chars of response for debugging
        response_preview = response.content[:200].replace('\n', '\\n')
        print(f"🔍 [LLM BATCH] Response preview: {response_preview}...")
        
        try:
            generated_code_map = json.loads(response.content.strip())
            print(f"🎯 [LLM BATCH] Successfully parsed {len(generated_code_map)} code snippets")
        except json.JSONDecodeError as e:
            print(f"⚠️  [LLM BATCH] JSON parsing failed: {e}")
            # Try multiple approaches to extract JSON from response
            content = response.content.strip()
            generated_code_map = None
            
            # Try 1: Extract from markdown code blocks
            if "```json" in content:
                try:
                    content = content.split("```json")[1].split("```")[0].strip()
                    generated_code_map = json.loads(content)
                    print(f"🔧 [LLM BATCH] Recovered JSON from markdown block")
                except:
                    pass
            elif "```" in content:
                try:
                    content = content.split("```")[1].split("```")[0].strip()
                    generated_code_map = json.loads(content)
                    print(f"🔧 [LLM BATCH] Recovered JSON from code block")
                except:
                    pass
            
            # Try 2: Look for JSON object boundaries
            if generated_code_map is None:
                try:
                    # Find first { and last }
                    start = content.find('{')
                    end = content.rfind('}') + 1
                    if start >= 0 and end > start:
                        json_content = content[start:end]
                        # Fix common escape issues
                        json_content = json_content.replace('\\n', '\\\\n').replace('\\"', '\\\\"')
                        generated_code_map = json.loads(json_content)
                        print(f"🔧 [LLM BATCH] Recovered JSON by boundary detection")
                except:
                    pass
            
            # Try 3: Replace problematic characters and retry
            if generated_code_map is None:
                try:
                    # Common fixes for LLM-generated JSON
                    fixed_content = content.replace('\n', '\\n').replace('\t', '\\t').replace('\r', '\\r')
                    # Don't double-escape already escaped backslashes
                    if '\\\\' not in fixed_content:
                        fixed_content = fixed_content.replace('\\', '\\\\')
                    generated_code_map = json.loads(fixed_content)
                    print(f"🔧 [LLM BATCH] Recovered JSON after escape fixes")
                except:
                    pass
            
            # Try 4: More aggressive JSON extraction and cleaning
            if generated_code_map is None:
                try:
                    import re
                    # Find JSON-like patterns and clean them
                    json_pattern = r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}'
                    matches = re.findall(json_pattern, content, re.DOTALL)
                    for match in matches:
                        try:
                            # Clean the match
                            clean_match = match.strip()
                            # Fix common JSON issues
                            clean_match = re.sub(r'",\s*}', '"}', clean_match)  # Remove trailing commas
                            clean_match = re.sub(r'",\s*]', '"]', clean_match)  # Remove trailing commas in arrays
                            generated_code_map = json.loads(clean_match)
                            print(f"🔧 [LLM BATCH] Recovered JSON using regex pattern matching")
                            break
                        except:
                            continue
                except:
                    pass
            
            # If all parsing attempts fail, raise the original error
            if generated_code_map is None:
                print(f"❌ [LLM BATCH] All JSON recovery attempts failed, falling back to individual generation")
                raise e
        
        # Build tasks from the batch response
        generated_tasks = []
        # Collect patterns for bulk save
        bulk_patterns: Dict[str, dict] = {}
        for spec in processor_specs:
            idx = spec["index"]
            code = generated_code_map.get(str(idx), f"# {spec['type']} → Code generation failed\\n# TODO: Implement manually")
            
            # Prepare pattern for bulk save later
            try:
                processor_class = spec["type"].split(".")[-1] if "." in spec["type"] else spec["type"]
                pattern_obj = {
                    "category": "llm_generated",
                    "databricks_equivalent": "LLM Generated Solution",
                    "description": f"Auto-generated pattern for {processor_class} based on properties analysis",
                    "code_template": code,
                    "best_practices": [
                        "Review and customize the generated code",
                        "Test thoroughly before production use",
                        "Consider processor-specific optimizations"
                    ],
                    "generated_from_properties": spec["properties"],
                    "generation_source": "llm_hybrid_approach"
                }
                bulk_patterns[processor_class] = pattern_obj
                # Also buffer immediately so a later flush will persist and init UC
                try:
                    from tools.pattern_tools import _buffer_generated_pattern
                    _buffer_generated_pattern(processor_class, pattern_obj)
                except Exception:
                    pass
            except Exception:
                pass
            
            task = {
                "id": spec["id"],
                "name": _safe_name(spec["name"]),
                "type": spec["type"],
                "code": code,
                "properties": spec["properties"],
                "chunk_id": chunk_id,
                "processor_index": idx
            }
            generated_tasks.append(task)
        
        # Flush patterns once per chunk (creates tables on first run)
        try:
            from tools.pattern_tools import flush_patterns_to_registry, dump_buffer_to_file, get_buffered_patterns
            # Always persist temp JSON snapshot for this chunk
            buf = get_buffered_patterns()
            if buf:
                tmp_file = out / f"chunks/{chunk_id}_pending_patterns.json"
                dump_buffer_to_file(str(tmp_file))
            # Attempt UC bulk write
            flush_patterns_to_registry()
        except Exception:
            pass

        print(f"✨ [LLM BATCH] Generated {len(generated_tasks)} processor tasks for {chunk_id}")
        logger.info(f"Generated code for {len(generated_tasks)} processors in single LLM call")
        return generated_tasks
        
    except Exception as e:
        logger.error(f"Batch code generation failed: {e}")
        # Sub-batch fallback to reduce per-processor LLM calls
        try:
            import os
            sub_batch_size = int(os.environ.get("LLM_SUB_BATCH_SIZE", "10"))
        except Exception:
            sub_batch_size = 10

        if len(processors) > sub_batch_size:
            generated_tasks: List[Dict[str, Any]] = []
            for start in range(0, len(processors), sub_batch_size):
                subset = processors[start:start + sub_batch_size]
                subset_id = f"{chunk_id}__sb{start // sub_batch_size}"
                try:
                    # Reuse batch pathway for each sub-batch
                    sub_tasks = _generate_batch_processor_code(subset, subset_id, project)
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
                            "processor_index": start + idx
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
                    "processor_index": idx
                }
                generated_tasks.append(task)
            return generated_tasks

def _default_notebook_path(project: str) -> str:
    user = os.environ.get("WORKSPACE_USER") or os.environ.get("USER_EMAIL") or "Shared"
    base = f"/Users/{user}" if "@" in user else "/Shared"
    proj_name = _safe_name(project)
    return f"{base}/{proj_name}/main"

__all__ = [
    "orchestrate_nifi_migration",
    "convert_flow", 
    "build_migration_plan",
    "orchestrate_chunked_nifi_migration",
    "process_nifi_chunk",
]

@tool
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
        if xml_content.strip().startswith('<?xml') or xml_content.strip().startswith('<'):
            # Input is XML content
            root = ET.fromstring(xml_content)
        else:
            # Input is likely a file path
            import os
            if os.path.exists(xml_content):
                with open(xml_content, 'r') as f:
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
        from collections import defaultdict, deque

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


@tool
def convert_flow(
    xml_path: str,
    out_dir: str,
    project: str,
    job: str,
    notebook_path: str = "",
    emit_job_json: bool = True,
    deploy_job: bool = False,
    also_import_notebook: bool = True,
    # Cluster controls
    existing_cluster_id: str = "",
    node_type_id: str = "Standard_DS3_v2",
    spark_version: str = "16.4.x-scala2.12",
    num_workers: int = 0,
    autotermination_minutes: int = 60,
) -> str:
    """
    Convert a NiFi XML flow into a Databricks project scaffold and WRITE artifacts.
    Returns a JSON summary with output_dir and list of step files.
    """

    # --- Create a subfolder named after the project ---
    root = Path(out_dir)
    proj_name = _safe_name(project)
    out = root / proj_name
    (out / "src/steps").mkdir(parents=True, exist_ok=True)
    (out / "jobs").mkdir(parents=True, exist_ok=True)
    (out / "conf").mkdir(parents=True, exist_ok=True)
    (out / "notebooks").mkdir(parents=True, exist_ok=True)

    # --- Default notebook path if not provided ---
    if not notebook_path:
        user = os.environ.get("WORKSPACE_USER") or os.environ.get("USER_EMAIL") or "Shared"
        base = f"/Users/{user}" if "@" in user else "/Shared"
        notebook_path = f"{base}/nifi2dbx/{proj_name}/main"

    # Read NiFi XML (DBFS or local path)
    xml_text = _read_text(xml_path)

    # 1) Parse & plan
    parsed_js = parse_nifi_template_impl(xml_text)
    processors = parsed_js.get("processors", [])
    plan_js = json.loads(build_migration_plan.func(xml_text))

    # Persist plan
    _write_text(out / "conf/plan.json", json.dumps(plan_js, indent=2))

    # DAG-aware Job config that preserves NiFi dependencies
    dag_job_cfg = create_job_config_from_plan.func(
        job_name=job,
        notebook_path=notebook_path,
        plan_json=json.dumps(plan_js),
        cluster_id=existing_cluster_id,
        node_type_id=node_type_id,
        spark_version=spark_version,
        num_workers=num_workers,
        autotermination_minutes=autotermination_minutes,
    )
    _write_text(out / "jobs/job.dag.json", dag_job_cfg)

    ordered = plan_js.get("tasks", [])
    params_js = extract_nifi_parameters_and_services_impl(xml_text)

    # 2) Per-processor code generation
    step_files: List[Path] = []
    for idx, task in enumerate(ordered, start=10):
        full = next(
            (p for p in processors if p.get("type") == task.get("type") or p.get("name") == task.get("name")),
            task,
        )
        props = full.get("properties", {}) or {}
        proc_type = task.get("type", "Unknown")
        name_sn = _safe_name(task.get("name") or proc_type.split(".")[-1])

        code = generate_databricks_code.func(
            processor_type=proc_type,
            properties=json.dumps(props),
        )

        # Fallback: suggest Auto Loader for file ingestion if needed
        if ("GetFile" in proc_type or "ListFile" in proc_type) and "cloudFiles" not in code:
            al = json.loads(suggest_autoloader_options.func(json.dumps(props)))
            code = f"# Suggested Auto Loader for {proc_type}\n{al['code']}\n# Tips:\n# - " + "\n# - ".join(al["tips"])

        # Unescape the code to convert \n to actual newlines, etc.
        unescaped_code = _unescape_code(code)
        step_path = out / f"src/steps/{idx:02d}_{name_sn}.py"
        _write_text(step_path, unescaped_code)
        step_files.append(step_path)

    # 3) Bundle + README
    bundle_yaml = scaffold_asset_bundle.func(project, job, notebook_path)
    _write_text(out / "databricks.yml", bundle_yaml)

    readme = [
        f"# {project}",
        "",
        "Generated from NiFi flow.",
        "",
        "## Contents",
        "- `src/steps/` individual processor translations",
        "- `databricks.yml` Databricks Asset Bundle (jobs-as-code)",
        "- `jobs/` Jobs 2.1 JSON (DAG-based and single-task)",
        "- `conf/` parameter contexts & controller services",
        "",
        "## Next steps",
        "1. Review/merge per-step code into your main notebook or DLT pipeline.",
        "2. Use `databricks bundle validate && databricks bundle deploy` (if using Bundles).",
        "3. Or create a job from `jobs/job.json` and run it.",
    ]
    _write_text(out / "README.md", "\n".join(readme))

    # 4) Save Parameter Contexts & Controller Services
    _write_text(out / "conf/parameter_contexts.json", json.dumps(params_js, indent=2))

    # 5) Orchestrator notebook
    orchestrator = textwrap.dedent("""\
    # Databricks notebook source
    # Orchestrator notebook
    dbutils.widgets.text("STEP_MODULE", "")
    step_mod = dbutils.widgets.get("STEP_MODULE")

    import importlib.util, sys, os, glob

    def run_module(rel_path: str):
        root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        full_path = os.path.join(root, rel_path)
        if not os.path.exists(full_path):
            raise FileNotFoundError(f"Module not found: {full_path}")
        spec = importlib.util.spec_from_file_location("step_mod", full_path)
        mod = importlib.util.module_from_spec(spec)
        sys.modules["step_mod"] = mod
        spec.loader.exec_module(mod)

    if step_mod:
        print(f"Running single step: {step_mod}")
        run_module(step_mod)
    else:
        root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        steps = sorted(glob.glob(os.path.join(root, "src", "steps", "*.py")))
        print("No STEP_MODULE provided; running all steps sequentially:")
        for s in steps:
            rel = os.path.relpath(s, root)
            print(f" -> {rel}")
            run_module(rel)
    """)
    _write_text(out / "notebooks/main", orchestrator)

    # 6) Jobs JSON + optional deploy
    if emit_job_json or deploy_job:
        job_cfg = create_job_config.func(job, notebook_path)
        _write_text(out / "jobs/job.json", job_cfg)
        if deploy_job:
            res = deploy_and_run_job.func(job_cfg)
            try:
                print(f"Deploy result: {res[:200]}...")
            except Exception:
                pass

    # 7) DLT config (optional scaffold)
    dlt_cfg = generate_dlt_pipeline_config.func(
        pipeline_name=f"{project}_pipeline",
        catalog="main",
        db_schema="default",
        notebook_path=notebook_path,
    )
    _write_text(out / "conf/dlt_pipeline.json", dlt_cfg)

    # 8) Import orchestrator into /Workspace
    if also_import_notebook:
        host = os.environ.get("DATABRICKS_HOST") or os.environ.get("DATABRICKS_HOSTNAME")
        token = os.environ.get("DATABRICKS_TOKEN")
        if host and token:
            if not host.startswith("http"):
                host = "https://" + host
            headers = {"Authorization": f"Bearer {token}"}

            gs = requests.get(
                f"{host}/api/2.0/workspace/get-status",
                headers=headers,
                params={"path": notebook_path},
                timeout=30,
            )
            exists = gs.status_code == 200 and gs.json().get("object_type") == "NOTEBOOK"
            if not exists:
                nb_path = out / "notebooks/main"
                src = _read_text(nb_path)
                payload = {
                    "path": notebook_path,
                    "format": "SOURCE",
                    "language": "PYTHON",
                    "overwrite": True,
                    "content": base64.b64encode(src.encode()).decode(),
                }
                r = requests.post(
                    f"{host}/api/2.0/workspace/import",
                    headers=headers,
                    json=payload,
                    timeout=60,
                )
                try:
                    print(f"Notebook import: {r.status_code} {r.text[:160]}")
                except Exception:
                    pass

    summary = {
        "output_dir": str(out),
        "steps_written": [str(p) for p in step_files],
        "notebook_path": notebook_path,
        "continue_required": False,
        "tool_name": "convert_flow"
    }
    return json.dumps(summary, indent=2)

@tool
def orchestrate_nifi_migration(
    xml_path: str,
    out_dir: str,
    project: str,
    job: str,
    notebook_path: str = "",
    existing_cluster_id: str = "",
    deploy: bool = False,
) -> str:
    """
    End-to-end orchestration:
    - Convert NiFi XML to Databricks scaffold (steps, conf, jobs, notebooks).
    - Build DAG-aware job JSON (reusing existing cluster if provided).
    - Optionally deploy the job (but not run it).
    Returns JSON summary with output_dir, steps, job_config_path, and optional deploy_result.
    """

    # --- ensure default notebook path if none provided
    if not notebook_path:
        notebook_path = _default_notebook_path(project)

    # --- 1) Run conversion (this already decides the final project subdir)
    summary = json.loads(convert_flow.func(
        xml_path=xml_path,
        out_dir=out_dir,
        project=project,
        job=job,
        notebook_path=notebook_path,
        emit_job_json=True,
        deploy_job=False,
        also_import_notebook=True,
        existing_cluster_id=existing_cluster_id,
    ))

    # Always work under the returned project subdir
    project_out = Path(summary["output_dir"])

    # --- 2) Build plan directly from XML text
    xml_text = _read_text(xml_path)
    plan_json = build_migration_plan.func(xml_text)

    # --- 3) DAG-aware job JSON using NiFi task dependencies
    dag_job_json = create_job_config_from_plan.func(
        job_name=job,
        notebook_path=notebook_path,
        plan_json=plan_json,
        cluster_id=existing_cluster_id,
    )

    jobs_dir = project_out / "jobs"
    jobs_dir.mkdir(parents=True, exist_ok=True)
    dag_path = jobs_dir / "job.dag.json"
    _write_text(dag_path, dag_job_json)

    result = {
        "output_dir": str(project_out),
        "steps_written": summary["steps_written"],
        "job_config_path": str(dag_path),
        "continue_required": False,
        "tool_name": "orchestrate_nifi_migration"
    }

    # --- 4) Optionally create the job (don't run)
    if deploy:
        deploy_res = deploy_and_run_job.func(dag_job_json, run_now=False)
        try:
            result["deploy_result"] = (
                json.loads(deploy_res) if str(deploy_res).startswith("{") else deploy_res
            )
        except Exception:
            result["deploy_result"] = deploy_res

    return json.dumps(result, indent=2)

@tool
def process_nifi_chunk(
    chunk_data: str, 
    project: str,
    chunk_index: int = 0
) -> str:
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
            "tool_name": "process_nifi_chunk"
        }
        
        return json.dumps(result, indent=2)
        
    except Exception as e:
        return json.dumps({"error": f"Error processing chunk: {str(e)}", "chunk_data": chunk_data[:200]})

@tool 
def orchestrate_chunked_nifi_migration(
    xml_path: str,
    out_dir: str, 
    project: str,
    job: str,
    notebook_path: str = "",
    max_processors_per_chunk: int = 25,
    existing_cluster_id: str = "",
    deploy: bool = False
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
        max_processors_per_chunk: Maximum processors per chunk (default: 25)
        existing_cluster_id: Existing cluster ID to use
        deploy: Whether to deploy the job to Databricks
        
    Returns:
        JSON summary with chunking statistics, processing results, and final workflow
    """
    try:
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
            return json.dumps({"error": f"Workflow map extraction failed: {workflow_map_result['error']}"})
        
        # Save complete workflow map for reference
        _write_text(out / "conf/complete_workflow_map.json", json.dumps(workflow_map_result, indent=2))
        
        # Step 1: Chunk the XML by process groups
        chunking_result = json.loads(chunk_nifi_xml_by_process_groups.func(
            xml_content=xml_text,
            max_processors_per_chunk=max_processors_per_chunk
        ))
        
        if "error" in chunking_result:
            return json.dumps({"error": f"Chunking failed: {chunking_result['error']}"})
        
        chunks = chunking_result["chunks"]
        cross_chunk_links = chunking_result["cross_chunk_links"]
        summary = chunking_result["summary"]
        
        # Save chunking results
        _write_text(out / "conf/chunking_result.json", json.dumps(chunking_result, indent=2))
        
        # Step 2: Process each chunk individually
        chunk_results = []
        all_step_files = []
        
        print(f"📋 [MIGRATION] Processing {len(chunks)} chunks with {summary['total_processors']} total processors")
        print(f"🎯 [MIGRATION] Target: max {max_processors_per_chunk} processors per chunk")
        
        for i, chunk in enumerate(chunks):
            chunk_processor_count = len(chunk.get("processors", []))
            print(f"\n📦 [CHUNK {i+1}/{len(chunks)}] Processing {chunk_processor_count} processors...")
            chunk_data = json.dumps(chunk)
            
            # Process this chunk
            chunk_result = json.loads(process_nifi_chunk.func(
                chunk_data=chunk_data,
                project=project,
                chunk_index=i
            ))
            
            if "error" in chunk_result:
                print(f"❌ [CHUNK {i+1}/{len(chunks)}] Error: {chunk_result['error']}")
                logger.warning(f"Error processing chunk {i}: {chunk_result['error']}")
                continue
            
            print(f"✅ [CHUNK {i+1}/{len(chunks)}] Generated {chunk_result.get('task_count', 0)} tasks")
            chunk_results.append(chunk_result)
            
            # Write individual task files for this chunk
            for task in chunk_result["tasks"]:
                task_name = task["name"]
                code = task["code"]
                # Unescape the code to convert \n to actual newlines, etc.
                unescaped_code = _unescape_code(code)
                step_path = out / f"src/steps/{i:02d}_{task_name}.py"
                _write_text(step_path, unescaped_code)
                all_step_files.append(str(step_path))
            
            # Save chunk processing result
            _write_text(out / f"chunks/chunk_{i}_result.json", json.dumps(chunk_result, indent=2))
        
        # Step 3: Reconstruct the full workflow with complete connectivity map
        workflow_result = json.loads(reconstruct_full_workflow.func(
            chunk_results_json=json.dumps(chunk_results),
            cross_chunk_links_json=json.dumps(cross_chunk_links),
            workflow_map_json=json.dumps(workflow_map_result)
        ))
        
        if "error" in workflow_result:
            return json.dumps({"error": f"Workflow reconstruction failed: {workflow_result['error']}"})
        
        # Save reconstructed workflow
        _write_text(out / "conf/reconstructed_workflow.json", json.dumps(workflow_result, indent=2))
        
        # Step 4: Generate final Databricks job configuration
        final_job_config = workflow_result.get("databricks_job_config", {})
        final_job_config.update({
            "name": job,
            "email_notifications": {
                "on_failure": [os.environ.get("NOTIFICATION_EMAIL", "")],
            } if os.environ.get("NOTIFICATION_EMAIL") else {},
        })
        
        # Enhance job config with cluster settings
        if existing_cluster_id:
            for task in final_job_config.get("tasks", []):
                task["existing_cluster_id"] = existing_cluster_id
        else:
            cluster_config = {
                "spark_version": "16.4.x-scala2.12",
                "node_type_id": "Standard_DS3_v2", 
                "num_workers": 0,
                "autotermination_minutes": 60
            }
            for task in final_job_config.get("tasks", []):
                task["new_cluster"] = cluster_config
        
        _write_text(out / "jobs/job.chunked.json", json.dumps(final_job_config, indent=2))
        
        # Step 5: Finalize pattern persistence at end of migration
        try:
            from tools.pattern_tools import flush_patterns_to_registry, dump_buffer_to_file, get_buffered_patterns
            # Persist any remaining buffered patterns to temp file and UC
            buf = get_buffered_patterns()
            if buf:
                tmp_file = out / "conf/pending_patterns.final.json"
                dump_buffer_to_file(str(tmp_file))
            flush_patterns_to_registry()
        except Exception:
            pass

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
            "## Next steps",
            "1. Review the chunked migration results in `conf/`",
            "2. Test individual chunks if needed using files in `chunks/`",
            "3. Deploy the final job using `jobs/job.chunked.json`",
            "4. Monitor cross-chunk dependencies for correct execution order",
        ]
        _write_text(out / "README.md", "\n".join(readme))
        
        # Step 7: Save parameter contexts and controller services
        params_js = extract_nifi_parameters_and_services_impl(xml_text)
        _write_text(out / "conf/parameter_contexts.json", json.dumps(params_js, indent=2))
        
        # Step 8: Generate orchestrator notebook (enhanced for chunked processing)
        orchestrator = textwrap.dedent(f"""\
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
        """)
        _write_text(out / "notebooks/main", orchestrator)
        
        # Step 9: Optional deployment
        deploy_result = None
        if deploy:
            deploy_result = deploy_and_run_job.func(json.dumps(final_job_config), run_now=False)
        
        # Final result summary
        total_tasks = sum(len(cr["tasks"]) for cr in chunk_results)
        
        print(f"\n🎉 [MIGRATION COMPLETE]")
        print(f"📊 [SUMMARY] Processed {summary['total_processors']} processors → {total_tasks} tasks")
        print(f"📊 [SUMMARY] Used {len(chunk_results)} chunks (max {max_processors_per_chunk} processors/chunk)")
        print(f"📊 [SUMMARY] Generated {len(all_step_files)} step files")
        print(f"📊 [SUMMARY] Output directory: {out}")
        if deploy:
            print(f"🚀 [DEPLOY] Job deployment: {deploy_result}")
        
        result = {
            "migration_type": "chunked",
            "output_dir": str(out),
            "chunking_summary": summary,
            "chunks_processed": len(chunk_results),
            "total_tasks_generated": total_tasks,
            "step_files_written": all_step_files,
            "cross_chunk_links_count": len(cross_chunk_links),
            "final_job_config_path": str(out / "jobs/job.chunked.json"),
            "notebook_path": notebook_path,
            "deploy_result": deploy_result,
            "continue_required": False,
            "tool_name": "orchestrate_chunked_nifi_migration"
        }
        
        return json.dumps(result, indent=2)
        
    except Exception as e:
        return json.dumps({"error": f"Chunked migration failed: {str(e)}"})


def _analyze_nifi_requirements_internal(xml_content: str) -> dict:
    """Internal helper to analyze NiFi architecture requirements without tool call."""
    try:
        root = ET.fromstring(xml_content)
        
        # Processor classification (copied from xml_tools.py)
        streaming_sources = {
            "ListenHTTP", "ConsumeKafka", "ListenTCP", "ListenUDP", "ListenSyslog",
            "ConsumeJMS", "ConsumeMQTT", "ConsumeAMQP", "GetTwitter", "ListenRELP"
        }
        
        batch_sources = {
            "GetFile", "ListFile", "FetchFile", "GetFTP", "GetSFTP", 
            "FetchS3Object", "ListS3", "GetHDFS", "QueryDatabaseTable"
        }
        
        transform_processors = {
            "EvaluateJsonPath", "UpdateAttribute", "ReplaceText", "TransformXml",
            "ConvertRecord", "SplitText", "SplitJson", "MergeContent", "CompressContent",
            "EncryptContent", "HashContent", "ValidateRecord", "LookupRecord"
        }
        
        routing_processors = {
            "RouteOnAttribute", "RouteOnContent", "RouteText", "RouteJSON",
            "DistributeLoad", "ControlRate", "PriorizeAttribute"
        }
        
        json_processors = {
            "EvaluateJsonPath", "SplitJson", "ConvertJSONToSQL", "JoltTransformJSON"
        }
        
        external_sinks = {
            "PublishKafka", "InvokeHTTP", "PutEmail", "PutSFTP", "PutFTP",
            "PublishJMS", "PublishMQTT", "PutElasticsearch", "PutSlack"
        }
        
        feature_flags = {
            "has_streaming": False,
            "has_batch": False,
            "has_transforms": False,
            "has_external_sinks": False,
            "has_routing": False,
            "has_json_processing": False
        }
        
        processor_analysis = {
            "sources": [],
            "transforms": [],
            "sinks": [],
            "total_count": 0
        }
        
        # Analyze all processors
        for processor in root.findall(".//processors"):
            proc_type = (processor.findtext("type") or "").strip()
            proc_name = (processor.findtext("name") or "Unknown").strip()
            
            class_name = proc_type.split(".")[-1] if "." in proc_type else proc_type
            processor_analysis["total_count"] += 1
            
            if class_name in streaming_sources:
                feature_flags["has_streaming"] = True
                processor_analysis["sources"].append({
                    "name": proc_name, "type": class_name, "category": "streaming_source"
                })
            elif class_name in batch_sources:
                feature_flags["has_batch"] = True
                processor_analysis["sources"].append({
                    "name": proc_name, "type": class_name, "category": "batch_source"
                })
            elif class_name in transform_processors:
                feature_flags["has_transforms"] = True
                processor_analysis["transforms"].append({
                    "name": proc_name, "type": class_name, "category": "transform"
                })
            elif class_name in routing_processors:
                feature_flags["has_routing"] = True
                processor_analysis["transforms"].append({
                    "name": proc_name, "type": class_name, "category": "routing"
                })
            elif class_name in external_sinks:
                feature_flags["has_external_sinks"] = True
                processor_analysis["sinks"].append({
                    "name": proc_name, "type": class_name, "category": "external_sink"
                })
            
            if class_name in json_processors:
                feature_flags["has_json_processing"] = True
        
        # Determine complexity level
        complexity_factors = [
            feature_flags["has_streaming"] and feature_flags["has_batch"],
            feature_flags["has_routing"],
            feature_flags["has_json_processing"],
            len(processor_analysis["sinks"]) > 2,
            processor_analysis["total_count"] > 10
        ]
        
        complexity_score = sum(complexity_factors)
        if complexity_score >= 3:
            complexity_level = "complex"
        elif complexity_score >= 1:
            complexity_level = "moderate"
        else:
            complexity_level = "simple"
        
        return {
            "feature_flags": feature_flags,
            "processor_analysis": processor_analysis,
            "complexity_level": complexity_level
        }
    except Exception as e:
        logger.error(f"Error analyzing NiFi requirements: {e}")
        return {"error": str(e)}

def _recommend_architecture_internal(analysis: dict) -> dict:
    """Internal helper to recommend Databricks architecture without tool call."""
    try:
        feature_flags = analysis["feature_flags"]
        processor_analysis = analysis["processor_analysis"] 
        complexity_level = analysis["complexity_level"]
        
        reasoning = []
        confidence = "high"
        alternative_options = []
        
        # Decision Rule 1: Pure batch processing
        if (feature_flags["has_batch"] and 
            not feature_flags["has_streaming"] and 
            not feature_flags["has_routing"]):
            
            recommendation = "databricks_job"
            reasoning.append("Only batch file sources detected (GetFile, ListFile)")
            reasoning.append("No streaming sources or complex routing logic")
            reasoning.append("Simple batch orchestration is sufficient")
            
            architecture_details = {
                "job_type": "scheduled_batch",
                "source_pattern": "Auto Loader for file ingestion",
                "sink_pattern": "Delta Lake writes",
                "scheduling": "Triggered or scheduled execution"
            }
            
        # Decision Rule 2: Streaming sources present
        elif feature_flags["has_streaming"]:
            
            if (feature_flags["has_transforms"] or 
                feature_flags["has_routing"] or 
                feature_flags["has_json_processing"]):
                
                recommendation = "dlt_pipeline"
                reasoning.append("Streaming sources detected (ListenHTTP, ConsumeKafka, etc.)")
                reasoning.append("Complex transformations and/or routing logic present")
                reasoning.append("DLT provides best streaming ETL capabilities")
                
                architecture_details = {
                    "pipeline_type": "streaming_etl",
                    "source_pattern": "Structured Streaming sources",
                    "transform_pattern": "Declarative SQL/PySpark transformations",
                    "sink_pattern": "Delta Live Tables with data quality"
                }
            else:
                recommendation = "structured_streaming"
                reasoning.append("Streaming sources present but minimal transformations")
                reasoning.append("Custom Structured Streaming may be more appropriate")
                
                architecture_details = {
                    "pipeline_type": "custom_streaming",
                    "source_pattern": "readStream() operations",
                    "sink_pattern": "writeStream() to Delta tables"
                }
        
        # Default fallback
        else:
            recommendation = "databricks_job"
            reasoning.append("Standard ETL pattern detected")
            reasoning.append("Databricks Job provides good orchestration capabilities")
            confidence = "medium"
            
            architecture_details = {
                "job_type": "multi_task_etl",
                "orchestration": "Task dependencies based on processor connections"
            }
        
        return {
            "recommendation": recommendation,
            "confidence": confidence, 
            "reasoning": reasoning,
            "architecture_details": architecture_details,
            "alternative_options": alternative_options,
            "analysis_summary": {
                "total_processors": processor_analysis["total_count"],
                "complexity": complexity_level,
                "key_features": [k for k, v in feature_flags.items() if v]
            }
        }
    except Exception as e:
        logger.error(f"Error recommending architecture: {e}")
        return {"error": str(e)}

@tool
def orchestrate_intelligent_nifi_migration(
    xml_path: str,
    out_dir: str,
    project: str,
    job: str = "",
    notebook_path: str = "",
    existing_cluster_id: str = "",
    deploy: bool = False,
    max_processors_per_chunk: int = 25
) -> str:
    """
    Intelligently migrate NiFi workflow by analyzing XML and automatically choosing the best Databricks architecture.
    
    Uses AI-powered decision system to determine whether to generate:
    - Databricks Job (batch orchestration)
    - DLT Pipeline (streaming ETL)
    - Structured Streaming (custom streaming logic)
    
    Args:
        xml_path: Path to NiFi XML template file
        out_dir: Output directory for generated files
        project: Project name for generated assets
        job: Job name (optional, defaults to project name)
        notebook_path: Databricks notebook path for job execution
        existing_cluster_id: Existing cluster to use (optional)
        deploy: Whether to deploy the generated job/pipeline
        max_processors_per_chunk: Max processors per chunk for large workflows
    
    Returns:
        JSON with migration results, architecture decision, and deployment details
    """
    try:
        # Read XML content
        if xml_path.startswith('/'):
            xml_content = _read_text(xml_path)
        else:
            xml_content = xml_path  # Assume it's actual XML content
            
        # Analyze architecture requirements (internal, no tool call)
        logger.info(f"Analyzing NiFi architecture requirements for intelligent migration...")
        analysis = _analyze_nifi_requirements_internal(xml_content)
        if "error" in analysis:
            return json.dumps({"error": f"Analysis failed: {analysis['error']}"})
        
        # Get architecture recommendation (internal, no tool call)
        recommendation = _recommend_architecture_internal(analysis)
        
        logger.info(f"Architecture analysis complete:")
        logger.info(f"  - Total processors: {analysis['processor_analysis']['total_count']}")
        logger.info(f"  - Complexity: {analysis['complexity_level']}")
        logger.info(f"  - Key features: {analysis['feature_flags']}")
        logger.info(f"  - Recommendation: {recommendation['recommendation']} (confidence: {recommendation['confidence']})")
        
        # Execute migration based on recommendation (use internal function to avoid tool chaining)
        architecture_type = recommendation["recommendation"]
        migration_result = {}
        
        if architecture_type == "databricks_job":
            logger.info("Executing Databricks Job migration...")
            
            # Call chunked migration using .func() but this is the final migration call
            migration_result_str = orchestrate_chunked_nifi_migration.func(
                xml_path=xml_path,
                out_dir=out_dir,
                project=project,
                job=job or f"{project}_job",
                notebook_path=notebook_path,
                existing_cluster_id=existing_cluster_id,
                deploy=deploy,
                max_processors_per_chunk=max_processors_per_chunk
            )
            migration_result = json.loads(migration_result_str)
            
        elif architecture_type == "dlt_pipeline":
            logger.info("Executing DLT Pipeline migration...")
            
            # Use DLT-specific migration approach
            try:
                # Create a simple DLT config without calling external tools
                dlt_result = {
                    "name": f"{project}_dlt_pipeline",
                    "storage": f"/pipelines/{project}_dlt_pipeline",
                    "target": f"main.{project}_dlt",
                    "development": True,
                    "continuous": True,
                    "libraries": [{"notebook": {"path": notebook_path or f"/Workspace/Users/me@company.com/{project}/main"}}],
                }
                
                migration_result = {
                    "migration_type": "dlt_pipeline",
                    "dlt_config": dlt_result,
                    "project_path": out_dir,
                    "pipeline_name": f"{project}_dlt_pipeline"
                }
                
            except Exception as dlt_error:
                logger.warning(f"DLT generation failed: {dlt_error}")
                logger.info("Falling back to Databricks Job migration...")
                
                # Fallback to job migration using .func()
                migration_result_str = orchestrate_chunked_nifi_migration.func(
                    xml_path=xml_path,
                    out_dir=out_dir,
                    project=project,
                    job=job or f"{project}_job",
                    notebook_path=notebook_path,
                    existing_cluster_id=existing_cluster_id,
                    deploy=deploy,
                    max_processors_per_chunk=max_processors_per_chunk
                )
                migration_result = json.loads(migration_result_str)
                
        elif architecture_type == "structured_streaming":
            logger.info("Executing Structured Streaming migration...")
            
            # Use job migration with streaming-optimized settings using .func()
            migration_result_str = orchestrate_chunked_nifi_migration.func(
                xml_path=xml_path,
                out_dir=out_dir,
                project=project,
                job=job or f"{project}_streaming_job",
                notebook_path=notebook_path,
                existing_cluster_id=existing_cluster_id,
                deploy=deploy,
                max_processors_per_chunk=max_processors_per_chunk
            )
            migration_result = json.loads(migration_result_str)
            
            # Add streaming-specific notes
            migration_result["streaming_notes"] = [
                "Generated as Databricks Job with streaming-capable tasks",
                "Consider converting to dedicated Structured Streaming application",
                "Review generated code for streaming optimizations"
            ]
        
        # Combine results
        result = {
            "intelligent_migration": True,
            "architecture_analysis": analysis,
            "architecture_recommendation": recommendation,
            "migration_execution": migration_result,
            "success": True,
            "continue_required": False,
            "tool_name": "orchestrate_intelligent_nifi_migration"
        }
        
        # Save analysis results to output directory
        try:
            analysis_file = Path(out_dir) / _safe_name(project) / "conf" / "architecture_analysis.json"
            analysis_file.parent.mkdir(parents=True, exist_ok=True)
            
            combined_analysis = {
                "analysis": analysis,
                "recommendation": recommendation,
                "execution_summary": {
                    "architecture_chosen": architecture_type,
                    "confidence": recommendation["confidence"],
                    "reasoning": recommendation["reasoning"]
                }
            }
            
            _write_text(str(analysis_file), json.dumps(combined_analysis, indent=2))
            result["analysis_file"] = str(analysis_file)
            
        except Exception as save_error:
            logger.warning(f"Could not save analysis file: {save_error}")
        
        logger.info(f"Intelligent migration completed successfully!")
        logger.info(f"Architecture chosen: {architecture_type}")
        logger.info(f"Confidence: {recommendation['confidence']}")
        
        return json.dumps(result, indent=2)
        
    except Exception as e:
        logger.error(f"Intelligent migration failed: {str(e)}")
        return json.dumps({
            "intelligent_migration": True,
            "success": False,
            "error": str(e),
            "fallback_suggestion": "Try using orchestrate_chunked_nifi_migration directly"
        })

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

        step_path = out / f"src/steps/{idx:02d}_{name_sn}.py"
        _write_text(step_path, code)
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
        
        # Process each processor in the chunk
        for idx, processor in enumerate(processors):
            proc_type = processor.get("type", "Unknown")
            proc_name = processor.get("name", f"processor_{idx}")
            props = processor.get("properties", {})
            
            # Generate Databricks code for this processor
            code = generate_databricks_code.func(
                processor_type=proc_type,
                properties=json.dumps(props),
            )
            
            # Enhance code with Auto Loader suggestions if needed
            if ("GetFile" in proc_type or "ListFile" in proc_type) and "cloudFiles" not in code:
                al = json.loads(suggest_autoloader_options.func(json.dumps(props)))
                code = f"# Suggested Auto Loader for {proc_type}\n{al['code']}\n# Tips:\n# - " + "\n# - ".join(al["tips"])
            
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
        
        for i, chunk in enumerate(chunks):
            chunk_data = json.dumps(chunk)
            
            # Process this chunk
            chunk_result = json.loads(process_nifi_chunk.func(
                chunk_data=chunk_data,
                project=project,
                chunk_index=i
            ))
            
            if "error" in chunk_result:
                logger.warning(f"Error processing chunk {i}: {chunk_result['error']}")
                continue
            
            chunk_results.append(chunk_result)
            
            # Write individual task files for this chunk
            for task in chunk_result["tasks"]:
                task_name = task["name"]
                code = task["code"]
                step_path = out / f"src/steps/{i:02d}_{task_name}.py"
                _write_text(step_path, code)
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
        
        # Step 5: Generate project artifacts
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
        
        # Step 6: Save parameter contexts and controller services
        params_js = extract_nifi_parameters_and_services_impl(xml_text)
        _write_text(out / "conf/parameter_contexts.json", json.dumps(params_js, indent=2))
        
        # Step 7: Generate orchestrator notebook (enhanced for chunked processing)
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
        
        # Step 8: Optional deployment
        deploy_result = None
        if deploy:
            deploy_result = deploy_and_run_job.func(json.dumps(final_job_config), run_now=False)
        
        # Final result summary
        result = {
            "migration_type": "chunked",
            "output_dir": str(out),
            "chunking_summary": summary,
            "chunks_processed": len(chunk_results),
            "total_tasks_generated": sum(len(cr["tasks"]) for cr in chunk_results),
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

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

def _default_notebook_path(project: str) -> str:
    user = os.environ.get("WORKSPACE_USER") or os.environ.get("USER_EMAIL") or "Shared"
    base = f"/Users/{user}" if "@" in user else "/Shared"
    proj_name = _safe_name(project)
    return f"{base}/{proj_name}/main"

__all__ = [
    "orchestrate_nifi_migration",
    "convert_flow",
    "build_migration_plan",
]

@tool
def build_migration_plan(xml_content: str) -> str:
    """
    Produce a topologically sorted DAG of NiFi processors based on Connections.

    Returns JSON:
      {
        "tasks": [{"id": "...", "name": "...", "type": "..."}, ...],
        "edges": [["src_id","dst_id"], ...],
        "note": "..."
      }
    """
    try:
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
    }

    # --- 4) Optionally create the job (don’t run)
    if deploy:
        deploy_res = deploy_and_run_job.func(dag_job_json, run_now=False)
        try:
            result["deploy_result"] = (
                json.loads(deploy_res) if str(deploy_res).startswith("{") else deploy_res
            )
        except Exception:
            result["deploy_result"] = deploy_res

    return json.dumps(result, indent=2)

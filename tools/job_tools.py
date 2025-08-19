# tools/job_tools.py
# Databricks Jobs creation, config-from-plan, deployment, and bundle scaffold.

from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Dict, List, Optional

import requests
from langchain_core.tools import tool

from utils.file_ops import safe_name as _safe_name

__all__ = [
    "create_job_config",
    "create_job_config_from_plan",
    "deploy_and_run_job",
    "scaffold_asset_bundle",
]


@tool
def create_job_config(
    job_name: str,
    notebook_path: str,
    schedule: str = "",
    cluster_id: str = "",
    node_type_id: str = "Standard_DS3_v2",
    spark_version: str = "16.4.x-scala2.12",
    num_workers: int = 0,
    autotermination_minutes: int = 60,
    notification_email: str = "",
) -> str:
    """
    Create a Databricks job configuration JSON for a single-notebook task.
    """
    if cluster_id:
        cluster_block = {"existing_cluster_id": cluster_id}
    else:
        cluster_block = {
            "new_cluster": {
                "spark_version": spark_version,
                "node_type_id": node_type_id,
                "num_workers": num_workers,
                "autotermination_minutes": autotermination_minutes,
                "spark_conf": {
                    "spark.sql.adaptive.enabled": "true",
                    "spark.sql.adaptive.coalescePartitions.enabled": "true",
                },
            }
        }
        if num_workers == 0:
            cluster_block["new_cluster"]["spark_conf"].update({
                "spark.databricks.cluster.profile": "singleNode",
                "spark.master": "local[*]",
            })

    job_config = {
        "name": job_name,
        "tasks": [
            {
                "task_key": f"{job_name}_task",
                "notebook_task": {"notebook_path": notebook_path, "base_parameters": {}},
                **cluster_block,
                "timeout_seconds": 3600,
                "max_retries": 2,
                "retry_on_timeout": True,
            }
        ],
        "max_concurrent_runs": 1,
    }

    if notification_email:
        job_config["email_notifications"] = {"on_failure": [notification_email]}

    if schedule:
        job_config["schedule"] = {
            "quartz_cron_expression": schedule,
            "timezone_id": "UTC",
            "pause_status": "UNPAUSED",
        }

    return json.dumps(job_config, indent=2)


@tool
def create_job_config_from_plan(
    job_name: str,
    notebook_path: str,
    plan_json: str,
    # cluster selection
    cluster_id: str = "",
    node_type_id: str = "Standard_DS3_v2",
    spark_version: str = "16.4.x-scala2.12",
    num_workers: int = 0,
    autotermination_minutes: int = 60,
    # task behavior
    timeout_seconds: int = 3600,
    max_retries: int = 2,
    # notifications
    notification_email: Optional[str] = None,
) -> str:
    """
    Build a Jobs 2.1 JSON where each NiFi processor becomes a task and edges become depends_on.
    Every task runs the same notebook with base_parameters.STEP_MODULE set to that step file.
    """
    plan = json.loads(plan_json)
    tasks_meta: List[Dict] = plan.get("tasks", [])
    edges: List[List[str]] = plan.get("edges", [])

    # Map NiFi id → task_key & step module path
    id_to_key: Dict[str, str] = {}
    id_to_module: Dict[str, str] = {}
    for idx, t in enumerate(tasks_meta, start=10):
        key = f"step_{idx:02d}"
        safe = _safe_name((t.get("name") or t.get("type", "step")).split(".")[-1])
        module_rel = f"src/steps/{idx:02d}_{safe}.py"
        id_to_key[t["id"]] = key
        id_to_module[t["id"]] = module_rel

    # Reverse lookup for dependencies
    parents = {t["id"]: [] for t in tasks_meta}
    for s, d in edges:
        if d in parents:
            parents[d].append(s)

    def _cluster_block():
        if cluster_id:
            return {"existing_cluster_id": cluster_id}
        newc = {
            "new_cluster": {
                "spark_version": spark_version,
                "node_type_id": node_type_id,
                "num_workers": num_workers,
                "autotermination_minutes": autotermination_minutes,
            }
        }
        if num_workers == 0:
            newc["new_cluster"]["spark_conf"] = {
                "spark.databricks.cluster.profile": "singleNode",
                "spark.master": "local[*]",
            }
        return newc

    tasks = []
    for t in tasks_meta:
        tid = t["id"]
        task = {
            "task_key": id_to_key[tid],
            "notebook_task": {
                "notebook_path": notebook_path,
                "base_parameters": {"STEP_MODULE": id_to_module[tid]},
            },
            "timeout_seconds": timeout_seconds,
            "max_retries": max_retries,
            **_cluster_block(),
        }
        if parents[tid]:
            task["depends_on"] = [{"task_key": id_to_key[p]} for p in parents[tid]]
        tasks.append(task)

    job_cfg = {"name": job_name, "tasks": tasks, "max_concurrent_runs": 1}
    if notification_email:
        job_cfg["email_notifications"] = {"on_failure": [notification_email]}

    return json.dumps(job_cfg, indent=2)


@tool
def deploy_and_run_job(job_config_json: str, run_now: bool = True) -> str:
    """
    Create a Databricks Job via REST 2.1. If run_now=True, also trigger a run.
    Returns JSON: {"job_id": ..., "run_id": ...?} or error text.
    """
    host = os.environ.get("DATABRICKS_HOST") or os.environ.get("DATABRICKS_HOSTNAME")
    token = os.environ.get("DATABRICKS_TOKEN")
    if not (host and token):
        return "Missing DATABRICKS_HOST and/or DATABRICKS_TOKEN"
    if not host.startswith("http"):
        host = "https://" + host

    try:
        cfg = json.loads(job_config_json)
    except Exception as e:
        return f"Invalid job_config_json: {e}"

    headers = {"Authorization": f"Bearer {token}"}

    create = requests.post(f"{host}/api/2.1/jobs/create", json=cfg, headers=headers, timeout=60)
    if create.status_code >= 300:
        return f"Create failed: {create.status_code} {create.text}"
    job_id = create.json().get("job_id")

    if run_now:
        run = requests.post(
            f"{host}/api/2.1/jobs/run-now",
            json={"job_id": job_id},
            headers=headers,
            timeout=60,
        )
        if run.status_code >= 300:
            return json.dumps({"job_id": job_id, "run_error": f"{run.status_code} {run.text}"}, indent=2)
        return json.dumps({"job_id": job_id, "run_id": run.json().get("run_id")}, indent=2)

    return json.dumps({"job_id": job_id}, indent=2)


@tool
def scaffold_asset_bundle(
    project_name: str,
    job_name: str,
    notebook_path: str,
    existing_cluster_id: str = "",
    node_type_id: str = "Standard_DS3_v2",
    spark_version: str = "16.4.x-scala2.12",
    num_workers: int = 0,
    autotermination_minutes: int = 60,
) -> str:
    """
    Return a minimal databricks.yml for a Bundle with one job.
    """
    if existing_cluster_id:
        cluster_block = f"          existing_cluster_id: {existing_cluster_id}"
    else:
        single_node_conf = ""
        if num_workers == 0:
            single_node_conf = (
                "            spark_conf:\n"
                "              spark.databricks.cluster.profile: singleNode\n"
                "              spark.master: local[*]\n"
            )
        cluster_block = (
            "          new_cluster:\n"
            f"            spark_version: {spark_version}\n"
            f"            node_type_id: {node_type_id}\n"
            f"            num_workers: {num_workers}\n"
            f"            autotermination_minutes: {autotermination_minutes}\n"
            f"{single_node_conf}"
        )

    bundle = f"""bundle:
  name: {project_name}

resources:
  jobs:
    {job_name}:
      name: {job_name}
      tasks:
        - task_key: main
          notebook_task:
            notebook_path: "{notebook_path}"
{cluster_block}

targets:
  dev:
    default: true
"""
    return bundle

import json
import os
from typing import Annotated, Any, Generator, Optional, Sequence, TypedDict, Union, List, Dict
from uuid import uuid4
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path
import logging
from dotenv import load_dotenv
import textwrap
import base64, requests

from registry import PatternRegistryUC
from config import logger, DATABRICKS_HOSTNAME, DATABRICKS_TOKEN, MODEL_ENDPOINT, NOTIFICATION_EMAIL
from utils import safe_name, write_text, read_text
from utils import parse_nifi_template, extract_nifi_parameters_and_services

import mlflow
from databricks_langchain import ChatDatabricks
from langchain_core.language_models import LanguageModelLike
from langchain_core.messages import (
    AIMessage,
    AIMessageChunk,
    BaseMessage,
)
from langchain_core.runnables import RunnableConfig, RunnableLambda
from langchain_core.tools import BaseTool
from langgraph.graph import END, StateGraph
from langgraph.graph.message import add_messages
from langgraph.prebuilt.tool_node import ToolNode
from mlflow.pyfunc import ResponsesAgent
from mlflow.types.responses import (
    ResponsesAgentRequest,
    ResponsesAgentResponse,
    ResponsesAgentStreamEvent,
)
from langchain_core.tools import tool

## Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

## Environment Variables
env_path = (Path(__file__).parent / ".env") if "__file__" in globals() else Path(".env")
if env_path.exists():
    load_dotenv(env_path)
    logger.info(f"Loaded .env from {env_path}")
else:
    logger.info("No .env file found; using cluster/job environment variables")

############################################
# Pattern Registry for Scalability
############################################
# Initialize global pattern registry
pattern_registry = PatternRegistryUC()

############################################
# Define your LLM endpoint and system prompt
############################################

# Configure ChatDatabricks properly with explicit credentials
if not DATABRICKS_TOKEN:
    raise ValueError("DATABRICKS_TOKEN environment variable is required")

# Set environment variables for authentication (ChatDatabricks reads these)
os.environ['DATABRICKS_TOKEN'] = DATABRICKS_TOKEN
os.environ['DATABRICKS_HOST'] = DATABRICKS_HOSTNAME  # Note: DATABRICKS_HOST not DATABRICKS_HOSTNAME

# Initialize ChatDatabricks exactly like your working agent.py
llm = ChatDatabricks(endpoint=MODEL_ENDPOINT)

# Updated system prompt for NiFi to Databricks migration
system_prompt = """You are an expert in Apache NiFi and Databricks migration. 
Your role is to help users convert NiFi workflows to Databricks pipelines using:
- Auto Loader for file ingestion (replacing GetFile/ListFile processors)
- Delta Lake for data storage (replacing PutHDFS/PutFile processors)
- Structured Streaming for real-time processing
- Databricks Jobs for orchestration

Always provide executable PySpark code and explain the migration patterns."""

###############################################################################
## Custom Tools for NiFi to Databricks Migration
###############################################################################

@tool
def orchestrate_nifi_migration(
    xml_path: str,
    out_dir: str,
    project: str,
    job: str,
    notebook_path: str,
    existing_cluster_id: str = "",
    deploy: bool = False,
) -> str:
    """
    End-to-end: convert NiFi to Databricks artifacts, build a DAG-aware job that
    reuses an existing cluster when provided, and optionally create (not run) the job.
    Returns a JSON summary with output_dir, steps, job_config_path, and (optional) job_id.
    """
    # 1) Convert (writes steps, orchestrator notebook, conf/plan.json, jobs/job.dag.json)
    summary = json.loads(convert_flow.func(
        xml_path=xml_path,
        out_dir=out_dir,
        project=project,
        job=job,
        notebook_path=notebook_path,
        emit_job_json=True,
        deploy_job=False,
        also_import_notebook=True,
        # make convert_flow forward this to DAG config creation (see section 3)
        existing_cluster_id=existing_cluster_id
    ))

    # 2) Build plan from XML (avoid file IO ambiguity)
    xml_text = Path(xml_path).read_text(encoding="utf-8")
    plan_json = build_migration_plan.func(xml_text)

    # 3) Create DAG job JSON that reuses the cluster when provided
    dag_job_json = create_job_config_from_plan.func(
        job_name=job,
        notebook_path=notebook_path,
        plan_json=plan_json,
        cluster_id=existing_cluster_id
    )
    out = Path(out_dir)
    (out / "jobs").mkdir(parents=True, exist_ok=True)
    dag_path = out / "jobs" / "job.dag.json"
    dag_path.write_text(dag_job_json, encoding="utf-8")

    result = {
        "output_dir": summary["output_dir"],
        "steps_written": summary["steps_written"],
        "job_config_path": str(dag_path),
    }

    # 4) Optionally create the job (don’t run unless you want to)
    if deploy:
        deploy_res = deploy_and_run_job.func(dag_job_json, run_now=False)
        result["deploy_result"] = json.loads(deploy_res) if deploy_res.startswith("{") else deploy_res

    return json.dumps(result, indent=2)

@tool
def create_job_config_from_plan(
    job_name: str,
    notebook_path: str,
    plan_json: str,
    # --- cluster selection ---
    cluster_id: str = "",                 # if provided, job will use this existing cluster
    node_type_id: str = "Standard_DS3_v2",# Azure-safe default if creating new cluster
    spark_version: str = "16.4.x-scala2.12",
    num_workers: int = 0,                 # 0 => single-node style new cluster
    autotermination_minutes: int = 60,
    # --- task behavior ---
    timeout_seconds: int = 3600,
    max_retries: int = 2,
    # --- notifications (optional) ---
    notification_email: Optional[str] = None,
) -> str:
    """
    Build a Databricks Jobs 2.1 JSON where each NiFi processor becomes a task and
    NiFi edges become `depends_on`. Each task runs the *same* orchestrator notebook
    (notebook_path) and passes a STEP_MODULE parameter pointing to the generated step file.

    Prefer passing `cluster_id` to reuse your running cluster. If omitted, a new
    single-node Azure cluster is created with the provided node_type_id/spark_version.
    """
    plan = json.loads(plan_json)
    tasks_meta: List[Dict] = plan.get("tasks", [])
    edges: List[List[str]] = plan.get("edges", [])

    # Map NiFi id -> Databricks task_key and step module path
    id_to_key: Dict[str, str] = {}
    id_to_module: Dict[str, str] = {}
    for idx, t in enumerate(tasks_meta, start=10):
        key = f"step_{idx:02d}"
        safe = safe_name((t.get("name") or t.get("type", "step")).split(".")[-1])
        module_rel = f"src/steps/{idx:02d}_{safe}.py"
        id_to_key[t["id"]] = key
        id_to_module[t["id"]] = module_rel

    # Build reverse lookups for dependencies
    parents = {t["id"]: [] for t in tasks_meta}
    for s, d in edges:
        if d in parents:
            parents[d].append(s)

    # Common cluster block (either reuse existing or define a new one)
    def _cluster_block():
        if cluster_id:
            return {"existing_cluster_id": cluster_id}
        # new cluster (Azure-safe defaults; single-node like behavior with num_workers=0)
        newc = {
            "new_cluster": {
                "spark_version": spark_version,
                "node_type_id": node_type_id,
                "num_workers": num_workers,
                "autotermination_minutes": autotermination_minutes,
            }
        }
        # help single-node semantics
        if num_workers == 0:
            newc["new_cluster"]["spark_conf"] = {"spark.databricks.cluster.profile": "singleNode",
                                                 "spark.master": "local[*]"}
        return newc

    # Build job tasks
    tasks = []
    for t in tasks_meta:
        tid = t["id"]
        task = {
            "task_key": id_to_key[tid],
            "notebook_task": {
                "notebook_path": notebook_path,
                "base_parameters": {
                    "STEP_MODULE": id_to_module[tid]
                }
            },
            "timeout_seconds": timeout_seconds,
            "max_retries": max_retries,
        }
        # attach cluster selection
        task.update(_cluster_block())

        # add dependencies if any
        if parents[tid]:
            task["depends_on"] = [{"task_key": id_to_key[p]} for p in parents[tid]]

        tasks.append(task)

    job_cfg = {
        "name": job_name,
        "tasks": tasks,
        "max_concurrent_runs": 1,
    }
    # add email only if provided (avoid null which Jobs API rejects)
    if notification_email:
        job_cfg["email_notifications"] = {"on_failure": [notification_email]}

    return json.dumps(job_cfg, indent=2)

@tool
def parse_nifi_template(xml_content: str) -> str:
    """Parse a NiFi XML template and extract processors, properties, and connections.
    
    Args:
        xml_content: The XML content of the NiFi template
    """
    try:
        root = ET.fromstring(xml_content)
        
        processors = []
        connections = []
        
        # Extract processors
        for processor in root.findall(".//processors"):
            proc_info = {
                "name": processor.find("name").text if processor.find("name") is not None else "Unknown",
                "type": processor.find("type").text if processor.find("type") is not None else "Unknown",
                "properties": {}
            }
            
            # Extract properties
            properties = processor.find(".//properties")
            if properties is not None:
                for prop in properties.findall("entry"):
                    key_elem = prop.find("key")
                    value_elem = prop.find("value")
                    if key_elem is not None and value_elem is not None:
                        proc_info["properties"][key_elem.text] = value_elem.text
            
            processors.append(proc_info)
        
        # Extract connections
        for connection in root.findall(".//connections"):
            source = connection.find(".//source/id")
            destination = connection.find(".//destination/id")
            relationships = connection.find(".//selectedRelationships")
            
            conn_info = {
                "source": source.text if source is not None else "Unknown",
                "destination": destination.text if destination is not None else "Unknown",
                "relationships": relationships.text if relationships is not None else []
            }
            connections.append(conn_info)
        
        result = {
            "processors": processors,
            "connections": connections,
            "processor_count": len(processors),
            "connection_count": len(connections)
        }
        
        return json.dumps(result, indent=2)
        
    except ET.ParseError as e:
        return f"Error parsing XML: {str(e)}"
    except Exception as e:
        return f"Unexpected error: {str(e)}"

def _render_pattern(processor_class: str, properties: dict) -> dict:
    """
    Look up a NiFi processor migration pattern in UC.
    If it's missing, create a stub entry directly in Delta via PatternRegistryUC.
    """

    # Look up existing pattern in UC
    pattern = pattern_registry.get_pattern(processor_class)

    if not pattern:
        lc = processor_class.lower()

        # Handle common processors with a reasonable default stub
        if "getfile" in lc or "listfile" in lc:
            pattern = {
                "databricks_equivalent": "Auto Loader",
                "description": "File ingestion via Auto Loader.",
                "best_practices": [
                    "Use schemaLocation for schema tracking",
                    "Enable includeExistingFiles for initial backfill",
                    "Use cleanSource after successful processing"
                ],
                "code_template": (
                    "df = (spark.readStream\n"
                    "      .format('cloudFiles')\n"
                    "      .option('cloudFiles.format', '{format}')\n"
                    "      .load('{path}'))"
                ),
                "last_seen_properties": properties or {}
            }

        elif "puthdfs" in lc or "putfile" in lc:
            pattern = {
                "databricks_equivalent": "Delta Lake",
                "description": "Transactional storage in Delta.",
                "best_practices": [
                    "Partition by frequently filtered columns",
                    "Compact small files with OPTIMIZE",
                ],
                "code_template": "df.write.format('delta').mode('{mode}').save('{path}')",
                "last_seen_properties": properties or {}
            }

        else:
            # Unknown processor → generic stub
            pattern = {
                "databricks_equivalent": "Unknown",
                "description": "",
                "best_practices": [],
                "code_template": (
                    f"# TODO: Implement conversion for {processor_class}\n"
                    f"# Properties seen: {properties}\n"
                ),
                "last_seen_properties": properties or {}
            }

        # Persist stub into UC table
        pattern_registry.add_pattern(processor_class, pattern)

    # Render placeholders into template, if available
    code = pattern.get("code_template")
    if code:
        injections = {"processor_class": processor_class, **(properties or {})}
        for k, v in injections.items():
            code = code.replace(f"{{{k}}}", str(v))

    return {
        "equivalent": pattern.get("databricks_equivalent", "Unknown"),
        "description": pattern.get("description", ""),
        "best_practices": pattern.get("best_practices", []),
        "code": code,
    }


@tool
def generate_databricks_code(processor_type: str, properties: str = "{}") -> str:
    """Generate equivalent Databricks/PySpark code for a NiFi processor.
    
    Args:
        processor_type: The NiFi processor type to convert
        properties: JSON string of processor properties (default: "{}")
    """
    # Parse properties if it's a string
    if isinstance(properties, str):
        try:
            properties = json.loads(properties)
        except:
            properties = {}
    
    # Find the processor class name
    processor_class = processor_type.split('.')[-1] if '.' in processor_type else processor_type
    
    # Use single source of truth
    rendered = _render_pattern(processor_class, properties)
    
    if rendered["code"]:
        code = f"# {processor_class} → {rendered['equivalent']}\n"
        if rendered["description"]:
            code += f"# {rendered['description']}\n"
        code += f"\n{rendered['code']}"
        
        # Add best practices as comments
        if rendered["best_practices"]:
            code += "\n\n# Best Practices:\n"
            for practice in rendered["best_practices"]:
                code += f"# - {practice}\n"
        
        return code
    
    # If no pattern found, return generic code
    return f"""
# Generic processor conversion for: {processor_type}
# Properties: {json.dumps(properties, indent=2)}

# TODO: Implement specific logic for {processor_type}
# Review the properties and implement equivalent Databricks logic
df = spark.read.format("delta").load("/path/to/data")
# Add your transformation logic here
"""

@tool
def get_migration_pattern(nifi_component: str, properties: str = "{}") -> str:
    """Get best practices and patterns for migrating NiFi components to Databricks.
    
    Args:
        nifi_component: The NiFi component or pattern to migrate
        properties: JSON string of processor properties (default: "{}")
    """
    # Parse properties if it's a string
    if isinstance(properties, str):
        try:
            properties = json.loads(properties)
        except:
            properties = {}
    
    # Use single source of truth
    rendered = _render_pattern(nifi_component, properties)
    
    if rendered["equivalent"] != "Unknown":
        response = [
            f"**Migration Pattern: {nifi_component} → {rendered['equivalent']}**",
            "",
            rendered["description"]
        ]
        
        if rendered["best_practices"]:
            response += ["", "Best Practices:"]
            response += [f"- {practice}" for practice in rendered["best_practices"]]
        
        if rendered["code"]:
            response += ["", "Code Template:", "```python", rendered["code"], "```"]
        
        return "\n".join(response)
    
    # If no pattern found, return generic guidance
    return f"""
**General Migration Guidelines for {nifi_component}**
1. Identify the data flow pattern
2. Map to equivalent Databricks components
3. Implement error handling and monitoring
4. Test with sample data
5. Optimize for performance
6. Document the migration approach
"""

@tool
def create_job_config(
    job_name: str,
    notebook_path: str,
    schedule: str = "",
    cluster_id: str = "",                 # if provided, use this cluster
    node_type_id: str = "Standard_DS3_v2",# fallback if creating new cluster
    spark_version: str = "16.4.x-scala2.12",
    num_workers: int = 0,                 # 0 => single-node
    autotermination_minutes: int = 60,
    notification_email: str = "",
) -> str:
    """Create a Databricks job configuration JSON.
    
    Args:
        job_name: Name of the Databricks job
        notebook_path: Path to the Databricks notebook
        schedule: Optional cron schedule expression (default: no schedule)
        cluster_id: Use an existing cluster if provided
        node_type_id, spark_version, num_workers, autotermination_minutes:
            Used only when creating a new cluster
        notification_email: Optional email for failure notifications
    """

    # --- cluster block ---
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
        if num_workers == 0:  # single-node tweak
            cluster_block["new_cluster"]["spark_conf"].update({
                "spark.databricks.cluster.profile": "singleNode",
                "spark.master": "local[*]"
            })

    job_config = {
        "name": job_name,
        "tasks": [
            {
                "task_key": f"{job_name}_task",
                "notebook_task": {
                    "notebook_path": notebook_path,
                    "base_parameters": {}
                },
                **cluster_block,   # attach cluster settings here
                "timeout_seconds": 3600,
                "max_retries": 2,
                "retry_on_timeout": True
            }
        ],
        "max_concurrent_runs": 1
    }

    if notification_email:
        job_config["email_notifications"] = {"on_failure": [notification_email]}

    if schedule:
        job_config["schedule"] = {
            "quartz_cron_expression": schedule,
            "timezone_id": "UTC",
            "pause_status": "UNPAUSED"
        }
    
    return json.dumps(job_config, indent=2)

@tool
def extract_nifi_parameters_and_services(xml_content: str) -> str:
    """Return NiFi Parameter Contexts and Controller Services with suggested Databricks mappings (secrets, widgets, job params)."""
    try:
        root = ET.fromstring(xml_content)
        out = {"parameter_contexts": [], "controller_services": [], "suggested_mappings": []}

        # Parameter Contexts
        for pc in root.findall(".//parameterContexts/parameterContext"):
            name = (pc.findtext("component/name") or "unnamed").strip()
            params = []
            for p in pc.findall(".//component/parameters/parameter"):
                params.append({
                    "name": p.findtext("parameter/name"),
                    "value": p.findtext("parameter/value"),
                    "sensitive": (p.findtext("parameter/sensitive") == "true"),
                })
            out["parameter_contexts"].append({"name": name, "parameters": params})

        # Controller Services
        for cs in root.findall(".//controllerServices/controllerService"):
            c = cs.find("component")
            out["controller_services"].append({
                "id": cs.findtext("id"),
                "name": c.findtext("name"),
                "type": c.findtext("type"),
                "properties": {e.findtext("name"): e.findtext("value") for e in c.findall(".//properties/entry")},
            })

        # Simple mapping rules → Databricks
        for cs in out["controller_services"]:
            t = (cs["type"] or "").lower()
            if "dbcp" in t or "jdbc" in t:
                out["suggested_mappings"].append({
                    "nifi": cs["name"],
                    "databricks_equivalent": "JDBC via spark.read/write + Databricks Secrets",
                    "how": "Store URL/user/password in a secret scope; use JDBC jars on cluster.",
                })
            if "sslcontextservice" in t:
                out["suggested_mappings"].append({
                    "nifi": cs["name"],
                    "databricks_equivalent": "Secure endpoints + secrets-backed cert paths",
                    "how": "Upload certs to a secured location; reference with secrets / init scripts.",
                })
        return json.dumps(out, indent=2)
    except Exception as e:
        return f"Failed to parse NiFi XML: {e}"

@tool
def build_migration_plan(xml_content: str) -> str:
    """Produce a topologically sorted DAG of NiFi processors based on Connections."""
    try:
        root = ET.fromstring(xml_content)
        # id → name/type
        procs = {}
        for pr in root.findall(".//processors"):
            pid = pr.findtext("id")
            procs[pid] = {
                "id": pid,
                "name": pr.findtext("name") or pid,
                "type": pr.findtext("type") or "Unknown"
            }

        edges = []
        for conn in root.findall(".//connections"):
            src = conn.findtext(".//source/id")
            dst = conn.findtext(".//destination/id")
            if src and dst:
                edges.append((src, dst))

        # Kahn's algorithm
        from collections import defaultdict, deque
        indeg=defaultdict(int); graph=defaultdict(list)
        for s,d in edges:
            graph[s].append(d); indeg[d]+=1
            if s not in indeg: indeg[s]+=0

        q=deque([n for n in indeg if indeg[n]==0])
        ordered=[]
        while q:
            n=q.popleft()
            if n in procs: ordered.append(procs[n])
            for v in graph[n]:
                indeg[v]-=1
                if indeg[v]==0: q.append(v)

        plan={"tasks": ordered, "edges": edges, "note":"Use this order to compose Jobs tasks or DLT dependencies"}
        return json.dumps(plan, indent=2)
    except Exception as e:
        return f"Failed building plan: {e}"

@tool
def suggest_autoloader_options(properties: str = "{}") -> str:
    """Given NiFi GetFile/ListFile-like properties, suggest Auto Loader code & options."""
    props = json.loads(properties) if properties else {}
    path = props.get("Input Directory") or props.get("Directory") or "/mnt/raw"
    fmt = (props.get("File Filter") or "*.json").split(".")[-1]
    fmt = "csv" if fmt.lower() in ["csv"] else "json" if fmt.lower() in ["json"] else "parquet"

    code = f"""from pyspark.sql.functions import *
df = (spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "{fmt}")
      .option("cloudFiles.inferColumnTypes", "true")
      .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
      .load("{path}"))"""
    tips = [
        "Use cloudFiles.schemaLocation for checkpoint/schema tracking.",
        "Use cloudFiles.includeExistingFiles=true to backfill once.",
        "Set cloudFiles.validateOptions for strictness; cleanSource MOVE/DELETE for hygiene.",
    ]
    return json.dumps({"code": code, "tips": tips}, indent=2)

@tool
def scaffold_asset_bundle(
    project_name: str,
    job_name: str,
    notebook_path: str,
    existing_cluster_id: str = "",
    node_type_id: str = "Standard_DS3_v2",       # Azure-safe default if creating new cluster
    spark_version: str = "16.4.x-scala2.12",
    num_workers: int = 0,                        # 0 => single-node semantics
    autotermination_minutes: int = 60,
) -> str:
    """
    Return a minimal databricks.yml for a bundle with one job that runs a notebook.
    If existing_cluster_id is provided, the job will reuse that cluster.
    Otherwise it defines a small new cluster (single-node by default).
    """

    if existing_cluster_id:
        # block for reusing an existing cluster
        cluster_block = f"""          existing_cluster_id: {existing_cluster_id}"""
    else:
        # block for creating a new cluster
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

    # Final YAML with clean indentation
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

@tool
def generate_dlt_expectations(table_name: str, rules_json: str) -> str:
    """Return SQL to create a DLT/Lakeflow dataset with expectations from simple rules."""
    try:
        rules = json.loads(rules_json) if rules_json else {}
        ex_lines = []
        for name, expr in rules.items():
            ex_lines.append(f"EXPECT {name} : {expr}")
        ex_block = "\n  ".join(ex_lines) if ex_lines else ""
        sql = f"""CREATE OR REFRESH STREAMING TABLE {table_name}
  {ex_block}
AS SELECT * FROM STREAM(LIVE.source_table);"""
        return sql
    except Exception as e:
        return f"Invalid rules: {e}"

@tool
def deploy_and_run_job(job_config_json: str, run_now: bool = True) -> str:
    """
    Create a Databricks Job via REST 2.1. If run_now=True, also trigger a run.
    Args:
        job_config_json: Full Jobs 2.1 JSON payload as a string.
        run_now: True -> create + run; False -> create only.
    Returns:
        JSON string with {"job_id": ..., "run_id": ...?}
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

    # Create the job
    create = requests.post(f"{host}/api/2.1/jobs/create", json=cfg, headers=headers, timeout=60)
    if create.status_code >= 300:
        return f"Create failed: {create.status_code} {create.text}"
    job_id = create.json().get("job_id")

    # Optionally start a run
    if run_now:
        run = requests.post(f"{host}/api/2.1/jobs/run-now", json={"job_id": job_id}, headers=headers, timeout=60)
        if run.status_code >= 300:
            # Job exists but run failed – return job_id so you can fix and run later
            return json.dumps({"job_id": job_id, "run_error": f"{run.status_code} {run.text}"}, indent=2)
        return json.dumps({"job_id": job_id, "run_id": run.json().get("run_id")}, indent=2)

    # Create only
    return json.dumps({"job_id": job_id}, indent=2)

@tool
def evaluate_pipeline_outputs(src_path: str, dst_path: str, key_cols_csv: str = "", float_tol: float = 1e-6) -> str:
    """Compare two Delta/Parquet tables or folders on Databricks with robust float handling."""
    try:
        key_cols = [c.strip() for c in key_cols_csv.split(",") if c.strip()]
        
        # This would need to be run in a Databricks environment with spark available
        # For now, return a template for the comparison logic
        comparison_code = f"""
# Pipeline Output Comparison Template
from pyspark.sql import functions as F

# Load datasets
df1 = spark.read.load("{src_path}")  # Source
df2 = spark.read.load("{dst_path}")  # Destination

# Basic counts
src_count = df1.count()
dst_count = df2.count()

# Schema comparison
schema_equal = (df1.schema == df2.schema)

# Null counts per column
def get_nulls(df):
    return {{c: df.filter(F.col(c).isNull()).count() for c in df.columns}}

src_nulls = get_nulls(df1)
dst_nulls = get_nulls(df2)

# Key-based comparison if keys provided
key_cols = {key_cols}
float_tolerance = {float_tol}

print(f"Source count: {{src_count}}")
print(f"Destination count: {{dst_count}}")
print(f"Schema equal: {{schema_equal}}")
print(f"Source nulls: {{src_nulls}}")
print(f"Destination nulls: {{dst_nulls}}")
"""
        return comparison_code
    except Exception as e:
        return f"Error generating comparison: {e}"

@tool
def generate_dlt_pipeline_config(pipeline_name: str, catalog: str, db_schema: str, notebook_path: str) -> str:
    """Return minimal JSON config for a DLT/Lakeflow pipeline."""
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
def convert_flow(xml_path: str,
                 out_dir: str = "/dbfs/FileStore/nifi2dbx_out",
                 project: str = "nifi2dbx",
                 job: str = "nifi2dbx_job",
                 notebook_path: str = "/Workspace/Users/you@example.com/nifi2dbx/main",
                 emit_job_json: bool = True,
                 deploy_job: bool = False,
                 also_import_notebook: bool = True,
                 # NEW: cluster controls
                 existing_cluster_id: str = "",
                 node_type_id: str = "Standard_DS3_v2",
                 spark_version: str = "16.4.x-scala2.12",
                 num_workers: int = 0,
                 autotermination_minutes: int = 60) -> str:
    """
    Convert a NiFi XML flow into a Databricks project scaffold and WRITE artifacts.
    Returns the output directory. Requires DATABRICKS_HOST/TOKEN for notebook import or job deploy.
    """
    out = Path(out_dir)
    (out / "src/steps").mkdir(parents=True, exist_ok=True)

    # Read NiFi XML (DBFS or local path)
    xml_text = Path(xml_path).read_text(encoding="utf-8")

    # 1) Parse and plan
    parsed_js = json.loads(parse_nifi_template.func(xml_text))
    processors = parsed_js.get("processors", [])
    plan_js = json.loads(build_migration_plan.func(xml_text))

    ## Write plan
    write_text(out / "conf/plan.json", json.dumps(plan_js, indent=2))

    # Write a DAG-aware Job config that preserves NiFi dependencies
    dag_job_cfg = create_job_config_from_plan.func(
        job_name=job,
        notebook_path=notebook_path,
        plan_json=json.dumps(plan_js),
        cluster_id=existing_cluster_id,             # reuse your running cluster if provided
        node_type_id=node_type_id,                  # otherwise create Azure-safe new cluster
        spark_version=spark_version,
        num_workers=num_workers,
        autotermination_minutes=autotermination_minutes,
    )
    write_text(out / "jobs/job.dag.json", dag_job_cfg)

    ordered = plan_js.get("tasks", [])
    params_js = json.loads(extract_nifi_parameters_and_services.func(xml_text))

    # 2) Per-processor code generation
    step_files = []
    for idx, task in enumerate(ordered, start=10):
        full = next(
            (p for p in processors
             if p.get("type") == task.get("type") or p.get("name") == task.get("name")),
            task,
        )
        props = full.get("properties", {})
        proc_type = task.get("type", "Unknown")
        name_sn = safe_name(task.get("name") or proc_type.split(".")[-1])

        code = generate_databricks_code.func(
            processor_type=proc_type,
            properties=json.dumps(props),
        )

        # Fallback: suggest Auto Loader for file ingestion
        if ("GetFile" in proc_type or "ListFile" in proc_type) and "cloudFiles" not in code:
            al = json.loads(suggest_autoloader_options.func(json.dumps(props)))
            code = f"# Suggested Auto Loader for {proc_type}\n{al['code']}\n# Tips:\n# - " + "\n# - ".join(al["tips"])

        step_path = out / f"src/steps/{idx:02d}_{name_sn}.py"
        write_text(step_path, code)
        step_files.append(step_path)

    # 3) Bundle + README
    bundle_yaml = scaffold_asset_bundle.func(project, job, notebook_path)
    write_text(out / "databricks.yml", bundle_yaml)

    readme = [
        f"# {project}",
        "",
        "Generated from NiFi flow.",
        "",
        "## Contents",
        "- `src/steps/` individual processor translations",
        "- `databricks.yml` Databricks Asset Bundle (jobs-as-code)",
        "- `jobs/` optional Jobs 2.1 JSON",
        "- `conf/` mappings from NiFi Parameter Contexts & Controller Services",
        "",
        "## Next steps",
        "1. Review/merge the per-step code into your main notebook or DLT pipeline.",
        "2. Use `databricks bundle validate && databricks bundle deploy` (if using Bundles).",
        "3. Or create a job from `jobs/job.json` and run it.",
    ]
    write_text(out / "README.md", "\n".join(readme))

    # 4) Save Parameter Contexts & Controller Services
    write_text(out / "conf/parameter_contexts.json", json.dumps(params_js, indent=2))

    # 5) Orchestrator notebook that supports STEP_MODULE (single-step) or all-steps
    orchestrator = textwrap.dedent("""\
    # Databricks notebook source
    # Orchestrator notebook
    # - When run by a Job task, the task should pass base_parameters: { "STEP_MODULE": "src/steps/10_SomeStep.py" }
    # - When opened manually (no parameter), it runs all steps sequentially in numeric filename order.

    dbutils.widgets.text("STEP_MODULE", "")
    step_mod = dbutils.widgets.get("STEP_MODULE")

    import importlib.util, sys, os, glob

    def run_module(rel_path: str):
        # Project root is the parent of notebooks/
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
    write_text(out / "notebooks/main.py", orchestrator)

    # 6) Optional: Jobs JSON + optional run
    if emit_job_json or deploy_job:
        job_cfg = create_job_config.func(job, notebook_path)
        write_text(out / "jobs/job.json", job_cfg)
        if deploy_job:
            res = deploy_and_run_job.func(job_cfg)
            logger.info(f"Deploy result: {res}")

    # 7) DLT config (optional scaffold)
    dlt_cfg = generate_dlt_pipeline_config.func(
        pipeline_name=f"{project}_pipeline",
        catalog="main",
        db_schema="default",
        notebook_path=notebook_path,
    )
    write_text(out / "conf/dlt_pipeline.json", dlt_cfg)

    # 8) Optional: import orchestrator into /Workspace
    if also_import_notebook:
        host = os.environ.get("DATABRICKS_HOST") or os.environ.get("DATABRICKS_HOSTNAME")
        token = os.environ.get("DATABRICKS_TOKEN")
        if host and token:
            if not host.startswith("http"):
                host = "https://" + host
            headers = {"Authorization": f"Bearer {token}"}

            # skip import if the notebook already exists
            gs = requests.get(f"{host}/api/2.0/workspace/get-status",
                            headers=headers,
                            params={"path": notebook_path},
                            timeout=30)
            exists = gs.status_code == 200 and gs.json().get("object_type") == "NOTEBOOK"
            if not exists:
                src = (out / "notebooks/main.py").read_text(encoding="utf-8")
                payload = {
                    "path": notebook_path,
                    "format": "SOURCE",
                    "language": "PYTHON",
                    "overwrite": True,
                    "content": base64.b64encode(src.encode()).decode(),
                }
                r = requests.post(f"{host}/api/2.0/workspace/import",
                                headers=headers, json=payload, timeout=60)
                logger.info(f"Notebook import: {r.status_code} {r.text[:200]}")
            else:
                logger.info(f"Notebook already exists at {notebook_path}; skipping import.")

    summary = {
        "output_dir": str(out),
        "steps_written": [str(p) for p in step_files],
    }
    return json.dumps(summary, indent=2)

# Initialize custom NiFi migration tools 
tools = [
    orchestrate_nifi_migration,
    convert_flow,
    create_job_config_from_plan,
    deploy_and_run_job,
    build_migration_plan,
    parse_nifi_template,
    generate_databricks_code,
    get_migration_pattern,
    create_job_config,
    extract_nifi_parameters_and_services,
    suggest_autoloader_options,
    scaffold_asset_bundle,
    generate_dlt_expectations,
    evaluate_pipeline_outputs,
    generate_dlt_pipeline_config,
]

#####################
## Define agent logic
#####################

class AgentState(TypedDict):
    messages: Annotated[Sequence[BaseMessage], add_messages]
    custom_inputs: Optional[dict[str, Any]]
    custom_outputs: Optional[dict[str, Any]]

def create_tool_calling_agent(
    model: LanguageModelLike,
    tools: Union[ToolNode, Sequence[BaseTool]],
    system_prompt: Optional[str] = None,
):
    # Now we can bind tools since we're using @tool decorator (like working agent)
    model = model.bind_tools(tools)

    # Define the function that determines which node to go to
    def should_continue(state: AgentState):
        messages = state["messages"]
        last_message = messages[-1]
        # If there are function calls, continue. else, end
        if isinstance(last_message, AIMessage) and last_message.tool_calls:
            return "continue"
        else:
            return "end"

    if system_prompt:
        preprocessor = RunnableLambda(
            lambda state: [{"role": "system", "content": system_prompt}] + state["messages"]
        )
    else:
        preprocessor = RunnableLambda(lambda state: state["messages"])
    model_runnable = preprocessor | model

    def call_model(
        state: AgentState,
        config: RunnableConfig,
    ):
        response = model_runnable.invoke(state, config)
        return {"messages": [response]}

    workflow = StateGraph(AgentState)
    workflow.add_node("agent", RunnableLambda(call_model))
    workflow.add_node("tools", ToolNode(tools))
    
    workflow.set_entry_point("agent")
    workflow.add_conditional_edges(
        "agent",
        should_continue,
        {
            "continue": "tools",
            "end": END,
        },
    )
    workflow.add_edge("tools", "agent")

    return workflow.compile()

class LangGraphResponsesAgent(ResponsesAgent):
    def __init__(self, agent):
        self.agent = agent

    def _responses_to_cc(self, message: dict[str, Any]) -> list[dict[str, Any]]:
        """Convert from a Responses API output item to ChatCompletion messages."""
        msg_type = message.get("type")
        if msg_type == "function_call":
            return [
                {
                    "role": "assistant",
                    "content": "tool call",
                    "tool_calls": [
                        {
                            "id": message["call_id"],
                            "type": "function",
                            "function": {
                                "arguments": message["arguments"],
                                "name": message["name"],
                            },
                        }
                    ],
                }
            ]
        elif msg_type == "message" and isinstance(message["content"], list):
            return [
                {"role": message["role"], "content": content["text"]}
                for content in message["content"]
            ]
        elif msg_type == "reasoning":
            return [{"role": "assistant", "content": json.dumps(message["summary"])}]
        elif msg_type == "function_call_output":
            return [
                {
                    "role": "tool",
                    "content": message["output"],
                    "tool_call_id": message["call_id"],
                }
            ]
        compatible_keys = ["role", "content", "name", "tool_calls", "tool_call_id"]
        filtered = {k: v for k, v in message.items() if k in compatible_keys}
        return [filtered] if filtered else []

    def _prep_msgs_for_cc_llm(self, responses_input) -> list[dict[str, Any]]:
        "Convert from Responses input items to ChatCompletion dictionaries"
        cc_msgs = []
        for msg in responses_input:
            cc_msgs.extend(self._responses_to_cc(msg.model_dump()))
        return cc_msgs

    def _langchain_to_responses(self, messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
        "Convert from ChatCompletion dict to Responses output item dictionaries"
        for message in messages:
            message = message.model_dump()
            role = message["type"]
            if role == "ai":
                if tool_calls := message.get("tool_calls"):
                    return [
                        self.create_function_call_item(
                            id=message.get("id") or str(uuid4()),
                            call_id=tool_call["id"],
                            name=tool_call["name"],
                            arguments=json.dumps(tool_call["args"]),
                        )
                        for tool_call in tool_calls
                    ]
                else:
                    return [
                        self.create_text_output_item(
                            text=message["content"],
                            id=message.get("id") or str(uuid4()),
                        )
                    ]
            elif role == "tool":
                return [
                    self.create_function_call_output_item(
                        call_id=message["tool_call_id"],
                        output=message["content"],
                    )
                ]
            elif role == "user":
                return [message]

    def predict(self, request: ResponsesAgentRequest) -> ResponsesAgentResponse:
        outputs = [
            event.item
            for event in self.predict_stream(request)
            if event.type == "response.output_item.done"
        ]
        return ResponsesAgentResponse(output=outputs, custom_outputs=request.custom_inputs if hasattr(request, 'custom_inputs') else {})

    def predict_stream(
        self,
        request: ResponsesAgentRequest,
    ) -> Generator[ResponsesAgentStreamEvent, None, None]:
        # Handle both dict and ResponsesAgentRequest
        if isinstance(request, dict):
            # Convert dict format to ResponsesAgentRequest
            request = ResponsesAgentRequest(
                input=request.get("input", []),
                custom_inputs=request.get("custom_inputs", {})
            )
        
        cc_msgs = []
        for msg in request.input:
            if isinstance(msg, dict):
                # Direct dict format from test
                cc_msgs.append(msg)
            else:
                # ResponsesAgentRequest format
                cc_msgs.extend(self._responses_to_cc(msg.model_dump()))

        for event in self.agent.stream({"messages": cc_msgs}, stream_mode=["updates", "messages"]):
            if event[0] == "updates":
                for node_data in event[1].values():
                    for item in self._langchain_to_responses(node_data["messages"]):
                        yield ResponsesAgentStreamEvent(type="response.output_item.done", item=item)
            # filter the streamed messages to just the generated text messages
            elif event[0] == "messages":
                try:
                    chunk = event[1][0]
                    if isinstance(chunk, AIMessageChunk) and (content := chunk.content):
                        yield ResponsesAgentStreamEvent(
                            **self.create_text_delta(delta=content, item_id=chunk.id),
                        )
                except Exception as e:
                    print(e)

# Create the agent object, and specify it as the agent object to use when
# loading the agent back for inference via mlflow.models.set_model()
try:
    # Try to enable MLflow autolog if available
    mlflow.langchain.autolog()
    logger.info("MLflow autolog enabled")
except Exception as e:
    logger.warning(f"MLflow autolog not available: {e}")
    logger.info("Continuing without MLflow tracking")

# Create the agent
agent = create_tool_calling_agent(llm, tools, system_prompt)
AGENT = LangGraphResponsesAgent(agent)

# Set model for MLflow if needed
try:
    mlflow.models.set_model(AGENT)
    logger.info("Agent registered with MLflow")
except Exception as e:
    logger.warning(f"Could not register with MLflow: {e}")
    logger.info("Agent created successfully without MLflow registration")

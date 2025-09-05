# tools/migration_tools.py
# NiFi → Databricks conversion orchestrators and flow utilities.

from __future__ import annotations

import json
import os
import xml.etree.ElementTree as ET
from collections import defaultdict, deque
from pathlib import Path
from typing import Any, Dict, List

from utils import safe_name as _safe_name
from utils import write_text as _write_text


def _default_notebook_path(project: str) -> str:
    user = os.environ.get("WORKSPACE_USER") or os.environ.get("USER_EMAIL") or "Shared"
    base = f"/Users/{user}" if "@" in user else "/Shared"
    proj_name = _safe_name(project)
    return f"{base}/{proj_name}/main"


__all__ = [
    "build_migration_plan",
    "orchestrate_focused_nifi_migration",
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


def orchestrate_focused_nifi_migration(
    xml_path: str,
    pruned_processors: List[Dict[str, Any]],
    semantic_flows: Dict[str, Any],
    out_dir: str,
    project: str,
    notebook_path: str = "",
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

        total_complex = len(data_transformation_procs) + len(external_processing_procs)
        print(
            f"📊 [PROCESSOR BREAKDOWN] {len(pruned_processors)} essential processors ({total_complex} complex, {len(data_movement_procs)} simple)"
        )

        # Setup output directory - simple structure for migration guide
        root = Path(out_dir)
        proj_name = _safe_name(project)
        out = root / proj_name
        out.mkdir(
            parents=True, exist_ok=True
        )  # Simple output directory for migration guide

        if not notebook_path:
            notebook_path = _default_notebook_path(project)

        # Generate comprehensive migration guide with analysis and recommendations
        print(
            f"📋 [GUIDE GENERATION] Creating comprehensive migration guide for {len(pruned_processors)} processors..."
        )

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

        # Import at function level to avoid circular imports
        from .migration_guide_generator import generate_migration_guide

        migration_guide = generate_migration_guide(
            processors=pruned_processors,
            semantic_flows=semantic_flows,
            project_name=project,
            analysis=focused_analysis,
        )

        # Save the migration guide
        guide_path = out / "MIGRATION_GUIDE.md"
        print(f"💾 [GUIDE] Writing guide to: {guide_path}")
        print(f"📄 [GUIDE] Guide content length: {len(migration_guide)} characters")
        _write_text(guide_path, migration_guide)
        print(f"✅ [GUIDE] Successfully written to: {guide_path}")

        # Migration guide generated - no deployment needed
        print(
            f"✅ [GUIDE COMPLETE] Migration guide saved to {out / 'MIGRATION_GUIDE.md'}"
        )

        result = {
            "migration_type": "comprehensive_guide",
            "processors_analyzed": len(pruned_processors),
            "breakdown": focused_analysis["breakdown"],
            "output_directory": str(out),
            "migration_guide_path": str(out / "MIGRATION_GUIDE.md"),
            "semantic_flows_applied": True,
            "approach": "Migration guide with recommendations instead of fragmented code generation",
        }

        return json.dumps(result, indent=2)

    except Exception as e:
        return json.dumps({"error": f"Focused migration failed: {str(e)}"})

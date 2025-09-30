"""Run the full NiFi → Databricks analysis pipeline for a single XML upload."""

from __future__ import annotations

import datetime as _dt
from pathlib import Path
from typing import Any, Callable, Dict, List, Tuple

from tools.classification import classify_workflow
from tools.nifi_table_lineage import analyze_nifi_table_lineage
from tools.script_extraction import extract_all_scripts_from_nifi_xml
from tools.table_extraction import extract_all_tables_from_nifi_xml
from tools.variable_extraction import extract_variable_dependencies

AnalysisCallback = Callable[[List[Dict[str, Any]]], None]


def _normalize_step_name(step: str) -> str:
    return step.replace("_", " ").title()


def run_full_analysis(
    *,
    uploaded_bytes: bytes,
    file_name: str,
    session_state: Dict[str, Any],
    on_update: AnalysisCallback | None = None,
    output_dir: Path | None = None,
) -> List[Dict[str, Any]]:
    """Run all analysis steps and store results in the provided session state.

    Args:
        uploaded_bytes: Raw NiFi XML file contents.
        file_name: Original filename (used for session keys and logs).
        session_state: Streamlit session-state-like mapping.
        on_update: Optional callback receiving the latest progress list.
        output_dir: Optional directory for lineage CSV outputs.

    Returns:
        Progress log entries describing each step status.
    """

    progress: List[Dict[str, Any]] = []

    def _emit() -> None:
        session_state["analysis_progress"] = list(progress)
        if on_update:
            on_update(list(progress))

    def _start(step: str) -> Dict[str, Any]:
        entry = {"step": step, "label": _normalize_step_name(step), "status": "running"}
        progress.append(entry)
        _emit()
        return entry

    def _complete(entry: Dict[str, Any], *, message: str | None = None) -> None:
        entry["status"] = "completed"
        if message:
            entry["message"] = message
        _emit()

    def _fail(entry: Dict[str, Any], exc: Exception) -> None:
        entry["status"] = "failed"
        entry["message"] = str(exc)
        _emit()
        raise

    xml_path: Path | None = None
    try:
        tmp_dir = Path(session_state.get("analysis_tmp_dir", ""))
        if not tmp_dir or not tmp_dir.exists():
            tmp_dir = Path(".streamlit_analysis")
            tmp_dir.mkdir(parents=True, exist_ok=True)
            session_state["analysis_tmp_dir"] = str(tmp_dir)
        xml_path = tmp_dir / f"{file_name}"
        xml_path.write_bytes(uploaded_bytes)

        # Processor classification
        step = _start("classification")
        classification_result = classify_workflow(str(xml_path))
        session_state[f"classification_results_{file_name}"] = classification_result
        _complete(
            step,
            message=f"{len(classification_result.get('classifications', []))} processors",
        )

        # Table extraction
        step = _start("table_extraction")
        table_results = extract_all_tables_from_nifi_xml(str(xml_path))
        session_state[f"table_results_{file_name}"] = table_results
        _complete(step, message=f"{len(table_results)} tables")

        # Script extraction
        step = _start("script_extraction")
        script_results = extract_all_scripts_from_nifi_xml(str(xml_path))
        session_state[f"script_results_{file_name}"] = script_results
        _complete(step, message=f"{len(script_results)} processors with scripts")

        # Lineage analysis
        step = _start("lineage_analysis")
        lineage_outdir = output_dir or (tmp_dir / "lineage_outputs")
        lineage_outdir.mkdir(parents=True, exist_ok=True)
        lineage_result = analyze_nifi_table_lineage(
            str(xml_path),
            outdir=str(lineage_outdir),
            table_results=table_results,
            write_inter_chains=True,
        )
        session_state[f"lineage_results_{file_name}"] = lineage_result
        _complete(step, message=f"{lineage_result.get('all_chains', 0)} chains")

        # Variable dependencies
        step = _start("variable_dependencies")
        variable_result = extract_variable_dependencies(xml_path=str(xml_path))
        session_state[f"variable_results_{file_name}"] = variable_result
        _complete(
            step,
            message=f"{variable_result.get('total_variables', 0)} variables",
        )

        session_state["analysis_summary"] = {
            "file_name": file_name,
            "timestamp": _dt.datetime.utcnow().isoformat() + "Z",
            "processor_count": len(classification_result.get("classifications", [])),
        }
        return progress

    except Exception as exc:  # pragma: no cover - visual feedback path
        if progress:
            _fail(progress[-1], exc)
        raise

    finally:
        _emit()

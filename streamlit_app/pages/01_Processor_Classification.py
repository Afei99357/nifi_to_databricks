#!/usr/bin/env python3

import json
import os
import shutil
import sys
import tempfile
import zipfile
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Add parent directory to Python path to find tools and config (MUST be before imports)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import pandas as pd
import streamlit as st

from tools.classification import classify_workflow


def render_summary_metrics(summary: Dict[str, int]) -> None:
    if not summary:
        return
    st.markdown("### 📊 Migration categories")
    items = sorted(summary.items())
    columns = st.columns(min(4, len(items)))
    for idx, (category, count) in enumerate(items):
        columns[idx % len(columns)].metric(category, int(count))


def render_processor_detail(record: Dict[str, Any]) -> None:
    st.markdown(
        f"#### Processor: {record.get('name') or record.get('processor_id') or 'Unknown'}"
    )
    col1, col2, col3 = st.columns(3)
    col1.markdown(f"**ID:** {record.get('processor_id', '—')}")
    col2.markdown(
        f"**Type:** {record.get('short_type') or record.get('processor_type')}"
    )
    col3.markdown(
        f"**Group:** {record.get('parent_group', record.get('parentGroupId', 'Root'))}"
    )

    category = record.get("migration_category", "Unknown")
    target = record.get("databricks_target", "—")
    confidence = record.get("confidence")
    source = record.get("classification_source", record.get("source", "rule"))
    notes = record.get("notes")
    rule = record.get("rule")

    cols = st.columns(3)
    cols[0].markdown(f"**Category:** {category}")
    cols[1].markdown(f"**Target:** {target}")
    if isinstance(confidence, (int, float)):
        cols[2].markdown(f"**Confidence:** {confidence:.2f}")
    else:
        cols[2].markdown(f"**Confidence:** {confidence or '—'}")

    cols = st.columns(2)
    cols[0].markdown(f"**Source:** {source}")
    cols[1].markdown(f"**Rule:** {rule or '—'}")

    if notes:
        st.markdown(f"**Notes:** {notes}")

    evidence = record.get("feature_evidence") or {}
    if evidence:
        with st.expander("Feature evidence", expanded=False):
            st.json(evidence)

    properties = record.get("properties") or {}
    if properties:
        with st.expander("Processor properties", expanded=False):
            st.json(properties)


def render_classification_result(result: Any, *, key_prefix: str) -> None:
    if not isinstance(result, dict):
        st.error(f"❌ Classification failed: {result}")
        return

    workflow = result.get("workflow", {})
    if workflow:
        st.caption(
            f"Workflow: {workflow.get('filename', 'Unknown')} • "
            f"Processors: {workflow.get('processor_count', 'N/A')} • "
            f"Connections: {workflow.get('connection_count', 'N/A')}"
        )

    render_summary_metrics(result.get("summary", {}))

    ambiguous_count = len(result.get("ambiguous", []))
    if ambiguous_count:
        st.warning(f"{ambiguous_count} processor(s) need manual review or overrides.")

    records = result.get("classifications", [])
    if not records:
        st.info("No processors were classified.")
        return

    df = pd.DataFrame(records)
    display_columns = [
        "processor_id",
        "name",
        "short_type",
        "migration_category",
        "databricks_target",
        "confidence",
        "rule",
        "classification_source",
    ]
    existing_columns = [col for col in display_columns if col in df.columns]
    st.markdown("### 🗂️ Classified processors")
    st.dataframe(
        df[existing_columns] if existing_columns else df,
        use_container_width=True,
        hide_index=True,
    )

    export_df = df[existing_columns] if existing_columns else df
    csv_export = export_df.to_csv(index=False)
    st.download_button(
        "📥 Download classification CSV",
        data=csv_export,
        file_name="nifi_classification.csv",
        mime="text/csv",
        use_container_width=True,
        key=f"{key_prefix}_csv_download",
    )

    json_export = json.dumps(result, indent=2, ensure_ascii=False)
    st.download_button(
        "📥 Download classification JSON",
        data=json_export,
        file_name="nifi_classification.json",
        mime="application/json",
        use_container_width=True,
        key=f"{key_prefix}_json_download",
    )

    record_map = {
        rec.get("processor_id"): rec for rec in records if rec.get("processor_id")
    }
    if not record_map:
        return

    select_options = sorted(record_map.keys())

    def _format_option(proc_id: str) -> str:
        rec = record_map.get(proc_id, {})
        name = rec.get("name") or proc_id
        short_type = rec.get("short_type") or rec.get("processor_type")
        category = rec.get("migration_category") or "Unknown"
        return f"{name} · {short_type} → {category}"

    selected_proc = st.selectbox(
        "Select a processor to inspect",
        select_options,
        format_func=_format_option,
        key=f"{key_prefix}_detail_select",
    )

    if selected_proc:
        render_processor_detail(record_map[selected_proc])


def handle_upload_flow() -> None:
    uploaded_file = st.session_state.get("uploaded_file")
    if not uploaded_file:
        st.warning("⚠️ No file selected. Please go back to Dashboard to upload a file.")
        if st.button("🔙 Back to Dashboard", key="back_without_file"):
            st.switch_page("Dashboard.py")
        return

    st.success(f"✅ Ready to classify: {uploaded_file.name}")

    cache_key = f"classification_results_{uploaded_file.name}"
    cached_result = st.session_state.get(cache_key)
    running = st.session_state.get("classification_running", False)
    auto_start = st.session_state.get("auto_start_migration", False)

    col1, col2 = st.columns(2)
    with col1:
        run_requested = (
            st.button(
                "🚀 Run Classification",
                use_container_width=True,
                disabled=running,
                key="run_classification_button",
            )
            or auto_start
        )

    with col2:
        if st.button(
            "🔙 Back to Dashboard",
            disabled=running,
            use_container_width=True,
            key="back_to_dashboard_button",
        ):
            st.switch_page("Dashboard.py")

    if auto_start:
        st.session_state["auto_start_migration"] = False

    if isinstance(cached_result, dict) and not run_requested:
        st.info("📋 Showing cached classification results.")
        render_classification_result(cached_result, key_prefix=cache_key)
        return
    if isinstance(cached_result, str) and not run_requested:
        st.error(f"❌ Previous classification failed: {cached_result}")
        return

    if run_requested and not running:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xml") as tmp_file:
            tmp_file.write(uploaded_file.getvalue())
            tmp_path = tmp_file.name

        st.session_state["classification_running"] = True
        try:
            with st.spinner("Running declarative classifier..."):
                result = classify_workflow(tmp_path)
            st.success("✅ Processor classification completed!")
            st.session_state[cache_key] = result
            render_classification_result(result, key_prefix=cache_key)
        except Exception as exc:  # pragma: no cover - UI feedback
            error_msg = str(exc)
            st.error(f"❌ Classification failed: {error_msg}")
            st.code(error_msg)
            st.session_state[cache_key] = error_msg
        finally:
            st.session_state["classification_running"] = False
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
    elif running:
        st.info("Classification already running. Please wait…")


def _candidate_json_names(template_key: str) -> List[str]:
    base = Path(str(template_key))
    candidates = {str(template_key), base.name, base.stem}
    names: List[str] = []
    for cand in candidates:
        if not cand:
            continue
        if cand.endswith(".json"):
            names.append(cand)
        else:
            names.append(f"{cand}.json")
    seen: List[str] = []
    for name in names:
        if name not in seen:
            seen.append(name)
    return seen


@st.cache_data(show_spinner=False)
def load_saved_json(
    json_dir: str, template_key: str
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    if not json_dir:
        return None, None
    base = Path(json_dir).expanduser()
    if not base.exists():
        return None, None
    for candidate_name in _candidate_json_names(template_key):
        candidate_path = base / candidate_name
        if candidate_path.exists() and candidate_path.is_file():
            try:
                with candidate_path.open("r", encoding="utf-8") as fh:
                    return json.load(fh), str(candidate_path)
            except json.JSONDecodeError:
                return None, str(candidate_path)
    return None, None


def handle_saved_results_flow() -> None:
    st.markdown("### 📂 Review derived classification outputs")
    summary_file = st.file_uploader(
        "Upload derived summary CSV or ZIP bundle",
        type=["csv", "zip"],
        key="saved_summary_uploader",
    )

    default_dir = st.session_state.get(
        "saved_results_dir", "derived_classification_results"
    )

    if summary_file is None:
        json_dir = st.text_input(
            "Directory containing per-template JSON files",
            value=default_dir,
            key="saved_results_dir_input",
        )
        st.session_state["saved_results_dir"] = json_dir
        st.info(
            "Upload either the generated `summary.csv` or a ZIP archive that "
            "contains the summary and JSON outputs from `classify_all_workflows.py`."
        )
        return

    filename = summary_file.name or ""
    suffix = Path(filename).suffix.lower()

    summary_df: Optional[pd.DataFrame] = None
    summary_source_dir: Optional[Path] = None

    if suffix == ".zip":
        # Clean up any previous extraction directory.
        extracted_dir = st.session_state.get("saved_results_extracted_dir")
        if extracted_dir and Path(extracted_dir).exists():
            shutil.rmtree(extracted_dir, ignore_errors=True)

        temp_dir = Path(tempfile.mkdtemp(prefix="derived_upload_"))
        try:
            with zipfile.ZipFile(BytesIO(summary_file.read())) as zf:
                zf.extractall(temp_dir)
        except zipfile.BadZipFile:
            st.error("Uploaded file is not a valid ZIP archive.")
            shutil.rmtree(temp_dir, ignore_errors=True)
            return

        summary_candidates = sorted(temp_dir.rglob("summary.csv"))
        if not summary_candidates:
            st.error("No `summary.csv` found inside the uploaded archive.")
            shutil.rmtree(temp_dir, ignore_errors=True)
            return

        summary_path = summary_candidates[0]
        try:
            summary_df = pd.read_csv(summary_path)
        except Exception as exc:  # pragma: no cover - UI feedback
            st.error(f"Unable to read extracted summary CSV: {exc}")
            shutil.rmtree(temp_dir, ignore_errors=True)
            return

        st.session_state["saved_results_extracted_dir"] = str(temp_dir)
        summary_source_dir = summary_path.parent
        st.session_state["last_summary_name"] = str(summary_path)
    else:
        try:
            summary_df = pd.read_csv(BytesIO(summary_file.getvalue()))
        except Exception as exc:  # pragma: no cover - UI feedback
            st.error(f"Unable to read CSV: {exc}")
            return
        st.session_state["last_summary_name"] = filename

    json_dir_default = (
        str(summary_source_dir)
        if summary_source_dir is not None
        else st.session_state.get("saved_results_dir", default_dir)
    )

    json_dir = json_dir_default
    st.session_state["saved_results_dir"] = json_dir

    if summary_df.empty:
        st.warning("Summary CSV is empty.")
        return

    template_col = None
    for candidate in ("template", "template_path"):
        if candidate in summary_df.columns:
            template_col = candidate
            break
    if template_col is None:
        st.error("Summary CSV must include a `template` or `template_path` column.")
        return

    summary_df["__template_key"] = summary_df[template_col].apply(
        lambda value: Path(str(value)).stem
    )

    template_options = sorted(summary_df["__template_key"].unique())
    selected_template = st.selectbox(
        "Select a template",
        template_options,
        key="saved_template_select",
    )

    template_df = summary_df[summary_df["__template_key"] == selected_template].drop(
        columns=["__template_key"]
    )

    st.markdown(
        f"#### Processors for template `{selected_template}` "
        f"({len(template_df)} rows)"
    )
    st.dataframe(
        template_df,
        use_container_width=True,
        hide_index=True,
    )

    json_result, json_path = load_saved_json(json_dir, selected_template)
    if json_result is None:
        if json_path:
            st.error(f"Failed to parse JSON at {json_path}. Verify the file contents.")
        else:
            st.info(
                "JSON file not found in the specified directory. Update the path "
                "or run the batch classifier to regenerate outputs."
            )
        return

    st.success(f"Loaded detailed evidence from {json_path}.")
    render_classification_result(json_result, key_prefix=f"saved_{selected_template}")


# Configure the page
st.set_page_config(
    page_title="Processor Classification & Pruning", page_icon="🚀", layout="wide"
)


def main():
    st.title("🚀 NiFi Processor Classification & Pruning")

    tabs = st.tabs(["Upload workflow", "Review saved results"])

    with tabs[0]:
        handle_upload_flow()

    with tabs[1]:
        handle_saved_results_flow()


if __name__ == "__main__":
    main()

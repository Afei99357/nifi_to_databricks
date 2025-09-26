#!/usr/bin/env python3

import os
import sys
from pathlib import Path

# Add parent directory to Python path to find tools (MUST be before imports)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from typing import Dict, List

import pandas as pd
import streamlit as st

from tools.conversion.snippet_store import SNIPPET_STORE_FILE
from tools.workbench.full_analysis import run_full_analysis
from tools.xml_tools import extract_processor_info

ANALYSIS_RESULT_PREFIXES = [
    "classification_results_",
    "table_results_",
    "script_results_",
    "lineage_results_",
    "variable_results_",
    "processor_info_",
    "snippet_store_loaded",
]


def _clear_analysis_state(file_name: str | None = None) -> None:
    """Remove cached analysis artefacts from session state."""

    for key in list(st.session_state.keys()):
        if key in {"analysis_progress", "analysis_summary", "analysis_running"}:
            st.session_state.pop(key, None)

    for flag in [
        "auto_start_migration",
        "auto_start_table_extraction",
        "auto_start_script_extraction",
        "auto_start_table_lineage",
        "auto_start_variable_analysis",
        "classification_running",
        "table_extraction_running",
        "script_extraction_running",
        "table_lineage_running",
        "variable_analysis_running",
    ]:
        st.session_state.pop(flag, None)

    tmp_dir = st.session_state.get("analysis_tmp_dir")

    if file_name:
        for prefix in ANALYSIS_RESULT_PREFIXES:
            st.session_state.pop(f"{prefix}{file_name}", None)
        if tmp_dir:
            xml_path = Path(tmp_dir) / file_name
            if xml_path.exists():
                xml_path.unlink(missing_ok=True)
    else:
        if tmp_dir:
            st.session_state.pop("analysis_tmp_dir", None)

    # Remove cached processor snippets so downstream pages reset
    if SNIPPET_STORE_FILE.exists():
        try:
            SNIPPET_STORE_FILE.unlink()
        except OSError:
            pass


# Configure the page
st.set_page_config(page_title="NiFi Analyzer Tools", page_icon="", layout="wide")


def main():
    st.title("NiFi Analyzer Tools")

    st.markdown("Upload your NiFi XML template file to begin the migration process.")

    uploaded_file = st.file_uploader(
        "Upload NiFi XML file",
        type=["xml"],
        help="Select your NiFi template (.xml) file",
    )

    # Check for existing file in session state if no new upload
    if not uploaded_file and "uploaded_file" in st.session_state:
        uploaded_file = st.session_state["uploaded_file"]
        st.info(f"📁 Current file: {uploaded_file.name} (uploaded previously)")

    # Show migration option when file is available
    def _format_progress(log: List[Dict[str, str]]) -> str:
        if not log:
            return ""
        icons = {"running": "⏳", "completed": "✅", "failed": "❌"}
        lines = []
        for entry in log:
            icon = icons.get(entry.get("status"), "•")
            label = entry.get("label") or entry.get("step", "Step")
            status = entry.get("status", "")
            message = entry.get("message")
            if status == "running":
                detail = "(running)"
            elif status == "completed":
                detail = f"— {message}" if message else "— completed"
            elif status == "failed":
                detail = f"— failed: {message}" if message else "— failed"
            else:
                detail = ""
            lines.append(f"{icon} **{label}** {detail}")
        return "\n".join(lines)

    progress_placeholder = st.empty()

    def _update_progress(log: List[Dict[str, str]]) -> None:
        text = _format_progress(log)
        if text:
            progress_placeholder.markdown(text)
        else:
            progress_placeholder.empty()

    if uploaded_file:
        st.success(f"✅ File uploaded: {uploaded_file.name}")
        st.session_state["uploaded_file"] = uploaded_file

        control_cols = st.columns([3, 1])
        run_clicked = False

        analysis_running = st.session_state.get("analysis_running", False)

        with control_cols[0]:
            run_clicked = st.button(
                "🔍 Start Analysis" if not analysis_running else "Analyzing…",
                use_container_width=True,
                disabled=analysis_running,
            )

        with control_cols[1]:
            if st.button(
                "🔄 Re-run Analysis",
                use_container_width=True,
                disabled=analysis_running,
            ):
                run_clicked = True

        if run_clicked and not analysis_running:
            st.session_state["analysis_running"] = True
            st.session_state["analysis_progress"] = []
            st.session_state.pop("analysis_summary", None)

            try:
                run_full_analysis(
                    uploaded_bytes=uploaded_file.getvalue(),
                    file_name=uploaded_file.name,
                    session_state=st.session_state,
                    on_update=_update_progress,
                )
                st.success("🎉 Full analysis completed.")
            except Exception as exc:  # pragma: no cover - interactive feedback
                st.error(f"❌ Analysis failed: {exc}")
            finally:
                st.session_state["analysis_running"] = False

        # Show latest progress log if available
        if not st.session_state.get("analysis_running"):
            progress_log = st.session_state.get("analysis_progress", [])
            if progress_log:
                _update_progress(progress_log)

        summary = st.session_state.get("analysis_summary")
        if summary and summary.get("file_name") == uploaded_file.name:
            st.caption(f"Last analyzed on {summary.get('timestamp', 'unknown')}")

        # Navigation shortcuts displayed once data is available
        if st.session_state.get("analysis_progress"):
            st.markdown("---")
            st.markdown("### Explore results")
            nav_cols = st.columns(5)
            with nav_cols[0]:
                if st.button("🚀 Processors", use_container_width=True):
                    st.switch_page("pages/01_Processor_Classification.py")
            with nav_cols[1]:
                if st.button("🗄️ Tables", use_container_width=True):
                    st.switch_page("pages/02_Table_Extraction.py")
            with nav_cols[2]:
                if st.button("📜 Scripts", use_container_width=True):
                    st.switch_page("pages/03_Script_Extraction.py")
            with nav_cols[3]:
                if st.button("📊 Lineage", use_container_width=True):
                    st.switch_page("pages/04_Lineage_Connections.py")
            with nav_cols[4]:
                if st.button("🔄 Variables", use_container_width=True):
                    st.switch_page("pages/05_Variable_Dependencies.py")

        if st.button(
            "🧹 Clear File", use_container_width=True, disabled=analysis_running
        ):
            _clear_analysis_state(uploaded_file.name)
            if "uploaded_file" in st.session_state:
                del st.session_state["uploaded_file"]
            st.rerun()

        # Processor Information Section
        st.markdown("---")

        # Extract and cache processor information
        processor_cache_key = f"processor_info_{uploaded_file.name}"

        if processor_cache_key not in st.session_state:
            xml_content = uploaded_file.getvalue().decode("utf-8")
            processors = extract_processor_info(xml_content)
            st.session_state[processor_cache_key] = processors
        else:
            processors = st.session_state[processor_cache_key]

        # Display processor information table
        if processors:
            with st.expander(
                f"📋 Processor Information ({len(processors)} processors)",
                expanded=False,
            ):
                st.markdown("#### Raw Processor Details")
                st.info(
                    "This shows basic information extracted from all processors in the workflow."
                )

                # Convert to DataFrame for display
                df = pd.DataFrame(processors)

                # Shorten processor types for better display
                df["type"] = df["type"].apply(
                    lambda x: x.split(".")[-1] if "." in x else x
                )

                # Filter controls
                col1, col2 = st.columns([1, 2])
                with col1:
                    # Processor type filter
                    unique_types = ["All"] + sorted(df["type"].unique().tolist())
                    selected_type = st.selectbox(
                        "Filter by Processor Type:",
                        unique_types,
                        key="processor_type_filter",
                    )

                with col2:
                    # Text search filter
                    search_term = st.text_input(
                        "Search Processors:",
                        placeholder="Search Name, Type, ID, or Comments",
                        key="processor_search",
                    )

                # Apply filters
                filtered_df = df.copy()

                # Filter by processor type
                if selected_type != "All":
                    filtered_df = filtered_df[filtered_df["type"] == selected_type]

                # Filter by search term
                if search_term:
                    search_mask = (
                        filtered_df["name"].str.contains(
                            search_term, case=False, na=False
                        )
                        | filtered_df["type"].str.contains(
                            search_term, case=False, na=False
                        )
                        | filtered_df["id"].str.contains(
                            search_term, case=False, na=False
                        )
                        | filtered_df["comments"].str.contains(
                            search_term, case=False, na=False
                        )
                    )
                    filtered_df = filtered_df[search_mask]

                # Show filtered results count
                if len(filtered_df) != len(df):
                    st.info(f"Showing {len(filtered_df)} of {len(df)} processors")

                # Display filtered table
                if not filtered_df.empty:
                    # Reorder columns: Name, Type, ID, Comments
                    display_df = filtered_df[["name", "type", "id", "comments"]]
                    display_df.columns = ["Name", "Type", "ID", "Comments"]

                    st.dataframe(display_df, use_container_width=True, hide_index=False)

                    # Download button
                    csv = filtered_df.to_csv(index=False)
                    st.download_button(
                        label=f"📥 Download Processor Info ({len(filtered_df)} items)",
                        data=csv,
                        file_name=f"nifi_processors_{uploaded_file.name.replace('.xml', '')}.csv",
                        mime="text/csv",
                        use_container_width=True,
                    )
                else:
                    st.warning("No processors match the current filters.")
        else:
            st.warning("⚠️ No processors found in the uploaded file.")

    else:
        st.info("Upload a NiFi XML file to get started")


if __name__ == "__main__":
    main()

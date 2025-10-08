#!/usr/bin/env python3

import os
import sys

import pandas as pd

# Add parent directory to Python path to find tools and config (MUST be before imports)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import streamlit as st

# Configure the page
st.set_page_config(page_title="Lineage & Connections", page_icon="📊", layout="wide")


def display_lineage_results(result, uploaded_file):
    """Display table lineage results from either fresh run or cache"""
    # Display summary metrics
    proc_tables = result.get("processor_tables", {})
    processors_with_reads = sum(1 for info in proc_tables.values() if info.get("reads"))
    processors_with_writes = sum(
        1 for info in proc_tables.values() if info.get("writes")
    )
    unique_tables_read = {
        table for info in proc_tables.values() for table in info.get("reads", [])
    }
    unique_tables_written = {
        table for info in proc_tables.values() for table in info.get("writes", [])
    }

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Processors with Reads", processors_with_reads)
    with col2:
        st.metric("Processors with Writes", processors_with_writes)
    with col3:
        st.metric("Unique Tables Read", len(unique_tables_read))
    with col4:
        st.metric("Unique Tables Written", len(unique_tables_written))

    # Display processor table usage summary
    st.markdown("### 🗂️ Table Usage by Processor")
    table_entries = []
    for pid, info in proc_tables.items():
        reads = info.get("reads", [])
        writes = info.get("writes", [])
        if not reads and not writes:
            continue
        table_entries.append(
            {
                "Processor": info.get("processor_name", pid),
                "Processor ID": pid,
                "Processor Type": info.get("processor_type"),
                "Tables Read": ", ".join(reads),
                "Tables Written": ", ".join(writes),
            }
        )

    if table_entries:
        table_df = pd.DataFrame(table_entries)
        st.dataframe(table_df, use_container_width=True, hide_index=True)
    else:
        st.info("No processors with table interactions were detected.")

    st.markdown("---")
    if table_entries:
        try:
            summary_csv = pd.DataFrame(table_entries).to_csv(index=False)
            st.download_button(
                label="📥 Download Processor Table Summary",
                data=summary_csv,
                file_name=f"processor_tables_{uploaded_file.name.replace('.xml', '')}.csv",
                mime="text/csv",
                use_container_width=True,
            )
        except Exception as e:
            st.error(f"Error preparing summary CSV: {e}")


def main():
    st.title("📊 Lineage & Connections")
    st.markdown(
        "**Analyze table lineage and processor connections extracted from NiFi workflows.**"
    )

    uploaded_file = st.session_state.get("uploaded_file", None)

    if not uploaded_file:
        st.warning("⚠️ No file selected. Please go back to Dashboard to upload a file.")
        if st.button("🔙 Back to Dashboard"):
            st.switch_page("Dashboard.py")
        return

    st.success(f"✅ Processing file: {uploaded_file.name}")

    lineage_cache_key = f"lineage_results_{uploaded_file.name}"
    cached_lineage = st.session_state.get(lineage_cache_key)

    if isinstance(cached_lineage, dict):
        st.info(
            "📋 Showing cached lineage analysis generated via the Dashboard analysis."
        )
        display_lineage_results(cached_lineage, uploaded_file)
    elif isinstance(cached_lineage, str):
        st.error(f"❌ Lineage analysis failed: {cached_lineage}")
        if st.button("🔙 Back to Dashboard", use_container_width=True):
            st.switch_page("Dashboard.py")
    else:
        st.warning(
            "Run the full analysis from the Dashboard to generate lineage results."
        )
        if st.button("🔙 Back to Dashboard", use_container_width=True):
            st.switch_page("Dashboard.py")


if __name__ == "__main__":
    main()

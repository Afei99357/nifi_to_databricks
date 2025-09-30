#!/usr/bin/env python3

import os
import sys

import pandas as pd

# Add parent directory to Python path to find tools and config (MUST be before imports)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import streamlit as st

from tools.table_extraction import get_table_summary

# Configure the page
st.set_page_config(page_title="Table Extraction", page_icon="🗄️", layout="wide")


def display_table_results(tables, uploaded_file):
    """Display table extraction results"""
    if isinstance(tables, str):
        st.error(f"❌ Table extraction failed: {tables}")
        return

    if not isinstance(tables, list):
        st.error(f"❌ Table extraction failed: Invalid result format - {type(tables)}")
        return

    try:
        # Get summary statistics
        summary = get_table_summary(tables)

        # Display summary metrics
        st.markdown("### 📊 Table Extraction Summary")

        col1, col2 = st.columns(2)
        with col1:
            st.metric("Total Tables", summary["total_tables"])
        with col2:
            st.metric("Property Types", len(summary["property_types"]))

        if not tables:
            st.info("No tables found in the workflow.")
            return

        # Property types breakdown
        if summary["property_types"]:
            st.markdown("### 📝 Property Types")
            pr_df = pd.DataFrame(
                list(summary["property_types"].items()),
                columns=["Property Name", "Count"],
            )
            st.dataframe(pr_df, hide_index=True, use_container_width=True)

        # Main table display
        st.markdown("### 🗄️ Extracted Tables")

        # Create main dataframe
        table_df = pd.DataFrame(tables)

        # Filter controls
        col1, col2 = st.columns(2)
        with col1:
            # Processor type filter
            proc_types = ["All"] + sorted(table_df["processor_type"].unique().tolist())
            selected_proc_type = st.selectbox(
                "Filter by Processor Type:", proc_types, key="processor_type_filter"
            )

        with col2:
            # Text search filter
            search_term = st.text_input(
                "Search Table Names:",
                placeholder="Enter table name (e.g., 'stdf', 'files')",
                key="table_search",
            )

        # Apply filters
        filtered_df = table_df.copy()

        if selected_proc_type != "All":
            filtered_df = filtered_df[
                filtered_df["processor_type"] == selected_proc_type
            ]

        if search_term:
            filtered_df = filtered_df[
                filtered_df["table_name"].str.contains(
                    search_term, case=False, na=False
                )
            ]

        # Show filtered results count
        if len(filtered_df) != len(table_df):
            st.info(f"Showing {len(filtered_df)} of {len(table_df)} tables")

        # Display main table with better formatting
        if not filtered_df.empty:
            columns = [
                "table_name",
                "processor_name",
                "processor_type",
                "property_name",
                "processor_id",
            ]
            if "io_type" in filtered_df.columns:
                columns.append("io_type")
            if "source" in filtered_df.columns:
                columns.append("source")
            if "sql_clause" in filtered_df.columns:
                columns.append("sql_clause")

            display_df = filtered_df[columns].copy()
            display_df.columns = [
                "Table Name",
                "Processor Name",
                "Processor Type",
                "Property Name",
                "Processor ID",
                *(["I/O Type"] if "io_type" in filtered_df.columns else []),
                *(["Source"] if "source" in filtered_df.columns else []),
                *(["SQL Clause"] if "sql_clause" in filtered_df.columns else []),
            ]

            st.dataframe(
                display_df,
                use_container_width=True,
                hide_index=True,
                height=None,  # Remove height limit to show all rows
                column_config={
                    "Table Name": st.column_config.TextColumn(
                        "Table Name", width="medium"
                    ),
                    "Processor Name": st.column_config.TextColumn(
                        "Processor Name", width="medium"
                    ),
                    "Processor Type": st.column_config.TextColumn(
                        "Processor Type", width="small"
                    ),
                    "Property Name": st.column_config.TextColumn(
                        "Property Name", width="medium"
                    ),
                    "Processor ID": st.column_config.TextColumn(
                        "Processor ID", width="small"
                    ),
                    **(
                        {
                            "I/O Type": st.column_config.TextColumn(
                                "I/O Type", width="small"
                            )
                        }
                        if "I/O Type" in display_df.columns
                        else {}
                    ),
                    **(
                        {"Source": st.column_config.TextColumn("Source", width="small")}
                        if "Source" in display_df.columns
                        else {}
                    ),
                    **(
                        {
                            "SQL Clause": st.column_config.TextColumn(
                                "SQL Clause", width="medium"
                            )
                        }
                        if "SQL Clause" in display_df.columns
                        else {}
                    ),
                },
            )

        else:
            st.warning("No tables match the current filters.")

        # Download button
        if not filtered_df.empty:
            csv_data = filtered_df.to_csv(index=False)
            st.download_button(
                label=f"📥 Download Tables ({len(filtered_df)} items)",
                data=csv_data,
                file_name=f"nifi_tables_{uploaded_file.name.replace('.xml', '')}.csv",
                mime="text/csv",
                use_container_width=True,
            )

    except Exception as e:
        st.error(f"❌ Error displaying table extraction results: {e}")


def main():
    st.title("🗄️ Table Extraction")
    st.markdown(
        "**Extract table references from NiFi workflows across SQL, NoSQL, Hive, HBase, and other data sources.**"
    )

    uploaded_file = st.session_state.get("uploaded_file", None)

    if not uploaded_file:
        st.warning("⚠️ No file selected. Please go back to Dashboard to upload a file.")
        if st.button("🔙 Back to Dashboard"):
            st.switch_page("Dashboard.py")
        return

    st.success(f"✅ Processing file: {uploaded_file.name}")

    table_cache_key = f"table_results_{uploaded_file.name}"
    cached_result = st.session_state.get(table_cache_key)

    if isinstance(cached_result, list):
        st.info(
            "📋 Showing cached table extraction results generated via the Dashboard analysis."
        )
        display_table_results(cached_result, uploaded_file)
    elif isinstance(cached_result, str):
        st.error(f"❌ Table extraction failed: {cached_result}")
        if st.button("🔙 Back to Dashboard", use_container_width=True):
            st.switch_page("Dashboard.py")
    else:
        st.warning(
            "Run the full analysis from the Dashboard to generate table extraction results."
        )
        if st.button("🔙 Back to Dashboard", use_container_width=True):
            st.switch_page("Dashboard.py")


if __name__ == "__main__":
    main()

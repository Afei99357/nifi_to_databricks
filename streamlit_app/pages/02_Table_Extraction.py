#!/usr/bin/env python3

import os
import sys
import tempfile

import pandas as pd

# Add parent directory to Python path to find tools and config (MUST be before imports)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import streamlit as st

from tools.table_extraction import extract_all_tables_from_nifi_xml, get_table_summary

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

        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Tables", summary["total_tables"])
        with col2:
            st.metric("Processor Types", len(summary["top_processors"]))
        with col3:
            st.metric("Property Types", len(summary["property_types"]))

        if not tables:
            st.info("No tables found in the workflow.")
            return

        # Processor types breakdown
        if summary["top_processors"]:
            st.markdown("### 🔧 Top Processor Types")
            pt_df = pd.DataFrame(
                list(summary["top_processors"].items()),
                columns=["Processor Type", "Table Count"],
            )
            col1, col2 = st.columns([1, 2])
            with col1:
                st.dataframe(pt_df, hide_index=True, use_container_width=True)
            with col2:
                st.bar_chart(pt_df.set_index("Processor Type")["Table Count"])

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
            # Prepare display dataframe with cleaner column names
            display_df = filtered_df[
                [
                    "table_name",
                    "processor_name",
                    "processor_type",
                    "property_name",
                    "processor_id",
                ]
            ].copy()
            display_df.columns = [
                "Table Name",
                "Processor Name",
                "Processor Type",
                "Property Name",
                "Processor ID",
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
                },
            )

            # Detailed view for selected table
            if len(filtered_df) > 0:
                st.markdown("### 🔍 Table Details")

                # Table selection for details
                table_options = ["None"] + filtered_df["table_name"].tolist()
                selected_table = st.selectbox(
                    "Select table for detailed information:",
                    table_options,
                    key="table_detail_select",
                )

                if selected_table != "None":
                    table_details = filtered_df[
                        filtered_df["table_name"] == selected_table
                    ].iloc[0]

                    col1, col2 = st.columns(2)
                    with col1:
                        st.markdown(f"**Table Name:** `{table_details['table_name']}`")
                        st.markdown(
                            f"**Processor Name:** {table_details['processor_name']}"
                        )
                        st.markdown(
                            f"**Property Name:** {table_details['property_name']}"
                        )

                    with col2:
                        st.markdown(
                            f"**Processor Type:** {table_details['processor_type']}"
                        )
                        st.markdown(
                            f"**Processor ID:** `{table_details['processor_id']}`"
                        )

        else:
            st.warning("No tables match the current filters.")

        # Download buttons
        if not table_df.empty:
            col1, col2 = st.columns(2)
            with col1:
                # Download filtered results
                filtered_csv = filtered_df.to_csv(index=False)
                st.download_button(
                    label=f"📥 Download Filtered Tables ({len(filtered_df)} items)",
                    data=filtered_csv,
                    file_name=f"nifi_tables_filtered_{uploaded_file.name.replace('.xml', '')}.csv",
                    mime="text/csv",
                    use_container_width=True,
                )
            with col2:
                # Download all results
                all_csv = table_df.to_csv(index=False)
                st.download_button(
                    label=f"📥 Download All Tables ({len(table_df)} items)",
                    data=all_csv,
                    file_name=f"nifi_tables_all_{uploaded_file.name.replace('.xml', '')}.csv",
                    mime="text/csv",
                    use_container_width=True,
                )

    except Exception as e:
        st.error(f"❌ Error displaying table extraction results: {e}")
        st.write(f"**Debug - Exception type:** {type(e)}")
        st.write(f"**Debug - Exception details:** {str(e)}")
        import traceback

        st.code(traceback.format_exc())


def main():
    st.title("🗄️ Table Extraction")
    st.markdown(
        "**Extract table references from NiFi workflows across SQL, NoSQL, Hive, HBase, and other data sources.**"
    )

    # Check for uploaded file from Dashboard
    uploaded_file = st.session_state.get("uploaded_file", None)

    if uploaded_file:
        st.success(f"✅ Processing file: {uploaded_file.name}")
    else:
        st.warning("⚠️ No file selected. Please go back to Dashboard to upload a file.")
        if st.button("🔙 Back to Dashboard"):
            st.switch_page("Dashboard.py")
        return

    # Check for cached table results
    table_cache_key = f"table_results_{uploaded_file.name}"
    cached_result = st.session_state.get(table_cache_key, None)

    # Check if table extraction is running
    extraction_running = st.session_state.get("table_extraction_running", False)

    # Check for auto-start flag from Dashboard
    auto_start = st.session_state.get("auto_start_table_extraction", False)

    # Dynamic layout based on whether Extract Tables button should be shown
    if cached_result or auto_start:
        # Only show Back to Dashboard button
        if st.button(
            "🔙 Back to Dashboard",
            disabled=extraction_running,
            help=("Cannot navigate during extraction" if extraction_running else None),
        ):
            st.switch_page("Dashboard.py")
        run_extraction = auto_start
    else:
        # Show both buttons when no results exist
        col1, col2 = st.columns(2)

        with col1:
            run_extraction = (
                st.button(
                    "🗄️ Extract Tables",
                    use_container_width=True,
                    disabled=extraction_running,
                )
                or auto_start
            )

        with col2:
            if st.button(
                "🔙 Back to Dashboard",
                disabled=extraction_running,
                help=(
                    "Cannot navigate during extraction" if extraction_running else None
                ),
            ):
                st.switch_page("Dashboard.py")

    # Clear auto-start flag after checking
    if auto_start:
        st.session_state["auto_start_table_extraction"] = False

    # Display cached results if available
    if cached_result and not run_extraction:
        st.info(
            "📋 Showing cached table extraction results. Click 'Extract Tables' to regenerate."
        )
        display_table_results(cached_result, uploaded_file)

    # Run table extraction
    if uploaded_file and run_extraction and not extraction_running:
        # Save temp file
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xml") as tmp_file:
            tmp_file.write(uploaded_file.getvalue())
            tmp_xml_path = tmp_file.name

        # Set extraction running flag
        st.session_state["table_extraction_running"] = True

        try:
            # Show spinner during extraction
            with st.spinner(
                "🗄️ Extracting table references... Please do not navigate away."
            ):
                tables = extract_all_tables_from_nifi_xml(xml_path=tmp_xml_path)

            st.success("✅ Table extraction completed!")

            # Cache the result
            st.session_state[table_cache_key] = tables

            # Display the results
            display_table_results(tables, uploaded_file)

        except Exception as e:
            st.error(f"❌ Table extraction failed: {e}")
            st.write("**Debug info:**")
            st.code(str(e))

            # Cache the error for consistency
            st.session_state[table_cache_key] = str(e)
        finally:
            # Clear extraction running flag
            st.session_state["table_extraction_running"] = False
            # Clean up temp file
            if os.path.exists(tmp_xml_path):
                os.unlink(tmp_xml_path)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3

import os
import sys
import tempfile

import pandas as pd

# Add parent directory to Python path to find tools and config (MUST be before imports)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import streamlit as st

from tools.script_extraction import extract_all_scripts_from_nifi_xml
from tools.table_extraction import extract_all_tables_from_nifi_xml, get_table_summary

# Configure the page
st.set_page_config(page_title="Table Extraction", page_icon="🗄️", layout="wide")


def display_script_results(scripts, uploaded_file):
    """Display script extraction results"""
    if isinstance(scripts, str):
        st.error(f"❌ Script extraction failed: {scripts}")
        return

    if not isinstance(scripts, list):
        st.error(
            f"❌ Script extraction failed: Invalid result format - {type(scripts)}"
        )
        return

    try:
        # Display summary metrics
        st.markdown("### 📊 Script Extraction Summary")

        total_processors = len(scripts)
        total_scripts = sum(result["script_count"] for result in scripts)

        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Processors with Scripts", total_processors)
        with col2:
            st.metric("Total Scripts", total_scripts)
        with col3:
            external_deps = set()
            for result in scripts:
                external_deps.update(result["external_hosts"])
            st.metric("External Dependencies", len(external_deps))

        if not scripts:
            st.info("No scripts found in the workflow.")
            return

        # Create detailed table
        script_details = []
        for result in scripts:
            for script in result["scripts"]:
                script_details.append(
                    {
                        "Script Path": script["path"],
                        "Script Type": script["type"],
                        "Migration Priority": script["migration_priority"],
                        "Processor Name": result["processor_name"],
                        "Processor Type": result["processor_type"],
                        "Processor ID": result["processor_id"],
                    }
                )

            for executable in result["executables"]:
                script_details.append(
                    {
                        "Script Path": (
                            executable["command"][:80] + "..."
                            if len(executable["command"]) > 80
                            else executable["command"]
                        ),
                        "Script Type": executable["type"],
                        "Migration Priority": executable["migration_priority"],
                        "Processor Name": result["processor_name"],
                        "Processor Type": result["processor_type"],
                        "Processor ID": result["processor_id"],
                    }
                )

        if script_details:
            st.markdown("### 📋 Script Details")
            script_df = pd.DataFrame(script_details)

            # Filter controls
            col1, col2, col3 = st.columns(3)
            with col1:
                script_types = ["All"] + sorted(
                    script_df["Script Type"].unique().tolist()
                )
                selected_script_type = st.selectbox(
                    "Filter by Script Type:", script_types, key="script_type_filter"
                )

            with col2:
                priorities = ["All"] + sorted(
                    script_df["Migration Priority"].unique().tolist()
                )
                selected_priority = st.selectbox(
                    "Filter by Priority:", priorities, key="priority_filter"
                )

            with col3:
                search_script = st.text_input(
                    "Search Scripts:",
                    placeholder="Enter script name",
                    key="script_search",
                )

            # Apply filters
            filtered_script_df = script_df.copy()

            if selected_script_type != "All":
                filtered_script_df = filtered_script_df[
                    filtered_script_df["Script Type"] == selected_script_type
                ]

            if selected_priority != "All":
                filtered_script_df = filtered_script_df[
                    filtered_script_df["Migration Priority"] == selected_priority
                ]

            if search_script:
                filtered_script_df = filtered_script_df[
                    filtered_script_df["Script Path"].str.contains(
                        search_script, case=False, na=False
                    )
                ]

            # Show filtered results count
            if len(filtered_script_df) != len(script_df):
                st.info(
                    f"Showing {len(filtered_script_df)} of {len(script_df)} scripts"
                )

            # Display table
            if not filtered_script_df.empty:
                st.dataframe(
                    filtered_script_df,
                    use_container_width=True,
                    hide_index=False,
                    height=None,
                    column_config={
                        "Script Path": st.column_config.TextColumn(
                            "Script Path", width="large"
                        ),
                        "Script Type": st.column_config.TextColumn(
                            "Script Type", width="small"
                        ),
                        "Migration Priority": st.column_config.TextColumn(
                            "Migration Priority", width="small"
                        ),
                        "Processor Name": st.column_config.TextColumn(
                            "Processor Name", width="medium"
                        ),
                        "Processor Type": st.column_config.TextColumn(
                            "Processor Type", width="medium"
                        ),
                        "Processor ID": st.column_config.TextColumn(
                            "Processor ID", width="small"
                        ),
                    },
                )
            else:
                st.warning("No scripts match the current filters.")

        # External dependencies
        if external_deps:
            st.markdown("### 🌐 External Dependencies")
            deps_df = pd.DataFrame(list(external_deps), columns=["External Host"])
            st.dataframe(deps_df, use_container_width=True, hide_index=False)

        # Download button
        if script_details:
            script_csv = pd.DataFrame(script_details).to_csv(index=False)
            st.download_button(
                label=f"📥 Download Scripts ({len(script_details)} items)",
                data=script_csv,
                file_name=f"nifi_scripts_{uploaded_file.name.replace('.xml', '')}.csv",
                mime="text/csv",
                use_container_width=True,
            )

    except Exception as e:
        st.error(f"❌ Error displaying script extraction results: {e}")
        st.code(f"Exception: {str(e)}")


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

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Tables", summary["total_tables"])
        with col2:
            st.metric("Data Sources", len(summary["data_sources"]))
        with col3:
            st.metric("Detection Methods", len(summary["detection_methods"]))
        with col4:
            st.metric("Processor Types", len(summary["top_processors"]))

        if not tables:
            st.info("No tables found in the workflow.")
            return

        # Data source breakdown
        if summary["data_sources"]:
            st.markdown("### 📈 Data Sources Breakdown")
            ds_df = pd.DataFrame(
                list(summary["data_sources"].items()),
                columns=["Data Source", "Table Count"],
            )
            col1, col2 = st.columns([1, 2])
            with col1:
                st.dataframe(ds_df, hide_index=True, use_container_width=True)
            with col2:
                st.bar_chart(ds_df.set_index("Data Source")["Table Count"])

        # Detection methods breakdown
        if summary["detection_methods"]:
            st.markdown("### 🔍 Detection Methods")
            dm_df = pd.DataFrame(
                list(summary["detection_methods"].items()),
                columns=["Detection Method", "Count"],
            )
            st.dataframe(dm_df, hide_index=True, use_container_width=True)

        # Main table display
        st.markdown("### 🗄️ Extracted Tables")

        # Create main dataframe
        table_df = pd.DataFrame(tables)

        # Filter controls
        col1, col2, col3 = st.columns(3)
        with col1:
            # Data source filter
            data_sources = ["All"] + sorted(table_df["data_source"].unique().tolist())
            selected_source = st.selectbox(
                "Filter by Data Source:", data_sources, key="data_source_filter"
            )

        with col2:
            # Processor type filter
            proc_types = ["All"] + sorted(table_df["processor_type"].unique().tolist())
            selected_proc_type = st.selectbox(
                "Filter by Processor Type:", proc_types, key="processor_type_filter"
            )

        with col3:
            # Text search filter
            search_term = st.text_input(
                "Search Table Names:",
                placeholder="Enter table name (e.g., 'stdf', 'files')",
                key="table_search",
            )

        # Apply filters
        filtered_df = table_df.copy()

        if selected_source != "All":
            filtered_df = filtered_df[filtered_df["data_source"] == selected_source]

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
                    "data_source",
                    "processor_name",
                    "processor_id",
                    "processor_type",
                    "detection_method",
                ]
            ].copy()
            display_df.columns = [
                "Table Name",
                "Data Source",
                "Processor Name",
                "Processor ID",
                "Processor Type",
                "Detection Method",
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
                    "Data Source": st.column_config.TextColumn(
                        "Data Source", width="small"
                    ),
                    "Processor Name": st.column_config.TextColumn(
                        "Processor Name", width="medium"
                    ),
                    "Processor ID": st.column_config.TextColumn(
                        "Processor ID", width="small"
                    ),
                    "Processor Type": st.column_config.TextColumn(
                        "Processor Type", width="medium"
                    ),
                    "Detection Method": st.column_config.TextColumn(
                        "Detection Method", width="small"
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
                        st.markdown(f"**Data Source:** {table_details['data_source']}")
                        st.markdown(
                            f"**Detection Method:** {table_details['detection_method']}"
                        )

                    with col2:
                        st.markdown(
                            f"**Processor Name:** {table_details['processor_name']}"
                        )
                        st.markdown(
                            f"**Processor Type:** {table_details['processor_type']}"
                        )
                        st.markdown(
                            f"**Property Name:** {table_details['property_name']}"
                        )

                    if table_details.get("processor_id"):
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
    st.title("🗄️ Table & Script Extraction")
    st.markdown(
        "**Extract table references and external scripts from NiFi workflows.**"
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

    # Tab navigation
    tab_options = ["Tables", "External Scripts"]

    # Use radio for persistent tab selection
    current_tab = st.radio(
        "Select Analysis:", tab_options, horizontal=True, key="extraction_tab_selection"
    )

    if current_tab == "Tables":
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
                help=(
                    "Cannot navigate during extraction" if extraction_running else None
                ),
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
                        "Cannot navigate during extraction"
                        if extraction_running
                        else None
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

    elif current_tab == "External Scripts":
        # Check for cached script results
        script_cache_key = f"script_results_{uploaded_file.name}"
        cached_scripts = st.session_state.get(script_cache_key, None)

        # Check if script extraction is running
        script_extraction_running = st.session_state.get(
            "script_extraction_running", False
        )

        # Control buttons
        col1, col2 = st.columns(2)

        with col1:
            run_script_extraction = st.button(
                "📜 Extract Scripts",
                use_container_width=True,
                disabled=script_extraction_running,
            )

        with col2:
            if st.button(
                "🔙 Back to Dashboard",
                disabled=script_extraction_running,
                help=(
                    "Cannot navigate during extraction"
                    if script_extraction_running
                    else None
                ),
            ):
                st.switch_page("Dashboard.py")

        # Display cached results if available
        if cached_scripts and not run_script_extraction:
            st.info(
                "📋 Showing cached script extraction results. Click 'Extract Scripts' to regenerate."
            )
            display_script_results(cached_scripts, uploaded_file)

        # Run script extraction
        if uploaded_file and run_script_extraction and not script_extraction_running:
            # Save temp file
            with tempfile.NamedTemporaryFile(delete=False, suffix=".xml") as tmp_file:
                tmp_file.write(uploaded_file.getvalue())
                tmp_xml_path = tmp_file.name

            # Set extraction running flag
            st.session_state["script_extraction_running"] = True

            try:
                # Show spinner during extraction
                with st.spinner(
                    "📜 Extracting external scripts... Please do not navigate away."
                ):
                    scripts = extract_all_scripts_from_nifi_xml(xml_path=tmp_xml_path)

                st.success("✅ Script extraction completed!")

                # Cache the result
                st.session_state[script_cache_key] = scripts

                # Display the results
                display_script_results(scripts, uploaded_file)

            except Exception as e:
                st.error(f"❌ Script extraction failed: {e}")
                st.write("**Debug info:**")
                st.code(str(e))

                # Cache the error for consistency
                st.session_state[script_cache_key] = str(e)
            finally:
                # Clear extraction running flag
                st.session_state["script_extraction_running"] = False
                # Clean up temp file
                if os.path.exists(tmp_xml_path):
                    os.unlink(tmp_xml_path)


if __name__ == "__main__":
    main()

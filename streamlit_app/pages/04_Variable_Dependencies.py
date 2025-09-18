#!/usr/bin/env python3

import os
import sys
import tempfile

import pandas as pd

# Add parent directory to Python path to find tools and config (MUST be before imports)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import streamlit as st

from tools.variable_extraction import extract_variable_dependencies

# Configure the page
st.set_page_config(page_title="Variable Dependencies", page_icon="🔄", layout="wide")


def display_variable_results(result, uploaded_file=None):
    """Display comprehensive variable dependency analysis results"""
    # Note: uploaded_file parameter preserved for API compatibility but not currently used
    # Handle error cases
    if isinstance(result, str):
        st.error(f"❌ Variable analysis failed: {result}")
        return

    if not isinstance(result, dict):
        st.error(f"❌ Variable analysis failed: Invalid result format - {type(result)}")
        return

    try:
        # Display summary metrics
        st.markdown("### 📊 Variable Analysis Overview")

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Variables", result.get("total_variables", 0))
        with col2:
            st.metric("Defined Variables", result.get("defined_variables", 0))
        with col3:
            st.metric("Unknown Source Variables", result.get("external_variables", 0))
        with col4:
            st.metric("Total Processors", result.get("total_processors", 0))

        variables = result.get("variables", {})
        processors = result.get("processors", {})

        if not variables:
            st.warning("No variables found in the workflow.")
            return

        # Sticky tabs implementation
        TABS = [
            "📋 Variable Details",
            "📝 Variable Actions",
            "🌐 Variable Flow Connections",
        ]

        # Read active tab from URL first, then session
        active = st.query_params.get("tab", TABS[0])
        if active not in TABS:
            active = st.session_state.get("active_tab", TABS[0])

        # Render a faux tab bar
        idx = TABS.index(active)
        choice = st.radio(
            "Section",
            TABS,
            index=idx,
            horizontal=True,
            label_visibility="collapsed",
            key="active_tab",
        )

        # Keep URL in sync so refresh/back works
        st.query_params.update({"tab": choice})

        # Tab 1: Variable Details
        if choice == "📋 Variable Details":
            st.markdown("### 📋 Variable Details")
            st.info(
                "All variables in the workflow with their source processors (where they are extracted from)."
            )

            # Create comprehensive variable details table
            all_variables_data = []

            for var_name, var_data in variables.items():
                clean_var_name = var_name.strip()

                # Get all definitions (processors that extract/create this variable)
                definitions = var_data.get("definitions", [])

                if definitions:
                    # Variable has definitions - show each processor that defines it
                    for defn in definitions:
                        all_variables_data.append(
                            {
                                "Variable Name": clean_var_name,
                                "Processor Name": defn["processor_name"],
                                "Processor ID": defn["processor_id"],
                                "Processor Type": defn["processor_type"].split(".")[-1],
                                "Group Name": defn.get("parent_group_name", "Root"),
                                "Source": "Known",
                                "Property": defn["property_name"],
                                "Value": defn["property_value"],
                            }
                        )
                else:
                    # External variable - show processors that use it
                    usages = var_data.get("usages", [])
                    for usage in usages:
                        all_variables_data.append(
                            {
                                "Variable Name": clean_var_name,
                                "Processor Name": usage["processor_name"],
                                "Processor ID": usage["processor_id"],
                                "Processor Type": usage["processor_type"].split(".")[
                                    -1
                                ],
                                "Group Name": usage.get("parent_group_name", "Root"),
                                "Source": "Unknown Source",
                                "Property": usage["property_name"],
                                "Value": f"Usage: {usage['variable_expression']} in {usage['processor_name']}",
                            }
                        )

            if all_variables_data:
                details_df = pd.DataFrame(all_variables_data)

                # Enhanced column-based filter controls
                st.markdown("#### 🔧 Filter Controls")

                # Create filter columns (removed Processor Type for cleaner layout)
                filter_col1, filter_col2, filter_col3, filter_col4 = st.columns(4)

                with filter_col1:
                    unique_vars = ["All"] + sorted(
                        details_df["Variable Name"].unique().tolist()
                    )
                    selected_var_filter = st.selectbox(
                        "Variable Name:", unique_vars, key="var_filter"
                    )

                with filter_col2:
                    unique_processors = ["All"] + sorted(
                        details_df["Processor Name"].unique().tolist()
                    )
                    selected_processor_filter = st.selectbox(
                        "Processor Name:", unique_processors, key="proc_filter"
                    )

                with filter_col3:
                    unique_groups = ["All"] + sorted(
                        details_df["Group Name"].unique().tolist()
                    )
                    selected_group_filter = st.selectbox(
                        "Group Name:", unique_groups, key="group_filter"
                    )

                with filter_col4:
                    unique_sources = ["All"] + sorted(
                        details_df["Source"].unique().tolist()
                    )
                    selected_source_filter = st.selectbox(
                        "Source:", unique_sources, key="source_filter"
                    )

                # Add text search across all columns
                st.markdown("#### 🔍 Text Search")
                search_text = st.text_input(
                    "Search across all columns:",
                    placeholder="Enter text to search in any column...",
                    key="global_search",
                )

                # Clear filters button
                if st.button("🗑️ Clear All Filters", key="clear_filters"):
                    # Clear all filters by rerunning with default values
                    st.rerun()

                # Apply all filters
                filtered_details_df = details_df.copy()

                # Apply dropdown filters
                if selected_var_filter != "All":
                    filtered_details_df = filtered_details_df[
                        filtered_details_df["Variable Name"] == selected_var_filter
                    ]

                if selected_processor_filter != "All":
                    filtered_details_df = filtered_details_df[
                        filtered_details_df["Processor Name"]
                        == selected_processor_filter
                    ]

                if selected_group_filter != "All":
                    filtered_details_df = filtered_details_df[
                        filtered_details_df["Group Name"] == selected_group_filter
                    ]

                if selected_source_filter != "All":
                    filtered_details_df = filtered_details_df[
                        filtered_details_df["Source"] == selected_source_filter
                    ]

                # Apply text search across all columns
                if search_text:
                    search_mask = (
                        filtered_details_df.astype(str)
                        .apply(
                            lambda x: x.str.contains(search_text, case=False, na=False)
                        )
                        .any(axis=1)
                    )
                    filtered_details_df = filtered_details_df[search_mask]

                # Show active filters and results count
                active_filters = []
                if selected_var_filter != "All":
                    active_filters.append(f"Variable: {selected_var_filter}")
                if selected_processor_filter != "All":
                    active_filters.append(f"Processor: {selected_processor_filter}")
                if selected_group_filter != "All":
                    active_filters.append(f"Group: {selected_group_filter}")
                if selected_source_filter != "All":
                    active_filters.append(f"Source: {selected_source_filter}")
                if search_text:
                    active_filters.append(f"Search: '{search_text}'")

                if active_filters:
                    st.info(
                        f"📊 **Showing {len(filtered_details_df)} of {len(details_df)} records** | "
                        f"**Active filters:** {', '.join(active_filters)}"
                    )
                elif len(filtered_details_df) != len(details_df):
                    st.info(
                        f"Showing {len(filtered_details_df)} of {len(details_df)} variable definitions"
                    )

                # Display table with column configuration
                st.dataframe(
                    filtered_details_df,
                    use_container_width=True,
                    hide_index=False,
                    column_config={
                        "Variable Name": st.column_config.TextColumn(
                            "Variable Name", width="medium"
                        ),
                        "Processor Name": st.column_config.TextColumn(
                            "Processor Name", width="medium"
                        ),
                        "Processor ID": st.column_config.TextColumn(
                            "Processor ID", width="small"
                        ),
                        "Processor Type": st.column_config.TextColumn(
                            "Processor Type", width="small"
                        ),
                        "Group Name": st.column_config.TextColumn(
                            "Group Name", width="medium"
                        ),
                        "Source": st.column_config.TextColumn("Source", width="small"),
                        "Property": st.column_config.TextColumn(
                            "Property", width="medium"
                        ),
                        "Value": st.column_config.TextColumn("Value", width="large"),
                    },
                )

                # Download button
                csv_data = filtered_details_df.to_csv(index=False)
                st.download_button(
                    label=f"📥 Download Variable Details ({len(filtered_details_df)} items)",
                    data=csv_data,
                    file_name="variable_details.csv",
                    mime="text/csv",
                    use_container_width=True,
                )
            else:
                st.warning("No variable details available.")

        # Tab 2: Variable Actions
        elif choice == "📝 Variable Actions":
            st.markdown("### 📝 Variable Actions Analysis")
            st.info("Analyze how variables are defined and used across processors.")

            # Create action summary table
            action_data = []
            for var_name, var_data in variables.items():
                clean_var_name = var_name.strip()  # Clean variable name for display
                action_data.append(
                    {
                        "Variable Name": f"${{{clean_var_name}}}",
                        "Defines": var_data["definition_count"],
                        "Uses": var_data["usage_count"],
                        "Total Processors": var_data["processor_count"],
                        "Status": (
                            "Unknown Source" if var_data["is_external"] else "Known"
                        ),
                    }
                )

            if action_data:
                action_df = pd.DataFrame(action_data)

                # Filter controls
                col1, col2 = st.columns(2)
                with col1:
                    status_filter = st.selectbox(
                        "Filter by Status:",
                        ["All", "Known", "Unknown Source"],
                    )

                with col2:
                    min_usage = st.number_input(
                        "Minimum Usage Count:",
                        min_value=0,
                        max_value=(
                            int(action_df["Uses"].max()) if not action_df.empty else 0
                        ),
                        value=0,
                    )

                # Apply filters
                filtered_df = action_df.copy()
                if status_filter != "All":
                    filtered_df = filtered_df[filtered_df["Status"] == status_filter]
                if min_usage > 0:
                    filtered_df = filtered_df[filtered_df["Uses"] >= min_usage]

                # Sort by total processors
                filtered_df = filtered_df.sort_values(
                    "Total Processors", ascending=False
                )

                # Collapsible table display section (defaultly closed)
                with st.expander("📊 Variable Summary Table", expanded=False):
                    st.dataframe(
                        filtered_df,
                        use_container_width=True,
                        hide_index=False,
                        column_config={
                            "Variable Name": st.column_config.TextColumn(
                                "Variable Name", width="medium"
                            ),
                            "Defines": st.column_config.NumberColumn(
                                "Defines", width="small"
                            ),
                            "Uses": st.column_config.NumberColumn(
                                "Uses", width="small"
                            ),
                            "Total Processors": st.column_config.NumberColumn(
                                "Total Processors", width="small"
                            ),
                            "Status": st.column_config.TextColumn(
                                "Status", width="small"
                            ),
                        },
                    )

                # Variable details
                if not filtered_df.empty:
                    # Clean variable names for detailed selection
                    clean_var_names = [
                        v.replace("${", "").replace("}", "").strip()
                        for v in filtered_df["Variable Name"].tolist()
                    ]
                    detail_options = ["None"] + clean_var_names

                    # Default to first variable instead of "None"

                    selected_var_detail = st.selectbox(
                        "Select variable for detailed analysis:",
                        options=detail_options,
                        index=(
                            1 if clean_var_names else 0
                        ),  # Select first variable by default
                    )

                    # Find the original variable key (may contain whitespace)
                    original_var_key = None
                    if selected_var_detail != "None":
                        for var_key in variables.keys():
                            if var_key.strip() == selected_var_detail:
                                original_var_key = var_key
                                break

                    if original_var_key and original_var_key in variables:
                        var_detail = variables[original_var_key]

                        st.markdown(
                            f"#### Variable Details: `${{{selected_var_detail}}}`"
                        )

                        # Add explanation box
                        st.info(
                            f"""
                            **What this shows:** The variable `${{{selected_var_detail}}}` analysis reveals:

                            • **Definitions**: Processors that SET this variable's value (typically UpdateAttribute processors)
                            • **Usages**: Processors that READ/USE this variable in their operations

                            This helps you understand the data flow and dependencies in your NiFi workflow.
                            """
                        )

                        # Clean table-based detailed view
                        st.markdown("### 📋 Detailed Analysis")

                        # Definitions Table
                        definitions = var_detail.get("definitions", [])
                        st.markdown(
                            f"#### 📝 Definitions ({len(definitions)} processors)"
                        )
                        if definitions:
                            st.markdown("*Processors that define/set this variable:*")

                            definitions_data = []
                            for defn in definitions:
                                definitions_data.append(
                                    {
                                        "Processor Name": defn["processor_name"],
                                        "Processor Type": defn["processor_type"].split(
                                            "."
                                        )[-1],
                                        "Processor ID": defn["processor_id"],
                                        "Property": defn["property_name"],
                                        "Value": defn["property_value"],
                                    }
                                )

                            definitions_df = pd.DataFrame(definitions_data)
                            st.dataframe(
                                definitions_df,
                                use_container_width=True,
                                hide_index=False,
                                column_config={
                                    "Processor Name": st.column_config.TextColumn(
                                        "Processor Name", width="medium"
                                    ),
                                    "Processor Type": st.column_config.TextColumn(
                                        "Type", width="small"
                                    ),
                                    "Processor ID": st.column_config.TextColumn(
                                        "ID", width="small"
                                    ),
                                    "Property": st.column_config.TextColumn(
                                        "Property", width="medium"
                                    ),
                                    "Value": st.column_config.TextColumn(
                                        "Value", width="large"
                                    ),
                                },
                            )
                        else:
                            st.warning(
                                "🔍 **Unknown Source Variable** - No definitions found (source unknown)"
                            )

                        # Usages Table
                        usages = var_detail.get("usages", [])
                        st.markdown(f"#### 📖 Usages ({len(usages)} processors)")
                        if usages:
                            st.markdown("*Processors that read/use this variable:*")

                            usages_data = []
                            for usage in usages:
                                usages_data.append(
                                    {
                                        "Processor Name": usage["processor_name"],
                                        "Processor Type": usage["processor_type"].split(
                                            "."
                                        )[-1],
                                        "Processor ID": usage["processor_id"],
                                        "Property": usage["property_name"],
                                        "Expression": f"Usage: {usage['variable_expression']} in {usage['processor_name']}",
                                        "Has Functions": (
                                            "Yes"
                                            if usage.get("has_functions")
                                            else "No"
                                        ),
                                        "Context": usage["usage_context"],
                                    }
                                )

                            usages_df = pd.DataFrame(usages_data)
                            st.dataframe(
                                usages_df,
                                use_container_width=True,
                                hide_index=False,
                                column_config={
                                    "Processor Name": st.column_config.TextColumn(
                                        "Processor Name", width="medium"
                                    ),
                                    "Processor Type": st.column_config.TextColumn(
                                        "Type", width="small"
                                    ),
                                    "Processor ID": st.column_config.TextColumn(
                                        "ID", width="small"
                                    ),
                                    "Property": st.column_config.TextColumn(
                                        "Property", width="medium"
                                    ),
                                    "Expression": st.column_config.TextColumn(
                                        "Expression", width="large"
                                    ),
                                    "Has Functions": st.column_config.TextColumn(
                                        "Functions", width="small"
                                    ),
                                    "Context": st.column_config.TextColumn(
                                        "Context", width="medium"
                                    ),
                                },
                            )
                        else:
                            st.info("ℹ️ No usages found")

        # Tab 3: Variable Flow Connections
        elif choice == "🌐 Variable Flow Connections":
            st.markdown("### 🌐 Variable Flow Connections")
            st.info(
                "Shows variable flow paths between connected processors. Each row represents one hop where a variable flows from a defining processor to a using processor through NiFi connections. Only variables with actual flow paths are included (not all variables flow between processors)."
            )

            # Build connection flow data
            connection_flows = []
            for var_name, var_data in variables.items():
                clean_var_name = var_name.strip()  # Clean variable name
                for flow in var_data.get("flows", []):
                    processors = flow.get("processors", [])
                    relationships = flow.get("relationships", [])

                    for i in range(len(processors) - 1):
                        source = processors[i]
                        target = processors[i + 1]
                        rel = relationships[i] if i < len(relationships) else "unknown"

                        connection_flows.append(
                            {
                                "Variable": f"${{{clean_var_name}}}",
                                "Source Processor": source["processor_name"],
                                "Source ID": source["processor_id"],
                                "Target Processor": target["processor_name"],
                                "Target ID": target["processor_id"],
                                "Connection Type": rel,
                                "Flow Chain": f"{source['processor_name']} → {target['processor_name']}",
                            }
                        )

            if connection_flows:
                conn_df = pd.DataFrame(connection_flows)

                # Filter controls
                col1, col2 = st.columns(2)
                with col1:
                    # Clean variable names for dropdown (remove ${} wrapper)
                    clean_var_options = ["All"] + sorted(
                        [
                            var.replace("${", "").replace("}", "")
                            for var in conn_df["Variable"].unique().tolist()
                        ]
                    )
                    var_filter_clean = st.selectbox(
                        "Filter by Variable:",
                        clean_var_options,
                    )

                    # Convert back to ${} format for filtering
                    var_filter = (
                        f"${{{var_filter_clean}}}"
                        if var_filter_clean != "All"
                        else "All"
                    )

                with col2:
                    conn_type_filter = st.selectbox(
                        "Filter by Connection Type:",
                        ["All"] + sorted(conn_df["Connection Type"].unique().tolist()),
                    )

                # Apply filters
                filtered_conn_df = conn_df.copy()
                if var_filter != "All":
                    filtered_conn_df = filtered_conn_df[
                        filtered_conn_df["Variable"] == var_filter
                    ]
                if conn_type_filter != "All":
                    filtered_conn_df = filtered_conn_df[
                        filtered_conn_df["Connection Type"] == conn_type_filter
                    ]

                st.dataframe(
                    filtered_conn_df,
                    use_container_width=True,
                    hide_index=False,
                    column_config={
                        "Variable": st.column_config.TextColumn(
                            "Variable", width="medium"
                        ),
                        "Source Processor": st.column_config.TextColumn(
                            "Source Processor", width="medium"
                        ),
                        "Source ID": st.column_config.TextColumn(
                            "Source ID", width="small"
                        ),
                        "Target Processor": st.column_config.TextColumn(
                            "Target Processor", width="medium"
                        ),
                        "Target ID": st.column_config.TextColumn(
                            "Target ID", width="small"
                        ),
                        "Connection Type": st.column_config.TextColumn(
                            "Connection Type", width="small"
                        ),
                        "Flow Chain": st.column_config.TextColumn(
                            "Flow Chain", width="large"
                        ),
                    },
                )

                # Variable Flow Chains for selected variable
                if var_filter != "All":
                    selected_var_flows = []
                    for var_name, var_data in variables.items():
                        clean_var_name = var_name.strip()
                        if f"${{{clean_var_name}}}" == var_filter:
                            selected_var_flows = var_data.get("flows", [])
                            break

                    if selected_var_flows:
                        st.markdown(
                            f"#### 🔗 Variable Flow Chains for `{var_filter_clean}`"
                        )
                        st.info(
                            "Complete flow chains showing how this variable moves through connected processors."
                        )

                        # Control for number of chains to display
                        max_chains = len(selected_var_flows)
                        if max_chains > 10:
                            num_chains_to_show = st.slider(
                                "Number of flow chains to display:",
                                min_value=1,
                                max_value=max_chains,
                                value=min(10, max_chains),
                            )
                        else:
                            num_chains_to_show = max_chains

                        for i, flow in enumerate(
                            selected_var_flows[:num_chains_to_show]
                        ):
                            with st.expander(
                                f"Flow Chain {i+1} (Length: {flow['chain_length']})",
                                expanded=i == 0,
                            ):
                                # Show processor chain
                                chain_processors = flow.get("processors", [])
                                if chain_processors:
                                    chain_text = " → ".join(
                                        [
                                            f"**{p['processor_name']}** ({p['processor_id']})"
                                            for p in chain_processors
                                        ]
                                    )
                                    st.markdown(chain_text)

                                    # Show relationship types
                                    relationships = flow.get("relationships", [])
                                    if relationships:
                                        rel_text = " → ".join(relationships)
                                        st.caption(f"Connection types: {rel_text}")

                        if num_chains_to_show < len(selected_var_flows):
                            st.info(
                                f"📋 Showing {num_chains_to_show} of {len(selected_var_flows)} total flow chains (use slider to show more)"
                            )
            else:
                st.info("No variable flow connections found.")

    except Exception as e:
        st.error(f"❌ Error displaying variable analysis results: {e}")
        st.write(f"**Debug - Exception type:** {type(e)}")
        st.write(f"**Debug - Exception details:** {str(e)}")
        import traceback

        st.code(traceback.format_exc())


def main():
    st.title("🔄 Variable Dependencies")
    st.markdown(
        "**Analyze variable definitions, transformations, and usage patterns across NiFi processors.**"
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

    # Check for cached variable results
    variable_cache_key = f"variable_results_{uploaded_file.name}"
    cached_result = st.session_state.get(variable_cache_key, None)

    # Check if variable analysis is running
    analysis_running = st.session_state.get("variable_analysis_running", False)

    # Check for auto-start flag from Dashboard
    auto_start = st.session_state.get("auto_start_variable_analysis", False)

    # Dynamic layout based on whether Analyze Variables button should be shown
    if cached_result or auto_start:
        # Only show Back to Dashboard button
        if st.button(
            "🔙 Back to Dashboard",
            disabled=analysis_running,
            help="Cannot navigate during analysis" if analysis_running else None,
        ):
            st.switch_page("Dashboard.py")
        run_analysis = auto_start
    else:
        # Show both buttons when no results exist
        col1, col2 = st.columns(2)

        with col1:
            run_analysis = (
                st.button(
                    "🔄 Analyze Variables",
                    use_container_width=True,
                    disabled=analysis_running,
                )
                or auto_start
            )

        with col2:
            if st.button(
                "🔙 Back to Dashboard",
                disabled=analysis_running,
                help="Cannot navigate during analysis" if analysis_running else None,
            ):
                st.switch_page("Dashboard.py")

    # Clear auto-start flag after checking
    if auto_start:
        st.session_state["auto_start_variable_analysis"] = False

    # Display cached results if available
    if cached_result and not run_analysis:
        st.info(
            "📋 Showing cached variable analysis results. Click 'Analyze Variables' to regenerate."
        )
        display_variable_results(cached_result, uploaded_file)

    # Run variable analysis
    if uploaded_file and run_analysis and not analysis_running:
        # Save temp file
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xml") as tmp_file:
            tmp_file.write(uploaded_file.getvalue())
            tmp_xml_path = tmp_file.name

        # Set analysis running flag
        st.session_state["variable_analysis_running"] = True

        try:
            # Show spinner during analysis
            with st.spinner(
                "🔄 Analyzing variable dependencies... Please do not navigate away."
            ):
                result = extract_variable_dependencies(xml_path=tmp_xml_path)

            st.success("✅ Variable analysis completed!")

            # Cache the result
            st.session_state[variable_cache_key] = result

            # Display the results
            display_variable_results(result, uploaded_file)

        except Exception as e:
            st.error(f"❌ Variable analysis failed: {e}")
            st.write("**Debug info:**")
            st.code(str(e))

            # Cache the error for consistency
            st.session_state[variable_cache_key] = str(e)
        finally:
            # Clear analysis running flag
            st.session_state["variable_analysis_running"] = False
            # Clean up temp file
            if os.path.exists(tmp_xml_path):
                os.unlink(tmp_xml_path)


if __name__ == "__main__":
    main()

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


def display_variable_results(result, uploaded_file):
    """Display comprehensive variable dependency analysis results"""
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
            st.metric("External Variables", result.get("external_variables", 0))
        with col4:
            st.metric("Total Processors", result.get("total_processors", 0))

        variables = result.get("variables", {})
        processors = result.get("processors", {})

        if not variables:
            st.warning("No variables found in the workflow.")
            return

        # Tabs for different analysis views
        tab1, tab2, tab3, tab4 = st.tabs(
            [
                "🔄 Variable Flow Tracking",
                "📝 Variable Actions",
                "🌐 Flow Connections",
                "📋 Variable Summary",
            ]
        )

        # Tab 1: Variable Flow Tracking
        with tab1:
            st.markdown("### 🔄 Variable Flow Tracking")
            st.info(
                "Trace how variables flow from definition through modification to usage."
            )

            # Variable selector
            variable_names = sorted(variables.keys())
            if variable_names:
                selected_var = st.selectbox(
                    "Select Variable to Trace:",
                    options=variable_names,
                    key="flow_var_selector",
                )

                if selected_var and selected_var in variables:
                    var_data = variables[selected_var]

                    st.markdown(f"#### Variable Lineage: `${{{selected_var}}}`")

                    # Flow Statistics - horizontal layout
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("Total Processors", var_data["processor_count"])
                    with col2:
                        st.metric("Definitions", var_data["definition_count"])
                    with col3:
                        st.metric("Usages", var_data["usage_count"])
                    with col4:
                        if var_data["is_external"]:
                            st.error("⚠️ External")
                        else:
                            st.success("✅ Internal")

                    # Flow chain table - full width
                    st.markdown("**🔄 Processor Flow Chain:**")

                    # Build flow chain table
                    flow_data = []

                    # Add definitions
                    for definition in var_data.get("definitions", []):
                        flow_data.append(
                            {
                                "Processor Name": definition["processor_name"],
                                "Processor Type": definition["processor_type"].split(
                                    "."
                                )[-1],
                                "Processor ID": definition["processor_id"][:8] + "...",
                                "Action": "DEFINES",
                                "Details": f"Property: {definition['property_name']}",
                                "Value/Expression": (
                                    definition["property_value"][:50] + "..."
                                    if len(definition["property_value"]) > 50
                                    else definition["property_value"]
                                ),
                            }
                        )

                    # Add transformations
                    for transform in var_data.get("transformations", []):
                        flow_data.append(
                            {
                                "Processor Name": transform["processor_name"],
                                "Processor Type": transform["processor_type"].split(
                                    "."
                                )[-1],
                                "Processor ID": transform["processor_id"][:8] + "...",
                                "Action": (
                                    "MODIFIES"
                                    if transform["transformation_type"]
                                    == "modification"
                                    else "TRANSFORMS"
                                ),
                                "Details": f"→ {transform['output_variable']}",
                                "Value/Expression": (
                                    transform["transformation_expression"][:50] + "..."
                                    if len(transform["transformation_expression"]) > 50
                                    else transform["transformation_expression"]
                                ),
                            }
                        )

                    # Add usages
                    for usage in var_data.get("usages", []):
                        action = "USES"
                        if "RouteOnAttribute" in usage["processor_type"]:
                            action = "EVALUATES"
                        elif "LogMessage" in usage["processor_type"]:
                            action = "LOGS"
                        elif "ExecuteStreamCommand" in usage["processor_type"]:
                            action = "EXECUTES"

                        flow_data.append(
                            {
                                "Processor Name": usage["processor_name"],
                                "Processor Type": usage["processor_type"].split(".")[
                                    -1
                                ],
                                "Processor ID": usage["processor_id"][:8] + "...",
                                "Action": action,
                                "Details": f"In: {usage['property_name']}",
                                "Value/Expression": usage["variable_expression"]
                                + (
                                    " (with functions)"
                                    if usage.get("has_functions")
                                    else ""
                                ),
                            }
                        )

                    if flow_data:
                        flow_df = pd.DataFrame(flow_data)
                        st.dataframe(flow_df, use_container_width=True, hide_index=True)
                    else:
                        st.info("No flow data available for this variable.")

                    # Flow chains visualization
                    flows = var_data.get("flows", [])
                    if flows:
                        st.markdown("#### 🔗 Variable Flow Chains")
                        for i, flow in enumerate(flows[:5]):  # Show top 5 flows
                            with st.expander(
                                f"Flow Chain {i+1} (Length: {flow['chain_length']})",
                                expanded=i == 0,
                            ):
                                # Show processor chain
                                chain_processors = flow.get("processors", [])
                                if chain_processors:
                                    chain_text = " → ".join(
                                        [
                                            f"**{p['processor_name']}** ({p['processor_type'].split('.')[-1]})"
                                            for p in chain_processors
                                        ]
                                    )
                                    st.markdown(chain_text)

                                    # Show relationship types
                                    relationships = flow.get("relationships", [])
                                    if relationships:
                                        rel_text = " → ".join(relationships)
                                        st.caption(f"Connection types: {rel_text}")

        # Tab 2: Variable Actions
        with tab2:
            st.markdown("### 📝 Variable Actions Analysis")
            st.info(
                "Analyze how variables are defined, modified, and used across processors."
            )

            # Create action summary table
            action_data = []
            for var_name, var_data in variables.items():
                action_data.append(
                    {
                        "Variable Name": f"${{{var_name}}}",
                        "Defines": var_data["definition_count"],
                        "Modifies": len(var_data.get("transformations", [])),
                        "Uses": var_data["usage_count"],
                        "Total Flow": var_data["processor_count"],
                        "Status": "External" if var_data["is_external"] else "Internal",
                    }
                )

            if action_data:
                action_df = pd.DataFrame(action_data)

                # Filter controls
                col1, col2 = st.columns(2)
                with col1:
                    status_filter = st.selectbox(
                        "Filter by Status:",
                        ["All", "Internal", "External"],
                        key="action_status_filter",
                    )

                with col2:
                    min_usage = st.number_input(
                        "Minimum Usage Count:",
                        min_value=0,
                        max_value=(
                            int(action_df["Uses"].max()) if not action_df.empty else 0
                        ),
                        value=0,
                        key="action_usage_filter",
                    )

                # Apply filters
                filtered_df = action_df.copy()
                if status_filter != "All":
                    filtered_df = filtered_df[filtered_df["Status"] == status_filter]
                if min_usage > 0:
                    filtered_df = filtered_df[filtered_df["Uses"] >= min_usage]

                # Sort by total flow
                filtered_df = filtered_df.sort_values("Total Flow", ascending=False)
                st.dataframe(filtered_df, use_container_width=True, hide_index=True)

                # Variable details
                if not filtered_df.empty:
                    selected_var_detail = st.selectbox(
                        "Select variable for detailed analysis:",
                        options=["None"]
                        + [
                            v.replace("${", "").replace("}", "")
                            for v in filtered_df["Variable Name"].tolist()
                        ],
                        key="action_var_detail",
                    )

                    if (
                        selected_var_detail != "None"
                        and selected_var_detail in variables
                    ):
                        var_detail = variables[selected_var_detail]

                        st.markdown(
                            f"#### Variable Details: `${{{selected_var_detail}}}`"
                        )

                        col1, col2, col3 = st.columns(3)

                        with col1:
                            st.markdown("**Definitions:**")
                            definitions = var_detail.get("definitions", [])
                            if definitions:
                                for defn in definitions:
                                    st.write(f"• **{defn['processor_name']}**")
                                    st.write(
                                        f"  ↳ Type: {defn['processor_type'].split('.')[-1]}"
                                    )
                                    st.write(f"  ↳ ID: {defn['processor_id'][:8]}...")
                                    st.write(
                                        f"  ↳ Value: `{defn['property_value'][:30]}...`"
                                    )
                            else:
                                st.write("No definitions (external variable)")

                        with col2:
                            st.markdown("**Transformations:**")
                            transformations = var_detail.get("transformations", [])
                            if transformations:
                                for trans in transformations:
                                    st.write(f"• **{trans['processor_name']}**")
                                    st.write(
                                        f"  ↳ Type: {trans['transformation_type']}"
                                    )
                                    st.write(
                                        f"  ↳ Output: `{trans['output_variable']}`"
                                    )
                            else:
                                st.write("No transformations found")

                        with col3:
                            st.markdown("**Usages:**")
                            usages = var_detail.get("usages", [])[:5]  # Show first 5
                            if usages:
                                for usage in usages:
                                    st.write(f"• **{usage['processor_name']}**")
                                    st.write(
                                        f"  ↳ Type: {usage['processor_type'].split('.')[-1]}"
                                    )
                                    st.write(f"  ↳ Context: {usage['usage_context']}")
                                if len(var_detail.get("usages", [])) > 5:
                                    st.write(
                                        f"... and {len(var_detail.get('usages', [])) - 5} more"
                                    )
                            else:
                                st.write("No usages found")

        # Tab 3: Flow Connections
        with tab3:
            st.markdown("### 🌐 Variable Flow Through Connections")
            st.info("Shows how variables move through processor connections.")

            # Build connection flow data
            connection_flows = []
            for var_name, var_data in variables.items():
                for flow in var_data.get("flows", []):
                    processors = flow.get("processors", [])
                    relationships = flow.get("relationships", [])

                    for i in range(len(processors) - 1):
                        source = processors[i]
                        target = processors[i + 1]
                        rel = relationships[i] if i < len(relationships) else "unknown"

                        connection_flows.append(
                            {
                                "Variable": f"${{{var_name}}}",
                                "Source Processor": source["processor_name"],
                                "Source ID": source["processor_id"][:8] + "...",
                                "Target Processor": target["processor_name"],
                                "Target ID": target["processor_id"][:8] + "...",
                                "Connection Type": rel,
                                "Flow Chain": f"{source['processor_name']} → {target['processor_name']}",
                            }
                        )

            if connection_flows:
                conn_df = pd.DataFrame(connection_flows)

                # Filter controls
                col1, col2 = st.columns(2)
                with col1:
                    var_filter = st.selectbox(
                        "Filter by Variable:",
                        ["All"] + sorted(conn_df["Variable"].unique().tolist()),
                        key="conn_var_filter",
                    )

                with col2:
                    conn_type_filter = st.selectbox(
                        "Filter by Connection Type:",
                        ["All"] + sorted(conn_df["Connection Type"].unique().tolist()),
                        key="conn_type_filter",
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
                    filtered_conn_df, use_container_width=True, hide_index=True
                )
            else:
                st.info("No variable flow connections found.")

        # Tab 4: Variable Summary
        with tab4:
            st.markdown("### 📋 Variable Summary Report")

            # Generate summary report
            report_lines = [
                f"# Variable Dependencies Analysis Report",
                f"",
                f"**Analysis Date:** {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}",
                f"**Workflow File:** {uploaded_file.name}",
                f"",
                f"## Summary Statistics",
                f"",
                f"- **Total Variables Found:** {result.get('total_variables', 0)}",
                f"- **Internally Defined Variables:** {result.get('defined_variables', 0)}",
                f"- **External Variables:** {result.get('external_variables', 0)}",
                f"- **Total Processors Analyzed:** {result.get('total_processors', 0)}",
                f"",
            ]

            # Top variables by usage
            if variables:
                sorted_vars = sorted(
                    variables.items(),
                    key=lambda x: x[1]["processor_count"],
                    reverse=True,
                )

                report_lines.extend(
                    [
                        f"## Most Used Variables",
                        f"",
                    ]
                )

                for i, (var_name, var_data) in enumerate(sorted_vars[:10]):
                    report_lines.append(
                        f"{i+1}. **${{{var_name}}}** - Used by {var_data['processor_count']} processors"
                    )

                report_lines.extend(
                    [
                        f"",
                        f"## External Variable Dependencies",
                        f"",
                    ]
                )

                external_vars = [
                    (name, data)
                    for name, data in variables.items()
                    if data["is_external"]
                ]
                if external_vars:
                    for var_name, var_data in external_vars:
                        report_lines.append(
                            f"- **${{{var_name}}}** - Used by {var_data['usage_count']} processors"
                        )
                else:
                    report_lines.append("- No external variables found")

            report = "\n".join(report_lines)
            st.markdown(report)

            # Download button
            st.download_button(
                label="📥 Download Variable Analysis Report",
                data=report,
                file_name=f"variable_analysis_{uploaded_file.name.replace('.xml', '')}.md",
                mime="text/markdown",
                use_container_width=True,
            )

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

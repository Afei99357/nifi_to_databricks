#!/usr/bin/env python3

import os
import sys
import tempfile

import pandas as pd

# Add parent directory to Python path to find tools and config (MUST be before imports)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import streamlit as st

from tools.dependency_extraction import (
    extract_all_processor_dependencies,
    generate_dependency_report,
)

# Configure the page
st.set_page_config(page_title="Processor Dependencies", page_icon="🔗", layout="wide")


def display_dependency_results(result, uploaded_file):
    """Display comprehensive dependency analysis results"""
    # Handle error cases
    if isinstance(result, str):
        st.error(f"❌ Dependency analysis failed: {result}")
        return

    if not isinstance(result, dict):
        st.error(
            f"❌ Dependency analysis failed: Invalid result format - {type(result)}"
        )
        return

    try:
        # Display summary metrics
        stats = result.get("dependency_statistics", {})
        if stats:
            st.markdown("### 📊 Dependency Overview")

            # Top row metrics
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Total Processors", result.get("total_processors", 0))
            with col2:
                st.metric("Total Dependencies", stats.get("total_dependencies", 0))
            with col3:
                st.metric(
                    "Highly Connected", stats.get("highly_connected_processors", 0)
                )
            with col4:
                st.metric(
                    "Isolated Processors",
                    stats.get("processors_with_no_dependencies", 0),
                )

            # Bottom row metrics
            col5, col6, col7, col8 = st.columns(4)
            with col5:
                st.metric(
                    "Avg Dependencies",
                    f"{stats.get('average_dependencies_per_processor', 0):.1f}",
                )
            with col6:
                st.metric("Max Dependencies", stats.get("max_dependencies", 0))
            with col7:
                st.metric("Max Dependents", stats.get("max_dependents", 0))
            with col8:
                circular_count = len(
                    result.get("impact_analysis", {}).get("circular_dependencies", [])
                )
                st.metric(
                    "Circular Dependencies",
                    circular_count,
                    delta=None if circular_count == 0 else f"⚠️ {circular_count}",
                )

        # Tabs for different analysis views
        tab1, tab2, tab3, tab4, tab5 = st.tabs(
            [
                "🔍 Processor Dependencies",
                "⚡ Impact Analysis",
                "📊 Variable Dependencies",
                "🔗 Connection Map",
                "📋 Full Report",
            ]
        )

        # Tab 1: Processor Dependencies
        with tab1:
            st.markdown("### 🔍 Processor Dependency Analysis")
            st.info("Analyze dependencies for each processor in the workflow.")

            comprehensive_deps = result.get("dependencies", {}).get(
                "comprehensive_mapping", {}
            )
            if comprehensive_deps:
                # Create processor dependency table
                dependency_data = []
                for proc_id, dep_info in comprehensive_deps.items():
                    dependency_data.append(
                        {
                            "Processor Name": dep_info.get("processor_name", "Unknown"),
                            "Processor Type": dep_info.get("processor_type", "Unknown"),
                            "Dependencies": dep_info.get("dependency_count", 0),
                            "Dependents": dep_info.get("dependent_count", 0),
                            "Total Relationships": dep_info.get(
                                "total_relationships", 0
                            ),
                            "Processor ID": proc_id,
                        }
                    )

                df = pd.DataFrame(dependency_data)

                # Filter controls
                col1, col2 = st.columns([1, 2])
                with col1:
                    # Processor type filter
                    processor_types = ["All"] + sorted(
                        df["Processor Type"].unique().tolist()
                    )
                    selected_type = st.selectbox(
                        "Filter by Processor Type:",
                        processor_types,
                        key="dep_type_filter",
                    )

                with col2:
                    # Text search filter
                    search_term = st.text_input(
                        "Search Processors:",
                        placeholder="Search by name or type",
                        key="dep_search",
                    )

                # Apply filters
                filtered_df = df.copy()

                if selected_type != "All":
                    filtered_df = filtered_df[
                        filtered_df["Processor Type"] == selected_type
                    ]

                if search_term:
                    search_mask = filtered_df["Processor Name"].str.contains(
                        search_term, case=False, na=False
                    ) | filtered_df["Processor Type"].str.contains(
                        search_term, case=False, na=False
                    )
                    filtered_df = filtered_df[search_mask]

                # Show filtered results count
                if len(filtered_df) != len(df):
                    st.info(f"Showing {len(filtered_df)} of {len(df)} processors")

                # Display table
                if not filtered_df.empty:
                    # Sort by total relationships (most connected first)
                    display_df = filtered_df.sort_values(
                        "Total Relationships", ascending=False
                    )
                    st.dataframe(
                        display_df[
                            [
                                "Processor Name",
                                "Processor Type",
                                "Dependencies",
                                "Dependents",
                                "Total Relationships",
                            ]
                        ],
                        use_container_width=True,
                        hide_index=True,
                    )

                    # Detailed dependency view
                    selected_processor = st.selectbox(
                        "Select processor for detailed dependency view:",
                        options=["None"] + filtered_df["Processor Name"].tolist(),
                        key="selected_processor",
                    )

                    if selected_processor != "None":
                        # Find processor details
                        selected_id = filtered_df[
                            filtered_df["Processor Name"] == selected_processor
                        ]["Processor ID"].iloc[0]
                        proc_details = comprehensive_deps[selected_id]

                        st.markdown(f"#### Dependencies for: {selected_processor}")

                        col1, col2 = st.columns(2)

                        with col1:
                            st.markdown("**🔽 Depends On:**")
                            depends_on = proc_details.get("depends_on", [])
                            if depends_on:
                                for dep in depends_on:
                                    st.write(
                                        f"• **{dep.get('dependency_processor_name', 'Unknown')}** ({dep.get('dependency_type', 'unknown')})"
                                    )
                                    st.write(f"  ↳ {dep.get('details', 'No details')}")
                            else:
                                st.write("No dependencies found")

                        with col2:
                            st.markdown("**🔼 Dependents:**")
                            dependents = proc_details.get("dependents", [])
                            if dependents:
                                for dep in dependents:
                                    st.write(
                                        f"• **{dep.get('dependent_processor_name', 'Unknown')}** ({dep.get('dependency_type', 'unknown')})"
                                    )
                                    st.write(f"  ↳ {dep.get('details', 'No details')}")
                            else:
                                st.write("No dependents found")

                else:
                    st.warning("No processors match the current filters.")

        # Tab 2: Impact Analysis
        with tab2:
            st.markdown("### ⚡ Impact Analysis")
            impact = result.get("impact_analysis", {})

            # High Impact Processors
            high_impact = impact.get("high_impact_processors", [])
            if high_impact:
                st.markdown("#### 🎯 High Impact Processors")
                st.warning(
                    "Changes to these processors will affect many others in the workflow."
                )

                impact_data = []
                for proc in high_impact:
                    impact_data.append(
                        {
                            "Processor Name": proc.get("processor_name", "Unknown"),
                            "Processor Type": proc.get("processor_type", "Unknown"),
                            "Dependent Count": proc.get("dependent_count", 0),
                            "Impact Level": proc.get("impact_level", "unknown").title(),
                        }
                    )

                impact_df = pd.DataFrame(impact_data)
                st.dataframe(impact_df, use_container_width=True, hide_index=True)

            # Isolated Processors
            isolated = impact.get("isolated_processors", [])
            if isolated:
                st.markdown("#### 🏝️ Isolated Processors")
                st.info(
                    "These processors have no dependencies and can be modified independently."
                )

                isolated_data = []
                for proc in isolated:
                    isolated_data.append(
                        {
                            "Processor Name": proc.get("processor_name", "Unknown"),
                            "Processor Type": proc.get("processor_type", "Unknown"),
                        }
                    )

                isolated_df = pd.DataFrame(isolated_data)
                st.dataframe(isolated_df, use_container_width=True, hide_index=True)

            # Dependency Chains
            chains = impact.get("dependency_chains", [])
            if chains:
                st.markdown("#### 🔗 Dependency Chains")
                st.info("Sequential dependency chains in the workflow (longest first).")

                for i, chain in enumerate(chains[:5]):  # Show top 5 chains
                    with st.expander(
                        f"Chain {i+1} (Length: {chain['chain_length']})",
                        expanded=True,
                    ):
                        chain_names = []
                        for proc in chain["processors"]:
                            chain_names.append(
                                f"**{proc['processor_name']}** ({proc['processor_type']})"
                            )

                        st.markdown(" → ".join(chain_names))

            # Circular Dependencies
            circular = impact.get("circular_dependencies", [])
            if circular:
                st.markdown("#### ⚠️ Circular Dependencies")
                st.error(
                    "These dependency cycles may cause issues and should be reviewed."
                )

                for i, cycle in enumerate(circular):
                    with st.expander(
                        f"Circular Dependency {i+1} (Length: {cycle['cycle_length']})",
                        expanded=True,
                    ):
                        cycle_names = []
                        for proc in cycle["processors"]:
                            cycle_names.append(f"**{proc['processor_name']}**")

                        # Show the cycle
                        cycle_display = " → ".join(cycle_names)
                        if cycle_names:
                            cycle_display += (
                                f" → **{cycle['processors'][0]['processor_name']}**"
                            )

                        st.markdown(cycle_display)

        # Tab 3: Variable Dependencies
        with tab3:
            st.markdown("### 📊 Variable Dependencies")
            var_deps = result.get("dependencies", {}).get("variable_dependencies", {})

            if var_deps:
                # Variables summary
                defined_vars = [
                    v for v in var_deps.values() if v.get("is_defined", False)
                ]
                undefined_vars = [
                    v for v in var_deps.values() if not v.get("is_defined", True)
                ]

                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Total Variables", len(var_deps))
                with col2:
                    st.metric("Defined Variables", len(defined_vars))
                with col3:
                    st.metric("External Variables", len(undefined_vars))

                # Variable details table
                var_data = []
                for var_name, var_info in var_deps.items():
                    var_data.append(
                        {
                            "Variable Name": f"${{{var_name}}}",
                            "Defined": (
                                "Yes" if var_info.get("is_defined", False) else "No"
                            ),
                            "Used": "Yes" if var_info.get("is_used", False) else "No",
                            "Definitions Count": len(var_info.get("definitions", [])),
                            "Usages Count": len(var_info.get("usages", [])),
                        }
                    )

                var_df = pd.DataFrame(var_data)

                # Filter for variables
                var_filter = st.selectbox(
                    "Filter variables:",
                    [
                        "All Variables",
                        "Defined Variables",
                        "External Variables",
                        "Unused Variables",
                    ],
                    key="var_filter",
                )

                filtered_var_df = var_df.copy()
                if var_filter == "Defined Variables":
                    filtered_var_df = var_df[var_df["Defined"] == "Yes"]
                elif var_filter == "External Variables":
                    filtered_var_df = var_df[var_df["Defined"] == "No"]
                elif var_filter == "Unused Variables":
                    filtered_var_df = var_df[var_df["Used"] == "No"]

                st.dataframe(filtered_var_df, use_container_width=True, hide_index=True)

                # Variable detail view
                if not filtered_var_df.empty:
                    selected_var = st.selectbox(
                        "Select variable for detailed view:",
                        options=["None"]
                        + [
                            v.replace("${", "").replace("}", "")
                            for v in filtered_var_df["Variable Name"].tolist()
                        ],
                        key="selected_variable",
                    )

                    if selected_var != "None":
                        var_info = var_deps.get(selected_var, {})

                        st.markdown(f"#### Variable Details: ${{{selected_var}}}")

                        col1, col2 = st.columns(2)

                        with col1:
                            st.markdown("**Definitions:**")
                            definitions = var_info.get("definitions", [])
                            if definitions:
                                for defn in definitions:
                                    st.write(
                                        f"• **{defn.get('processor_name', 'Unknown')}** ({defn.get('processor_type', 'unknown')})"
                                    )
                                    st.write(
                                        f"  ↳ Property: {defn.get('property_name', 'unknown')}"
                                    )
                            else:
                                st.write("No definitions found (external variable)")

                        with col2:
                            st.markdown("**Usages:**")
                            usages = var_info.get("usages", [])
                            if usages:
                                for usage in usages:
                                    st.write(
                                        f"• **{usage.get('processor_name', 'Unknown')}** ({usage.get('processor_type', 'unknown')})"
                                    )
                                    st.write(
                                        f"  ↳ Used in: {usage.get('property_name', 'unknown')}"
                                    )
                            else:
                                st.write("No usages found")

            else:
                st.info("No variable dependencies found in the workflow.")

        # Tab 4: Connection Map
        with tab4:
            st.markdown("### 🔗 Connection Map")
            conn_deps = result.get("dependencies", {}).get(
                "connection_dependencies", {}
            )

            if conn_deps:
                # Connection summary
                total_connections = sum(
                    len(proc.get("dependencies_outgoing", []))
                    for proc in conn_deps.values()
                )
                processors_with_connections = sum(
                    1
                    for proc in conn_deps.values()
                    if proc.get("total_outgoing", 0) > 0
                    or proc.get("total_incoming", 0) > 0
                )

                col1, col2 = st.columns(2)
                with col1:
                    st.metric("Total Connections", total_connections)
                with col2:
                    st.metric("Connected Processors", processors_with_connections)

                # Connection details table
                conn_data = []
                for proc_id, conn_info in conn_deps.items():
                    conn_data.append(
                        {
                            "Processor Name": conn_info.get(
                                "processor_name", "Unknown"
                            ),
                            "Processor Type": conn_info.get(
                                "processor_type", "Unknown"
                            ),
                            "Outgoing Connections": conn_info.get("total_outgoing", 0),
                            "Incoming Connections": conn_info.get("total_incoming", 0),
                            "Total Connections": conn_info.get("total_outgoing", 0)
                            + conn_info.get("total_incoming", 0),
                            "Processor ID": proc_id,
                        }
                    )

                conn_df = pd.DataFrame(conn_data)

                # Sort by total connections
                conn_df = conn_df.sort_values("Total Connections", ascending=False)
                st.dataframe(
                    conn_df[
                        [
                            "Processor Name",
                            "Processor Type",
                            "Outgoing Connections",
                            "Incoming Connections",
                            "Total Connections",
                        ]
                    ],
                    use_container_width=True,
                    hide_index=True,
                )

            else:
                st.info("No connection dependencies found in the workflow.")

        # Tab 5: Full Report
        with tab5:
            st.markdown("### 📋 Comprehensive Dependency Report")

            # Generate and display full report
            report = generate_dependency_report(result)
            st.markdown(report)

            # Download buttons
            col1, col2 = st.columns(2)
            with col1:
                st.download_button(
                    label="📥 Download Dependency Report",
                    data=report,
                    file_name=f"dependency_analysis_{uploaded_file.name.replace('.xml', '')}.md",
                    mime="text/markdown",
                    use_container_width=True,
                )

            with col2:
                # Download dependency data as JSON
                import json

                dependency_json = json.dumps(result, indent=2, default=str)
                st.download_button(
                    label="📥 Download Raw Data (JSON)",
                    data=dependency_json,
                    file_name=f"dependency_data_{uploaded_file.name.replace('.xml', '')}.json",
                    mime="application/json",
                    use_container_width=True,
                )

    except Exception as e:
        st.error(f"❌ Error displaying dependency analysis results: {e}")
        st.write(f"**Debug - Exception type:** {type(e)}")
        st.write(f"**Debug - Exception details:** {str(e)}")
        import traceback

        st.code(traceback.format_exc())


def main():
    st.title("🔗 Processor Dependencies")
    st.markdown(
        "**Comprehensive dependency analysis across ALL processors in the NiFi workflow.**"
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

    # Check for cached dependency results
    dependency_cache_key = f"dependency_results_{uploaded_file.name}"
    cached_result = st.session_state.get(dependency_cache_key, None)

    # Check if dependency analysis is running
    analysis_running = st.session_state.get("dependency_analysis_running", False)

    # Check for auto-start flag from Dashboard
    auto_start = st.session_state.get("auto_start_dependency_analysis", False)

    # Dynamic layout based on whether Analyze Dependencies button should be shown
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
                    "🔗 Analyze Dependencies",
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
        st.session_state["auto_start_dependency_analysis"] = False

    # Display cached results if available
    if cached_result and not run_analysis:
        st.info(
            "📋 Showing cached dependency analysis results. Click 'Analyze Dependencies' to regenerate."
        )
        display_dependency_results(cached_result, uploaded_file)

    # Run dependency analysis
    if uploaded_file and run_analysis and not analysis_running:
        # Save temp file
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xml") as tmp_file:
            tmp_file.write(uploaded_file.getvalue())
            tmp_xml_path = tmp_file.name

        # Set analysis running flag
        st.session_state["dependency_analysis_running"] = True

        try:
            # Show spinner during analysis
            with st.spinner(
                "🔗 Analyzing processor dependencies... Please do not navigate away."
            ):
                result = extract_all_processor_dependencies(xml_path=tmp_xml_path)

            st.success("✅ Dependency analysis completed!")

            # Cache the result
            st.session_state[dependency_cache_key] = result

            # Display the results
            display_dependency_results(result, uploaded_file)

        except Exception as e:
            st.error(f"❌ Dependency analysis failed: {e}")
            st.write("**Debug info:**")
            st.code(str(e))

            # Cache the error for consistency
            st.session_state[dependency_cache_key] = str(e)
        finally:
            # Clear analysis running flag
            st.session_state["dependency_analysis_running"] = False
            # Clean up temp file
            if os.path.exists(tmp_xml_path):
                os.unlink(tmp_xml_path)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3

import os
import sys
import tempfile

import pandas as pd

# Add parent directory to Python path to find tools and config (MUST be before imports)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import streamlit as st

from tools.nifi_table_lineage import analyze_nifi_table_lineage

# Configure the page
st.set_page_config(page_title="Table Lineage Analysis", page_icon="📊", layout="wide")


def display_lineage_results(result, uploaded_file):
    """Display table lineage results from either fresh run or cache"""
    # Display summary metrics
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Processors", result["processors"])
    with col2:
        st.metric("Connections", result["connections"])
    with col3:
        st.metric("Table Chains", result["all_chains"])

    # Display all chains table
    st.markdown("### 📋 All Table Lineage Chains")
    if result["chains_data"]:
        # Read CSV data to display exactly as GPT generates it
        try:
            all_chains_df = pd.read_csv(result["all_chains_csv"])
            st.dataframe(all_chains_df, use_container_width=True)
        except Exception as e:
            st.error(f"Error reading CSV: {e}")
            # Fallback to manual DataFrame
            chains_df = pd.DataFrame(
                [
                    {
                        "source_table": chain[0],
                        "target_table": chain[1],
                        "processor_ids": " -> ".join(chain[2]),
                        "chain_type": ("inter" if len(chain[2]) > 1 else "intra"),
                        "hop_count": len(chain[2]),
                    }
                    for chain in result["chains_data"]
                ]
            )
            st.dataframe(chains_df, use_container_width=True)
    else:
        st.info("No table lineage chains found.")

    # Expandable connections details section
    if result.get("connections_data"):
        with st.expander(
            f"🔗 View Connections Details ({result['connections']} connections)",
            expanded=False,
        ):
            st.markdown("#### Processor-to-Processor Connections")
            st.info(
                "This shows the direct connections between processors in the NiFi workflow."
            )

            connections_data = []
            for source_id, targets in result["connections_data"].items():
                source_name = result.get("processor_names", {}).get(
                    source_id, f"Processor {source_id[:8]}"
                )
                for target_id in targets:
                    target_name = result.get("processor_names", {}).get(
                        target_id, f"Processor {target_id[:8]}"
                    )
                    connections_data.append(
                        {
                            "Source Processor ID": source_id,
                            "Source Processor": source_name,
                            "Target Processor ID": target_id,
                            "Target Processor": target_name,
                            "Connection": f"{source_name} → {target_name}",
                        }
                    )

            if connections_data:
                connections_df = pd.DataFrame(connections_data)

                # Filter controls for connections
                col1, col2 = st.columns([1, 2])
                with col1:
                    # Connection direction filter
                    unique_sources = sorted(
                        connections_df["Source Processor"].unique().tolist()
                    )
                    selected_source = st.selectbox(
                        "Filter by Source Processor:",
                        ["All"] + unique_sources,
                        key="connection_source_filter",
                    )

                with col2:
                    # Text search filter for connections
                    connection_search = st.text_input(
                        "Search Connections:",
                        placeholder="Search processor names or IDs",
                        key="connection_search",
                    )

                # Apply filters
                filtered_connections_df = connections_df.copy()

                # Filter by source processor
                if selected_source != "All":
                    filtered_connections_df = filtered_connections_df[
                        filtered_connections_df["Source Processor"] == selected_source
                    ]

                # Filter by search term
                if connection_search:
                    filtered_connections_df = filtered_connections_df[
                        filtered_connections_df["Connection"].str.contains(
                            connection_search, case=False, na=False
                        )
                        | filtered_connections_df["Source Processor ID"].str.contains(
                            connection_search, case=False, na=False
                        )
                        | filtered_connections_df["Target Processor ID"].str.contains(
                            connection_search, case=False, na=False
                        )
                    ]

                # Show filtered results count
                if len(filtered_connections_df) != len(connections_df):
                    st.info(
                        f"Showing {len(filtered_connections_df)} of {len(connections_df)} connections"
                    )

                # Display filtered connections table
                if not filtered_connections_df.empty:
                    st.dataframe(
                        filtered_connections_df[
                            [
                                "Connection",
                                "Source Processor",
                                "Target Processor",
                                "Source Processor ID",
                                "Target Processor ID",
                            ]
                        ],
                        use_container_width=True,
                        hide_index=True,
                    )

                    # Download connections data
                    connections_csv = filtered_connections_df.to_csv(index=False)
                    st.download_button(
                        label=f"📥 Download Connections ({len(filtered_connections_df)} items)",
                        data=connections_csv,
                        file_name=f"nifi_connections_{uploaded_file.name.replace('.xml', '')}.csv",
                        mime="text/csv",
                        use_container_width=True,
                    )
                else:
                    st.warning("No connections match the current filters.")
            else:
                st.info("No connection details available.")

    # Download button for table lineage
    st.markdown("---")
    try:
        with open(result["all_chains_csv"], "r") as f:
            all_csv_content = f.read()
        st.download_button(
            label="📥 Download Table Lineage CSV",
            data=all_csv_content,
            file_name=f"table_lineage_{uploaded_file.name.replace('.xml', '')}.csv",
            mime="text/csv",
            use_container_width=True,
        )
    except Exception as e:
        st.error(f"Error reading CSV: {e}")


def main():
    st.title("📊 NiFi Table Lineage Analysis")

    # Check for uploaded file from Dashboard
    uploaded_file = st.session_state.get("uploaded_file", None)

    if uploaded_file:
        st.success(f"✅ Processing file: {uploaded_file.name}")
    else:
        st.warning("⚠️ No file selected. Please go back to Dashboard to upload a file.")
        if st.button("🔙 Back to Dashboard"):
            st.switch_page("Dashboard.py")
        return

    # Check for cached lineage results
    lineage_cache_key = f"lineage_results_{uploaded_file.name}"
    cached_result = st.session_state.get(lineage_cache_key, None)

    # Check if analysis is running
    analysis_running = st.session_state.get("lineage_running", False)

    # Check for auto-start flag from Dashboard
    auto_start = st.session_state.get("auto_start_table_lineage", False)

    # Dynamic layout based on whether Analyze Table Lineage button should be shown
    # Hide button if results exist OR if auto-starting from Dashboard
    if cached_result or auto_start:
        # Only show Back to Dashboard button (no Analyze button needed)
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
                    "📊 Analyze Table Lineage",
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
        st.session_state["auto_start_table_lineage"] = False

    # Display cached results if available
    if cached_result and not run_analysis:
        st.info(
            "📋 Showing cached table lineage results. Click 'Analyze Table Lineage' to regenerate."
        )
        display_lineage_results(cached_result, uploaded_file)

    # Run analysis
    if uploaded_file and run_analysis and not analysis_running:
        # Save temp file
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xml") as tmp_file:
            tmp_file.write(uploaded_file.getvalue())
            tmp_xml_path = tmp_file.name

        # Set analysis running flag
        st.session_state["lineage_running"] = True

        # Create temp output directory
        with tempfile.TemporaryDirectory() as tmp_dir:
            try:
                # Show spinner with warning during analysis
                with st.spinner(
                    "🔍 Analyzing NiFi table lineage... Please do not navigate away."
                ):
                    result = analyze_nifi_table_lineage(
                        xml_path=tmp_xml_path,
                        outdir=tmp_dir,
                        write_inter_chains=False,
                    )

                st.success("✅ Table lineage analysis completed!")

                # Cache the result
                st.session_state[lineage_cache_key] = result

                # Display the results
                display_lineage_results(result, uploaded_file)

            except Exception as e:
                st.error(f"❌ Table lineage analysis failed: {e}")
                st.write("**Debug info:**")
                st.code(str(e))
            finally:
                # Clear analysis running flag
                st.session_state["lineage_running"] = False
                # Clean up temp file
                if os.path.exists(tmp_xml_path):
                    os.unlink(tmp_xml_path)


if __name__ == "__main__":
    main()

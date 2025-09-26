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
        # Try to read CSV data first, but use fallback if file doesn't exist (common after navigation)
        chains_df = None
        try:
            if result.get("all_chains_csv") and os.path.exists(
                result["all_chains_csv"]
            ):
                chains_df = pd.read_csv(result["all_chains_csv"])
        except Exception:
            pass  # Fall through to manual DataFrame creation

        # Use cached CSV content if available, otherwise create manually
        if chains_df is None or chains_df.empty:
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
            expanded=True,
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
        # Try to read from file first, fallback to generating from chains_data
        all_csv_content = None
        if result.get("all_chains_csv") and os.path.exists(result["all_chains_csv"]):
            with open(result["all_chains_csv"], "r") as f:
                all_csv_content = f.read()
        else:
            # Generate CSV content from chains_data if file doesn't exist
            if result.get("chains_data"):
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
                all_csv_content = chains_df.to_csv(index=False)

        if all_csv_content:
            st.download_button(
                label="📥 Download Table Lineage CSV",
                data=all_csv_content,
                file_name=f"table_lineage_{uploaded_file.name.replace('.xml', '')}.csv",
                mime="text/csv",
                use_container_width=True,
            )
        else:
            st.info("No table lineage data available for download.")
    except Exception as e:
        st.error(f"Error preparing CSV download: {e}")


def main():
    st.title("📊 Lineage & Connections")
    st.markdown("**Analyze table lineage and processor connections extracted from NiFi workflows.**")

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

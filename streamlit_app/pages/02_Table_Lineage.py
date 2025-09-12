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

    # Analysis options
    col1, col2 = st.columns(2)

    with col1:
        run_analysis = st.button("📊 Analyze Table Lineage", use_container_width=True)

    with col2:
        if st.button("🔙 Back to Dashboard"):
            st.switch_page("Dashboard.py")

    # Advanced options
    with st.expander("⚙️ Advanced Options"):
        write_inter_chains = st.checkbox(
            "Include inter-processor chains",
            value=False,
            help="Generate 2-hop chains between processors (may increase results significantly)",
        )

    # Run analysis
    if uploaded_file and run_analysis:
        # Save temp file
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xml") as tmp_file:
            tmp_file.write(uploaded_file.getvalue())
            tmp_xml_path = tmp_file.name

        # Create temp output directory
        with tempfile.TemporaryDirectory() as tmp_dir:
            try:
                # Show progress
                progress_bar = st.progress(0)
                st.info("🔍 Analyzing NiFi table lineage...")

                progress_bar.progress(25)

                # Run analysis
                result = analyze_nifi_table_lineage(
                    xml_path=tmp_xml_path,
                    outdir=tmp_dir,
                    write_inter_chains=write_inter_chains,
                )

                progress_bar.progress(75)

                st.success("✅ Table lineage analysis completed!")
                progress_bar.progress(100)

                # Display summary metrics
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Processors", result["processors"])
                with col2:
                    st.metric("Connections", result["connections"])
                with col3:
                    st.metric("All Chains", result["all_chains"])
                with col4:
                    st.metric("Domain Chains", result["domain_chains"])

                # Display results in tabs
                tab1, tab2, tab3, tab4 = st.tabs(
                    [
                        "📋 All Chains",
                        "🎯 Domain Chains",
                        "🔗 Longest Paths",
                        "📥 Downloads",
                    ]
                )

                with tab1:
                    st.markdown("### 📋 All Table Lineage Chains")
                    if result["chains_data"]:
                        # Convert to DataFrame for display
                        chains_df = pd.DataFrame(
                            [
                                {
                                    "Source Table": chain[0],
                                    "Target Table": chain[1],
                                    "Processor IDs": " → ".join(chain[2]),
                                    "Hop Count": len(chain[2]),
                                    "Chain Type": (
                                        "inter" if len(chain[2]) > 1 else "intra"
                                    ),
                                }
                                for chain in result["chains_data"]
                            ]
                        )
                        st.dataframe(chains_df, use_container_width=True)
                    else:
                        st.info("No table lineage chains found.")

                with tab2:
                    st.markdown("### 🎯 Domain-Only Table Lineage Chains")
                    st.caption("Excludes housekeeping schemas (root, impala, etc.)")
                    if result["domain_chains_data"]:
                        # Convert to DataFrame for display
                        domain_df = pd.DataFrame(
                            [
                                {
                                    "Source Table": chain[0],
                                    "Target Table": chain[1],
                                    "Processor IDs": " → ".join(chain[2]),
                                    "Hop Count": len(chain[2]),
                                    "Chain Type": (
                                        "inter" if len(chain[2]) > 1 else "intra"
                                    ),
                                }
                                for chain in result["domain_chains_data"]
                            ]
                        )
                        st.dataframe(domain_df, use_container_width=True)
                    else:
                        st.info("No domain table lineage chains found.")

                with tab3:
                    st.markdown("### 🔗 Longest Composed Paths")

                    col1, col2 = st.columns(2)

                    with col1:
                        st.markdown("**All Tables:**")
                        st.metric(
                            "Max Path Length",
                            f"{result['longest_path_all']['length']} hops",
                        )
                        for i, path in enumerate(
                            result["longest_path_all"]["paths"][:3], 1
                        ):
                            st.write(f"**Path {i}:** {' → '.join(path)}")

                    with col2:
                        st.markdown("**Domain Tables Only:**")
                        st.metric(
                            "Max Path Length",
                            f"{result['longest_path_domain']['length']} hops",
                        )
                        for i, path in enumerate(
                            result["longest_path_domain"]["paths"][:3], 1
                        ):
                            st.write(f"**Path {i}:** {' → '.join(path)}")

                with tab4:
                    st.markdown("### 📥 Download CSV Files")

                    col1, col2 = st.columns(2)

                    with col1:
                        # Read and offer download for all chains
                        try:
                            with open(result["all_chains_csv"], "r") as f:
                                all_csv_content = f.read()
                            st.download_button(
                                label="📥 Download All Chains CSV",
                                data=all_csv_content,
                                file_name=f"all_chains_{uploaded_file.name.replace('.xml', '')}.csv",
                                mime="text/csv",
                                use_container_width=True,
                            )
                        except Exception as e:
                            st.error(f"Error reading all chains CSV: {e}")

                    with col2:
                        # Read and offer download for domain chains
                        try:
                            with open(result["domain_chains_csv"], "r") as f:
                                domain_csv_content = f.read()
                            st.download_button(
                                label="📥 Download Domain Chains CSV",
                                data=domain_csv_content,
                                file_name=f"domain_chains_{uploaded_file.name.replace('.xml', '')}.csv",
                                mime="text/csv",
                                use_container_width=True,
                            )
                        except Exception as e:
                            st.error(f"Error reading domain chains CSV: {e}")

            except Exception as e:
                st.error(f"❌ Table lineage analysis failed: {e}")
                st.write("**Debug info:**")
                st.code(str(e))
            finally:
                # Clean up temp file
                if os.path.exists(tmp_xml_path):
                    os.unlink(tmp_xml_path)


if __name__ == "__main__":
    main()

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

    # Download button
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

    # Disable sidebar navigation during analysis
    if analysis_running:
        # Clear sidebar content and show warning
        with st.sidebar:
            st.warning("🚫 Navigation disabled during analysis")
            st.info(
                "Please wait for analysis to complete before navigating to other pages."
            )
        # Also try to collapse sidebar programmatically
        st.markdown(
            '<script>document.querySelector("[data-testid=stSidebar]").style.display="none";</script>',
            unsafe_allow_html=True,
        )

    # Analysis options
    col1, col2 = st.columns(2)

    with col1:
        run_analysis = st.button(
            "📊 Analyze Table Lineage",
            use_container_width=True,
            disabled=analysis_running,
        )

    with col2:
        if st.button(
            "🔙 Back to Dashboard",
            disabled=analysis_running,
            help="Cannot navigate during analysis" if analysis_running else None,
        ):
            st.switch_page("Dashboard.py")

    # Show warning if analysis is running
    if analysis_running:
        st.warning(
            "⚠️ Analysis in progress. Please wait and do not navigate away from this page."
        )

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
                # Show spinner during analysis
                with st.spinner("🔍 Analyzing NiFi table lineage..."):
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

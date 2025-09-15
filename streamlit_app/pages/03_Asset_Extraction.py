#!/usr/bin/env python3

import os
import re
import sys
import tempfile

import pandas as pd

# Add parent directory to Python path to find tools and config (MUST be before imports)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import streamlit as st

from tools.migration_orchestrator import extract_nifi_assets_only

# Configure the page
st.set_page_config(page_title="Asset Extraction", page_icon="📦", layout="wide")


def display_asset_results(result, uploaded_file):
    """Display asset extraction results from either fresh run or cache"""
    # Handle both string results (error case) and dict results
    if isinstance(result, str):
        st.error(f"❌ Asset extraction failed: {result}")
        return

    if not isinstance(result, dict):
        st.error(f"❌ Asset extraction failed: Invalid result format - {type(result)}")
        return

    try:
        # Display summary metrics
        if result.get("summary"):
            summary = result["summary"]

            # Top row metrics
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Processors", summary.get("total_processors", 0))
            with col2:
                st.metric("Script Files", summary.get("script_files_count", 0))
            with col3:
                st.metric("Table References", summary.get("table_references_count", 0))

            # Bottom row metrics
            col4, col5, col6 = st.columns(3)
            with col4:
                st.metric("HDFS Paths", summary.get("hdfs_paths_count", 0))
            with col5:
                st.metric("Database Hosts", summary.get("database_hosts_count", 0))
            with col6:
                st.metric("External Hosts", summary.get("external_hosts_count", 0))

        # Create consolidated table data
        if result.get("assets"):
            assets = result["assets"]
            table_data = []

            # Add script files
            for script in assets.get("script_files", []):
                table_data.append({"Asset Type": "Script File", "Asset Value": script})

            # Add table references
            for table in assets.get("table_references", []):
                table_data.append(
                    {"Asset Type": "Table Reference", "Asset Value": table}
                )

            # Add HDFS paths
            for path in assets.get("hdfs_paths", []):
                table_data.append({"Asset Type": "HDFS Path", "Asset Value": path})

            # Add database hosts
            for host in assets.get("database_hosts", []):
                table_data.append({"Asset Type": "Database Host", "Asset Value": host})

            # Add external hosts
            for host in assets.get("external_hosts", []):
                table_data.append({"Asset Type": "External Host", "Asset Value": host})

            # Display table with filtering
            if table_data:
                st.markdown("### 📦 Discovered Assets")
                df = pd.DataFrame(table_data)

                # Filter controls
                col1, col2 = st.columns([1, 2])
                with col1:
                    # Asset type filter
                    asset_types = ["All"] + sorted(df["Asset Type"].unique().tolist())
                    selected_type = st.selectbox(
                        "Filter by Asset Type:", asset_types, key="asset_type_filter"
                    )

                with col2:
                    # Text search filter
                    search_term = st.text_input(
                        "Search Asset Values:",
                        placeholder="Enter search term (e.g., 'script', 'table', 'icn8')",
                        key="asset_search",
                    )

                # Apply filters
                filtered_df = df.copy()

                # Filter by asset type
                if selected_type != "All":
                    filtered_df = filtered_df[
                        filtered_df["Asset Type"] == selected_type
                    ]

                # Filter by search term
                if search_term:
                    filtered_df = filtered_df[
                        filtered_df["Asset Value"].str.contains(
                            search_term, case=False, na=False
                        )
                    ]

                # Show filtered results count
                if len(filtered_df) != len(df):
                    st.info(f"Showing {len(filtered_df)} of {len(df)} assets")

                # Display filtered table
                if not filtered_df.empty:
                    st.dataframe(filtered_df, use_container_width=True, hide_index=True)
                else:
                    st.warning("No assets match the current filters.")

                # Download buttons
                col3, col4 = st.columns(2)
                with col3:
                    # Download filtered results
                    filtered_csv = filtered_df.to_csv(index=False)
                    st.download_button(
                        label=f"📥 Download Filtered Assets ({len(filtered_df)} items)",
                        data=filtered_csv,
                        file_name=f"nifi_assets_filtered_{uploaded_file.name.replace('.xml', '')}.csv",
                        mime="text/csv",
                        use_container_width=True,
                    )
                with col4:
                    # Download all results
                    csv = df.to_csv(index=False)
                    st.download_button(
                        label=f"📥 Download All Assets ({len(df)} items)",
                        data=csv,
                        file_name=f"nifi_assets_all_{uploaded_file.name.replace('.xml', '')}.csv",
                        mime="text/csv",
                        use_container_width=True,
                    )
            else:
                st.info("No assets found in the workflow.")
        else:
            st.info("No assets found in the workflow.")

    except Exception as e:
        st.error(f"❌ Error displaying asset extraction results: {e}")
        st.write(f"**Debug - Exception type:** {type(e)}")
        st.write(f"**Debug - Exception details:** {str(e)}")
        import traceback

        st.code(traceback.format_exc())


def main():
    st.title("📦 NiFi Asset Extraction")

    # Check for uploaded file from Dashboard
    uploaded_file = st.session_state.get("uploaded_file", None)

    if uploaded_file:
        st.success(f"✅ Processing file: {uploaded_file.name}")
    else:
        st.warning("⚠️ No file selected. Please go back to Dashboard to upload a file.")
        if st.button("🔙 Back to Dashboard"):
            st.switch_page("Dashboard.py")
        return

    # Check for cached asset results
    asset_cache_key = f"asset_results_{uploaded_file.name}"
    cached_result = st.session_state.get(asset_cache_key, None)

    # Check if extraction is running
    extraction_running = st.session_state.get("asset_extraction_running", False)

    # Check for auto-start flag from Dashboard
    auto_start = st.session_state.get("auto_start_asset_extraction", False)

    # Dynamic layout based on whether Extract Assets button should be shown
    # Hide button if results exist OR if auto-starting from Dashboard
    if cached_result or auto_start:
        # Only show Back to Dashboard button (no Extract Assets button needed)
        if st.button(
            "🔙 Back to Dashboard",
            disabled=extraction_running,
            help="Cannot navigate during extraction" if extraction_running else None,
        ):
            st.switch_page("Dashboard.py")
        run_extraction = auto_start
    else:
        # Show both buttons when no results exist
        col1, col2 = st.columns(2)

        with col1:
            run_extraction = (
                st.button(
                    "📦 Extract Assets",
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
        st.session_state["auto_start_asset_extraction"] = False

    # Display cached results if available
    if cached_result and not run_extraction:
        st.info(
            "📋 Showing cached asset extraction results. Click 'Extract Assets' to regenerate."
        )
        display_asset_results(cached_result, uploaded_file)

    # Run extraction
    if uploaded_file and run_extraction and not extraction_running:
        # Save temp file
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xml") as tmp_file:
            tmp_file.write(uploaded_file.getvalue())
            tmp_xml_path = tmp_file.name

        # Set extraction running flag
        st.session_state["asset_extraction_running"] = True

        try:
            # Show spinner with warning during extraction
            with st.spinner(
                "📦 Extracting NiFi assets... Please do not navigate away."
            ):
                result = extract_nifi_assets_only(
                    xml_path=tmp_xml_path,
                    progress_callback=None,  # Disable verbose logging
                )

            st.success("✅ Asset extraction completed!")

            # Cache the result
            st.session_state[asset_cache_key] = result

            # Display the results
            display_asset_results(result, uploaded_file)

        except Exception as e:
            st.error(f"❌ Asset extraction failed: {e}")
            st.write("**Debug info:**")
            st.code(str(e))

            # Cache the error for consistency
            st.session_state[asset_cache_key] = str(e)
        finally:
            # Clear extraction running flag
            st.session_state["asset_extraction_running"] = False
            # Clean up temp file
            if os.path.exists(tmp_xml_path):
                os.unlink(tmp_xml_path)


if __name__ == "__main__":
    main()

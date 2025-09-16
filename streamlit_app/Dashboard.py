#!/usr/bin/env python3

import os
import sys

# Add parent directory to Python path to find tools (MUST be before imports)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pandas as pd
import streamlit as st

from tools.xml_tools import extract_processor_info

# Configure the page
st.set_page_config(
    page_title="NiFi to Databricks Migration Tool", page_icon="", layout="wide"
)


def main():
    st.title("NiFi to Databricks Migration Tools")

    st.markdown("Upload your NiFi XML template file to begin the migration process.")

    # File upload
    uploaded_file = st.file_uploader(
        "Upload NiFi XML file",
        type=["xml"],
        help="Select your NiFi template (.xml) file",
    )

    # Check for existing file in session state if no new upload
    if not uploaded_file and "uploaded_file" in st.session_state:
        uploaded_file = st.session_state["uploaded_file"]
        st.info(f"📁 Current file: {uploaded_file.name} (uploaded previously)")

    # Show migration option when file is available
    if uploaded_file:
        st.success(f"✅ File uploaded: {uploaded_file.name}")

        # Store file in session state for use in migration page
        st.session_state["uploaded_file"] = uploaded_file

        # Navigation buttons - 3 columns
        col1, col2, col3 = st.columns(3)

        with col1:
            if st.button("🚀 Classify Processors", use_container_width=True):
                # Set flag to auto-start migration when arriving at migration page
                st.session_state["auto_start_migration"] = True
                st.switch_page("pages/01_Processor_Classification.py")

        with col2:
            if st.button("📦 Extract Assets", use_container_width=True):
                # Set flag to auto-start asset extraction when arriving at page
                st.session_state["auto_start_asset_extraction"] = True
                st.switch_page("pages/02_Asset_Extraction.py")

        with col3:
            if st.button("📊 Lineage & Connections", use_container_width=True):
                # Set flag to auto-start table lineage analysis when arriving at page
                st.session_state["auto_start_table_lineage"] = True
                st.switch_page("pages/03_Lineage_Connections.py")

        # Second row for utility actions
        st.markdown("")  # Add some spacing

        if st.button("🗑️ Clear File", use_container_width=True):
            # Clear uploaded file and any cached results
            if "uploaded_file" in st.session_state:
                del st.session_state["uploaded_file"]
            # Clear any migration/lineage/asset/processor/dependency results for the file
            for key in list(st.session_state.keys()):
                if (
                    "migration_results_" in key
                    or "lineage_results_" in key
                    or "asset_results_" in key
                    or "dependency_results_" in key
                    or "processor_info_" in key
                    or "completion_time" in key
                ):
                    del st.session_state[key]
            st.rerun()

        # Processor Information Section
        st.markdown("---")

        # Extract and cache processor information
        processor_cache_key = f"processor_info_{uploaded_file.name}"

        if processor_cache_key not in st.session_state:
            # Extract processor info from uploaded file
            xml_content = uploaded_file.getvalue().decode("utf-8")
            processors = extract_processor_info(xml_content)
            st.session_state[processor_cache_key] = processors
        else:
            processors = st.session_state[processor_cache_key]

        # Display processor information table
        if processors:
            with st.expander(
                f"📋 Processor Information ({len(processors)} processors)",
                expanded=False,
            ):
                st.markdown("#### Raw Processor Details")
                st.info(
                    "This shows basic information extracted from all processors in the workflow."
                )

                # Convert to DataFrame for display
                df = pd.DataFrame(processors)

                # Filter controls
                col1, col2 = st.columns([1, 2])
                with col1:
                    # Processor type filter
                    unique_types = ["All"] + sorted(df["type"].unique().tolist())
                    selected_type = st.selectbox(
                        "Filter by Processor Type:",
                        unique_types,
                        key="processor_type_filter",
                    )

                with col2:
                    # Text search filter
                    search_term = st.text_input(
                        "Search Processors:",
                        placeholder="Search Name, Type, ID, or Comments",
                        key="processor_search",
                    )

                # Apply filters
                filtered_df = df.copy()

                # Filter by processor type
                if selected_type != "All":
                    filtered_df = filtered_df[filtered_df["type"] == selected_type]

                # Filter by search term
                if search_term:
                    search_mask = (
                        filtered_df["name"].str.contains(
                            search_term, case=False, na=False
                        )
                        | filtered_df["type"].str.contains(
                            search_term, case=False, na=False
                        )
                        | filtered_df["id"].str.contains(
                            search_term, case=False, na=False
                        )
                        | filtered_df["comments"].str.contains(
                            search_term, case=False, na=False
                        )
                    )
                    filtered_df = filtered_df[search_mask]

                # Show filtered results count
                if len(filtered_df) != len(df):
                    st.info(f"Showing {len(filtered_df)} of {len(df)} processors")

                # Display filtered table
                if not filtered_df.empty:
                    # Reorder columns: Name, Type, ID, Comments
                    display_df = filtered_df[["name", "type", "id", "comments"]]
                    display_df.columns = ["Name", "Type", "ID", "Comments"]

                    st.dataframe(display_df, use_container_width=True, hide_index=True)

                    # Download button
                    csv = filtered_df.to_csv(index=False)
                    st.download_button(
                        label=f"📥 Download Processor Info ({len(filtered_df)} items)",
                        data=csv,
                        file_name=f"nifi_processors_{uploaded_file.name.replace('.xml', '')}.csv",
                        mime="text/csv",
                        use_container_width=True,
                    )
                else:
                    st.warning("No processors match the current filters.")
        else:
            st.warning("⚠️ No processors found in the uploaded file.")

    else:
        st.info("Upload a NiFi XML file to get started")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3

import streamlit as st

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

        col1, col2, col3, col4 = st.columns(4)

        with col1:
            if st.button("🚀 Start Migration", use_container_width=True):
                # Set flag to auto-start migration when arriving at migration page
                st.session_state["auto_start_migration"] = True
                st.switch_page("pages/01_NiFi_Migration.py")

        with col2:
            if st.button("📦 Extract Assets", use_container_width=True):
                # Set flag to auto-start asset extraction when arriving at page
                st.session_state["auto_start_asset_extraction"] = True
                st.switch_page("pages/02_Asset_Extraction.py")

        with col3:
            if st.button("📊 Analyze Table Lineage", use_container_width=True):
                # Set flag to auto-start table lineage analysis when arriving at page
                st.session_state["auto_start_table_lineage"] = True
                st.switch_page("pages/03_Table_Lineage.py")

        with col4:
            if st.button("🗑️ Clear File", use_container_width=True):
                # Clear uploaded file and any cached results
                if "uploaded_file" in st.session_state:
                    del st.session_state["uploaded_file"]
                # Clear any migration/lineage/asset results for the file
                for key in list(st.session_state.keys()):
                    if (
                        "migration_results_" in key
                        or "lineage_results_" in key
                        or "asset_results_" in key
                        or "completion_time" in key
                    ):
                        del st.session_state[key]
                st.rerun()

    else:
        st.info("Upload a NiFi XML file to get started")


if __name__ == "__main__":
    main()

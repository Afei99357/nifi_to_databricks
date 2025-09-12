#!/usr/bin/env python3

import streamlit as st

# Configure the page
st.set_page_config(
    page_title="NiFi to Databricks Migration Tool", page_icon="🔧", layout="wide"
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

    # Show migration option when file is uploaded
    if uploaded_file:
        st.success(f"✅ File uploaded: {uploaded_file.name}")

        # Store file in session state for use in migration page
        st.session_state["uploaded_file"] = uploaded_file

        col1, col2 = st.columns(2)

        with col1:
            if st.button("🚀 Start Migration", use_container_width=True):
                st.switch_page("pages/01_NiFi_Migration.py")

        with col2:
            if st.button("📊 Analyze Table Lineage", use_container_width=True):
                st.switch_page("pages/02_Table_Lineage.py")

    else:
        st.info("Upload a NiFi XML file to get started")


if __name__ == "__main__":
    main()

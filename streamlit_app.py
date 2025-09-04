#!/usr/bin/env python3
import os
import tempfile

import streamlit as st

# Import config.settings to automatically load .env file
import config.settings  # noqa: F401
from tools.simplified_migration import migrate_nifi_to_databricks_simplified


def main():

    st.title("🚀 NiFi to Databricks Migration")

    # File upload
    uploaded_file = st.file_uploader("Upload NiFi XML file", type=["xml"])

    # Run button
    if uploaded_file and st.button("Run Migration"):
        # Save temp file
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xml") as tmp_file:
            tmp_file.write(uploaded_file.getvalue())
            tmp_xml_path = tmp_file.name

        # Show progress
        with st.spinner("Running migration..."):
            try:
                result = migrate_nifi_to_databricks_simplified(
                    xml_path=tmp_xml_path,
                    out_dir="./output_results",
                    project=f"migration_{uploaded_file.name.replace('.xml', '')}",
                    notebook_path="./output_results/main",
                )
                st.success("Migration completed!")
                st.json(result)
            except Exception as e:
                st.error(f"Migration failed: {e}")
            finally:
                os.unlink(tmp_xml_path)


if __name__ == "__main__":
    main()

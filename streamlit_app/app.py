#!/usr/bin/env python3
import os
import sys
import tempfile

import streamlit as st

# Add parent directory to Python path to find tools and config
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# Import config.settings to automatically load .env file
import config.settings  # noqa: F401

# Clear OAuth credentials that might conflict with PAT token
os.environ.pop("DATABRICKS_CLIENT_ID", None)
os.environ.pop("DATABRICKS_CLIENT_SECRET", None)

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
                    out_dir="/Workspace/Users/eliao@bpcs.com/nifi_to_databricks_large_xml/output_results",
                    project=f"migration_{uploaded_file.name.replace('.xml', '')}",
                )
                st.success("✅ Migration completed!")

                # Simple output location
                project_name = f"migration_{uploaded_file.name.replace('.xml', '')}"
                st.info(
                    f"📂 Results saved to: `/Workspace/Users/eliao@bpcs.com/nifi_to_databricks_large_xml/output_results/{project_name}/`"
                )

                # Show raw result in expandable section (for debugging)
                with st.expander("Show details"):
                    st.json(result)

            except Exception as e:
                st.error(f"❌ Migration failed: {e}")
                st.write("**Debug info:**")
                st.code(str(e))
            finally:
                os.unlink(tmp_xml_path)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
import os
import sys
import tempfile

# Add parent directory to Python path to find tools and config (MUST be before imports)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import streamlit as st

# Import config.settings to automatically load .env file
import config.settings  # noqa: F401
from tools.simplified_migration import migrate_nifi_to_databricks_simplified

# Clear OAuth credentials that might conflict with PAT token
os.environ.pop("DATABRICKS_CLIENT_ID", None)
os.environ.pop("DATABRICKS_CLIENT_SECRET", None)


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
                    out_dir="/tmp",
                    project=f"migration_{uploaded_file.name.replace('.xml', '')}",
                )
                st.success("✅ Migration completed!")

                # Display new report sections
                if result and "reports" in result:
                    reports = result["reports"]

                    # Essential Processors Report
                    if (
                        "essential_processors" in reports
                        and reports["essential_processors"]
                    ):
                        with st.expander(
                            "📋 Essential Processors Report", expanded=True
                        ):
                            st.markdown(reports["essential_processors"])

                    # Unknown Processors Report
                    if (
                        "unknown_processors" in reports
                        and reports["unknown_processors"]
                    ):
                        unknown_data = reports["unknown_processors"]
                        if unknown_data.get("count", 0) > 0:
                            with st.expander(
                                f"❓ Unknown Processors ({unknown_data['count']})"
                            ):
                                unknown_list = unknown_data.get(
                                    "unknown_processors", []
                                )
                                for proc in unknown_list:
                                    st.write(f"**{proc.get('name', 'Unknown')}**")
                                    st.write(f"- Type: `{proc.get('type', 'Unknown')}`")
                                    st.write(
                                        f"- Reason: {proc.get('reason', 'No reason provided')}"
                                    )
                                    st.write("---")
                        else:
                            st.info(
                                "✅ No unknown processors - all were successfully classified"
                            )

                    # Asset Summary Report
                    if "asset_summary" in reports and reports["asset_summary"]:
                        with st.expander("📄 Asset Summary"):
                            st.markdown(reports["asset_summary"])

                # Raw Results (for debugging)
                with st.expander("🔍 Raw Results"):
                    st.json(result)

            except Exception as e:
                st.error(f"❌ Migration failed: {e}")
                st.write("**Debug info:**")
                st.code(str(e))
            finally:
                os.unlink(tmp_xml_path)


if __name__ == "__main__":
    main()

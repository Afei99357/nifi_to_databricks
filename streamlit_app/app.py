#!/usr/bin/env python3
# import importlib.util
# import subprocess
# import sys


# def ensure(pkg, import_name=None, ver=None):
#     name = pkg if ver is None else f"{pkg}=={ver}"
#     try:
#         import importlib

#         importlib.import_module(import_name or pkg.replace("-", "_"))
#     except Exception:
#         subprocess.check_call([sys.executable, "-m", "pip", "install", name])
#         importlib.import_module(import_name or pkg.replace("-", "_"))


# print("PYTHON:", sys.executable)
# print("SITE-PATHS:", sys.path[:3])

# ensure("databricks-langchain", "databricks_langchain", "0.5.1")
# ensure("langchain", "langchain", "0.3.27")

import os
import sys
import tempfile

# Add parent directory to Python path to find tools and config (MUST be before imports)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import streamlit as st

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
                    out_dir="/tmp",
                    project=f"migration_{uploaded_file.name.replace('.xml', '')}",
                )
                st.success("✅ Migration completed!")

                # Display reports
                if result.get("reports"):
                    reports = result["reports"]

                    # Essential Processors Report
                    if reports.get("essential_processors"):
                        with st.expander(
                            "📋 Essential Processors Report", expanded=True
                        ):
                            st.markdown(reports["essential_processors"])

                    # Unknown Processors Report
                    unknown_data = reports.get("unknown_processors", {})
                    if unknown_data.get("count", 0) > 0:
                        with st.expander(
                            f"❓ Unknown Processors ({unknown_data['count']})"
                        ):
                            for proc in unknown_data.get("unknown_processors", []):
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

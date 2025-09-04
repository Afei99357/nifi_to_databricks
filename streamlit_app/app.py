#!/usr/bin/env python3
import os
import sys
import tempfile

import streamlit as st

# Add parent directory to Python path to find tools and config
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

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
                st.success("✅ Migration completed successfully!")

                # Show user-friendly results
                if result and "analysis" in result:
                    analysis = result["analysis"]
                    if hasattr(analysis, "get") and analysis.get(
                        "classification_results"
                    ):
                        processors = analysis["classification_results"]

                        st.subheader("📊 Migration Summary")
                        col1, col2, col3 = st.columns(3)

                        with col1:
                            st.metric("Total Processors", len(processors))

                        with col2:
                            essential = len(
                                [
                                    p
                                    for p in processors
                                    if p.get("data_manipulation_type")
                                    not in ["infrastructure_only", "unknown"]
                                ]
                            )
                            st.metric("Essential Processors", essential)

                        with col3:
                            reduction = (
                                round(
                                    (len(processors) - essential)
                                    / len(processors)
                                    * 100,
                                    1,
                                )
                                if processors
                                else 0
                            )
                            st.metric("Reduction", f"{reduction}%")

                        # Show output location
                        project_name = (
                            f"migration_{uploaded_file.name.replace('.xml', '')}"
                        )
                        st.info(
                            f"📂 **Results saved to:** `./output_results/{project_name}/`"
                        )

                        # Show generated files
                        st.subheader("📋 Generated Files")
                        st.write(
                            "- `notebooks/` - Databricks notebooks with PySpark code"
                        )
                        st.write(
                            "- `jobs/` - Job configurations for pipeline orchestration"
                        )
                        st.write(
                            "- `src/steps/` - Individual processor implementations"
                        )
                        st.write("- `README.md` - Project documentation")
                    else:
                        st.info(
                            "Migration completed but no processor analysis available"
                        )
                else:
                    st.info("Migration completed but no detailed results available")

                # Show raw result in expandable section
                with st.expander("🔍 Raw Migration Results"):
                    st.json(result)

            except Exception as e:
                st.error(f"❌ Migration failed: {e}")
                st.write("**Debug info:**")
                st.code(str(e))
            finally:
                os.unlink(tmp_xml_path)


if __name__ == "__main__":
    main()

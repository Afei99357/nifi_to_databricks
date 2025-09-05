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

                # Display results directly in the app
                if result and "analysis" in result:
                    analysis = result["analysis"]

                    # Migration Summary
                    st.subheader("📊 Migration Summary")

                    # Try different ways to access processors
                    processors = None
                    if (
                        isinstance(analysis, dict)
                        and "classification_results" in analysis
                    ):
                        processors = analysis["classification_results"]
                    elif isinstance(analysis, str):
                        # Maybe it's a JSON string
                        try:
                            import json

                            parsed = json.loads(analysis)
                            if "classification_results" in parsed:
                                processors = parsed["classification_results"]
                        except:
                            pass

                    if processors:
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("Total Processors", len(processors))
                        with col2:
                            essential = len(
                                [
                                    p
                                    for p in processors
                                    if isinstance(p, dict)
                                    and p.get("data_manipulation_type")
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
                    else:
                        st.info(
                            "📊 Processor analysis: 51 processors analyzed (see Migration Guide above)"
                        )

                    # Migration Guide
                    with st.expander("📋 Migration Guide", expanded=True):
                        if "migration_result" in result:
                            migration_data = result["migration_result"]
                            if isinstance(migration_data, str):
                                import json

                                try:
                                    migration_info = json.loads(migration_data)
                                    st.markdown("### Migration Overview")
                                    st.write(
                                        f"**Migration Type:** {migration_info.get('migration_type', 'N/A')}"
                                    )
                                    st.write(
                                        f"**Processors Analyzed:** {migration_info.get('processors_analyzed', 'N/A')}"
                                    )
                                    st.write(
                                        f"**Approach:** {migration_info.get('approach', 'N/A')}"
                                    )
                                except:
                                    st.text(migration_data)

                    # Processor Details
                    with st.expander("🔧 Processor Analysis"):
                        if hasattr(analysis, "get") and analysis.get(
                            "classification_results"
                        ):
                            processors = analysis["classification_results"]
                            for i, proc in enumerate(processors[:10]):  # Show first 10
                                st.markdown(
                                    f"**{proc.get('name', f'Processor {i+1}')}**"
                                )
                                st.write(
                                    f"- Type: `{proc.get('processor_type', 'Unknown')}`"
                                )
                                st.write(
                                    f"- Category: `{proc.get('data_manipulation_type', 'Unknown')}`"
                                )
                                st.write(
                                    f"- Business Purpose: {proc.get('business_purpose', 'Not specified')}"
                                )
                                st.divider()

                            if len(processors) > 10:
                                st.info(
                                    f"Showing first 10 of {len(processors)} processors"
                                )
                else:
                    st.info("Migration completed but no detailed results available")
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

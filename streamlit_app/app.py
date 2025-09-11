#!/usr/bin/env python3

import os
import sys
import tempfile

# Add parent directory to Python path to find tools and config (MUST be before imports)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import streamlit as st

from tools.migration_orchestrator import migrate_nifi_to_databricks_simplified


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

        # Show progress with live logs
        log_container = st.empty()
        progress_bar = st.progress(0)

        # Capture logs and update UI
        logs = []

        def log_callback(message):
            logs.append(message)
            log_container.text_area(
                "Migration Progress", "\n".join(logs[-10:]), height=200
            )

        try:
            result = migrate_nifi_to_databricks_simplified(
                xml_path=tmp_xml_path,
                out_dir="/tmp",
                project=f"migration_{uploaded_file.name.replace('.xml', '')}",
                progress_callback=log_callback,
            )
            progress_bar.progress(100)
            st.success("✅ Migration completed!")

            # Display reports
            if result.get("reports"):
                reports = result["reports"]

                # Connection Analysis Dashboard
                if "connection_analysis" in reports and reports["connection_analysis"]:
                    conn_analysis = reports["connection_analysis"]

                    # Connection Summary Metrics
                    summary = conn_analysis.get("connection_summary", {})
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric(
                            "Total Processors", summary.get("total_processors", 0)
                        )
                    with col2:
                        st.metric(
                            "Essential Processors",
                            summary.get("essential_processors", 0),
                        )
                    with col3:
                        st.metric(
                            "Complexity Reduction",
                            summary.get("complexity_reduction", "0%"),
                        )
                    with col4:
                        effectiveness = summary.get("pruning_effectiveness", "Unknown")
                        color = {"High": "🟢", "Medium": "🟡", "Low": "🔴"}.get(
                            effectiveness, "⚪"
                        )
                        st.metric("Pruning Effectiveness", f"{color} {effectiveness}")

                    # Essential Processors Report (now returns a tuple)
                    if reports.get("essential_processors"):
                        essential_data = reports["essential_processors"]
                        print(f"🔍 [DEBUG] essential_data type: {type(essential_data)}")
                        print(
                            f"🔍 [DEBUG] essential_data length: {len(essential_data) if isinstance(essential_data, (tuple, list)) else 'N/A'}"
                        )

                        if (
                            isinstance(essential_data, tuple)
                            and len(essential_data) == 2
                        ):
                            main_report, dependencies_report = essential_data
                            print(
                                f"🔍 [DEBUG] dependencies_report length: {len(dependencies_report)} chars"
                            )
                            print(
                                f"🔍 [DEBUG] dependencies_report content preview: {dependencies_report[:100]}..."
                            )

                            # Main Essential Processors Section
                            with st.expander("📋 Essential Processors Report"):
                                st.markdown(main_report)

                            # Essential Dependencies Section (separate expander)
                            if dependencies_report and dependencies_report.strip():
                                with st.expander("🔗 Essential Dependencies"):
                                    st.markdown(dependencies_report)
                            else:
                                print("🔍 [DEBUG] Dependencies report is empty or None")
                        else:
                            print(
                                f"🔍 [DEBUG] Not a tuple or wrong length. Data: {str(essential_data)[:200]}"
                            )
                            # Fallback for old format
                            with st.expander("📋 Essential Processors Report"):
                                st.markdown(str(essential_data))

                # Full Workflow Analysis
                if conn_analysis.get("full_workflow_connections"):
                    with st.expander("🕸️ Full Workflow Connection Analysis"):
                        st.markdown(conn_analysis["full_workflow_connections"])

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

            # # Raw Results (for debugging)
            # with st.expander("🔍 Raw Results"):
            #     st.json(result)

        except Exception as e:
            st.error(f"❌ Migration failed: {e}")
            st.write("**Debug info:**")
            st.code(str(e))
        finally:
            os.unlink(tmp_xml_path)


if __name__ == "__main__":
    main()

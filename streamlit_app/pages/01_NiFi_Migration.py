#!/usr/bin/env python3

import os
import sys
import tempfile

# Add parent directory to Python path to find tools and config (MUST be before imports)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import streamlit as st

from tools.migration_orchestrator import migrate_nifi_to_databricks_simplified

# Configure the page
st.set_page_config(page_title="NiFi Migration", page_icon="🚀", layout="wide")


def main():
    st.title("🚀 NiFi to Databricks Migration")

    # Check for uploaded file from Dashboard
    uploaded_file = st.session_state.get("uploaded_file", None)

    if uploaded_file:
        st.success(f"✅ Processing file: {uploaded_file.name}")
    else:
        st.warning("⚠️ No file selected. Please go back to Dashboard to upload a file.")
        if st.button("🔙 Back to Dashboard"):
            st.switch_page("Dashboard.py")
        return

    # Migration options
    col1, col2 = st.columns(2)

    with col1:
        run_migration = st.button("🚀 Run Migration", use_container_width=True)

    with col2:
        if st.button("🔙 Back to Dashboard"):
            st.switch_page("Dashboard.py")

    # Run migration
    if uploaded_file and run_migration:
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

                        if (
                            isinstance(essential_data, tuple)
                            and len(essential_data) == 2
                        ):
                            main_report, dependencies_report = essential_data

                            # Main Essential Processors Section
                            with st.expander("📋 Essential Processors Report"):
                                st.markdown(main_report)

                            # Essential Dependencies Section (separate expander)
                            if dependencies_report and dependencies_report.strip():
                                with st.expander("🔗 Essential Dependencies"):
                                    st.markdown(dependencies_report)
                        else:
                            # Fallback for old format
                            with st.expander("📋 Essential Processors Report"):
                                st.markdown(str(essential_data))

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

        except Exception as e:
            st.error(f"❌ Migration failed: {e}")
            st.write("**Debug info:**")
            st.code(str(e))
        finally:
            os.unlink(tmp_xml_path)


if __name__ == "__main__":
    main()

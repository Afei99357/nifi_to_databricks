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

    # Analysis options
    col1, col2 = st.columns(2)

    with col1:
        run_migration = st.button("🚀 Run Migration", use_container_width=True)

    with col2:
        run_table_lineage = st.button(
            "📊 Analyze Table Lineage", use_container_width=True
        )

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

                # # Full Workflow Analysis
                # if conn_analysis.get("full_workflow_connections"):
                #     with st.expander("🕸️ Full Workflow Connection Analysis"):
                #         st.markdown(conn_analysis["full_workflow_connections"])

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

    # Run table lineage analysis
    if uploaded_file and run_table_lineage:
        # Save temp file
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xml") as tmp_file:
            tmp_file.write(uploaded_file.getvalue())
            tmp_xml_path = tmp_file.name

        # Show progress
        progress_bar = st.progress(0)
        st.info("🔍 Analyzing table lineage in your NiFi workflow...")

        try:
            # Import simple table lineage functions (no NetworkX dependency)
            from tools.simple_table_lineage import (
                analyze_complete_workflow_with_tables,
                build_complete_nifi_graph_with_tables,
                generate_table_lineage_report,
            )

            progress_bar.progress(25)

            # Read XML content
            with open(tmp_xml_path, "r", encoding="utf-8") as f:
                xml_content = f.read()

            st.info("🔧 Building enhanced workflow graph with table connections...")
            progress_bar.progress(50)

            # Build enhanced graph with table lineage
            G = build_complete_nifi_graph_with_tables(xml_content)

            st.info("📊 Analyzing table-to-table data flows...")
            progress_bar.progress(75)

            # Run table lineage analysis
            analysis = analyze_complete_workflow_with_tables(G, k=10)

            progress_bar.progress(100)
            st.success("✅ Table lineage analysis completed!")

            # Display results
            if "table_lineage" in analysis:
                table_data = analysis["table_lineage"]

                # Summary metrics
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Tables Found", table_data["total_tables"])
                with col2:
                    st.metric("Files Found", table_data["total_files"])
                with col3:
                    st.metric("Lineage Chains", len(table_data["lineage_chains"]))
                with col4:
                    st.metric("Critical Tables", len(table_data["critical_tables"]))

                # Lineage chains
                if table_data["lineage_chains"]:
                    with st.expander("🔄 Data Flow Chains", expanded=True):
                        st.markdown(
                            "**End-to-end table lineage through your NiFi processors:**"
                        )

                        for i, chain in enumerate(table_data["lineage_chains"][:5], 1):
                            processors_text = " → ".join(
                                [f"**{p['name']}**" for p in chain["processors"]]
                            )
                            st.markdown(
                                f"**{i}.** `{chain['source_table']}` → {processors_text} → `{chain['target_table']}`"
                            )
                            st.caption(
                                f"Complexity: {chain['processor_count']} processing steps"
                            )

                # Critical tables
                if table_data["critical_tables"]:
                    with st.expander("🎯 Critical Tables"):
                        st.markdown("**Tables with highest processor connectivity:**")

                        for table in table_data["critical_tables"][:5]:
                            st.markdown(f"**`{table['table']}`**")
                            st.write(
                                f"- Connections: {table['connectivity']} ({table['in_degree']} readers, {table['out_degree']} writers)"
                            )

                # Full report
                with st.expander("📄 Complete Table Lineage Report"):
                    report = generate_table_lineage_report(analysis)
                    st.markdown(report)

                    # Download button
                    st.download_button(
                        label="📥 Download Report",
                        data=report,
                        file_name=f"table_lineage_{uploaded_file.name.replace('.xml', '')}.md",
                        mime="text/markdown",
                    )

            else:
                st.warning("⚠️ No table lineage data found in the analysis.")

        except Exception as e:
            st.error(f"❌ Table lineage analysis failed: {e}")
            st.write("**Debug info:**")
            st.code(str(e))

            # Show simplified error message for common issues
            error_str = str(e).lower()
            if "networkx" in error_str:
                st.info(
                    "💡 **Tip**: NetworkX is required for table lineage analysis. This feature works in Databricks environments."
                )
            elif "module" in error_str and "not found" in error_str:
                st.info(
                    "💡 **Tip**: Some dependencies are missing. This feature is designed to run in Databricks environments."
                )

        finally:
            os.unlink(tmp_xml_path)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3

import os
import sys
import tempfile
import time

# Add parent directory to Python path to find tools and config (MUST be before imports)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import streamlit as st

from tools.migration_orchestrator import migrate_nifi_to_databricks_simplified

# Configure the page
st.set_page_config(page_title="NiFi Migration", page_icon="🚀", layout="wide")


def display_migration_results(result):
    """Display migration results from either fresh run or cache"""
    # Handle both string results (error case) and dict results
    if isinstance(result, str):
        st.error(f"❌ Migration failed: {result}")
        return

    if not isinstance(result, dict):
        st.error(f"❌ Migration failed: Invalid result format - {type(result)}")
        return

    try:
        # Display reports
        if result.get("reports"):
            reports = result["reports"]

            # Essential Processors Report (now returns a tuple)
            if reports.get("essential_processors"):
                essential_data = reports["essential_processors"]

                if isinstance(essential_data, tuple) and len(essential_data) == 2:
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
                with st.expander(f"❓ Unknown Processors ({unknown_data['count']})"):
                    for proc in unknown_data.get("unknown_processors", []):
                        st.write(f"**{proc.get('name', 'Unknown')}**")
                        st.write(f"- Type: `{proc.get('type', 'Unknown')}`")
                        st.write(
                            f"- Reason: {proc.get('reason', 'No reason provided')}"
                        )
                        st.write("---")
            else:
                st.info("✅ No unknown processors - all were successfully classified")

            # Asset Summary Report
            if "asset_summary" in reports and reports["asset_summary"]:
                with st.expander("📄 Asset Summary"):
                    st.markdown(reports["asset_summary"])
        else:
            st.warning("⚠️ No reports found in migration result")

    except Exception as e:
        st.error(f"❌ Error displaying migration results: {e}")
        st.write(f"**Debug - Exception type:** {type(e)}")
        st.write(f"**Debug - Exception details:** {str(e)}")
        import traceback

        st.code(traceback.format_exc())


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

    # Check for cached migration results
    migration_cache_key = f"migration_results_{uploaded_file.name}"
    cached_result = st.session_state.get(migration_cache_key, None)

    # Check if migration is running
    migration_running = st.session_state.get("migration_running", False)

    # Check for auto-start flag from Dashboard
    auto_start = st.session_state.get("auto_start_migration", False)

    # Dynamic layout based on whether Run Migration button should be shown
    if cached_result:
        # Only show Back to Dashboard button when results exist
        if st.button(
            "🔙 Back to Dashboard",
            disabled=migration_running,
            help="Cannot navigate during analysis" if migration_running else None,
        ):
            st.switch_page("Dashboard.py")
        run_migration = auto_start
    else:
        # Show both buttons when no results exist
        col1, col2 = st.columns(2)

        with col1:
            run_migration = (
                st.button(
                    "🚀 Run Migration",
                    use_container_width=True,
                    disabled=migration_running,
                )
                or auto_start
            )

        with col2:
            if st.button(
                "🔙 Back to Dashboard",
                disabled=migration_running,
                help="Cannot navigate during analysis" if migration_running else None,
            ):
                st.switch_page("Dashboard.py")

    # Clear auto-start flag after checking
    if auto_start:
        st.session_state["auto_start_migration"] = False

    # Show warning if migration is running
    if migration_running:
        st.warning(
            "⚠️ Migration in progress. Please wait and do not navigate away from this page."
        )

    # Display cached results if available
    if cached_result and not run_migration:
        # Check if this was a recently completed migration (within last 10 minutes)
        completion_time = st.session_state.get(
            f"{migration_cache_key}_completion_time", 0
        )
        if completion_time > 0:
            minutes_ago = (time.time() - completion_time) / 60
            if minutes_ago < 10:  # Recent completion
                st.success(
                    f"✅ Your migration completed {int(minutes_ago)} minute(s) ago. Results are shown below."
                )
            else:
                st.info(
                    "📋 Showing cached migration results. Click 'Run Migration' to regenerate."
                )
        else:
            st.info(
                "📋 Showing cached migration results. Click 'Run Migration' to regenerate."
            )
        display_migration_results(cached_result)

    # Run migration
    if uploaded_file and run_migration and not migration_running:
        # Save temp file
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xml") as tmp_file:
            tmp_file.write(uploaded_file.getvalue())
            tmp_xml_path = tmp_file.name

        # Set migration running flag
        st.session_state["migration_running"] = True

        try:
            # Show spinner during migration
            with st.spinner("Running NiFi to Databricks migration..."):
                result = migrate_nifi_to_databricks_simplified(
                    xml_path=tmp_xml_path,
                    out_dir="/tmp",
                    project=f"migration_{uploaded_file.name.replace('.xml', '')}",
                    progress_callback=None,  # Disable verbose logging
                )

            st.success("✅ Migration completed!")

            # Cache the result with completion timestamp
            st.session_state[migration_cache_key] = result
            st.session_state[f"{migration_cache_key}_completion_time"] = time.time()

            # Display the results
            display_migration_results(result)

        except Exception as e:
            error_msg = str(e)
            st.error(f"❌ Migration failed: {error_msg}")
            st.write("**Debug info:**")
            st.code(error_msg)

            # Cache the error for consistency
            st.session_state[migration_cache_key] = error_msg
        finally:
            # Clear migration running flag
            st.session_state["migration_running"] = False
            os.unlink(tmp_xml_path)


if __name__ == "__main__":
    main()

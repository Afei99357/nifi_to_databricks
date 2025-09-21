#!/usr/bin/env python3

import os
import sys
import tempfile

# Add parent directory to Python path to find tools and config (MUST be before imports)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import pandas as pd
import streamlit as st

from tools.migration_orchestrator import migrate_nifi_to_databricks_simplified

# Configure the page
st.set_page_config(
    page_title="Processor Classification & Pruning", page_icon="🚀", layout="wide"
)


def display_migration_results(result):
    """Render declarative classification results."""
    if isinstance(result, str):
        st.error(f"❌ Classification failed: {result}")
        return

    if not isinstance(result, dict):
        st.error(f"❌ Classification failed: Invalid result format - {type(result)}")
        return

    migration_summary = result.get("migration_summary", {})
    if migration_summary:
        st.markdown("### 📊 Migration Categories")
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Business Logic", migration_summary.get("essential_processors", 0))
        col2.metric("Support", migration_summary.get("support_processors", 0))
        col3.metric(
            "Infrastructure", migration_summary.get("infrastructure_processors", 0)
        )
        col4.metric("Ambiguous", migration_summary.get("ambiguous_processors", 0))

    summary_markdown = result.get("reports", {}).get("classification_markdown")
    if summary_markdown:
        with st.expander("📋 Classification Summary", expanded=False):
            st.markdown(summary_markdown)

    classifications = result.get("classifications", [])
    if classifications:
        df = pd.DataFrame(classifications)
        display_columns = [
            "name",
            "short_type",
            "migration_category",
            "databricks_target",
            "confidence",
            "rule",
            "classification_source",
        ]
        existing_columns = [col for col in display_columns if col in df.columns]
        st.markdown("### 🗂️ Classified Processors")
        st.dataframe(
            df[existing_columns],
            use_container_width=True,
            hide_index=True,
        )

        csv_export = df.to_csv(index=False)
        st.download_button(
            "📥 Download Classification CSV",
            data=csv_export,
            file_name="nifi_classification.csv",
            mime="text/csv",
            use_container_width=True,
        )

    ambiguous = result.get("ambiguous", [])
    if ambiguous:
        st.warning(
            f"{len(ambiguous)} processor(s) need manual review due to low confidence."
        )
        amb_df = pd.DataFrame(ambiguous)
        amb_columns = [
            "name",
            "short_type",
            "migration_category",
            "databricks_target",
            "confidence",
        ]
        amb_existing = [col for col in amb_columns if col in amb_df.columns]
        st.dataframe(
            amb_df[amb_existing],
            use_container_width=True,
            hide_index=True,
        )


def main():
    st.title("🚀 NiFi Processor Classification & Pruning")

    # Check for uploaded file from Dashboard
    uploaded_file = st.session_state.get("uploaded_file", None)

    if uploaded_file:
        st.success(f"✅ Processing file: {uploaded_file.name}")
    else:
        st.warning("⚠️ No file selected. Please go back to Dashboard to upload a file.")
        if st.button("🔙 Back to Dashboard"):
            st.switch_page("Dashboard.py")
        return

    # Check for cached classification results
    migration_cache_key = f"migration_results_{uploaded_file.name}"
    cached_result = st.session_state.get(migration_cache_key, None)

    # Check if migration is running
    migration_running = st.session_state.get("migration_running", False)

    # Check for auto-start flag from Dashboard
    auto_start = st.session_state.get("auto_start_migration", False)

    # Dynamic layout based on whether Run Analysis button should be shown
    # Hide button if results exist OR if auto-starting from Dashboard
    if cached_result or auto_start:
        # Only show Back to Dashboard button (no Run Analysis button needed)
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
                    "🚀 Run Classification",
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

    # Display cached results if available
    if cached_result and not run_migration:
        st.info("📋 Showing cached classification results.")
        display_migration_results(cached_result)

    # Run classification
    if uploaded_file and run_migration and not migration_running:
        # Save temp file
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xml") as tmp_file:
            tmp_file.write(uploaded_file.getvalue())
            tmp_xml_path = tmp_file.name

        # Set migration running flag
        st.session_state["migration_running"] = True

        try:
            # Show spinner with warning during migration
            with st.spinner(
                "Running NiFi processor classification & pruning... Please do not navigate away."
            ):
                result = migrate_nifi_to_databricks_simplified(
                    xml_path=tmp_xml_path,
                    out_dir="/tmp",
                    project=f"migration_{uploaded_file.name.replace('.xml', '')}",
                    progress_callback=None,  # Disable verbose logging
                )

            st.success("✅ Processor classification & pruning completed!")

            # Cache the result
            st.session_state[migration_cache_key] = result

            # Display the results
            display_migration_results(result)

        except Exception as e:
            error_msg = str(e)
            st.error(f"❌ Classification failed: {error_msg}")
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

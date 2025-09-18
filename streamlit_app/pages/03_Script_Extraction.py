#!/usr/bin/env python3

import os
import sys
import tempfile

import pandas as pd

# Add parent directory to Python path to find tools and config (MUST be before imports)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import streamlit as st

from tools.script_extraction import extract_all_scripts_from_nifi_xml

# Configure the page
st.set_page_config(page_title="Script Extraction", page_icon="📜", layout="wide")


def display_script_results(scripts, uploaded_file):
    """Display script extraction results"""
    if isinstance(scripts, str):
        st.error(f"❌ Script extraction failed: {scripts}")
        return

    if not isinstance(scripts, list):
        st.error(
            f"❌ Script extraction failed: Invalid result format - {type(scripts)}"
        )
        return

    try:
        # Display summary metrics
        st.markdown("### 📊 Script Extraction Summary")

        total_processors = len(scripts)
        total_external_scripts = sum(
            len(result.get("external_scripts", [])) for result in scripts
        )
        total_inline_scripts = sum(
            len(result.get("inline_scripts", [])) for result in scripts
        )
        total_external_hosts = sum(
            len(result.get("external_hosts", [])) for result in scripts
        )

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Processors with Scripts", total_processors)
        with col2:
            st.metric("External Scripts", total_external_scripts)
        with col3:
            st.metric("Inline Scripts", total_inline_scripts)
        with col4:
            st.metric("External Hosts", total_external_hosts)

        if not scripts:
            st.info("No scripts found in the workflow.")
            return

        # Create detailed table
        script_details = []
        for result in scripts:
            # External script files
            for script in result.get("external_scripts", []):
                script_details.append(
                    {
                        "Script Path": script["path"],
                        "Script Type": script["type"],
                        "Source Type": "External File",
                        "Processor Name": result["processor_name"],
                        "Processor Type": result["processor_type"],
                        "Group Name": result["processor_group"],
                        "Processor ID": result["processor_id"],
                        "Confidence": "N/A",
                    }
                )

            # Inline scripts
            for inline_script in result.get("inline_scripts", []):
                # Create script path with query references if available
                script_path = f"{inline_script['property_name']} ({inline_script['line_count']} lines)"
                if inline_script.get("referenced_queries"):
                    refs = ", ".join(inline_script["referenced_queries"])
                    script_path = f"{script_path} → {refs}"

                script_details.append(
                    {
                        "Script Path": script_path,
                        "Script Type": inline_script["script_type"],
                        "Source Type": "Inline Script",
                        "Processor Name": result["processor_name"],
                        "Processor Type": result["processor_type"],
                        "Group Name": result["processor_group"],
                        "Processor ID": result["processor_id"],
                        "Confidence": f"{inline_script.get('confidence', 0.0):.2f}",
                    }
                )

        if script_details:
            st.markdown("### 📋 Script Details")
            script_df = pd.DataFrame(script_details)

            # Filter controls
            col1, col2, col3 = st.columns(3)
            with col1:
                script_types = ["All"] + sorted(
                    script_df["Script Type"].unique().tolist()
                )
                selected_script_type = st.selectbox(
                    "Filter by Script Type:", script_types, key="script_type_filter"
                )

            with col2:
                # Confidence filter (only for inline scripts)
                min_confidence = st.slider(
                    "Min Confidence:",
                    min_value=0.0,
                    max_value=1.0,
                    value=0.3,
                    step=0.05,
                    key="confidence_filter",
                )

            with col3:
                search_script = st.text_input(
                    "Search Scripts:",
                    placeholder="Enter script name",
                    key="script_search",
                )

            # Apply filters
            filtered_script_df = script_df.copy()

            if selected_script_type != "All":
                filtered_script_df = filtered_script_df[
                    filtered_script_df["Script Type"] == selected_script_type
                ]

            # Apply confidence filter (only to scripts with numeric confidence)
            if min_confidence > 0:
                mask = (filtered_script_df["Confidence"] == "N/A") | (
                    pd.to_numeric(filtered_script_df["Confidence"], errors="coerce")
                    >= min_confidence
                )
                filtered_script_df = filtered_script_df[mask]

            if search_script:
                filtered_script_df = filtered_script_df[
                    filtered_script_df["Script Path"].str.contains(
                        search_script, case=False, na=False
                    )
                ]

            # Show filtered results count
            if len(filtered_script_df) != len(script_df):
                st.info(
                    f"Showing {len(filtered_script_df)} of {len(script_df)} scripts"
                )

            # Display table
            if not filtered_script_df.empty:
                st.dataframe(
                    filtered_script_df,
                    use_container_width=True,
                    hide_index=False,
                    height=None,
                    column_config={
                        "Script Path": st.column_config.TextColumn(
                            "Script Path", width="large"
                        ),
                        "Script Type": st.column_config.TextColumn(
                            "Script Type", width="small"
                        ),
                        "Processor Name": st.column_config.TextColumn(
                            "Processor Name", width="medium"
                        ),
                        "Processor Type": st.column_config.TextColumn(
                            "Processor Type", width="medium"
                        ),
                        "Group Name": st.column_config.TextColumn(
                            "Group Name", width="medium"
                        ),
                        "Processor ID": st.column_config.TextColumn(
                            "Processor ID", width="small"
                        ),
                        "Confidence": st.column_config.TextColumn(
                            "Confidence", width="small"
                        ),
                    },
                )
            else:
                st.warning("No scripts match the current filters.")

        # Inline script details with preview
        inline_script_data = []
        for result in scripts:
            for inline_script in result.get("inline_scripts", []):
                inline_script_data.append(
                    {
                        "processor_name": result["processor_name"],
                        "processor_type": result["processor_type"],
                        "property_name": inline_script["property_name"],
                        "script_type": inline_script["script_type"],
                        "content_preview": inline_script.get(
                            "content_preview", inline_script.get("content", "")[:800]
                        ),
                        "line_count": inline_script["line_count"],
                        "confidence": inline_script.get("confidence", 0.0),
                        "referenced_queries": inline_script.get(
                            "referenced_queries", []
                        ),
                        "full_content": inline_script.get("content", ""),
                    }
                )

        if inline_script_data:
            st.markdown("### 📝 Inline Script Content")
            st.info(
                "These scripts are written directly in processor properties and will need to be migrated to external files or adapted for Databricks."
            )

            for i, script_data in enumerate(inline_script_data):
                with st.expander(
                    f"🐍 {script_data['processor_name']} - {script_data['property_name']} ({script_data['script_type']})"
                ):
                    col1, col2 = st.columns(2)
                    with col1:
                        st.markdown(f"**Processor:** {script_data['processor_name']}")
                        st.markdown(f"**Property:** {script_data['property_name']}")
                        st.markdown(f"**Script Type:** {script_data['script_type']}")
                    with col2:
                        st.markdown(f"**Lines:** {script_data['line_count']}")
                        st.markdown(f"**Confidence:** {script_data['confidence']:.2f}")
                        if script_data["referenced_queries"]:
                            st.markdown(
                                f"**References:** {', '.join(script_data['referenced_queries'])}"
                            )

                    st.markdown("**Script Preview:**")
                    # Show full content for shorter scripts, preview for longer ones
                    content_to_show = (
                        script_data["full_content"]
                        if len(script_data["full_content"]) <= 800
                        else script_data["content_preview"]
                    )
                    st.code(
                        content_to_show,
                        language=script_data["script_type"],
                    )

        # External dependencies
        external_deps = set()
        for result in scripts:
            external_deps.update(result["external_hosts"])
        if external_deps:
            st.markdown("### 🌐 External Dependencies")
            deps_df = pd.DataFrame(list(external_deps), columns=["External Host"])
            st.dataframe(deps_df, use_container_width=True, hide_index=False)

        # Download button
        if script_details:
            script_csv = pd.DataFrame(script_details).to_csv(index=False)
            st.download_button(
                label=f"📥 Download Scripts ({len(script_details)} items)",
                data=script_csv,
                file_name=f"nifi_scripts_{uploaded_file.name.replace('.xml', '')}.csv",
                mime="text/csv",
                use_container_width=True,
            )

    except Exception as e:
        st.error(f"❌ Error displaying script extraction results: {e}")
        st.code(f"Exception: {str(e)}")


def main():
    st.title("📜 Script Extraction")
    st.markdown("**Extract external scripts and executables from NiFi workflows.**")

    # Check for uploaded file from Dashboard
    uploaded_file = st.session_state.get("uploaded_file", None)

    if uploaded_file:
        st.success(f"✅ Processing file: {uploaded_file.name}")
    else:
        st.warning("⚠️ No file selected. Please go back to Dashboard to upload a file.")
        if st.button("🔙 Back to Dashboard"):
            st.switch_page("Dashboard.py")
        return

    # Check for cached script results
    script_cache_key = f"script_results_{uploaded_file.name}"
    cached_scripts = st.session_state.get(script_cache_key, None)

    # Check if script extraction is running
    script_extraction_running = st.session_state.get("script_extraction_running", False)

    # Check for auto-start flag from Dashboard
    auto_start_scripts = st.session_state.get("auto_start_script_extraction", False)

    # Dynamic layout based on whether Extract Scripts button should be shown
    if cached_scripts or auto_start_scripts:
        # Only show Back to Dashboard button
        if st.button(
            "🔙 Back to Dashboard",
            disabled=script_extraction_running,
            help=(
                "Cannot navigate during extraction"
                if script_extraction_running
                else None
            ),
        ):
            st.switch_page("Dashboard.py")
        run_script_extraction = auto_start_scripts
    else:
        # Show both buttons when no results exist
        col1, col2 = st.columns(2)

        with col1:
            run_script_extraction = st.button(
                "📜 Extract Scripts",
                use_container_width=True,
                disabled=script_extraction_running,
            )

        with col2:
            if st.button(
                "🔙 Back to Dashboard",
                disabled=script_extraction_running,
                help=(
                    "Cannot navigate during extraction"
                    if script_extraction_running
                    else None
                ),
            ):
                st.switch_page("Dashboard.py")

    # Clear auto-start flag after checking
    if auto_start_scripts:
        st.session_state["auto_start_script_extraction"] = False

    # Display cached results if available
    if cached_scripts and not run_script_extraction:
        st.info(
            "📋 Showing cached script extraction results. Click 'Extract Scripts' to regenerate."
        )
        display_script_results(cached_scripts, uploaded_file)

    # Run script extraction
    if uploaded_file and run_script_extraction and not script_extraction_running:
        # Save temp file
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xml") as tmp_file:
            tmp_file.write(uploaded_file.getvalue())
            tmp_xml_path = tmp_file.name

        # Set extraction running flag
        st.session_state["script_extraction_running"] = True

        try:
            # Show spinner during extraction
            with st.spinner(
                "📜 Extracting external scripts... Please do not navigate away."
            ):
                scripts = extract_all_scripts_from_nifi_xml(xml_path=tmp_xml_path)

            st.success("✅ Script extraction completed!")

            # Cache the result
            st.session_state[script_cache_key] = scripts

            # Display the results
            display_script_results(scripts, uploaded_file)

        except Exception as e:
            st.error(f"❌ Script extraction failed: {e}")
            st.write("**Debug info:**")
            st.code(str(e))

            # Cache the error for consistency
            st.session_state[script_cache_key] = str(e)
        finally:
            # Clear extraction running flag
            st.session_state["script_extraction_running"] = False
            # Clean up temp file
            if os.path.exists(tmp_xml_path):
                os.unlink(tmp_xml_path)


if __name__ == "__main__":
    main()

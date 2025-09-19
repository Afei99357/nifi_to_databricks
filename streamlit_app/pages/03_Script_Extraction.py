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
                        "processor_id": result["processor_id"],
                        "processor_group": result["processor_group"],
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

            # Create processor-centric interface
            col1, col2, col3 = st.columns([2, 1, 1])

            with col1:
                # Group scripts by processor for easier navigation
                processors_with_scripts: dict[str, list[dict]] = {}
                for script in inline_script_data:
                    proc_name = script["processor_name"]
                    if proc_name not in processors_with_scripts:
                        processors_with_scripts[proc_name] = []
                    processors_with_scripts[proc_name].append(script)

                # First select processor
                processor_names = ["Select a processor..."] + sorted(
                    processors_with_scripts.keys()
                )
                selected_processor = st.selectbox(
                    "🔍 Select processor to view its scripts:",
                    processor_names,
                    key="processor_selector",
                )

                # Then select script from that processor
                selected_script_idx = 0
                if selected_processor != "Select a processor...":
                    processor_scripts = processors_with_scripts[selected_processor]
                    script_options = [
                        f"{script['property_name']} ({script['script_type']}) - {script['line_count']} lines"
                        for script in processor_scripts
                    ]

                    if len(script_options) == 1:
                        st.info(f"📄 This processor has 1 script: {script_options[0]}")
                        # Auto-select the only script
                        selected_script_idx = next(
                            i
                            for i, script in enumerate(inline_script_data)
                            if script["processor_name"] == selected_processor
                        )
                    else:
                        selected_script_name = st.selectbox(
                            f"📄 Scripts in {selected_processor}:",
                            script_options,
                            key="script_in_processor_selector",
                        )

                        # Find the index in the full inline_script_data
                        for i, script in enumerate(inline_script_data):
                            if (
                                script["processor_name"] == selected_processor
                                and f"{script['property_name']} ({script['script_type']}) - {script['line_count']} lines"
                                == selected_script_name
                            ):
                                selected_script_idx = i
                                break

            with col2:
                # Filter by script type
                script_types = ["All"] + sorted(
                    set(script["script_type"] for script in inline_script_data)
                )
                selected_type = st.selectbox(
                    "Filter by type:", script_types, key="inline_type_filter"
                )

            with col3:
                # Filter by processor type
                processor_types = ["All"] + sorted(
                    set(script["processor_type"] for script in inline_script_data)
                )
                selected_proc_type = st.selectbox(
                    "Filter by processor:", processor_types, key="inline_proc_filter"
                )

            # Apply filters
            filtered_inline_data = inline_script_data.copy()
            if selected_type != "All":
                filtered_inline_data = [
                    s for s in filtered_inline_data if s["script_type"] == selected_type
                ]
            if selected_proc_type != "All":
                filtered_inline_data = [
                    s
                    for s in filtered_inline_data
                    if s["processor_type"] == selected_proc_type
                ]

            # Show count
            if len(filtered_inline_data) != len(inline_script_data):
                st.info(
                    f"Showing {len(filtered_inline_data)} of {len(inline_script_data)} inline scripts"
                )

            # Display mode selection
            display_mode = st.radio(
                "Display mode:",
                ["📋 Table View", "📄 Detailed View", "🎯 Selected Script Only"],
                horizontal=True,
                key="display_mode",
            )

            if display_mode == "📋 Table View":
                # Compact table view with clickable rows
                if filtered_inline_data:
                    table_data = []
                    for script in filtered_inline_data:
                        table_data.append(
                            {
                                "Processor": script["processor_name"],
                                "Group": script["processor_group"],
                                "Processor ID": script["processor_id"][:8]
                                + "...",  # Shortened for display
                                "Property": script["property_name"],
                                "Type": script["script_type"],
                                "Lines": script["line_count"],
                                "Confidence": f"{script['confidence']:.2f}",
                                "Preview": (
                                    script["content_preview"][:80]
                                    + "..."  # Shortened for more columns
                                    if len(script["content_preview"]) > 80
                                    else script["content_preview"]
                                ),
                            }
                        )

                    st.dataframe(
                        pd.DataFrame(table_data),
                        use_container_width=True,
                        height=400,
                        column_config={
                            "Processor": st.column_config.TextColumn(
                                "Processor", width="medium"
                            ),
                            "Group": st.column_config.TextColumn(
                                "Group", width="small"
                            ),
                            "Processor ID": st.column_config.TextColumn(
                                "Processor ID", width="small"
                            ),
                            "Property": st.column_config.TextColumn(
                                "Property", width="medium"
                            ),
                            "Type": st.column_config.TextColumn("Type", width="small"),
                            "Preview": st.column_config.TextColumn(
                                "Preview", width="large"
                            ),
                            "Confidence": st.column_config.NumberColumn(
                                "Confidence", format="%.2f", width="small"
                            ),
                        },
                    )

            elif display_mode == "🎯 Selected Script Only":
                # Show only the selected script
                if (
                    selected_processor != "Select a processor..."
                    and selected_script_idx < len(inline_script_data)
                ):
                    script_data = inline_script_data[selected_script_idx]
                    st.markdown(
                        f"#### {script_data['processor_name']} → {script_data['property_name']}"
                    )

                    col1, col2 = st.columns(2)
                    with col1:
                        st.markdown(f"**Script Type:** {script_data['script_type']}")
                        st.markdown(f"**Lines:** {script_data['line_count']}")
                        st.markdown(f"**Group:** {script_data['processor_group']}")
                    with col2:
                        st.markdown(f"**Confidence:** {script_data['confidence']:.2f}")
                        st.markdown(f"**Processor ID:** {script_data['processor_id']}")
                        if script_data["referenced_queries"]:
                            st.markdown(
                                f"**References:** {', '.join(script_data['referenced_queries'])}"
                            )

                    st.markdown("**Full Script Content:**")
                    st.code(
                        script_data["full_content"], language=script_data["script_type"]
                    )
                else:
                    st.info(
                        "👆 Please select a processor and script from the dropdowns above"
                    )

            else:  # Detailed View (improved version)
                # Paginated expandable view
                if filtered_inline_data:
                    # Pagination
                    items_per_page = st.selectbox(
                        "Scripts per page:", [5, 10, 20, 50], index=1, key="pagination"
                    )
                    total_pages = (len(filtered_inline_data) - 1) // items_per_page + 1

                    if total_pages > 1:
                        page = st.selectbox(
                            f"Page (1-{total_pages}):",
                            range(1, total_pages + 1),
                            key="page_selector",
                        )
                    else:
                        page = 1

                    start_idx = (page - 1) * items_per_page
                    end_idx = min(start_idx + items_per_page, len(filtered_inline_data))
                    page_data = filtered_inline_data[start_idx:end_idx]

                    st.markdown(
                        f"**Showing scripts {start_idx + 1}-{end_idx} of {len(filtered_inline_data)}**"
                    )

                    for script_data in page_data:
                        with st.expander(
                            f"🐍 {script_data['processor_name']} → {script_data['property_name']} ({script_data['script_type']}) - {script_data['line_count']} lines"
                        ):
                            col1, col2 = st.columns(2)
                            with col1:
                                st.markdown(
                                    f"**Processor:** {script_data['processor_name']}"
                                )
                                st.markdown(
                                    f"**Property:** {script_data['property_name']}"
                                )
                                st.markdown(
                                    f"**Script Type:** {script_data['script_type']}"
                                )
                                st.markdown(
                                    f"**Group:** {script_data['processor_group']}"
                                )
                            with col2:
                                st.markdown(f"**Lines:** {script_data['line_count']}")
                                st.markdown(
                                    f"**Confidence:** {script_data['confidence']:.2f}"
                                )
                                st.markdown(
                                    f"**Processor ID:** {script_data['processor_id']}"
                                )
                                if script_data["referenced_queries"]:
                                    st.markdown(
                                        f"**References:** {', '.join(script_data['referenced_queries'])}"
                                    )

                            st.markdown("**Script Content:**")
                            content_to_show = (
                                script_data["full_content"]
                                if len(script_data["full_content"]) <= 1000
                                else script_data["content_preview"]
                            )
                            st.code(
                                content_to_show, language=script_data["script_type"]
                            )
                else:
                    st.warning("No inline scripts match the current filters.")

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

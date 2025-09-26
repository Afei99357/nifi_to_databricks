#!/usr/bin/env python3

import os
import sys
from typing import Any, Dict, List

import pandas as pd

# Add parent directory to Python path to find tools and config (MUST be before imports)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import streamlit as st

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

        # Create detailed table and inline lookup
        script_details: List[Dict[str, object]] = []
        inline_script_data: List[Dict[str, object]] = []
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
                        "Lines": script.get("line_count"),
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
                        "Lines": inline_script.get("line_count"),
                    }
                )
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
                        "referenced_queries": inline_script.get(
                            "referenced_queries", []
                        ),
                        "full_content": inline_script.get("content", ""),
                    }
                )

        if script_details:
            st.markdown("### 📋 Script Details")
            script_df = pd.DataFrame(script_details)

            col1, col2, col3 = st.columns(3)
            with col1:
                script_types = ["All"] + sorted(
                    script_df["Script Type"].unique().tolist()
                )
                selected_script_type = st.selectbox(
                    "Filter by Script Type:", script_types, key="script_type_filter"
                )

            with col2:
                source_types = ["All"] + sorted(
                    script_df["Source Type"].unique().tolist()
                )
                selected_source_type = st.selectbox(
                    "Filter by Source:", source_types, key="source_filter"
                )

            with col3:
                search_script = st.text_input(
                    "Search Scripts:",
                    placeholder="Enter script path, processor name, or source label",
                    key="script_search",
                )

            filtered_script_df = script_df.copy()

            if selected_script_type != "All":
                filtered_script_df = filtered_script_df[
                    filtered_script_df["Script Type"] == selected_script_type
                ]

            if selected_source_type != "All":
                filtered_script_df = filtered_script_df[
                    filtered_script_df["Source Type"] == selected_source_type
                ]

            if search_script:
                search_lower = search_script.lower()
                mask = (
                    filtered_script_df["Script Path"]
                    .str.lower()
                    .str.contains(search_lower, na=False)
                    | filtered_script_df["Processor Name"]
                    .str.lower()
                    .str.contains(search_lower, na=False)
                    | filtered_script_df["Source Type"]
                    .str.lower()
                    .str.contains(search_lower, na=False)
                )
                filtered_script_df = filtered_script_df[mask]

            if len(filtered_script_df) != len(script_df):
                st.info(
                    f"Showing {len(filtered_script_df)} of {len(script_df)} scripts"
                )

            if not filtered_script_df.empty:
                display_df = filtered_script_df.copy().reset_index(drop=True)
                display_df.index = range(1, len(display_df) + 1)

                st.dataframe(
                    display_df,
                    use_container_width=True,
                    column_config={
                        "Script Path": st.column_config.TextColumn(
                            "Script", width="large"
                        ),
                        "Script Type": st.column_config.TextColumn(
                            "Type", width="small"
                        ),
                        "Source Type": st.column_config.TextColumn(
                            "Source", width="small"
                        ),
                        "Processor Name": st.column_config.TextColumn(
                            "Processor", width="medium"
                        ),
                        "Group Name": st.column_config.TextColumn(
                            "Group", width="medium"
                        ),
                        "Lines": st.column_config.NumberColumn("Lines", width="small"),
                    },
                )

                external_filtered_df = filtered_script_df[
                    filtered_script_df["Source Type"] == "External File"
                ]
                if not external_filtered_df.empty:
                    unique_external_paths = sorted(
                        {
                            str(path).strip()
                            for path in external_filtered_df["Script Path"].dropna()
                        }
                    )
                    if unique_external_paths:
                        payload = "\n".join(unique_external_paths)
                        st.download_button(
                            "📄 Download unique external filenames",
                            data=payload,
                            file_name="external_script_filenames.txt",
                            mime="text/plain",
                            use_container_width=True,
                        )

                inline_rows = display_df[
                    display_df["Source Type"] == "Inline Script"
                ].index.tolist()

                if inline_rows:
                    row_options = [
                        f"Row {idx}: {str(display_df.loc[idx, 'Processor Name'])} → "
                        f"{str(display_df.loc[idx, 'Script Path'])}"
                        for idx in inline_rows
                    ]
                    selected_row_label = st.selectbox(
                        "📄 View inline script from table:",
                        ["None"] + row_options,
                        key="inline_script_table_select",
                    )

                    if selected_row_label != "None":
                        selected_idx = inline_rows[
                            row_options.index(selected_row_label)
                        ]
                        target_proc = str(
                            display_df.loc[selected_idx, "Processor Name"]
                        )
                        target_script_display = str(
                            display_df.loc[selected_idx, "Script Path"]
                        )

                        for idx, script in enumerate(inline_script_data):
                            script_display = f"{script['property_name']} ({script['line_count']} lines)"
                            if (
                                str(script["processor_name"]) == target_proc
                                and script_display == target_script_display
                            ):
                                st.session_state["processor_selector"] = target_proc
                                st.session_state["script_in_processor_selector"] = (
                                    script_display
                                )
                                selected_processor = target_proc
                                selected_script_idx = idx
                                break

            else:
                st.warning("No scripts match the current filters.")

        if inline_script_data:
            st.markdown("### 📝 Inline Script Content")
            st.info(
                "These scripts are written directly in processor properties and will need to be migrated to external files or adapted for Databricks."
            )

            # Create processor-centric interface
            col1, col2 = st.columns([2, 1])

            with col1:
                # Group scripts by processor for easier navigation
                processors_with_scripts: Dict[str, List[Dict[str, Any]]] = {}
                for script in inline_script_data:
                    proc_name = str(script["processor_name"])
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
                        f"{script['property_name']} ({script['line_count']} lines)"
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
                                and f"{script['property_name']} ({script['line_count']} lines)"
                                == selected_script_name
                            ):
                                selected_script_idx = i
                                break

            with col2:
                # Filter by script type and optional search term
                script_types = ["All"] + sorted(
                    {str(script["script_type"]) for script in inline_script_data}
                )
                selected_type = st.selectbox(
                    "Filter by script type:", script_types, key="inline_type_filter"
                )
                search_inline = st.text_input(
                    "Search properties or preview:",
                    placeholder="e.g. query, filename, etc.",
                    key="inline_script_search",
                )

            # Apply filters
            filtered_inline_data = inline_script_data.copy()
            if selected_type != "All":
                filtered_inline_data = [
                    s for s in filtered_inline_data if s["script_type"] == selected_type
                ]
            if selected_processor != "Select a processor...":
                filtered_inline_data = [
                    s
                    for s in filtered_inline_data
                    if s["processor_name"] == selected_processor
                ]
            if search_inline:
                query = search_inline.lower()
                filtered_inline_data = [
                    s
                    for s in filtered_inline_data
                    if query in str(s["property_name"]).lower()
                    or query in str(s.get("content_preview") or "").lower()
                    or query in str(s.get("full_content") or "").lower()
                ]
            if selected_processor != "Select a processor...":
                filtered_inline_data = [
                    s
                    for s in filtered_inline_data
                    if s["processor_name"] == selected_processor
                ]

            # Show count
            if len(filtered_inline_data) != len(inline_script_data):
                st.info(
                    f"Showing {len(filtered_inline_data)} of {len(inline_script_data)} inline scripts"
                )

            # Detailed script view
            if (
                selected_processor != "Select a processor..."
                and selected_script_idx < len(inline_script_data)
            ):
                script_data = inline_script_data[selected_script_idx]
                with st.expander(
                    f"📄 {script_data['processor_name']} → {script_data['property_name']}",
                    expanded=True,
                ):
                    col1, col2 = st.columns(2)
                    with col1:
                        st.markdown(f"**Script Type:** {script_data['script_type']}")
                        st.markdown(f"**Lines:** {script_data['line_count']}")
                        st.markdown(f"**Group:** {script_data['processor_group']}")
                    with col2:
                        st.markdown(f"**Processor ID:** {script_data['processor_id']}")
                        raw_refs = script_data.get("referenced_queries")
                        ref_list = (
                            [str(ref) for ref in raw_refs]
                            if isinstance(raw_refs, list)
                            else []
                        )
                        if ref_list:
                            st.markdown(f"**References:** {', '.join(ref_list)}")

                    st.markdown("**Full Script Content:**")
                    st.code(
                        script_data["full_content"],
                        language=script_data["script_type"],
                    )
            else:
                st.info("👆 Select a processor and script to view its content.")

        # Download button
        if script_details:
            export_df = (
                filtered_script_df if not filtered_script_df.empty else script_df
            )
            script_csv = export_df.to_csv(index=False)
            export_count = len(export_df)
            st.download_button(
                label=f"📥 Download Scripts ({export_count} items)",
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

    uploaded_file = st.session_state.get("uploaded_file", None)

    if not uploaded_file:
        st.warning("⚠️ No file selected. Please go back to Dashboard to upload a file.")
        if st.button("🔙 Back to Dashboard"):
            st.switch_page("Dashboard.py")
        return

    st.success(f"✅ Processing file: {uploaded_file.name}")

    script_cache_key = f"script_results_{uploaded_file.name}"
    cached_scripts = st.session_state.get(script_cache_key)

    if isinstance(cached_scripts, list):
        st.info(
            "📋 Showing cached script extraction results generated via the Dashboard analysis."
        )
        display_script_results(cached_scripts, uploaded_file)
    elif isinstance(cached_scripts, str):
        st.error(f"❌ Script extraction failed: {cached_scripts}")
        if st.button("🔙 Back to Dashboard", use_container_width=True):
            st.switch_page("Dashboard.py")
    else:
        st.warning(
            "Run the full analysis from the Dashboard to generate script extraction results."
        )
        if st.button("🔙 Back to Dashboard", use_container_width=True):
            st.switch_page("Dashboard.py")


if __name__ == "__main__":
    main()

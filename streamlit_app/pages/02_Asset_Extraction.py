#!/usr/bin/env python3

import os
import sys
import tempfile

# Add parent directory to Python path to find tools and config (MUST be before imports)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import streamlit as st

from tools.migration_orchestrator import extract_nifi_assets_only

# Configure the page
st.set_page_config(page_title="Asset Extraction", page_icon="📦", layout="wide")


def display_asset_results(result, uploaded_file):
    """Display asset extraction results from either fresh run or cache"""
    # Handle both string results (error case) and dict results
    if isinstance(result, str):
        st.error(f"❌ Asset extraction failed: {result}")
        return

    if not isinstance(result, dict):
        st.error(f"❌ Asset extraction failed: Invalid result format - {type(result)}")
        return

    try:
        # Display summary metrics
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Total Processors", result.get("total_processors", 0))
        with col2:
            st.metric("Asset Categories", "Scripts, Paths, Hosts, Tables")

        # Display asset summary report
        if result.get("reports") and result["reports"].get("asset_summary"):
            st.markdown("### 📦 Discovered Assets")
            with st.expander("📄 Asset Summary", expanded=True):
                st.markdown(result["reports"]["asset_summary"])
        else:
            st.info("No assets found in the workflow.")

    except Exception as e:
        st.error(f"❌ Error displaying asset extraction results: {e}")
        st.write(f"**Debug - Exception type:** {type(e)}")
        st.write(f"**Debug - Exception details:** {str(e)}")
        import traceback

        st.code(traceback.format_exc())


def main():
    st.title("📦 NiFi Asset Extraction")

    # Check for uploaded file from Dashboard
    uploaded_file = st.session_state.get("uploaded_file", None)

    if uploaded_file:
        st.success(f"✅ Processing file: {uploaded_file.name}")
    else:
        st.warning("⚠️ No file selected. Please go back to Dashboard to upload a file.")
        if st.button("🔙 Back to Dashboard"):
            st.switch_page("Dashboard.py")
        return

    # Check for cached asset results
    asset_cache_key = f"asset_results_{uploaded_file.name}"
    cached_result = st.session_state.get(asset_cache_key, None)

    # Check if extraction is running
    extraction_running = st.session_state.get("asset_extraction_running", False)

    # Extraction options
    col1, col2 = st.columns(2)

    with col1:
        run_extraction = st.button(
            "📦 Extract Assets",
            use_container_width=True,
            disabled=extraction_running,
        )

    with col2:
        if st.button(
            "🔙 Back to Dashboard",
            disabled=extraction_running,
            help="Cannot navigate during extraction" if extraction_running else None,
        ):
            st.switch_page("Dashboard.py")

    # Display cached results if available
    if cached_result and not run_extraction:
        st.info(
            "📋 Showing cached asset extraction results. Click 'Extract Assets' to regenerate."
        )
        display_asset_results(cached_result, uploaded_file)

    # Run extraction
    if uploaded_file and run_extraction and not extraction_running:
        # Save temp file
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xml") as tmp_file:
            tmp_file.write(uploaded_file.getvalue())
            tmp_xml_path = tmp_file.name

        # Set extraction running flag
        st.session_state["asset_extraction_running"] = True

        try:
            # Show spinner with warning during extraction
            with st.spinner(
                "📦 Extracting NiFi assets... Please do not navigate away."
            ):
                result = extract_nifi_assets_only(
                    xml_path=tmp_xml_path,
                    progress_callback=None,  # Disable verbose logging
                )

            st.success("✅ Asset extraction completed!")

            # Cache the result
            st.session_state[asset_cache_key] = result

            # Display the results
            display_asset_results(result, uploaded_file)

        except Exception as e:
            st.error(f"❌ Asset extraction failed: {e}")
            st.write("**Debug info:**")
            st.code(str(e))

            # Cache the error for consistency
            st.session_state[asset_cache_key] = str(e)
        finally:
            # Clear extraction running flag
            st.session_state["asset_extraction_running"] = False
            # Clean up temp file
            if os.path.exists(tmp_xml_path):
                os.unlink(tmp_xml_path)


if __name__ == "__main__":
    main()

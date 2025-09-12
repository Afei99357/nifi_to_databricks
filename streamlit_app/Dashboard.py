#!/usr/bin/env python3

import streamlit as st

# Configure the page
st.set_page_config(
    page_title="NiFi to Databricks Migration Tool", page_icon="🔧", layout="wide"
)


def main():
    st.title("🔧 NiFi to Databricks Migration Tool")

    st.markdown(
        """
    Welcome to the NiFi to Databricks migration platform! This tool helps you convert
    Apache NiFi workflows into optimized Databricks pipelines.
    """
    )

    # Overview section
    st.markdown("## 🎯 What This Tool Does")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown(
            """
        **🚀 Migration Features:**
        - Analyzes NiFi XML templates
        - Classifies and optimizes processors
        - Generates Databricks Job configurations
        - Creates PySpark code equivalents
        - Provides migration reports
        """
        )

    with col2:
        st.markdown(
            """
        **📊 Analysis Capabilities:**
        - Processor classification (data_movement, data_transformation, etc.)
        - Infrastructure pruning for complexity reduction
        - Data flow chain detection
        - Asset extraction and cataloging
        """
        )

    # Quick start section
    st.markdown("## 🚀 Quick Start")

    st.markdown(
        """
    1. **📤 Upload NiFi XML**: Navigate to the **NiFi Migration** page to upload your NiFi template file
    2. **🔄 Run Migration**: Click the migration button to analyze and convert your workflow
    3. **📋 Review Results**: Examine the generated reports and Databricks configurations
    4. **🎯 Deploy**: Use the generated assets to create your Databricks pipeline
    """
    )

    # Navigation help
    st.markdown("## 📖 Navigation")

    st.info(
        """
    **📄 Available Pages:**
    - **NiFi Migration**: Upload and convert NiFi workflows to Databricks
    - More pages coming soon for table lineage analysis and advanced features...
    """
    )

    # System status
    st.markdown("## 📊 System Status")

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric("Migration Engine", "✅ Active")
    with col2:
        st.metric("Table Lineage", "🔄 Coming Soon")
    with col3:
        st.metric("Advanced Analytics", "🔄 Coming Soon")


if __name__ == "__main__":
    main()

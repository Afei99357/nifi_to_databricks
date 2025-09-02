# Databricks notebook source
# MAGIC %pip install -U -qqqq backoff uv json-repair

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import json

# Import all required modules at the top for cleaner code
from datetime import datetime

from tools.nifi_processor_classifier_tool import analyze_workflow_patterns
from tools.simplified_migration import (
    analyze_nifi_workflow_only,
    migrate_nifi_to_databricks_simplified,
)
from utils.workflow_summary import print_and_save_workflow_summary

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚀 Simplified NiFi to Databricks Migration
# MAGIC
# MAGIC This notebook demonstrates the direct function approach for NiFi migration:
# MAGIC - **Direct Function Calls**: No agent complexity or multi-round orchestration
# MAGIC - **Linear Pipeline**: analyze → prune → chain → migrate
# MAGIC - **Fast Execution**: No conversation state or agent overhead
# MAGIC - **Same Intelligence**: All analysis capabilities without agent wrapper

# COMMAND ----------

# MAGIC %md
# MAGIC #### Option A: Direct Analysis with Markdown Export (Recommended)
# MAGIC Analyzes the complex ICN8_BRS_Feedback workflow and generates comprehensive report.

# COMMAND ----------

# Analyze complex workflow directly
print("🧠 DIRECT WORKFLOW ANALYSIS - Complex Pipeline")
print("=" * 60)

# Direct analysis with automatic markdown export
complex_analysis_result = analyze_workflow_patterns(
    xml_path="/Volumes/eliao/nifi_to_databricks/nifi_files/ICN8_BRS_Feedback.xml",
    save_markdown=True,  # Automatically saves markdown report
    output_dir="/tmp/workflow_analysis",
)

print(
    f"✅ Analysis complete! Found {complex_analysis_result['analysis_summary']['total_processors']} processors"
)
print(
    f"💡 {complex_analysis_result['analysis_summary']['data_processing_ratio']:.1f}% are doing actual data processing"
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Option B: Detailed Analysis with Reports
# MAGIC Uses direct analysis functions for comprehensive workflow understanding.

# COMMAND ----------

print("🔍 DETAILED WORKFLOW ANALYSIS WITH REPORTS")

# Direct detailed analysis with automatic markdown export
detailed_analysis = analyze_workflow_patterns(
    xml_path="/Volumes/eliao/nifi_to_databricks/nifi_files/ICN8_BRS_Feedback.xml",
    save_markdown=True,
    output_dir="/tmp/detailed_analysis",
)

print("✅ Detailed analysis complete!")

# Display the analysis results
print("\n📋 DETAILED ANALYSIS RESULTS:")
print("=" * 60)
print(
    f"📊 Total processors: {detailed_analysis['analysis_summary']['total_processors']}"
)
print(
    f"💡 Data processing ratio: {detailed_analysis['analysis_summary']['data_processing_ratio']:.1f}%"
)
print(f"📄 Markdown report generated in /tmp/detailed_analysis/")

print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔬 Step 3.5: Test Simplified Migration Pipeline
# MAGIC
# MAGIC **NEW**: Test the simplified direct function call approach without agent complexity.

# COMMAND ----------

print("🧪 TESTING SIMPLIFIED MIGRATION PIPELINE")
print("=" * 60)

# Functions already imported at top - using them directly

# Option A: Analysis only (fast, no migration)
print("🔍 Option A: Analysis-only workflow understanding...")
analysis_result = analyze_nifi_workflow_only(
    xml_path="/Volumes/eliao/nifi_to_databricks/nifi_files/ICN8_BRS_Feedback.xml"
)

print("✅ Analysis complete!")
print("\n📊 ANALYSIS RESULTS SUMMARY:")
print("=" * 60)
print(f"🔍 Workflow Analysis: {analysis_result.get('workflow_analysis', 'N/A')}")
print(
    f"🏷️  Processor Classifications: {analysis_result.get('processor_classifications', 'N/A')}"
)
print(f"✂️  Pruned Processors: {analysis_result.get('pruned_processors', 'N/A')}")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🚀 Step 4: Simplified Complete Migration
# MAGIC
# MAGIC **NEW**: Direct function call approach - complete migration without agent complexity!

# COMMAND ----------

# Using imports from top cell
current = datetime.now().strftime("%Y%m%d%H%M%S")

print("🚀 SIMPLIFIED COMPLETE MIGRATION")
print("🎯 Direct function calls: analyze → prune → chain → migrate")

# Option B: Complete migration with semantic analysis
print("\n🔍 Option B: Complete migration with analysis pipeline...")
complete_result = migrate_nifi_to_databricks_simplified(
    xml_path="/Volumes/eliao/nifi_to_databricks/nifi_files/ICN8_BRS_Feedback.xml",
    out_dir="/Workspace/Users/eliao@bpcs.com/nifi_to_databricks_large_xml/output_results",
    project=f"simplified_feedback_{current}",
    notebook_path=f"/Workspace/Users/eliao@bpcs.com/nifi_to_databricks_large_xml/output_results/simplified_feedback_{current}/main",
    deploy=False,  # Don't auto-deploy, just create
)

print("✅ Simplified migration complete!")

# Display the migration results
print("\n📋 MIGRATION RESULTS SUMMARY:")
print("=" * 60)
print(
    f"✅ Migration Result: {complete_result['migration_result'].get('status', 'N/A')}"
)
print(f"📁 Output Directory: {complete_result['configuration']['out_dir']}")
print(f"🏷️  Project Name: {complete_result['configuration']['project']}")
print(
    f"🔍 Analysis Summary: {complete_result['analysis'].get('workflow_analysis', 'N/A')}"
)

print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 📋 Step 5: Standalone Workflow Analysis & Reporting
# MAGIC Demonstrates the new standalone analysis functions for comprehensive workflow reporting.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Using the New Workflow Summary Functions
# MAGIC Shows how to use the enhanced workflow summary capabilities with markdown export.

# COMMAND ----------

# Using imports from top cell

# First, let's use the direct analysis result we generated earlier
# This demonstrates the integrated workflow: analysis → summary → markdown export

if "analysis_result" in locals():
    # Save the analysis result to a temporary JSON file for demonstration
    temp_analysis_path = "/tmp/demo_analysis.json"
    with open(temp_analysis_path, "w") as f:
        json.dump(analysis_result, f, indent=2)

    print("📄 GENERATING UNIFIED WORKFLOW SUMMARY & MARKDOWN REPORT")
    print("=" * 60)

    # Use the unified function that both prints and saves markdown
    markdown_path = print_and_save_workflow_summary(
        temp_analysis_path,
        save_markdown=True,
        output_path="/tmp/workflow_comprehensive_report.md",
    )

    print(f"\n✅ Comprehensive report saved to: {markdown_path}")

    # Display a sample of the markdown content
    if markdown_path:
        with open(markdown_path, "r") as f:
            markdown_content = f.read()
            print(f"\n📄 MARKDOWN REPORT PREVIEW (first 500 chars):")
            print("-" * 50)
            print(markdown_content[:500] + "...")
            print("-" * 50)
            print(f"📏 Full report length: {len(markdown_content)} characters")
else:
    print("⚠️  Run the analysis steps above first to see this demonstration")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🆚 Comparison: Agent vs Simplified Migration Methods

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🎯 What's Different About Simplified Migration?
# MAGIC
# MAGIC | **Agent Method** | **Simplified Method** |
# MAGIC |-------------------|-------------------------|
# MAGIC | Multi-round agent orchestration | Direct function pipeline |
# MAGIC | LangGraph complexity | Simple Python function calls |
# MAGIC | Agent decides next steps | Pre-defined linear workflow |
# MAGIC | Complex conversation state | Stateless function execution |
# MAGIC | Multi-round tool calls | Single execution path |
# MAGIC | Agent reasoning overhead | Direct workflow execution |
# MAGIC
# MAGIC ### 🧠 New Intelligence Features:
# MAGIC
# MAGIC #### **Smart Processor Classification (Type-First Approach)**
# MAGIC - **Hybrid Analysis**: Rule-based + LLM for cost-effective classification
# MAGIC - **Type-First Logic**: `processor_type` → `properties` → `name` (as hint only)
# MAGIC - **SQL Detection**: Identifies actual SQL operations in UpdateAttribute/ExecuteStreamCommand
# MAGIC - **Impact Levels**: High/Medium/Low impact classification for migration prioritization
# MAGIC
# MAGIC #### **Comprehensive Reporting**
# MAGIC - **Console Output**: Human-readable summary with emoji formatting
# MAGIC - **Markdown Export**: Professional reports with tables, insights, and recommendations
# MAGIC - **Unified Functions**: Single calls for both analysis and reporting
# MAGIC - **Migration Guidance**: Architecture recommendations and focus areas
# MAGIC
# MAGIC #### **Business Intelligence**
# MAGIC - **Workflow Purpose**: "This workflow does sensor data collection for analytics"
# MAGIC - **Processing Ratio**: Shows percentage of processors doing actual data work
# MAGIC - **Key Operations**: Identifies repeated business operations across processors
# MAGIC - **Complexity Assessment**: High/Medium/Low workflow complexity ratings

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔄 Legacy Method (For Comparison)
# MAGIC *Use this section if you want to compare with the old hardcoded approach*

# COMMAND ----------

# Uncomment to run legacy chunked migration for comparison
# from datetime import datetime
# from mlflow.types.responses import ResponsesAgentRequest
# from agents import AGENT

# current = datetime.now().strftime("%Y%m%d%H%M%S")

# print("🔧 LEGACY MIGRATION - Chunked Method")
# print("=" * 60)

# req = ResponsesAgentRequest(
#     input=[
#         {
#             "role": "user",
#             "content": (
#                 "Run orchestrate_chunked_nifi_migration with:\n"
#                 "xml_path=/Volumes/eliao/nifi_to_databricks/nifi_files/ICN8_BRS_Feedback.xml\n"
#                 "out_dir=/Workspace/Users/eliao@bpcs.com/nifi_to_databricks_large_xml/output_results\n"
#                 f"project=legacy_feedback_{current}\n"
#                 f"job=legacy_job_{current}\n"
#                 f"notebook_path=/Workspace/Users/eliao@bpcs.com/nifi_to_databricks_large_xml/output_results/legacy_feedback_{current}/main\n"
#                 "max_processors_per_chunk=25\n"
#                 "existing_cluster_id=0722-181403-vd3u4c6r\n"
#                 "run_now=false"
#             ),
#         }
#     ]
# )

# resp = AGENT.predict(req)
# print("✅ Legacy migration complete!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🎓 Quick Start Guide

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚀 How to Use Intelligent NiFi Migration
# MAGIC
# MAGIC ### **Option 1: Direct Analysis (Recommended for Development)**
# MAGIC
# MAGIC **Standalone Analysis with Markdown Reports:**
# MAGIC ```python
# MAGIC from tools.nifi_processor_classifier_tool import analyze_workflow_patterns
# MAGIC
# MAGIC # Complete analysis with automatic markdown export
# MAGIC result = analyze_workflow_patterns(
# MAGIC     xml_path="/path/to/workflow.xml",
# MAGIC     save_markdown=True,
# MAGIC     output_dir="/output/analysis"
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC **Workflow Summary with Unified Reporting:**
# MAGIC ```python
# MAGIC from utils.workflow_summary import print_and_save_workflow_summary
# MAGIC
# MAGIC # Print to console AND save comprehensive markdown report
# MAGIC markdown_path = print_and_save_workflow_summary(
# MAGIC     json_file_path="/path/to/analysis.json",
# MAGIC     save_markdown=True
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ### **Option 2: Complete Migration (Recommended)**
# MAGIC
# MAGIC **Direct Function Pipeline - Complete End-to-End Migration:**
# MAGIC ```python
# MAGIC from tools.simplified_migration import migrate_nifi_to_databricks_simplified
# MAGIC
# MAGIC # Complete migration in one function call
# MAGIC result = migrate_nifi_to_databricks_simplified(
# MAGIC     xml_path="/path/to/workflow.xml",
# MAGIC     out_dir="/output",
# MAGIC     project="my_project",
# MAGIC     deploy=False
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC **What Happens Automatically:**
# MAGIC - 🔍 **Step 1**: Analyze workflow and classify processors
# MAGIC - ✂️ **Step 2**: Prune infrastructure-only processors
# MAGIC - 🔗 **Step 3**: Detect semantic data flow chains
# MAGIC - 🌊 **Step 4**: Create semantic data flows
# MAGIC - 🧠 **Step 5**: Execute intelligent migration
# MAGIC - ✅ **Complete**: Return comprehensive results
# MAGIC
# MAGIC **Alternative - Analysis Only:**
# MAGIC ```python
# MAGIC from tools.simplified_migration import analyze_nifi_workflow_only
# MAGIC
# MAGIC # Analysis only (fast, no migration)
# MAGIC analysis = analyze_nifi_workflow_only("/path/to/workflow.xml")
# MAGIC print(analysis['workflow_analysis'])
# MAGIC print(analysis['processor_classifications'])
# MAGIC print(analysis['semantic_flows'])
# MAGIC ```
# MAGIC
# MAGIC ### **Key Benefits:**
# MAGIC - 🚀 **Simplified Pipeline**: Linear function calls without agent complexity
# MAGIC - 🔄 **Complete Migration**: Handles everything from analysis to deployment in one function
# MAGIC - 📊 **Business Understanding**: Explains what your workflow actually does
# MAGIC - 🎯 **Best Architecture**: Recommends optimal Databricks patterns for your data
# MAGIC - ⚡ **Fast Execution**: No multi-round agent overhead or conversation state
# MAGIC
# MAGIC ### **Try Your Own Workflows:**
# MAGIC Replace the file paths above with your NiFi XML files and run the cells!
# MAGIC
# MAGIC ### **Key Benefits of Simplified Migration:**
# MAGIC - **Direct Functions**: No agent complexity or multi-round orchestration
# MAGIC - **Fast Execution**: Linear pipeline without conversation state overhead
# MAGIC - **Same Intelligence**: All analysis capabilities without agent wrapper
# MAGIC - **Predictable Results**: Deterministic execution flow, no agent decisions

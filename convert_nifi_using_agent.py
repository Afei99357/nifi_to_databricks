# Databricks notebook source
# MAGIC %pip install -U -qqqq backoff databricks-langchain langgraph==0.5.3 uv databricks-agents mlflow-skinny[databricks] json-repair

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 Intelligent NiFi Analysis & Migration
# MAGIC
# MAGIC This notebook demonstrates the new LLM-powered NiFi intelligence system that:
# MAGIC - **Analyzes** what your NiFi workflows actually do in business terms
# MAGIC - **Distinguishes** between data transformation vs infrastructure processors
# MAGIC - **Recommends** optimal Databricks architecture patterns
# MAGIC - **Migrates** intelligently based on workflow understanding

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔍 Step 1: Direct Workflow Analysis (New Smart Analysis)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Option A: Direct Analysis with Markdown Export (Recommended)
# MAGIC Uses the new `analyze_workflow_patterns` function to analyze workflows and automatically generate markdown reports.

# COMMAND ----------

from utils.nifi_analysis_utils import analyze_workflow_patterns

# Analyze simple workflow directly
print("🧠 DIRECT WORKFLOW ANALYSIS - Simple Pipeline")
print("=" * 60)

# Direct analysis with automatic markdown export
analysis_result = analyze_workflow_patterns(
    xml_path="/Volumes/eliao/nifi_to_databricks/nifi_files/nifi_pipeline_eric_1.xml",
    save_markdown=True,  # Automatically saves markdown report
    output_dir="/tmp/workflow_analysis",
)

print("\n📊 ANALYSIS SUMMARY:")
print(f"• Total Processors: {analysis_result['analysis_summary']['total_processors']}")
print(
    f"• Data Transformers: {analysis_result['analysis_summary']['data_transformation_processors']}"
)
print(
    f"• Data Movers: {analysis_result['analysis_summary']['data_movement_processors']}"
)
print(
    f"• Infrastructure: {analysis_result['analysis_summary']['infrastructure_processors']}"
)

print("✅ Direct workflow analysis complete with markdown export!")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Option B: Agent-Based Analysis (Interactive)
# MAGIC Uses the LangGraph agent for interactive workflow analysis.

# COMMAND ----------

from datetime import datetime

from mlflow.types.responses import ResponsesAgentRequest

from agents import AGENT

print("🧠 AGENT-BASED WORKFLOW ANALYSIS")
print("=" * 60)

# Ask the agent to analyze workflow using the XML file path
req = ResponsesAgentRequest(
    input=[
        {
            "role": "user",
            "content": "Use analyze_nifi_workflow_intelligence to analyze the NiFi workflow at /Volumes/eliao/nifi_to_databricks/nifi_files/nifi_pipeline_eric_1.xml and explain what it does in business terms. What processors actually transform data vs just move files?",
        }
    ]
)

analysis_resp = AGENT.predict(req)
print("✅ Agent-based workflow analysis complete!")

# Display the analysis results
print("\n📋 ANALYSIS RESULTS:")
print("=" * 60)

# Import utility functions for this step
from utils.response_utils import display_agent_response, save_agent_summary_to_markdown

# Save formatted analysis summary to markdown file
markdown_file = save_agent_summary_to_markdown(
    analysis_resp, "/tmp/workflow_analysis/simple_workflow_analysis_summary.md"
)

# Display in clean format
display_agent_response(analysis_resp)

print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🚀 Step 2: Intelligent Migration of Simple Workflow

# COMMAND ----------

from mlflow.types.responses import ResponsesAgentRequest

from agents import AGENT

current = datetime.now().strftime("%Y%m%d%H%M%S")

print("🚀 INTELLIGENT MIGRATION")
print("=" * 60)

req = ResponsesAgentRequest(
    input=[
        {
            "role": "user",
            "content": (
                "Run orchestrate_intelligent_nifi_migration with:\n"
                "xml_path=/Volumes/eliao/nifi_to_databricks/nifi_files/nifi_pipeline_eric_1.xml\n"
                "out_dir=/Workspace/Users/eliao@bpcs.com/nifi_to_databricks_large_xml/output_results\n"
                f"project=intelligent_simple_{current}\n"
                f"notebook_path=/Workspace/Users/eliao@bpcs.com/nifi_to_databricks_large_xml/output_results/intelligent_simple_{current}/main\n"
                "existing_cluster_id=0722-181403-vd3u4c6r\n"
                "run_now=true"
            ),
        }
    ]
)

resp = AGENT.predict(req)
print("✅ Intelligent migration complete!")

# Display the migration results using utilities
print("\n📋 SIMPLE MIGRATION RESULTS:")
print("=" * 60)

# Import utility functions for this step
from utils.response_utils import display_agent_response

# Save migration response to JSON
# json_file = save_agent_response_to_json(resp, "simple_workflow_migration.json")

# Display in clean format
display_agent_response(resp)

print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔍 Step 3: Complex Workflow Analysis (Data Processing Pipeline)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Option A: Direct Analysis with Markdown Export (Recommended)
# MAGIC Analyzes the complex ICN8_BRS_Feedback workflow and generates comprehensive report.

# COMMAND ----------

from utils.nifi_analysis_utils import analyze_workflow_patterns

# Analyze complex workflow directly
print("🧠 DIRECT WORKFLOW ANALYSIS - Complex Pipeline")
print("=" * 60)

# Direct analysis with automatic markdown export
complex_analysis_result = analyze_workflow_patterns(
    xml_path="/Volumes/eliao/nifi_to_databricks/nifi_files/ICN8_BRS_Feedback.xml",
    save_markdown=True,  # Automatically saves markdown report
    output_dir="/tmp/workflow_analysis",
)

print("\n📊 COMPLEX WORKFLOW SUMMARY:")
print(
    f"• Total Processors: {complex_analysis_result['analysis_summary']['total_processors']}"
)
print(
    f"• Data Transformers: {complex_analysis_result['analysis_summary']['data_transformation_processors']}"
)
print(
    f"• Data Movers: {complex_analysis_result['analysis_summary']['data_movement_processors']}"
)
print(
    f"• Infrastructure: {complex_analysis_result['analysis_summary']['infrastructure_processors']}"
)
print(
    f"• External Processors: {complex_analysis_result['analysis_summary']['external_processing_processors']}"
)

# Show some key insights
print(f"\n💡 KEY INSIGHTS:")
print(
    f"• Processing Ratio: {complex_analysis_result['analysis_summary']['data_processing_ratio']:.1f}%"
)
print(
    f"• High Impact Processors: {len([p for p in complex_analysis_result['processors_analysis'] if p.get('data_impact_level') == 'high'])}"
)

print("✅ Complex workflow analysis complete with comprehensive markdown report!")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Option B: Agent-Based Analysis (Interactive)
# MAGIC Uses the LangGraph agent for interactive complex workflow analysis.

# COMMAND ----------

from datetime import datetime

from mlflow.types.responses import ResponsesAgentRequest

from agents import AGENT

print("🧠 AGENT-BASED COMPLEX WORKFLOW ANALYSIS")
print("=" * 60)

# Analyze the complex ICN8_BRS_Feedback workflow using file path
req = ResponsesAgentRequest(
    input=[
        {
            "role": "user",
            "content": "Use analyze_nifi_workflow_intelligence to analyze the complex NiFi workflow at /Volumes/eliao/nifi_to_databricks/nifi_files/ICN8_BRS_Feedback.xml with 58+ processors. Help me understand what it actually does for the business and which processors do real data processing vs infrastructure work like logging and routing.",
        }
    ]
)

complex_analysis_resp = AGENT.predict(req)
print("✅ Agent-based complex workflow analysis complete!")

# Display the complex analysis results using utilities
print("\n📋 COMPLEX ANALYSIS RESULTS:")
print("=" * 60)

# Use utility functions to handle the response
from utils.response_utils import display_agent_response, save_agent_summary_to_markdown

# Save complex workflow formatted analysis summary to markdown
markdown_file = save_agent_summary_to_markdown(
    complex_analysis_resp, "/tmp/workflow_analysis/complex_workflow_analysis_summary.md"
)


# Display in clean format
display_agent_response(complex_analysis_resp)

print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🚀 Step 4: Intelligent Migration of Complex Workflow

# COMMAND ----------

current = datetime.now().strftime("%Y%m%d%H%M%S")

print("🚀 INTELLIGENT MIGRATION - Complex Workflow")
print("=" * 60)

# Use intelligent migration for the complex workflow
req = ResponsesAgentRequest(
    input=[
        {
            "role": "user",
            "content": (
                "Run orchestrate_intelligent_nifi_migration with:\n"
                "xml_path=/Volumes/eliao/nifi_to_databricks/nifi_files/ICN8_BRS_Feedback.xml\n"
                "out_dir=/Workspace/Users/eliao@bpcs.com/nifi_to_databricks_large_xml/output_results\n"
                f"project=intelligent_feedback_{current}\n"
                f"notebook_path=/Workspace/Users/eliao@bpcs.com/nifi_to_databricks_large_xml/output_results/intelligent_feedback_{current}/main\n"
                "existing_cluster_id=0722-181403-vd3u4c6r\n"
                "run_now=false"
            ),
        }
    ]
)

resp = AGENT.predict(req)
print("✅ Intelligent migration complete!")

# Display the complex migration results using utilities
print("\n📋 COMPLEX MIGRATION RESULTS:")
print("=" * 60)

# Import utility functions for this step
from utils.response_utils import display_agent_response

# # Save complex migration response to JSON
# json_file = save_agent_response_to_json(resp, "complex_workflow_migration.json")

# Display in clean format
display_agent_response(resp)

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

import json

from utils.workflow_summary import print_and_save_workflow_summary

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
# MAGIC ## 🆚 Comparison: Intelligent vs Legacy Migration Methods

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🎯 What's Different About Intelligent Migration?
# MAGIC
# MAGIC | **Legacy Method** | **Intelligent Method** |
# MAGIC |-------------------|-------------------------|
# MAGIC | Hardcoded processor templates | LLM analyzes what each processor actually does |
# MAGIC | Generic "data processing" jobs | Understands business purpose of workflows |
# MAGIC | All workflows treated the same | Distinguishes data transformation vs infrastructure |
# MAGIC | No workflow understanding | Explains workflow in business terms first |
# MAGIC | Manual architecture decisions | AI-recommended architecture patterns |
# MAGIC | Basic text output | Rich markdown reports with insights and recommendations |
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
# MAGIC from utils.nifi_analysis_utils import analyze_workflow_patterns
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
# MAGIC ### **Option 2: Agent-Based Approach (Production Workflows)**
# MAGIC
# MAGIC **Step 1: Let the Agent Analyze Your Workflow**
# MAGIC ```python
# MAGIC req = ResponsesAgentRequest(input=[{
# MAGIC     "role": "user",
# MAGIC     "content": "Use analyze_nifi_workflow_intelligence to analyze /path/to/workflow.xml"
# MAGIC }])
# MAGIC resp = AGENT.predict(req)
# MAGIC ```
# MAGIC
# MAGIC **Step 2: Run Intelligent Migration**
# MAGIC ```python
# MAGIC req = ResponsesAgentRequest(input=[{
# MAGIC     "role": "user",
# MAGIC     "content": "Run orchestrate_intelligent_nifi_migration with xml_path=/path/to/workflow.xml out_dir=/output project=my_project"
# MAGIC }])
# MAGIC resp = AGENT.predict(req)
# MAGIC ```
# MAGIC
# MAGIC ### **What You Get:**
# MAGIC - 🧠 **Business Understanding**: "This workflow does X for business purpose Y"
# MAGIC - 📊 **Smart Classification**: Data transformation vs data movement vs infrastructure processors
# MAGIC - 📄 **Rich Reports**: Professional markdown reports with tables and recommendations
# MAGIC - 🎯 **Architecture Recommendation**: Best Databricks pattern for your workflow
# MAGIC - 🚀 **Intelligent Migration**: Code generated based on actual workflow understanding
# MAGIC - 💡 **Migration Insights**: Focus areas, complexity assessment, and automation potential
# MAGIC
# MAGIC ### **Try Your Own Workflows:**
# MAGIC Replace the file paths above with your NiFi XML files and run the cells!
# MAGIC
# MAGIC ### **Key Benefits of the New Analysis System:**
# MAGIC - **Faster Development**: Direct analysis functions for rapid workflow understanding
# MAGIC - **Better Documentation**: Automatic markdown report generation with professional formatting
# MAGIC - **Smarter Classification**: Type-first approach with SQL operation detection
# MAGIC - **Migration Guidance**: Clear recommendations for Databricks architecture choices

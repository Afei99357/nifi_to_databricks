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
# MAGIC ### 🔍 Step 1: Analyze Simple Workflow (File Ingestion)

# COMMAND ----------

from datetime import datetime

from mlflow.types.responses import ResponsesAgentRequest

from agents import AGENT

print("🧠 INTELLIGENT NIFI ANALYSIS - Simple Workflow")
print("=" * 60)

# Ask the agent to analyze workflow first
req = ResponsesAgentRequest(
    input=[
        {
            "role": "user",
            "content": "Analyze my NiFi workflow at /Volumes/eliao/nifi_to_databricks/nifi_files/nifi_pipeline_eric_1.xml and explain what it does in business terms. What processors actually transform data vs just move files?",
        }
    ]
)

analysis_resp = AGENT.predict(req)
print("✅ Simple workflow analysis complete!")

# Display the analysis results
print("\n📋 ANALYSIS RESULTS:")
print("=" * 60)

# Debug: Let's see what's actually in the response
print("DEBUG: Response structure:")
print(f"Type: {type(analysis_resp)}")
print(f"Has output: {hasattr(analysis_resp, 'output')}")
if hasattr(analysis_resp, "output"):
    print(f"Output length: {len(analysis_resp.output)}")
    for i, item in enumerate(analysis_resp.output):
        print(f"Item {i}: {type(item)}")
        print(f"Item {i} attributes: {dir(item)}")
        if hasattr(item, "content"):
            print(f"Item {i} content: {item.content}")
        if hasattr(item, "type"):
            print(f"Item {i} type: {item.type}")

# Try different ways to extract content
for item in analysis_resp.output:
    print(f"\n--- Processing item of type: {type(item)} ---")

    # Method 1: Direct content access
    if hasattr(item, "content"):
        print("Found content attribute")
        if isinstance(item.content, list):
            for block in item.content:
                if hasattr(block, "text"):
                    print(f"Block text: {block.text}")
                else:
                    print(f"Block: {block}")
        elif hasattr(item.content, "text"):
            print(f"Content text: {item.content.text}")
        elif isinstance(item.content, str):
            print(f"Content string: {item.content}")
        else:
            print(f"Content (other): {item.content}")

    # Method 2: Check for text attribute directly
    if hasattr(item, "text"):
        print(f"Direct text: {item.text}")

    # Method 3: Convert to string representation
    print(f"String representation: {str(item)}")

print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🚀 Step 2: Intelligent Migration of Simple Workflow

# COMMAND ----------

current = datetime.now().strftime("%Y%m%d%H%M%S")

print("🚀 INTELLIGENT MIGRATION - Simple Workflow")
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

# Display the migration results
print("\n📋 SIMPLE MIGRATION RESULTS:")
print("=" * 60)
for item in resp.output:
    if hasattr(item, "content"):
        if isinstance(item.content, list):
            for block in item.content:
                if hasattr(block, "text"):
                    print(block.text)
        elif hasattr(item.content, "text"):
            print(item.content.text)
        elif isinstance(item.content, str):
            print(item.content)
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔍 Step 3: Analyze Complex Workflow (Data Processing Pipeline)

# COMMAND ----------

print("🧠 INTELLIGENT NIFI ANALYSIS - Complex Workflow")
print("=" * 60)

# Analyze the complex ICN8_BRS_Feedback workflow
req = ResponsesAgentRequest(
    input=[
        {
            "role": "user",
            "content": "Analyze my complex NiFi workflow at /Volumes/eliao/nifi_to_databricks/nifi_files/ICN8_BRS_Feedback.xml. This has 58+ processors - help me understand what it actually does for the business and which processors do real data processing vs infrastructure work like logging and routing.",
        }
    ]
)

complex_analysis_resp = AGENT.predict(req)
print("✅ Complex workflow analysis complete!")

# Display the complex analysis results
print("\n📋 COMPLEX ANALYSIS RESULTS:")
print("=" * 60)
for item in complex_analysis_resp.output:
    if hasattr(item, "content"):
        if isinstance(item.content, list):
            for block in item.content:
                if hasattr(block, "text"):
                    print(block.text)
        elif hasattr(item.content, "text"):
            print(item.content.text)
        elif isinstance(item.content, str):
            print(item.content)
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

# Display the complex migration results
print("\n📋 COMPLEX MIGRATION RESULTS:")
print("=" * 60)
for item in resp.output:
    if hasattr(item, "content"):
        if isinstance(item.content, list):
            for block in item.content:
                if hasattr(block, "text"):
                    print(block.text)
        elif hasattr(item.content, "text"):
            print(item.content.text)
        elif isinstance(item.content, str):
            print(item.content)
print("=" * 60)

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
# MAGIC
# MAGIC ### 🧠 Intelligence Features:
# MAGIC - **Business Purpose Analysis**: "This workflow does sensor data collection for analytics"
# MAGIC - **Processor Classification**: Identifies which processors actually transform data vs just logging/routing
# MAGIC - **Architecture Recommendations**: Suggests Jobs vs DLT vs Streaming based on patterns
# MAGIC - **Workflow Insights**: Key insights about complexity, data flow patterns, and processing intent

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
# MAGIC ### **Simple 2-Step Process:**
# MAGIC
# MAGIC **Step 1: Let the Agent Analyze Your Workflow**
# MAGIC ```python
# MAGIC req = ResponsesAgentRequest(input=[{
# MAGIC     "role": "user",
# MAGIC     "content": "Analyze my NiFi workflow at /path/to/workflow.xml"
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
# MAGIC - 🔧 **Smart Classification**: Which processors transform data vs infrastructure
# MAGIC - 🎯 **Architecture Recommendation**: Best Databricks pattern for your workflow
# MAGIC - 🚀 **Intelligent Migration**: Code generated based on actual workflow understanding
# MAGIC
# MAGIC ### **Try Your Own Workflows:**
# MAGIC Replace the file paths above with your NiFi XML files and run the cells!

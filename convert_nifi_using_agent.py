# Databricks notebook source
# MAGIC %md
# MAGIC ### Optional: Initiate the pattern tables from json files

# COMMAND ----------

from pattern_registry import PatternRegistryUC

# Make sure envs point at your UC tables (or pass in ctor):
# PATTERN_TABLE="eliao.nifi_to_databricks.processors"
# COMPLEX_TABLE="eliao.nifi_to_databricks.complex_patterns"

reg = PatternRegistryUC()

# If you have a file:
reg.seed_from_file("/Workspace/Users/eliao@bpcs.com/Agent_for_migrate_nifi_to_databricks/migration_nifi_patterns.json")

# OR, if you have the dict in-memory:
# reg.seed_from_blob(your_dict)


# COMMAND ----------

# MAGIC %pip install -U -qqqq backoff databricks-langchain langgraph==0.5.3 uv databricks-agents mlflow-skinny[databricks]
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from utils.xml_preprocess import summarize_nifi_template
from mlflow.types.responses import ResponsesAgentRequest
from agents import AGENT

xml_summary = summarize_nifi_template(
    "/Volumes/eliao/nifi_to_databricks/nifi_files/ICN8_BRS_Feedback.xml",
    max_nodes=300
)

req = ResponsesAgentRequest(input=[{
    "role": "user",
    "content": (
        "Run orchestrate_nifi_migration with:\n"
        "xml_path=/Volumes/eliao/nifi_to_databricks/nifi_files/ICN8_BRS_Feedback.xml\n"
        "out_dir=/Workspace/Users/eliao@bpcs.com/Agent_for_migrate_nifi_to_databricks/output_results\n"
        "project=nifi2dbx_test_2_feedback\n"
        "job=job_test_feedback\n"
        "notebook_path=/Workspace/Users/eliao@bpcs.com/Agent_for_migrate_nifi_to_databricks/output_results/nifi2dbx_test_2_feedback/main\n"
        "existing_cluster_id=0722-181403-vd3u4c6r\n"
        "deploy=true"
    )
}])

resp = AGENT.predict(req)

# Only print text outputs
for item in resp.output:
    if item.type == "message":
        for block in item.content:
            if block["type"] == "output_text":
                print(block["text"])  # list of output items; the tool's JSON will be in here


# COMMAND ----------

import xml.etree.ElementTree as ET

try:
    ET.parse("/Volumes/eliao/nifi_to_databricks/nifi_files/ICN8_BRS_Feedback.xml")
    print("✅ XML is well-formed")
except ET.ParseError as e:
    print("❌ XML parsing failed:", e)


# COMMAND ----------

from utils.xml_preprocess import summarize_nifi_template

xml_path = "/Volumes/eliao/nifi_to_databricks/nifi_files/ICN8_BRS_Feedback.xml"
summary = summarize_nifi_template(xml_path)
print(summary)


# COMMAND ----------

from mlflow.types.responses import ResponsesAgentRequest
from agents import AGENT

req = ResponsesAgentRequest(input=[{
    "role": "user",
    "content": (
        f"Run orchestrate_nifi_migration using this summarized NiFi XML:\n"
        f"{summary}\n"
        "out_dir=/Workspace/Users/eliao@bpcs.com/Agent_for_migrate_nifi_to_databricks/output_results\n"
        "project=nifi2dbx_feedback\n"
        "job=job_feedback\n"
        "deploy=true"
    )
}])

resp = AGENT.predict(req)
for item in resp.output:
    if item.type == "message":
        for block in item.content:
            if block["type"] == "output_text":
                print(block["text"])

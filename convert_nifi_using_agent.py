# Databricks notebook source
# MAGIC %pip install -U -qqqq backoff databricks-langchain langgraph==0.5.3 uv databricks-agents mlflow-skinny[databricks]

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC Simpler file

# COMMAND ----------

# MAGIC %md
# MAGIC Complex NIFI Workflow

# COMMAND ----------

from mlflow.types.responses import ResponsesAgentRequest
from agents import AGENT

req = ResponsesAgentRequest(input=[{
    "role": "user",
    "content": (
        "Run orchestrate_chunked_nifi_migration with:\n"
        "xml_path=/Volumes/eliao/nifi_to_databricks/nifi_files/nifi_pipeline_eric_embed_groups.xml\n"
        "out_dir=/Workspace/Users/eliao@bpcs.com/nifi_to_databricks_large_xml/output_results\n"
        "project=nifi2dbx_group_test\n"
        "job=job_test_group_test\n"
        "notebook_path=/Workspace/Users/eliao@bpcs.com/nifi_to_databricks_large_xml/output_results/nifi2dbx_group_test/main\n"
        "max_processors_per_chunk=25\n"
        "existing_cluster_id=0722-181403-vd3u4c6r\n"
        "deploy=true"
    )
}])

resp = AGENT.predict(req)

for item in resp.output:
    if item.type == "message":
        for block in item.content:
            if block["type"] == "output_text":
                print(block["text"])

# COMMAND ----------

# MAGIC %md
# MAGIC Complex NIFI Workflow

# COMMAND ----------

from mlflow.types.responses import ResponsesAgentRequest
from agents import AGENT

req = ResponsesAgentRequest(input=[{
    "role": "user",
    "content": (
        "Run orchestrate_chunked_nifi_migration with:\n"
        "xml_path=/Volumes/eliao/nifi_to_databricks/nifi_files/ICN8_BRS_Feedback.xml\n"
        "out_dir=/Workspace/Users/eliao@bpcs.com/nifi_to_databricks_large_xml/output_results\n"
        "project=nifi2dbx_feedback\n"
        "job=job_test_feedback\n"
        "notebook_path=/Workspace/Users/eliao@bpcs.com/nifi_to_databricks_large_xml/output_results/nifi2dbx_feedback/main\n"
        "max_processors_per_chunk=25\n"
        "existing_cluster_id=0722-181403-vd3u4c6r\n"
        "deploy=true"
    )
}])

resp = AGENT.predict(req)

for item in resp.output:
    if item.type == "message":
        for block in item.content:
            if block["type"] == "output_text":
                print(block["text"])

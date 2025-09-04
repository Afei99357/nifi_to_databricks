# Databricks notebook source
# MAGIC %pip install -U -qqqq backoff databricks-langchain uv json-repair

# COMMAND ----------

dbutils.library.restartPython()  # type: ignore

# COMMAND ----------

from datetime import datetime

from tools.simplified_migration import (
    analyze_nifi_workflow_only,
    migrate_nifi_to_databricks_simplified,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚀 Simplified NiFi to Databricks Migration
# MAGIC
# MAGIC This notebook demonstrates the simplified direct function approach:
# MAGIC - **Direct Function Calls**: No agent complexity
# MAGIC - **Linear Pipeline**: analyze → prune → chain → migrate
# MAGIC - **Complete Migration**: Generates production-ready Databricks assets

# COMMAND ----------

# MAGIC %md
# MAGIC ### Complete NiFi to Databricks Migration Example
# MAGIC
# MAGIC This example shows the complete migration process from a NiFi XML file to production-ready Databricks assets.

# COMMAND ----------

# Configuration
current = datetime.now().strftime("%Y%m%d%H%M%S")
xml_path = "/Volumes/eliao/nifi_to_databricks/nifi_files/query_configuration_frank.xml"
output_dir = (
    "/Workspace/Users/eliao@bpcs.com/nifi_to_databricks_large_xml/output_results"
)
project_name = f"nifi_migration_{current}"

print("🚀 COMPLETE NIFI TO DATABRICKS MIGRATION")
print("=" * 60)
print(f"📁 Input: {xml_path}")
print(f"📂 Output: {output_dir}/{project_name}")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Complete Migration
# MAGIC
# MAGIC Execute the full migration pipeline: analyze → prune → chain → migrate

# COMMAND ----------

print("🚀 COMPLETE MIGRATION PIPELINE")
print("-" * 40)

# Execute complete migration with all intelligence features
migration_result = migrate_nifi_to_databricks_simplified(
    xml_path=xml_path,
    out_dir=output_dir,
    project=project_name,
    notebook_path=f"{output_dir}/{project_name}/main",
    # Note: Now generates comprehensive migration guide instead of deployable jobs
)

print("✅ MIGRATION COMPLETED SUCCESSFULLY!")
print("=" * 60)
# type: ignore

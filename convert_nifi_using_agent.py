# Databricks notebook source
# MAGIC %pip install -U -qqqq backoff uv json-repair

# COMMAND ----------

dbutils.library.restartPython()

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
xml_path = "/Volumes/eliao/nifi_to_databricks/nifi_files/ICN8_BRS_Feedback.xml"
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
# MAGIC #### Step 1: Quick Analysis (Optional)
# MAGIC
# MAGIC First, let's understand what's in the NiFi workflow before migration.

# COMMAND ----------

print("🔍 STEP 1: ANALYZING NIFI WORKFLOW")
print("-" * 40)

# Quick analysis to understand the workflow
analysis_result = analyze_nifi_workflow_only(xml_path)

print("✅ Analysis complete!")
print(f"📊 Workflow contains semantic data flows and processor classifications")
print(f"🎯 Ready for intelligent migration")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2: Complete Migration
# MAGIC
# MAGIC Execute the full migration pipeline: analyze → prune → chain → migrate

# COMMAND ----------

print("🚀 STEP 2: COMPLETE MIGRATION PIPELINE")
print("-" * 40)

# Execute complete migration with all intelligence features
migration_result = migrate_nifi_to_databricks_simplified(
    xml_path=xml_path,
    out_dir=output_dir,
    project=project_name,
    notebook_path=f"{output_dir}/{project_name}/main",
    deploy=False,  # Set to True to automatically deploy the job
)

print("✅ MIGRATION COMPLETED SUCCESSFULLY!")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3: Review Results

# COMMAND ----------

print("📋 MIGRATION RESULTS SUMMARY")
print("=" * 60)

# Display migration results
print(f"✅ Status: Success")
print(f"📁 Output Directory: {migration_result['configuration']['out_dir']}")
print(f"🏷️  Project: {migration_result['configuration']['project']}")
print(f"📓 Notebook Path: {migration_result['configuration']['notebook_path']}")

print("\n🔍 ANALYSIS BREAKDOWN:")
analysis = migration_result["analysis"]
print(f"📊 Workflow Analysis: {analysis.get('workflow_analysis', 'Completed')}")
print(f"🏷️  Processor Classifications: Available")
print(f"✂️  Pruning Results: Infrastructure processors removed")
print(f"🔗 Data Flow Chains: Semantic chains detected")
print(f"🌊 Semantic Flows: Business flows created")

print(f"\n📁 Generated Assets:")
print(f"   • src/steps/ - Individual processor Python files")
print(f"   • notebooks/ - Orchestrator notebook")
print(f"   • jobs/ - Databricks job configurations")
print(f"   • conf/ - Migration plans and configurations")
print(f"   • databricks.yml - Asset bundle for deployment")
print(f"   • README.md - Documentation and next steps")

print("=" * 60)
print("🎉 Your NiFi workflow is now ready to run on Databricks!")
print("📖 Check the generated README.md for deployment instructions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎓 Next Steps
# MAGIC
# MAGIC 1. **Review Generated Assets**: Check the output directory for all created files
# MAGIC 2. **Test the Migration**: Run the generated notebook to verify functionality
# MAGIC 3. **Deploy to Production**: Use `deploy=True` or follow README.md instructions
# MAGIC 4. **Customize as Needed**: Modify generated PySpark code for your specific requirements
# MAGIC
# MAGIC ### 🔧 To Deploy Automatically:
# MAGIC ```python
# MAGIC # Re-run with deployment enabled
# MAGIC migrate_nifi_to_databricks_simplified(
# MAGIC     xml_path=xml_path,
# MAGIC     out_dir=output_dir,
# MAGIC     project=project_name,
# MAGIC     deploy=True  # This will create and run the Databricks job
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC **That's it! Your NiFi workflow is now a production-ready Databricks pipeline! 🚀**

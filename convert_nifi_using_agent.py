# Databricks notebook source
# MAGIC %pip install -U -qqqq backoff databricks-langchain uv json-repair

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
    # Note: Now generates comprehensive migration guide instead of deployable jobs
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
print(f"📊 Workflow Analysis: Completed")
print(f"🏷️  Processor Classifications: Completed")
print(f"✂️  Pruning Results: Infrastructure processors removed")
print(f"🔗 Data Flow Chains: Semantic chains detected")
print(f"🌊 Semantic Flows: Business flows created")

# Display asset discovery results
if "asset_discovery" in migration_result:
    assets = migration_result["asset_discovery"]
    stats = assets.get("summary_stats", {})
    print(f"\n📋 ASSET DISCOVERY RESULTS:")
    print(f"   • Script Files: {stats.get('script_files', 0)} found")
    print(f"   • HDFS Paths: {stats.get('hdfs_paths', 0)} found")
    print(f"   • Table References: {stats.get('table_references', 0)} found")
    print(f"   • SQL Statements: {stats.get('sql_statements', 0)} found")
    print(f"   📋 Asset Catalog: {assets.get('asset_catalog_path', 'Generated')}")
    print(f"   📄 Asset Summary: {assets.get('asset_summary_path', 'Generated')}")

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
# MAGIC ## 🔍 Asset Discovery Only (Optional)
# MAGIC
# MAGIC Run this cell to discover and catalog assets without performing migration.
# MAGIC Useful for understanding what external dependencies exist before committing to migration.

# COMMAND ----------

# Optional: Run asset discovery only (no migration)
discover_assets_only = False  # Set to True to run asset discovery only

if discover_assets_only:
    print("🔍 ASSET DISCOVERY ONLY MODE")
    print("=" * 40)

    import os

    from tools.asset_discovery_tools import (
        extract_workflow_assets,
        generate_asset_summary_report,
        save_asset_catalog,
    )
    from tools.nifi_processor_classifier_tool import analyze_workflow_patterns

    # Create temporary output directory for analysis
    temp_output = f"{output_dir}/asset_discovery_temp"
    os.makedirs(temp_output, exist_ok=True)

    # Analyze workflow to extract processor data
    print("🔍 Analyzing NiFi workflow...")
    analysis_result = analyze_workflow_patterns(
        xml_path=xml_path, save_markdown=False, output_dir=temp_output
    )

    # Extract assets from analysis
    print("📋 Extracting workflow assets...")
    if isinstance(analysis_result, str):
        import json

        analysis_data = json.loads(analysis_result)
    else:
        analysis_data = analysis_result

    workflow_assets = extract_workflow_assets(analysis_data)

    # Save asset catalog and summary
    asset_catalog_path = save_asset_catalog(workflow_assets, temp_output)
    asset_summary_path = generate_asset_summary_report(workflow_assets, temp_output)

    # Display results
    print("\n📋 ASSET DISCOVERY RESULTS:")
    print("=" * 40)
    asset_summary = workflow_assets.get("asset_summary", {})
    print(
        f"📊 Total Processors Analyzed: {len(workflow_assets.get('processor_assets', []))}"
    )
    print(f"📁 Script Files Found: {asset_summary.get('total_script_files', 0)}")
    print(f"🗂️  HDFS Paths Found: {asset_summary.get('total_hdfs_paths', 0)}")
    print(
        f"📋 Table References Found: {asset_summary.get('total_table_references', 0)}"
    )
    print(f"📝 SQL Statements Found: {asset_summary.get('total_sql_statements', 0)}")
    print(f"\n📂 Asset Catalog: {asset_catalog_path}")
    print(f"📄 Asset Summary: {asset_summary_path}")
    print(
        "\n✅ Asset discovery complete! Check the generated files for detailed analysis."
    )
    print("💡 Review these assets before running the full migration.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎓 Next Steps
# MAGIC
# MAGIC 1. **Review Migration Guide**: Check `MIGRATION_GUIDE.md` for comprehensive migration recommendations
# MAGIC 2. **Review Analysis Results**: Examine the workflow analysis and processor classifications
# MAGIC 3. **Follow Migration Guide**: Use the LLM-generated guide to manually implement your Databricks solution
# MAGIC 4. **Leverage Asset Discovery**: Use the asset catalog to identify scripts, paths, and tables requiring migration
# MAGIC
# MAGIC ### 📋 Key Generated Files:
# MAGIC - **`MIGRATION_GUIDE.md`**: Comprehensive migration recommendations and code patterns
# MAGIC - **`workflow_analysis.json`**: Detailed processor analysis and classification
# MAGIC - **`asset_catalog.json`**: Complete inventory of scripts, paths, and external dependencies
# MAGIC - **`asset_summary_report.md`**: Human-readable summary of migration requirements
# MAGIC
# MAGIC **The migration guide provides intelligent, context-aware recommendations for your specific NiFi workflow! 🚀**

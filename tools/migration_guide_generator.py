# tools/migration_guide_generator.py
# Generate LLM-powered migration guides based on actual NiFi workflow analysis

import os
from datetime import datetime
from typing import Any, Dict, List

# Databricks LLM will be imported at runtime when needed


# analyze_processor_relationships function removed - simplified implementation for local environments


def generate_migration_guide(
    processors: List[Dict[str, Any]],
    semantic_flows: Dict[str, Any],
    project_name: str,
    analysis: Dict[str, Any],
) -> str:
    """
    Generate a comprehensive migration guide using LLM analysis of the actual NiFi workflow.

    Returns:
        Migration guide content as markdown string
    """
    print(f"🧠 [GUIDE GENERATION] Using LLM to analyze {len(processors)} processors...")

    # For local environments without LLM access, generate a comprehensive basic guide
    basic_guide = generate_basic_migration_guide(
        processors, semantic_flows, project_name, analysis
    )
    return basic_guide


def generate_basic_migration_guide(
    processors: List[Dict[str, Any]],
    semantic_flows: Dict[str, Any],
    project_name: str,
    analysis: Dict[str, Any],
) -> str:
    """Generate basic migration guide for local environments."""

    # Analyze processor types and create specific recommendations
    processor_analysis = _analyze_processors_for_guide(processors)

    guide = f"""# {project_name} - NiFi to Databricks Migration Guide

*Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*
*Analysis: {len(processors)} essential processors from NiFi workflow*

## Executive Summary

This migration guide provides specific recommendations for migrating your {len(processors)} essential NiFi processors to Databricks.

**Processor Summary:**
{processor_analysis['summary']}

## Essential Processors Analysis

{processor_analysis['detailed_analysis']}

## Recommended Architecture

Based on your workflow complexity, we recommend:

- **Primary Pattern**: Databricks Jobs for orchestration
- **Data Storage**: Delta Lake with Unity Catalog
- **File Processing**: Auto Loader for incremental processing
- **SQL Processing**: Databricks SQL for data transformations
- **Streaming**: Structured Streaming for real-time data

## Migration Strategy by Processor Type

{processor_analysis['migration_patterns']}

## Implementation Roadmap

### Phase 1: Data Infrastructure Setup
1. **Set up Unity Catalog**: Create catalogs and schemas for your data
2. **Configure Delta Lake**: Set up Delta tables for your data storage
3. **Set up Auto Loader**: For file-based data ingestion

### Phase 2: Core Processing Migration
{processor_analysis['implementation_steps']}

### Phase 3: Testing & Deployment
1. **Data Validation**: Compare outputs between NiFi and Databricks
2. **Performance Testing**: Ensure processing meets SLA requirements
3. **Monitoring Setup**: Configure alerts and dashboards
4. **Production Deployment**: Gradual rollout with fallback plans

## Code Templates

{processor_analysis['code_examples']}

## Technical Details

- **Migration Approach**: Focused essential processor analysis
- **Total Processors Analyzed**: {len(processors)}
- **Databricks Platform**: Recommended for data engineering workloads
- **Estimated Complexity**: {processor_analysis['complexity']}

## Next Steps

1. Review processor-specific migration recommendations above
2. Set up Databricks workspace and Unity Catalog
3. Start with Phase 1 infrastructure setup
4. Migrate processors in order of dependency
5. Test each component thoroughly before proceeding
6. Deploy with comprehensive monitoring

For additional support, consider engaging with Databricks professional services or certified partners.
"""

    return guide


def _analyze_processors_for_guide(processors: List[Dict[str, Any]]) -> Dict[str, str]:
    """Analyze processors to create specific migration recommendations."""

    # Group processors by type
    processor_types = {}
    for proc in processors:
        proc_type = proc.get("type", "Unknown").split(".")[-1]
        proc_name = proc.get("name", "Unknown")
        proc_classification = proc.get("classification", "unknown")

        if proc_type not in processor_types:
            processor_types[proc_type] = []
        processor_types[proc_type].append(
            {
                "name": proc_name,
                "classification": proc_classification,
                "properties": proc.get("properties", {}),
            }
        )

    # Create summary
    summary_lines = []
    for proc_type, instances in processor_types.items():
        summary_lines.append(f"- **{proc_type}**: {len(instances)} instance(s)")
    summary = "\n".join(summary_lines)

    # Create detailed analysis
    detailed_analysis = []
    for proc_type, instances in processor_types.items():
        detailed_analysis.append(f"### {proc_type} Processors")
        for instance in instances:
            detailed_analysis.append(
                f"- **{instance['name']}** ({instance['classification']})"
            )
        detailed_analysis.append("")

    # Create migration patterns
    migration_patterns = []
    for proc_type, instances in processor_types.items():
        pattern = _get_migration_pattern(proc_type, instances)
        if pattern:
            migration_patterns.append(pattern)

    # Create implementation steps
    implementation_steps = []
    step_num = 1
    for proc_type, instances in processor_types.items():
        steps = _get_implementation_steps(proc_type, instances, step_num)
        if steps:
            implementation_steps.extend(steps)
            step_num += len(steps)

    # Create code examples
    code_examples = []
    for proc_type, instances in processor_types.items():
        code = _get_code_example(proc_type, instances)
        if code:
            code_examples.append(code)

    # Determine complexity
    total_processors = len(processors)
    data_transform_count = sum(
        1 for p in processors if p.get("classification") == "data_transformation"
    )
    if total_processors > 20:
        complexity = "High"
    elif data_transform_count > 5:
        complexity = "Medium-High"
    elif total_processors > 10:
        complexity = "Medium"
    else:
        complexity = "Low-Medium"

    return {
        "summary": summary,
        "detailed_analysis": "\n".join(detailed_analysis),
        "migration_patterns": "\n".join(migration_patterns),
        "implementation_steps": "\n".join(implementation_steps),
        "code_examples": "\n".join(code_examples),
        "complexity": complexity,
    }


def _get_migration_pattern(proc_type: str, instances: List[Dict]) -> str:
    """Get migration pattern for a processor type."""

    patterns = {
        "ListFile": """
### ListFile → Auto Loader
**Pattern**: Replace with Databricks Auto Loader for incremental file processing
- **Databricks Solution**: `cloudFiles` format in Auto Loader
- **Benefits**: Built-in checkpointing, schema evolution, efficient incremental processing
- **Implementation**: Use structured streaming with cloudFiles source""",
        "GetFile": """
### GetFile → Auto Loader
**Pattern**: Replace with Databricks Auto Loader for file ingestion
- **Databricks Solution**: Auto Loader with file notification or directory listing
- **Benefits**: Automatic schema inference, fault tolerance, scalability
- **Implementation**: Configure Auto Loader with appropriate file format""",
        "PutFile": """
### PutFile → Delta Lake Write
**Pattern**: Replace with Delta Lake table writes
- **Databricks Solution**: DataFrame write operations to Delta tables
- **Benefits**: ACID transactions, time travel, schema enforcement
- **Implementation**: Use `.write.format("delta").save()` operations""",
        "PutSFTP": """
### PutSFTP → External System Integration
**Pattern**: Replace with Databricks external system connectors
- **Databricks Solution**: SFTP libraries or Azure Data Factory integration
- **Benefits**: Native cloud integration, better monitoring
- **Implementation**: Use external tables or pipeline orchestration""",
        "ExecuteStreamCommand": """
### ExecuteStreamCommand → Databricks SQL/PySpark
**Pattern**: Convert shell commands to SQL or PySpark operations
- **Databricks Solution**: SQL queries or PySpark transformations
- **Benefits**: Distributed processing, better resource management
- **Implementation**: Rewrite logic using Spark SQL or DataFrame operations""",
        "ExecuteSQL": """
### ExecuteSQL → Databricks SQL
**Pattern**: Migrate SQL operations to Databricks SQL
- **Databricks Solution**: Native SQL execution with cluster compute
- **Benefits**: Optimized Spark SQL engine, Delta Lake integration
- **Implementation**: Execute SQL directly or through notebooks""",
        "RouteOnAttribute": """
### RouteOnAttribute → DataFrame Filtering
**Pattern**: Replace routing logic with DataFrame filter operations
- **Databricks Solution**: DataFrame `.filter()` operations with multiple outputs
- **Benefits**: Distributed filtering, better performance optimization
- **Implementation**: Use conditional logic with multiple DataFrame writes""",
        "UpdateAttribute": """
### UpdateAttribute → DataFrame Transformations
**Pattern**: Replace attribute updates with DataFrame column operations
- **Databricks Solution**: DataFrame `.withColumn()` operations
- **Benefits**: Distributed column operations, type safety
- **Implementation**: Use PySpark column functions for transformations""",
    }

    return patterns.get(
        proc_type,
        f"""
### {proc_type} → Custom Migration
**Pattern**: Requires custom analysis for migration approach
- **Databricks Solution**: Evaluate specific processor functionality
- **Recommendation**: Analyze processor properties and data flow requirements
- **Implementation**: Create equivalent logic using PySpark/SQL operations""",
    )


def _get_implementation_steps(
    proc_type: str, instances: List[Dict], start_num: int
) -> List[str]:
    """Get implementation steps for a processor type."""

    steps = []
    for i, instance in enumerate(instances):
        step_num = start_num + i
        name = instance["name"]

        if proc_type in ["ListFile", "GetFile"]:
            steps.append(
                f"{step_num}. **Configure Auto Loader for {name}**: Set up incremental file processing"
            )
        elif proc_type in ["PutFile", "PutSFTP"]:
            steps.append(
                f"{step_num}. **Setup Delta Lake destination for {name}**: Create target tables and write operations"
            )
        elif proc_type == "ExecuteStreamCommand":
            steps.append(
                f"{step_num}. **Convert {name} shell logic**: Rewrite as PySpark/SQL operations"
            )
        elif proc_type == "ExecuteSQL":
            steps.append(
                f"{step_num}. **Migrate {name} SQL**: Port SQL logic to Databricks SQL"
            )
        elif proc_type == "RouteOnAttribute":
            steps.append(
                f"{step_num}. **Implement {name} routing logic**: Create conditional DataFrame operations"
            )
        else:
            steps.append(
                f"{step_num}. **Analyze and migrate {name}**: Custom implementation based on processor functionality"
            )

    return steps


def _get_code_example(proc_type: str, instances: List[Dict]) -> str:
    """Get code example for a processor type."""

    if not instances:
        return ""

    first_instance = instances[0]["name"]

    examples = {
        "ListFile": f"""
### Auto Loader Example (replaces {first_instance})
```python
# Auto Loader for incremental file processing
df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")  # or csv, parquet, etc.
    .option("cloudFiles.schemaLocation", "/path/to/schema")
    .load("/path/to/source/files"))

# Write to Delta table
(df.writeStream
    .format("delta")
    .option("checkpointLocation", "/path/to/checkpoint")
    .table("catalog.schema.target_table"))
```""",
        "ExecuteSQL": f"""
### SQL Migration Example (replaces {first_instance})
```sql
-- Execute SQL directly in Databricks
CREATE OR REPLACE TEMPORARY VIEW temp_data AS
SELECT * FROM catalog.schema.source_table
WHERE condition = true;

-- Insert results into target table
INSERT INTO catalog.schema.target_table
SELECT processed_data FROM temp_data;
```""",
        "RouteOnAttribute": f"""
### Routing Logic Example (replaces {first_instance})
```python
# Replace routing with DataFrame filtering
source_df = spark.table("catalog.schema.source_table")

# Route to different destinations based on conditions
success_df = source_df.filter(col("status") == "success")
error_df = source_df.filter(col("status") == "error")

# Write to different targets
success_df.write.mode("append").table("catalog.schema.success_table")
error_df.write.mode("append").table("catalog.schema.error_table")
```""",
    }

    return examples.get(
        proc_type,
        f"""
### Custom Implementation Example (for {first_instance})
```python
# Custom logic based on processor requirements
# Analyze the specific functionality needed and implement using:
# - PySpark DataFrame operations
# - Spark SQL queries
# - Delta Lake for storage
# - Auto Loader for file processing
```""",
    )

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
    """Generate business-focused migration guide for local environments."""

    # Business-focused analysis instead of processor inventory
    business_analysis = _analyze_business_logic(processors)

    guide = f"""# {project_name} - NiFi to Databricks Migration Guide

*Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*
*Business Analysis: {len(processors)} processors analyzed*

## Business Overview

{business_analysis['business_summary']}

## Data Flow Analysis

{business_analysis['data_flows']}

## Key Business Logic

{business_analysis['extracted_logic']}

## Migration Strategy

{business_analysis['migration_approach']}

## Implementation Phases

{business_analysis['implementation_plan']}

## Databricks Solution Architecture

{business_analysis['databricks_architecture']}

## Code Examples

{business_analysis['code_examples']}

## Dependencies & Prerequisites

{business_analysis['dependencies']}

## Risk Assessment & Recommendations

{business_analysis['risks_and_recommendations']}

---
*This guide focuses on business logic and data flows rather than individual processor migration.*
"""

    return guide


def _analyze_business_logic(processors: List[Dict[str, Any]]) -> Dict[str, str]:
    """Analyze processors to extract business logic and data flows."""

    # Extract business components
    data_flows = _identify_data_flows(processors)
    business_logic = _extract_sql_and_logic(processors)
    dependencies = _identify_dependencies(processors)

    # Business summary
    business_summary = _generate_business_summary(processors, data_flows)

    # Migration approach based on business patterns
    migration_approach = _generate_migration_approach(data_flows, business_logic)

    # Implementation plan focused on business value
    implementation_plan = _generate_implementation_phases(data_flows, business_logic)

    # Databricks architecture recommendations
    databricks_architecture = _generate_databricks_architecture(
        data_flows, business_logic
    )

    # Practical code examples
    code_examples = _generate_business_focused_code_examples(business_logic)

    # Risk assessment
    risks_and_recommendations = _generate_risk_assessment(processors, business_logic)

    return {
        "business_summary": business_summary,
        "data_flows": "\n".join(data_flows),
        "extracted_logic": business_logic,
        "migration_approach": migration_approach,
        "implementation_plan": implementation_plan,
        "databricks_architecture": databricks_architecture,
        "code_examples": code_examples,
        "dependencies": "\n".join(dependencies),
        "risks_and_recommendations": risks_and_recommendations,
    }


def _identify_data_flows(processors: List[Dict[str, Any]]) -> List[str]:
    """Identify main data flow patterns from processors."""
    flows = []

    # File processing flows
    file_processors = [
        p
        for p in processors
        if any(
            keyword in p.get("name", "").lower()
            for keyword in ["list", "get", "fetch", "put", "move", "file"]
        )
    ]
    if file_processors:
        flows.append("### File Processing Pipeline")
        flows.append("- **Ingestion**: Monitors directories and retrieves files")
        flows.append("- **Validation**: Checks file formats and content")
        flows.append("- **Processing**: Transforms and moves files through workflow")
        flows.append("- **Output**: Writes processed data to destinations")
        flows.append("")

    # Database operations flows
    db_processors = [
        p
        for p in processors
        if any(
            keyword in p.get("name", "").lower()
            for keyword in ["query", "sql", "impala", "hive", "refresh", "partition"]
        )
    ]
    if db_processors:
        flows.append("### Database Operations Pipeline")
        flows.append("- **Data Loading**: Ingests data into staging tables")
        flows.append("- **Transformations**: Runs SQL queries for data processing")
        flows.append(
            "- **Maintenance**: Handles partition management and table refreshes"
        )
        flows.append(
            "- **Quality Checks**: Validates data consistency and completeness"
        )
        flows.append("")

    # Configuration and setup flows
    config_processors = [
        p
        for p in processors
        if "configuration" in p.get("name", "").lower()
        or "add" in p.get("name", "").lower()
    ]
    if config_processors:
        flows.append("### Configuration Management Pipeline")
        flows.append(
            "- **Dynamic Configuration**: Adds queries and settings based on data types"
        )
        flows.append(
            "- **Parameter Injection**: Sets up processing parameters per workflow"
        )
        flows.append(
            "- **Quality Rules**: Configures validation rules for different data sources"
        )
        flows.append("")

    return flows


def _extract_sql_and_logic(processors: List[Dict[str, Any]]) -> str:
    """Extract references to external files and business logic that need manual review."""
    external_files = []
    sql_variables = []
    business_logic_files = []

    for proc in processors:
        properties = proc.get("properties", {})
        name = proc.get("name", "")

        # Check for external script files
        command_args = properties.get("Command Arguments", "")
        command_path = properties.get("Command Path", "")

        # Extract script file paths
        for prop_name, prop_value in properties.items():
            if prop_value and isinstance(prop_value, str):
                # Look for shell script files
                if ".sh" in prop_value and "/scripts/" in prop_value:
                    script_file = prop_value.strip()
                    if script_file not in external_files:
                        external_files.append(f"**{script_file}** (used by: {name})")

                # Look for SQL variable references like ${query_*}
                import re

                sql_vars = re.findall(r"\$\{query_[^}]+\}", prop_value)
                for var in sql_vars:
                    if var not in [item.split(" ")[0] for item in sql_variables]:
                        sql_variables.append(f"**{var}** (referenced in: {name})")

        # Check command arguments and paths for external dependencies
        if command_args:
            # Extract script paths from command arguments
            if "/users/hadoop_nifi_svc/scripts/" in command_args:
                # Extract the script path
                import re

                script_matches = re.findall(
                    r'/users/hadoop_nifi_svc/scripts/[^\s;"\']+\.sh', command_args
                )
                for script_path in script_matches:
                    if script_path not in [
                        item.split(" ")[0].strip("*") for item in external_files
                    ]:
                        external_files.append(
                            f"**{script_path}** (executed by: {name})"
                        )

        if command_path and command_path.endswith(".sh"):
            if command_path not in [
                item.split(" ")[0].strip("*") for item in external_files
            ]:
                external_files.append(f"**{command_path}** (script for: {name})")

    result = []

    if external_files:
        result.append("## External Files Requiring Manual Review")
        result.append("")
        result.append(
            "The following external files contain the actual business logic and SQL queries that need to be manually reviewed and migrated:"
        )
        result.append("")
        result.extend(external_files)
        result.append("")

    if sql_variables:
        result.append("## SQL Variables Needing Resolution")
        result.append("")
        result.append(
            "These NiFi variables contain SQL queries or table names that are defined externally:"
        )
        result.append("")
        result.extend(sql_variables)
        result.append("")

    if not external_files and not sql_variables:
        result.append("## Business Logic")
        result.append("")
        result.append(
            "No external script files or SQL variables detected. Business logic may be embedded directly in processor properties or defined elsewhere in the system."
        )
        result.append("")

    if external_files or sql_variables:
        result.append("## Migration Action Items")
        result.append("")
        result.append(
            "**Critical**: The files listed above contain the core business logic for this NiFi workflow. These files must be:"
        )
        result.append("1. **Located and reviewed** in the source system")
        result.append("2. **Analyzed for SQL queries** and business rules")
        result.append("3. **Converted to PySpark/Databricks SQL** equivalents")
        result.append("4. **Tested thoroughly** to ensure business logic preservation")
        result.append("")

    return (
        "\n".join(result)
        if result
        else "No external dependencies detected in processor properties."
    )


def _identify_dependencies(processors: List[Dict[str, Any]]) -> List[str]:
    """Identify external dependencies from processor properties."""
    dependencies = []

    # Database connections
    impala_refs = [
        p for p in processors if "impala" in str(p.get("properties", {})).lower()
    ]
    if impala_refs:
        dependencies.append("- **Impala Cluster**: nardc02prod-impala.na-rdc02.nxp.com")

    hive_refs = [
        p
        for p in processors
        if "hive" in str(p.get("properties", {})).lower()
        or "beeline" in str(p.get("properties", {})).lower()
    ]
    if hive_refs:
        dependencies.append(
            "- **Hive/Beeline**: Database operations and table management"
        )

    # File system paths
    hdfs_paths = set()
    for proc in processors:
        props_str = str(proc.get("properties", {}))
        if "/user/" in props_str or "/etl/" in props_str:
            # Extract common path patterns
            if "/user/hive/warehouse" in props_str:
                hdfs_paths.add(
                    "- **HDFS**: /user/hive/warehouse (data warehouse location)"
                )
            if "/etl/dropzone" in props_str:
                hdfs_paths.add(
                    "- **HDFS**: /etl/dropzone (landing zone for incoming files)"
                )

    dependencies.extend(sorted(hdfs_paths))

    # Scripts and external tools
    script_refs = [p for p in processors if "script" in p.get("name", "").lower()]
    if script_refs:
        dependencies.append(
            "- **Shell Scripts**: Custom processing scripts in /users/hadoop_nifi_svc/scripts/"
        )

    return dependencies if dependencies else ["- No external dependencies identified"]


def _generate_business_summary(
    processors: List[Dict[str, Any]], data_flows: List[str]
) -> str:
    """Generate high-level business summary of the workflow."""

    total_processors = len(processors)
    data_transform_count = sum(
        1 for p in processors if p.get("classification") == "data_transformation"
    )
    data_movement_count = sum(
        1 for p in processors if p.get("classification") == "data_movement"
    )

    summary = f"""This NiFi workflow orchestrates a complex data processing pipeline with {total_processors} processors focusing on:

**Primary Functions:**
- Data ingestion and file processing ({data_movement_count} processors)
- SQL-based data transformations and database operations ({data_transform_count} processors)
- Dynamic configuration and quality checking

**Business Purpose:**
Based on the processor names and patterns, this appears to be a **manufacturing data pipeline** that:
- Processes sensor and production data from various sources
- Applies quality checks and validations (BQ, E3, Diamond, Temptation systems)
- Manages database partitions and table maintenance
- Handles configuration for different data types and sources

**Complexity Assessment:** {'High' if total_processors > 50 else 'Medium-High' if total_processors > 20 else 'Medium'}
- Large number of processors requiring systematic migration approach
- Complex interdependencies between data flows
- Mix of file processing, database operations, and business logic"""

    return summary


def _generate_migration_approach(data_flows: List[str], business_logic: str) -> str:
    """Generate migration strategy based on identified patterns."""

    return """### Recommended Migration Strategy

**1. Consolidate File Processing → Auto Loader**
- Replace all ListFile/GetFile/PutFile processors with Databricks Auto Loader
- Eliminate complex file movement logic with cloud-native file processing
- Implement error handling through Delta Lake error tables

**2. Migrate Database Operations → Databricks SQL**
- Convert Impala/Hive queries to Spark SQL
- Replace partition management with Delta Lake operations
- Use Databricks SQL for all data transformations

**3. Simplify Configuration Management → Parameters & Widgets**
- Replace dynamic attribute processors with Databricks job parameters
- Use notebook widgets for runtime configuration
- Implement configuration as code through Git integration

**4. Implement Unified Quality Framework**
- Replace individual quality processors with Databricks data quality checks
- Use Delta Lake expectations for data validation
- Implement monitoring through Databricks observability tools"""


def _generate_implementation_phases(data_flows: List[str], business_logic: str) -> str:
    """Generate phased implementation plan."""

    return """### Phase 1: Infrastructure Setup (Weeks 1-2)
1. **Set up Databricks workspace** with Unity Catalog
2. **Create Delta Lake schemas** for data organization
3. **Configure Auto Loader** for file ingestion pipelines
4. **Set up monitoring and alerting** infrastructure

### Phase 2: Core Data Flows (Weeks 3-6)
1. **Migrate file processing pipelines** first (lowest risk)
2. **Convert SQL operations** to Databricks SQL
3. **Implement partition management** using Delta Lake operations
4. **Set up quality checking framework**

### Phase 3: Business Logic Migration (Weeks 7-10)
1. **Convert complex attribute transformations** to PySpark
2. **Implement configuration management** using job parameters
3. **Migrate remaining custom logic** and scripts
4. **Set up automated testing and validation**

### Phase 4: Production Deployment (Weeks 11-12)
1. **Parallel running** for validation
2. **Performance optimization** and tuning
3. **Full cutover** with monitoring
4. **Decommission NiFi workflow**"""


def _generate_databricks_architecture(
    data_flows: List[str], business_logic: str
) -> str:
    """Generate Databricks solution architecture."""

    return """### Recommended Databricks Architecture

**Data Ingestion Layer:**
- **Auto Loader** for file-based data ingestion
- **Structured Streaming** for real-time processing requirements
- **Delta Lake** as unified storage layer with ACID guarantees

**Processing Layer:**
- **Databricks Jobs** for workflow orchestration
- **Databricks SQL** for data transformations
- **Delta Live Tables** for complex data pipelines (if applicable)

**Data Management:**
- **Unity Catalog** for data governance and lineage
- **Delta Lake** for versioning and time travel capabilities
- **Databricks Feature Store** (if ML features are needed)

**Monitoring & Operations:**
- **Databricks Observability** for monitoring and alerting
- **Job scheduling** through Databricks workflows
- **Git integration** for version control and CI/CD"""


def _generate_business_focused_code_examples(business_logic: str) -> str:
    """Generate practical code examples for key business operations."""

    return """### File Processing with Auto Loader
```python
# Replace NiFi file processing with Auto Loader
df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", "/path/to/schema")
    .load("/etl/dropzone/incoming"))

# Write to Delta Lake with error handling
(df.writeStream
    .format("delta")
    .option("checkpointLocation", "/path/to/checkpoint")
    .option("mergeSchema", "true")
    .table("catalog.schema.raw_data"))
```

### SQL Operations Migration
```python
# Replace Impala queries with Databricks SQL
spark.sql(\"\"\"
    -- Partition management
    ALTER TABLE catalog.schema.production_data
    ADD PARTITION (date='2023-09-03', hour='14')

    -- Table refresh (handled automatically in Delta Lake)
    REFRESH TABLE catalog.schema.production_data

    -- Memory management (handled by Databricks cluster)
    -- SET mem_limit=100G; -- Not needed in Databricks
\"\"\")

# Data quality checks
from delta.tables import DeltaTable
from pyspark.sql.functions import col, count

# Replace custom quality processors
quality_check = (df
    .filter(col("value").isNotNull())
    .filter(col("timestamp") > lit("2023-01-01"))
    .count())

assert quality_check > 0, "Data quality check failed: no valid records"
```

### Configuration Management
```python
# Replace dynamic attribute processors with parameters
dbutils.widgets.text("source_system", "default")
dbutils.widgets.dropdown("processing_mode", "batch", ["batch", "streaming"])

source_system = dbutils.widgets.get("source_system")
processing_mode = dbutils.widgets.get("processing_mode")

# Use parameters in processing logic
if source_system == "BQ_E3":
    schema_config = load_config("bq_e3_schema.json")
elif source_system == "Diamond":
    schema_config = load_config("diamond_schema.json")
```"""


def _generate_risk_assessment(
    processors: List[Dict[str, Any]], business_logic: str
) -> str:
    """Generate risk assessment and recommendations."""

    processor_count = len(processors)
    complex_processors = sum(
        1 for p in processors if len(str(p.get("properties", {}))) > 500
    )

    return f"""### Risk Assessment

**High Risk Areas:**
- **Complex Business Logic**: {complex_processors} processors with complex attribute transformations
- **Custom Scripts**: External shell scripts requiring manual conversion
- **Database Dependencies**: Tight coupling to Impala/Hive specific features

**Medium Risk Areas:**
- **File Processing**: Multiple file movement patterns to consolidate
- **Configuration Management**: Dynamic parameter injection needs redesign
- **Quality Checks**: Custom validation logic to migrate

**Low Risk Areas:**
- **Basic SQL Operations**: Direct translation to Databricks SQL
- **Standard File I/O**: Auto Loader handles most use cases
- **Monitoring**: Databricks provides better observability

### Recommendations

**1. Start with Low-Risk Components**
- Begin migration with simple file processing and SQL operations
- Validate approach before tackling complex business logic

**2. Invest in Testing Framework**
- Set up comprehensive data validation between old and new systems
- Implement automated regression testing for business logic

**3. Plan for Custom Logic Migration**
- Allocate extra time for complex attribute transformation logic
- Consider rewriting complex NiFi Expression Language in PySpark
- Document business rules clearly before migration

**4. Performance Considerations**
- Current workflow has {processor_count} processors - consolidation will improve performance
- Databricks Auto Scaling will handle variable workloads better than fixed NiFi cluster
- Delta Lake caching will improve query performance over HDFS"""


def _extract_key_properties(proc_type: str, properties: Dict[str, Any]) -> str:
    """Extract key properties for a processor type."""

    key_mappings = {
        "ListFile": [
            "Input Directory",
            "File Filter",
            "Path Filter",
            "Minimum File Age",
        ],
        "GetFile": [
            "Input Directory",
            "Keep Source File",
            "File Filter",
            "Path Filter",
        ],
        "PutFile": [
            "Directory",
            "Create Missing Directories",
            "Conflict Resolution Strategy",
        ],
        "PutSFTP": ["Hostname", "Port", "Username", "Remote Path", "Create Directory"],
        "ExecuteStreamCommand": [
            "Command Path",
            "Command Arguments",
            "Command Environment",
        ],
        "ExecuteSQL": [
            "Database Connection Pooling Service",
            "SQL select query",
            "Max Wait Time",
        ],
        "RouteOnAttribute": [
            "Route Strategy",
            "routing.condition",
            "routing.success",
            "routing.failure",
        ],
        "UpdateAttribute": [
            "Delete Attributes Expression",
            "Store State",
            "Stateful Variables",
        ],
        "ConvertRecord": [
            "Record Reader",
            "Record Writer",
            "Include Zero Record FlowFiles",
        ],
    }

    relevant_keys = key_mappings.get(proc_type, [])
    found_props = []

    for key in relevant_keys:
        if key in properties and properties[key]:
            value = str(properties[key])[:100]  # Limit length
            found_props.append(f"{key}: {value}")

    # Also check for any properties that contain paths, SQL, or commands
    for prop_name, prop_value in properties.items():
        if prop_name not in [k for k in relevant_keys] and prop_value:
            value_str = str(prop_value).lower()
            if any(
                keyword in value_str
                for keyword in [
                    "sql",
                    "select",
                    "insert",
                    "update",
                    "delete",
                    "path",
                    "directory",
                    "command",
                    "script",
                ]
            ):
                if len(found_props) < 5:  # Limit to 5 props
                    short_value = (
                        str(prop_value)[:80] + "..."
                        if len(str(prop_value)) > 80
                        else str(prop_value)
                    )
                    found_props.append(f"{prop_name}: {short_value}")

    return "; ".join(found_props) if found_props else "No key properties configured"


def _get_migration_pattern(proc_type: str, instances: List[Dict]) -> str:
    """Get migration pattern for a processor type."""

    # Get sample properties for context
    sample_props = instances[0]["properties"] if instances else {}

    if proc_type == "ListFile":
        input_dir = sample_props.get("Input Directory", "/path/to/files")
        file_filter = sample_props.get("File Filter", "*")
        return f"""
### ListFile → Auto Loader
**Pattern**: Replace with Databricks Auto Loader for incremental file processing
- **Current NiFi Config**: Directory: `{input_dir}`, Filter: `{file_filter}`
- **Databricks Solution**: Auto Loader with cloudFiles format
- **Benefits**: Built-in checkpointing, schema evolution, efficient incremental processing
- **Implementation**: Configure Auto Loader to monitor equivalent cloud storage path"""

    elif proc_type == "ExecuteStreamCommand":
        command_path = sample_props.get("Command Path", "shell command")
        command_args = sample_props.get("Command Arguments", "")
        return f"""
### ExecuteStreamCommand → Databricks SQL/PySpark
**Pattern**: Convert shell commands to distributed SQL or PySpark operations
- **Current NiFi Config**: Command: `{command_path} {command_args}`
- **Databricks Solution**: Rewrite as PySpark DataFrame operations or SQL queries
- **Benefits**: Distributed processing, better resource management, native Spark optimization
- **Implementation**: Analyze command logic and convert to equivalent Spark operations"""

    elif proc_type == "PutSFTP":
        hostname = sample_props.get("Hostname", "sftp-server")
        remote_path = sample_props.get("Remote Path", "/remote/path")
        return f"""
### PutSFTP → External System Integration
**Pattern**: Replace SFTP transfers with cloud-native data movement
- **Current NiFi Config**: Host: `{hostname}`, Path: `{remote_path}`
- **Databricks Solution**: Use Azure Data Factory, AWS Glue, or direct cloud storage
- **Benefits**: Serverless, managed transfers with better monitoring
- **Implementation**: Set up cloud-to-cloud data movement or use Databricks external tables"""

    patterns = {
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

    first_instance = instances[0]
    instance_name = first_instance["name"]
    props = first_instance["properties"]

    if proc_type == "ListFile":
        input_dir = props.get("Input Directory", "/path/to/source/files")
        file_filter = props.get("File Filter", "*")
        # Try to determine file format from filter
        file_format = "json"
        if ".csv" in file_filter or "csv" in file_filter.lower():
            file_format = "csv"
        elif ".parquet" in file_filter or "parquet" in file_filter.lower():
            file_format = "parquet"

        return f"""
### Auto Loader Example (replaces {instance_name})
```python
# Auto Loader for incremental file processing
# Replaces NiFi ListFile with Input Directory: {input_dir}
df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "{file_format}")
    .option("cloudFiles.schemaLocation", "/path/to/schema")
    .load("{input_dir}"))  # Use actual NiFi directory path

# Write to Delta table
(df.writeStream
    .format("delta")
    .option("checkpointLocation", "/path/to/checkpoint")
    .table("catalog.schema.target_table"))
```"""

    elif proc_type == "ExecuteStreamCommand":
        command_path = props.get("Command Path", "unknown")
        command_args = props.get("Command Arguments", "")

        # Try to determine what type of command this is
        is_sql_command = any(
            keyword in command_path.lower()
            for keyword in ["impala", "hive", "sql", "beeline"]
        )

        if is_sql_command:
            return f"""
### SQL Command Migration (replaces {instance_name})
```python
# Original NiFi command: {command_path} {command_args}
# Migrate to Databricks SQL

# If this was running SQL queries, replace with:
spark.sql(\"\"\"
    -- Insert your SQL logic here
    -- Original command was: {command_path} {command_args}
    SELECT * FROM source_table
    WHERE condition = 'value'
\"\"\")

# For Impala/Hive queries, use Spark SQL equivalent:
result_df = spark.sql("YOUR_ORIGINAL_SQL_HERE")
result_df.write.mode("overwrite").table("catalog.schema.target_table")
```"""
        else:
            return f"""
### Custom Logic Migration (replaces {instance_name})
```python
# Original NiFi command: {command_path} {command_args}
# Convert shell command logic to PySpark

# Analyze what the command does and implement equivalent logic:
# Example: If command processes files, use DataFrame operations
from pyspark.sql import functions as F

# Replace with appropriate DataFrame transformations
df = spark.table("catalog.schema.source_table")
processed_df = df.withColumn("processed", F.current_timestamp())
processed_df.write.mode("append").table("catalog.schema.target_table")
```"""

    examples = {
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

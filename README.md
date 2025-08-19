# NiFi to Databricks Migration Tool

An intelligent migration tool that converts Apache NiFi workflows (Hadoop-based data pipelines) into Databricks jobs using AI agents. The system leverages LangGraph agents with access to specialized tools to automate the complex process of translating NiFi processors, connections, and configurations into equivalent Databricks PySpark code and job definitions.

## 🎯 Overview

This project addresses the challenge of migrating legacy NiFi workflows to modern Databricks infrastructure. Instead of manual conversion, it uses:

- **AI-Powered Agent**: LangGraph-based conversational agent using Databricks Foundation Models
- **Pattern Registry**: Unity Catalog-backed repository of NiFi-to-Databricks conversion patterns
- **Automated Code Generation**: Converts NiFi processors to PySpark with proper error handling and best practices
- **Job Orchestration**: Creates Databricks Jobs with DAG-aware dependencies that mirror NiFi flow structure
- **Comprehensive Tooling**: Modular tools for XML parsing, pattern matching, job creation, and validation

## 🏗️ Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────────┐
│   NiFi XML     │───▶│   AI Agent       │───▶│   Databricks        │
│   Templates    │    │   + Tools        │    │   Jobs & Assets     │
└─────────────────┘    └──────────────────┘    └─────────────────────┘
                              │
                              ▼
                    ┌──────────────────┐
                    │ Pattern Registry │
                    │  (Unity Catalog) │
                    └──────────────────┘
```

### Core Components

- **Agent System** (`agents/`, `nifi_databricks_agent.py`): LangGraph-based conversational interface
- **Migration Tools** (`tools/`): Specialized tools for each aspect of the conversion process
- **Pattern Registry** (`registry/`): UC-backed pattern storage and retrieval
- **Configuration** (`config/`): Environment management and logging
- **Utilities** (`utils/`): File operations, XML processing, and helper functions

## 🚀 Quick Start

### Prerequisites

- Databricks workspace with Unity Catalog enabled
- Python environment with required dependencies
- NiFi XML template files for migration

### Environment Setup

1. **Create `.env` file in the project root**:

Copy and paste the following template into a new `.env` file:

```bash
# Databricks Configuration
DATABRICKS_TOKEN=your-databricks-personal-access-token
DATABRICKS_HOSTNAME=https://your-workspace.cloud.databricks.com
MODEL_ENDPOINT=databricks-meta-llama-3-3-70b-instruct

# Email Configuration (Optional)
NOTIFICATION_EMAIL=your-email@company.com

# Unity Catalog Pattern Registry (Optional - uses defaults if not specified)
# Uncomment and customize these if you want to use different UC table locations
# PATTERN_TABLE=your_catalog.your_schema.processors
# COMPLEX_TABLE=your_catalog.your_schema.complex_patterns
# META_TABLE=your_catalog.your_schema.patterns_meta
# RAW_TABLE=your_catalog.your_schema.patterns_raw_snapshots
```

**How to configure:**

- Replace `your-databricks-personal-access-token` with your actual Databricks token
- Replace `your-workspace.cloud.databricks.com` with your actual workspace URL
- Replace `your-email@company.com` with your email address
- The Unity Catalog tables are optional and will use these defaults if not specified:
  - `eliao.nifi_to_databricks.processors`
  - `eliao.nifi_to_databricks.complex_patterns`
  - `eliao.nifi_to_databricks.patterns_meta`
  - `eliao.nifi_to_databricks.patterns_raw_snapshots`

**Required Variables:**
- `DATABRICKS_TOKEN`: Personal access token or service principal token for authentication
- `DATABRICKS_HOSTNAME`: Full URL to your Databricks workspace (include https://)
- `MODEL_ENDPOINT`: Foundation model endpoint for the AI agent (uses Llama 3.3 70B by default)

**Optional Variables:**
- `NOTIFICATION_EMAIL`: Email for job failure notifications
- `PATTERN_TABLE`: Unity Catalog table for processor patterns
- `COMPLEX_TABLE`: Unity Catalog table for complex migration patterns  
- `META_TABLE`: Unity Catalog table for pattern metadata and versioning
- `RAW_TABLE`: Unity Catalog table for raw pattern snapshots

2. **Initialize Pattern Registry**:
```python
from registry import PatternRegistryUC

# Initialize with Unity Catalog tables
reg = PatternRegistryUC()

# Optionally seed from the JSON file (one-time setup)
reg.seed_from_file("migration_nifi_patterns.json")
```

### Basic Usage

#### Using the AI Agent (Recommended)

```python
# In Databricks notebook
from agents import AGENT
from mlflow.types.responses import ResponsesAgentRequest

# Migrate a NiFi workflow
req = ResponsesAgentRequest(input=[{
    "role": "user",
    "content": """
    Run orchestrate_nifi_migration with:
    xml_path=/Volumes/catalog/schema/nifi_files/my_workflow.xml
    out_dir=/Workspace/Users/me@company.com/migrations/output
    project=my_nifi_project
    job=my_migration_job
    notebook_path=/Workspace/Users/me@company.com/migrations/orchestrator
    existing_cluster_id=your-cluster-id
    deploy=true
    """
}])

response = AGENT.predict(req)
```

#### Programmatic Usage

```python
from tools.migration_tools import convert_flow

# Direct conversion without agent
result = convert_flow(
    xml_path="nifi_pipeline_file/example.xml",
    out_dir="output_results/my_project",
    project="my_project",
    job="migration_job",
    notebook_path="/Workspace/Users/me@company.com/project/main",
    existing_cluster_id="your-cluster-id",  # Optional: reuse existing cluster
    deploy_job=True  # Create and optionally run the job
)
```

## 📁 Input Files

Place your NiFi XML template files in the `nifi_pipeline_file/` directory:

- `json_log_process_pipeline.xml` - Example JSON log processing workflow
- `nifi_pipeline_eric_1.xml` - Sample NiFi pipeline template  
- `nifi_pipeline_eric_embed_groups.xml` - Complex workflow with nested groups

## 📊 Migration Process

### 1. XML Analysis
- Parses NiFi template XML to extract processors, connections, and properties
- Identifies parameter contexts and controller services
- Builds dependency graph from processor connections

### 2. Pattern Matching
- Maps each NiFi processor to Databricks equivalent using pattern registry
- Handles common patterns like:
  - **GetFile/ListFile** → Auto Loader with cloudFiles format
  - **PutHDFS/PutFile** → Delta Lake writes
  - **ConsumeKafka** → Structured Streaming Kafka source
  - **RouteOnAttribute** → DataFrame filter operations
  - **ExecuteSQL** → Spark SQL or JDBC operations

### 3. Code Generation
- Generates PySpark code for each processor with error handling
- Applies best practices and optimization patterns
- Creates modular step files for easier maintenance

### 4. Job Creation
- Builds Databricks Jobs with DAG-aware task dependencies
- Supports both new cluster creation and existing cluster reuse
- Generates asset bundles for infrastructure-as-code deployment

## 🔧 Available Tools

The agent has access to specialized tools in the `tools/` folder:

### Core Migration Tools
- **`xml_tools.py`**: NiFi XML parsing and template extraction
- **`migration_tools.py`**: Main conversion logic and orchestration
- **`pattern_tools.py`**: Pattern registry operations and template rendering

### Databricks Integration
- **`job_tools.py`**: Jobs API integration, creation, and deployment
- **`dlt_tools.py`**: Delta Live Tables pipeline generation
- **`eval_tools.py`**: Pipeline validation and data comparison utilities

## 📋 Generated Output Structure

Each migration creates a complete Databricks project:

```
output_results/project_name/
├── src/
│   └── steps/              # Individual processor conversions
│       ├── 10_GetFile.py
│       ├── 11_ListenHTTP.py
│       └── ...
├── notebooks/
│   └── main.py            # Orchestrator notebook
├── jobs/
│   ├── job.json           # Simple job configuration
│   └── job.dag.json       # DAG-aware job with dependencies
├── conf/
│   ├── plan.json          # Migration execution plan
│   ├── parameter_contexts.json
│   └── dlt_pipeline.json  # Delta Live Tables config
├── databricks.yml         # Asset bundle configuration
└── README.md              # Project-specific documentation
```

## 🔍 Common Migration Patterns

| NiFi Processor | Databricks Equivalent | Key Considerations |
|----------------|----------------------|-------------------|
| GetFile/ListFile | Auto Loader | Schema evolution, file format detection |
| PutHDFS/PutFile | Delta Lake | Partitioning, optimization, ACID properties |
| ConsumeKafka | Structured Streaming | Watermarking, checkpointing, rate limiting |
| PublishKafka | Kafka Sink | Serialization, topic configuration |
| RouteOnAttribute | DataFrame.filter() | Multiple output paths, condition logic |
| ConvertRecord | Format conversions | CSV↔JSON↔Parquet↔Delta transformations |
| ExecuteSQL | Spark SQL / JDBC | Connection pooling, query optimization |
| InvokeHTTP | UDF with requests | API rate limiting, error handling |

## 🧪 Testing and Validation

### Pattern Registry Testing
```python
from registry import PatternRegistryUC

reg = PatternRegistryUC()
pattern = reg.get_pattern("GetFile")
print(f"Pattern: {pattern['databricks_equivalent']}")
```

### Migration Output Validation
```python
from tools.eval_tools import evaluate_pipeline_outputs

# Compare original vs migrated data
comparison = evaluate_pipeline_outputs(
    src_path="/path/to/nifi/output",
    dst_path="/path/to/databricks/output",
    key_cols_csv="id,timestamp"
)
```

## 🔧 Development and Customization

### Adding New Processor Patterns

1. **Add to pattern registry**:
```python
reg.add_pattern("CustomProcessor", {
    "databricks_equivalent": "Custom Solution",
    "description": "Handles custom processing logic",
    "code_template": "# Custom PySpark code template",
    "best_practices": ["Practice 1", "Practice 2"]
})
```

2. **Update `migration_nifi_patterns.json`** for persistent storage

### Extending Tools

Create new tools in the `tools/` directory following the pattern:

```python
from langchain_core.tools import tool

@tool
def my_custom_tool(parameter: str) -> str:
    """Tool description for the agent."""
    # Implementation
    return result
```

## 🚨 Troubleshooting

### Common Issues

1. **Authentication Errors**: Verify `DATABRICKS_TOKEN` and `DATABRICKS_HOSTNAME` in `.env`
2. **Pattern Not Found**: Check if processor pattern exists in registry or JSON file
3. **Job Creation Failures**: Ensure cluster permissions and workspace access
4. **XML Parsing Errors**: Validate NiFi template XML structure

### Debug Mode

Enable detailed logging:
```python
import logging
logging.getLogger().setLevel(logging.DEBUG)
```

## 📚 Additional Resources

- **CLAUDE.md**: Detailed guidance for Claude Code interactions
- **Sample Workflows**: Example NiFi templates in `nifi_pipeline_file/`
- **Pattern Library**: Comprehensive patterns in `migration_nifi_patterns.json`
- **Generated Examples**: Sample outputs in `output_results/nifi2dbx_test_1/`

## 🤝 Contributing

1. Add new processor patterns to the registry
2. Extend tools for specific use cases  
3. Improve error handling and validation
4. Add support for additional NiFi components

## 📄 License

This project is designed for enterprise data platform migrations. Please review your organization's policies for AI-assisted code generation and data processing tools.
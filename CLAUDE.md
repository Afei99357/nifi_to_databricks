# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Architecture Overview

This is a NiFi to Databricks migration tool that uses LangGraph agents to convert Apache NiFi workflows into Databricks pipelines. The system provides both programmatic APIs and an agent-based interface for automating the migration process.

### Core Components

- **Agent System**: LangGraph-based conversational agent using Databricks Foundation Models
  - `agents/agent.py`: Main agent implementation with MLflow integration
  - `nifi_databricks_agent.py`: Full-featured agent with all migration tools
  - `convert_nifi_using_agent.py`: Databricks notebook interface for agent usage

- **Migration Tools**: Modular tools for different aspects of NiFi conversion
  - `tools/xml_tools.py`: NiFi XML parsing and template extraction
  - `tools/migration_tools.py`: Core conversion logic from NiFi to Databricks
  - `tools/job_tools.py`: Databricks Jobs API integration and job creation
  - `tools/pattern_tools.py`: Pattern matching and code template management
  - `tools/dlt_tools.py`: Delta Live Tables pipeline generation
  - `tools/eval_tools.py`: Pipeline validation and comparison utilities

- **Pattern Registry**: Unity Catalog-backed pattern storage
  - `registry/pattern_registry.py`: UC table management for migration patterns
  - `migration_nifi_patterns.json`: JSON-based pattern definitions and templates

- **Configuration**: Environment and settings management
  - `config/settings.py`: Environment variable loading and logging setup
  - Requires `.env` file with DATABRICKS_HOSTNAME, DATABRICKS_TOKEN, MODEL_ENDPOINT

### Migration Process

1. **XML Parsing**: Extracts processors, connections, and properties from NiFi templates
2. **Pattern Matching**: Maps NiFi processors to Databricks equivalents using UC patterns
3. **Code Generation**: Creates PySpark code for each processor with proper dependencies
4. **Job Creation**: Generates Databricks Jobs with DAG-aware task dependencies
5. **Asset Bundling**: Creates complete Databricks project with notebooks and configurations

## Common Development Tasks

### Testing the Migration Agent

```python
# In Databricks notebook
from agents import AGENT
from mlflow.types.responses import ResponsesAgentRequest

req = ResponsesAgentRequest(input=[{
    "role": "user", 
    "content": "Run orchestrate_nifi_migration with xml_path=<path> out_dir=<dir> project=<name>"
}])

resp = AGENT.predict(req)
```

### Running Migrations Programmatically

```python
from tools.migration_tools import convert_flow

# Convert NiFi XML to Databricks artifacts
result = convert_flow(
    xml_path="nifi_pipeline_file/example.xml",
    out_dir="output_results/project_name",
    project="my_project",
    job="my_job",
    notebook_path="/Workspace/Users/me@company.com/project/main"
)
```

### Pattern Registry Operations

```python
from registry import PatternRegistryUC

reg = PatternRegistryUC()
reg.seed_from_file("migration_nifi_patterns.json")  # Initialize patterns
pattern = reg.get_pattern("GetFile")                # Retrieve pattern
reg.add_pattern("CustomProcessor", {...})           # Add new pattern
```

## Environment Setup

Required environment variables:
- `DATABRICKS_HOSTNAME`: Your Databricks workspace URL
- `DATABRICKS_TOKEN`: Personal access token or service principal token  
- `MODEL_ENDPOINT`: Foundation model endpoint (default: databricks-meta-llama-3-3-70b-instruct)
- `NOTIFICATION_EMAIL`: Optional email for job failure notifications

## Key Migration Patterns

- **GetFile/ListFile** → Auto Loader with cloudFiles format
- **PutHDFS/PutFile** → Delta Lake writes with ACID guarantees
- **ConsumeKafka/PublishKafka** → Structured Streaming with Kafka source/sink
- **RouteOnAttribute** → DataFrame filter operations with multiple outputs
- **ConvertRecord** → Format conversions using DataFrame read/write
- **ExecuteSQL** → Spark SQL operations or JDBC connections

## Generated Output Structure

```
output_results/project_name/
├── src/steps/           # Individual processor conversions
├── notebooks/           # Orchestrator notebook for job execution
├── jobs/                # Databricks job configurations
├── conf/                # Migration plans and DLT configs
├── databricks.yml       # Asset bundle configuration
└── README.md           # Project documentation
```

## Testing and Validation

The system generates comparison utilities in `tools/eval_tools.py` for validating migration results against original NiFi outputs. Use the pattern registry to iteratively improve conversion accuracy for specific processor types.
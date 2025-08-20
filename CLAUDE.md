# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Architecture Overview

This is a NiFi to Databricks migration tool that uses LangGraph agents to convert Apache NiFi workflows into Databricks pipelines. The system provides both programmatic APIs and an agent-based interface for automating the migration process.

### Core Components

- **Agent System**: LangGraph-based conversational agent using Databricks Foundation Models
  - `agents/agent.py`: Main agent implementation with MLflow integration and all migration tools
  - `convert_nifi_using_agent.py`: Databricks notebook interface for agent usage

- **Migration Tools**: Modular tools for different aspects of NiFi conversion
  - `tools/xml_tools.py`: NiFi XML parsing and template extraction
  - `tools/migration_tools.py`: Core conversion logic from NiFi to Databricks
  - `tools/chunking_tools.py`: Large NiFi XML file chunking and reconstruction utilities
  - `tools/job_tools.py`: Databricks Jobs API integration and job creation
  - `tools/pattern_tools.py`: Pattern matching and code template management
  - `tools/dlt_tools.py`: Delta Live Tables pipeline generation
  - `tools/eval_tools.py`: Pipeline validation and comparison utilities

- **Pattern Registry**: Unity Catalog-backed pattern storage
  - `registry/pattern_registry.py`: UC table management for migration patterns

- **Configuration**: Environment and settings management
  - `config/settings.py`: Environment variable loading and logging setup
  - Requires `.env` file with DATABRICKS_HOSTNAME, DATABRICKS_TOKEN, MODEL_ENDPOINT

### Migration Process

#### Standard Migration (files <50 processors)
1. **XML Parsing**: Extracts processors, connections, and properties from NiFi templates
2. **Pattern Matching**: Maps NiFi processors to Databricks equivalents using UC patterns
3. **Code Generation**: Creates PySpark code for each processor with proper dependencies
4. **Job Creation**: Generates Databricks Jobs with DAG-aware task dependencies
5. **Asset Bundling**: Creates complete Databricks project with notebooks and configurations

#### Chunked Migration (large files >50 processors)
1. **XML Chunking**: Splits NiFi workflow by process groups while preserving graph relationships
2. **Chunk Processing**: Processes each chunk individually to avoid context limits
3. **Pattern Matching**: Maps NiFi processors to Databricks equivalents per chunk
4. **Code Generation**: Creates PySpark code for processors within each chunk
5. **Workflow Reconstruction**: Merges chunk results into complete multi-task Databricks job
6. **Asset Bundling**: Creates enhanced project structure with chunk analysis and dependencies

## Common Development Tasks

### Testing the Migration Agent

```python
# In Databricks notebook
from agents import AGENT
from mlflow.types.responses import ResponsesAgentRequest

# For regular-sized NiFi files (<50 processors)
req = ResponsesAgentRequest(input=[{
    "role": "user", 
    "content": "Run orchestrate_nifi_migration with xml_path=<path> out_dir=<dir> project=<name>"
}])

# For large NiFi files (>50 processors or complex workflows)
req = ResponsesAgentRequest(input=[{
    "role": "user", 
    "content": "Run orchestrate_chunked_nifi_migration with xml_path=<path> out_dir=<dir> project=<name> max_processors_per_chunk=25"
}])

resp = AGENT.predict(req)
```

### Running Migrations Programmatically

```python
from tools.migration_tools import convert_flow, orchestrate_chunked_nifi_migration

# Standard migration for smaller files
result = convert_flow(
    xml_path="nifi_pipeline_file/example.xml",
    out_dir="output_results/project_name",
    project="my_project",
    job="my_job",
    notebook_path="/Workspace/Users/me@company.com/project/main"
)

# Chunked migration for large files
result = orchestrate_chunked_nifi_migration(
    xml_path="nifi_pipeline_file/large_example.xml",
    out_dir="output_results/large_project",
    project="my_large_project",
    job="my_large_job",
    max_processors_per_chunk=25,
    notebook_path="/Workspace/Users/me@company.com/large_project/main"
)
```

### Pattern Registry Operations

```python
from registry import PatternRegistryUC

reg = PatternRegistryUC()
# Patterns are managed directly in Delta tables
pattern = reg.get_pattern("GetFile")                # Retrieve pattern from UC table
reg.add_pattern("CustomProcessor", {...})           # Add new pattern to UC table
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

### Standard Migration Output
```
output_results/project_name/
├── src/steps/           # Individual processor conversions
├── notebooks/           # Orchestrator notebook for job execution
├── jobs/                # Databricks job configurations
├── conf/                # Migration plans and DLT configs
├── databricks.yml       # Asset bundle configuration
└── README.md           # Project documentation
```

### Chunked Migration Output
```
output_results/project_name/
├── src/steps/           # Processor conversions grouped by chunks (e.g., 00_processor.py, 01_processor.py)
├── chunks/              # Individual chunk processing results and analysis
├── notebooks/           # Enhanced orchestrator with chunk-aware execution
├── jobs/                # Multi-task job configurations with cross-chunk dependencies
│   ├── job.json         # Standard single-task job
│   └── job.chunked.json # Multi-task job with proper dependencies
├── conf/                # Migration plans, chunking analysis, and reconstructed workflow
│   ├── chunking_result.json      # Original chunking analysis
│   ├── reconstructed_workflow.json # Final merged workflow
│   └── parameter_contexts.json   # NiFi parameters and controller services
├── databricks.yml       # Asset bundle configuration
└── README.md           # Enhanced documentation with chunking statistics
```

## Testing and Validation

The system generates comparison utilities in `tools/eval_tools.py` for validating migration results against original NiFi outputs. Use the pattern registry to iteratively improve conversion accuracy for specific processor types.
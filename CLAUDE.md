# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Architecture Overview

This is a NiFi to Databricks migration tool that uses LangGraph agents to convert Apache NiFi workflows into Databricks pipelines. The system features an intelligent architecture decision system that automatically analyzes NiFi XML and recommends the optimal Databricks architecture (Jobs, DLT Pipeline, or Structured Streaming).

**Performance Optimizations (v2.1):**
- **Batched LLM Generation**: Generates code for multiple processors in single requests (96% fewer API calls)
- **Single-Round Agent**: Simplified to complete migrations in exactly 1 agent round
- **Real-time Progress Tracking**: Visual indicators show migration progress and call counts
- **Robust Error Handling**: Graceful fallbacks and comprehensive logging
- **Enhanced JSON Parsing**: Explicit JSON format enforcement prevents escape sequence errors
- **Configurable Batch Sizes**: Tune batch sizes with `MAX_PROCESSORS_PER_CHUNK` and `LLM_SUB_BATCH_SIZE`

The system provides both programmatic APIs and an agent-based interface for automating the migration process.

### Core Components

- **Agent System**: LangGraph-based conversational agent using Databricks Foundation Models
  - `agents/agent.py`: Main agent implementation with MLflow integration and all migration tools
  - `convert_nifi_using_agent.py`: Databricks notebook interface for agent usage

- **Migration Tools**: Modular tools for different aspects of NiFi conversion
  - `tools/xml_tools.py`: NiFi XML parsing, template extraction, and intelligent architecture analysis
  - `tools/migration_tools.py`: Core conversion logic from NiFi to Databricks with intelligent migration orchestration
  - `tools/chunking_tools.py`: Large NiFi XML file chunking and reconstruction utilities
  - `tools/job_tools.py`: Databricks Jobs API integration and job creation
  - `tools/generator_tools.py`: Code generation utilities with LLM-powered PySpark code creation
  - `tools/dlt_tools.py`: Delta Live Tables pipeline generation
  - `tools/eval_tools.py`: Pipeline validation and comparison utilities

# Pattern Registry removed - generates fresh code each time

- **Configuration**: Environment and settings management
  - `config/settings.py`: Environment variable loading and logging setup
  - Requires `.env` file with DATABRICKS_HOSTNAME, DATABRICKS_TOKEN, MODEL_ENDPOINT

### Migration Process

#### Standard Migration (files <50 processors)
1. **XML Parsing**: Extracts processors, connections, and properties from NiFi templates
2. **Code Generation**: Creates PySpark code for each processor using builtin templates and LLM generation with proper dependencies
3. **Job Creation**: Generates Databricks Jobs with DAG-aware task dependencies
4. **Asset Bundling**: Creates complete Databricks project with notebooks and configurations

#### Chunked Migration (large files >50 processors)
1. **XML Chunking**: Splits NiFi workflow by process groups while preserving graph relationships
2. **Chunk Processing**: Processes each chunk individually to avoid context limits
3. **Code Generation**: Creates PySpark code for processors within each chunk using builtin templates and LLM generation
4. **Workflow Reconstruction**: Merges chunk results into complete multi-task Databricks job
5. **Asset Bundling**: Creates enhanced project structure with chunk analysis and dependencies

## Common Development Tasks

### Testing the Migration Agent

#### Intelligent Migration (Recommended)
The agent automatically analyzes your NiFi workflow and chooses the optimal Databricks architecture:

```python
# In Databricks notebook - Intelligent Architecture Decision
from agents import AGENT
from mlflow.types.responses import ResponsesAgentRequest

req = ResponsesAgentRequest(input=[{
    "role": "user",
    "content": "Run orchestrate_intelligent_nifi_migration with xml_path=<path> out_dir=<dir> project=<name>"
}])

resp = AGENT.predict(req)
```

This will:
1. Analyze the NiFi XML for processor types and patterns
2. Recommend optimal architecture (Databricks Jobs, DLT Pipeline, or Structured Streaming)
3. Execute the appropriate migration strategy
4. Save architecture analysis results

#### Manual Migration (Legacy)
For when you want to specify the approach manually:

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

#### Intelligent Migration (Recommended)
```python
from tools.migration_tools import orchestrate_intelligent_nifi_migration

# Intelligent migration - automatically chooses best architecture
result = orchestrate_intelligent_nifi_migration(
    xml_path="nifi_pipeline_file/example.xml",
    out_dir="output_results/intelligent_project",
    project="my_intelligent_project",
    notebook_path="/Workspace/Users/me@company.com/project/main",
    deploy=False  # Set to True to deploy automatically
)
```

#### Architecture Analysis Tools
```python
from tools.xml_tools import analyze_nifi_architecture_requirements, recommend_databricks_architecture

# Analyze architecture requirements
with open("nifi_pipeline_file/example.xml", 'r') as f:
    xml_content = f.read()

# Get feature analysis
analysis = analyze_nifi_architecture_requirements.func(xml_content)
print("Architecture Analysis:", analysis)

# Get architecture recommendation
recommendation = recommend_databricks_architecture.func(xml_content)
print("Recommendation:", recommendation)
```

#### Manual Migration (Legacy)
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

# Pattern Registry Operations removed - generates fresh code each time

## Environment Setup

Required environment variables:
- `DATABRICKS_HOSTNAME`: Your Databricks workspace URL
- `DATABRICKS_TOKEN`: Personal access token or service principal token
- `MODEL_ENDPOINT`: Foundation model endpoint (default: databricks-meta-llama-3-3-70b-instruct)
- `NOTIFICATION_EMAIL`: Optional email for job failure notifications

**Performance Configuration (v2.1):**
- `ENABLE_LLM_CODE_GENERATION`: Enable batched LLM generation (default: true)
- `MAX_PROCESSORS_PER_CHUNK`: Processors per batch (default: 20, tune 15-30)
- `LLM_SUB_BATCH_SIZE`: Sub-batch size for fallbacks (default: 10, recommended: 5)

**Optimal `.env` configuration:**
```bash
DATABRICKS_TOKEN=your-token
DATABRICKS_HOSTNAME=https://your-workspace.cloud.databricks.com
MODEL_ENDPOINT=databricks-meta-llama-3-3-70b-instruct
ENABLE_LLM_CODE_GENERATION=true
MAX_PROCESSORS_PER_CHUNK=20
LLM_SUB_BATCH_SIZE=5
```

**Batch Size Tuning Guidelines:**
- **Complex processors** (lots of properties): Use `MAX_PROCESSORS_PER_CHUNK=15`
- **Simple processors**: Use `MAX_PROCESSORS_PER_CHUNK=25`
- **Better fallback success**: Use `LLM_SUB_BATCH_SIZE=5` instead of default 10
- **JSON parsing issues**: Reduce both batch sizes for higher success rates

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

## Progress Tracking (v2.0)

The migration system now provides comprehensive progress tracking:

**Agent Level:**
```
🔧 [TOOL REQUEST] orchestrate_chunked_nifi_migration
🔄 [AGENT ROUND 1/5] Model requested tool call
✅ [AGENT COMPLETE] Migration finished successfully after 1 rounds
```

**Migration Level:**
```
📋 [MIGRATION] Processing 4 chunks with 87 total processors
📦 [CHUNK 1/4] Processing 25 processors...
🎉 [MIGRATION COMPLETE] 87 processors → 87 tasks
```

**LLM Batch Level:**
```
🧠 [LLM BATCH] Generating code for 25 processors in chunk_0
🔍 [LLM BATCH] Processor types: GetFile, EvaluateJsonPath, RouteOnAttribute
🚀 [LLM BATCH] Sending batch request to databricks-meta-llama-3-3-70b-instruct...
✅ [LLM BATCH] Received response, parsing generated code...
🎯 [LLM BATCH] Successfully parsed 20 code snippets
✨ [LLM BATCH] Generated 25 processor tasks for chunk_0
```

**JSON Parsing Recovery (v2.1):**
```
⚠️  [LLM BATCH] JSON parsing failed: Invalid \escape: line 18 column 1066
🔧 [LLM BATCH] Recovered JSON from markdown block
❌ [LLM BATCH] All JSON recovery attempts failed, falling back to individual generation
```

## Testing and Validation

The system generates comparison utilities in `tools/eval_tools.py` for validating migration results against original NiFi outputs.

**Performance Monitoring**: Track API call efficiency with the new progress indicators to ensure optimal resource usage.

## JSON Parsing Improvements (v2.1)

The system now includes enhanced JSON parsing reliability to prevent "Invalid \escape" errors:

### JSON Format Enforcement
- **Explicit prompt rules**: LLM is instructed on proper JSON escape sequences
- **Temperature control**: Uses `temperature=0.1` for more deterministic JSON responses
- **Format validation**: Multiple recovery attempts before falling back to individual generation

### Troubleshooting JSON Issues
If you see JSON parsing failures:

1. **Reduce batch size**: Lower `MAX_PROCESSORS_PER_CHUNK` from 20 to 15
2. **Improve fallback success**: Set `LLM_SUB_BATCH_SIZE=5` instead of default 10
3. **Check escape sequences**: The system now explicitly teaches LLM proper JSON escaping

### Success Rate Optimization
- **20 processors**: ~66% success rate (2/3 chunks)
- **15 processors**: ~80-90% success rate
- **5-8 processors (fallback)**: ~90% success rate

**Result**: Dramatically fewer expensive individual processor API calls.

## Code Quality Guidelines

### Exception Handling Best Practices

When working with this codebase, follow these exception handling guidelines to maintain clean, maintainable code:

**❌ Avoid these patterns:**
```python
# Don't use broad exception catching that silently ignores errors
try:
    some_operation()
except Exception:
    pass

# Don't wrap simple operations unnecessarily
try:
    value = int(os.environ.get("VAR", "10"))
except Exception:
    value = 10

# Don't create complex nested try-except blocks
try:
    # complex operation
    try:
        # nested operation
        try:
            # deeply nested operation
        except Exception:
            pass
    except Exception:
        pass
except Exception:
    pass
```

**✅ Use these patterns instead:**
```python
# Use specific exception types with meaningful handling
try:
    from optional_module import some_function
    some_function()
except ImportError:
    logger.warning("Optional module not available, skipping feature")

# Simple operations often don't need try-except
value = int(os.environ.get("VAR", "10"))  # os.environ.get() already handles missing keys

# Use single-level exception handling with specific recovery
try:
    result = complex_operation()
except (SpecificError, AnotherError) as e:
    logger.error(f"Operation failed: {e}")
    result = fallback_value
```

**Guidelines:**
- Only use try-except when you can meaningfully handle the exception
- Use specific exception types rather than broad `Exception` catching
- Log warnings/errors instead of silently ignoring with `pass`
- Avoid deep nesting of try-except blocks
- Simple operations like `os.environ.get()` or basic arithmetic rarely need exception handling
- If you must catch `Exception`, log it and provide meaningful fallback behavior

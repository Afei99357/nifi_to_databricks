# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Architecture Overview

This is a NiFi to Databricks migration tool that converts Apache NiFi workflows into Databricks pipelines. The system features an intelligent architecture decision system that automatically analyzes NiFi XML and recommends the optimal Databricks architecture (Jobs, DLT Pipeline, or Structured Streaming).

**Performance Optimizations (v2.1):**
- **Batched LLM Generation**: Generates code for multiple processors in single requests (96% fewer API calls)
- **Direct Function Pipeline**: Simplified linear execution without agent overhead
- **Real-time Progress Tracking**: Visual indicators show migration progress and call counts
- **Robust Error Handling**: Graceful fallbacks and comprehensive logging
- **Enhanced JSON Parsing**: Explicit JSON format enforcement prevents escape sequence errors
- **Configurable Batch Sizes**: Tune batch sizes with `MAX_PROCESSORS_PER_CHUNK` and `LLM_SUB_BATCH_SIZE`

The system provides programmatic APIs for automating the migration process.

### Core Components

- **Migration System**: Direct function call approach without agent complexity
  - `tools/migration_orchestrator.py`: Main migration functions for complete workflows
  - Entry point: `migrate_nifi_to_databricks_simplified()` and `analyze_nifi_workflow_only()`

- **Migration Tools**: Modular tools for different aspects of NiFi conversion
  - `tools/xml_tools.py`: NiFi XML parsing and template extraction
  - `tools/improved_classifier.py`: AI-powered processor classification using hybrid rule-based + LLM approach
  - `tools/improved_pruning.py`: Smart pruning that identifies essential data processing logic
  - `tools/asset_extraction.py`: Comprehensive scanning for scripts, databases, file paths, and external dependencies
  - `tools/dependency_extraction.py`: Complete dependency analysis across ALL processors with variable tracking, impact analysis, and circular dependency detection

- **Streamlit Web Application**: Interactive user interface for migrations
  - `streamlit_app/Dashboard.py`: Main dashboard for file upload and navigation
  - `streamlit_app/pages/01_Processor_Classification.py`: Migration analysis and execution page
  - `streamlit_app/pages/02_Processor_Dependencies.py`: Comprehensive dependency analysis (development branch)
  - `streamlit_app/pages/03_Asset_Extraction.py`: Asset discovery and migration planning
  - `streamlit_app/pages/04_Lineage_Connections.py`: Table lineage analysis page
  - Features result caching, navigation protection, and error handling

- **Table Lineage Analysis**: Dedicated data flow analysis system
  - `tools/nifi_table_lineage.py`: Analyzes NiFi XML to extract table-to-table data flows
  - Identifies SQL operations, variable resolution, and data dependencies
  - Filters column aliases vs real table references for accurate lineage
  - Outputs CSV reports with processor chains and hop counts

### Migration Process

#### Standard Migration (files <50 processors)
1. **XML Parsing**: Extracts processors, connections, and properties from NiFi templates
2. **Code Generation**: Creates PySpark code for each processor using builtin templates and LLM generation with proper dependencies
3. **Job Creation**: Generates Databricks Jobs with DAG-aware task dependencies
4. **Asset Bundling**: Creates complete Databricks project with notebooks and configurations

#### Batch Processing (large workflows)
1. **Processor Batching**: Splits large processor lists into configurable batch sizes for LLM generation
2. **Batch Processing**: Processes each batch individually to avoid JSON parsing issues
3. **Code Generation**: Creates PySpark code for processors within each batch using builtin templates and LLM generation
4. **Fallback Strategy**: Automatically retries failed batches with smaller batch sizes
5. **Asset Bundling**: Creates complete Databricks project with notebooks and configurations

## Common Development Tasks

### Environment Setup

Required environment variables:
- `DATABRICKS_HOSTNAME`: Your Databricks workspace URL
- `DATABRICKS_TOKEN`: Personal access token or service principal token
- `MODEL_ENDPOINT`: Foundation model endpoint (default: databricks-meta-llama-3-3-70b-instruct)
- `NOTIFICATION_EMAIL`: Optional email for job failure notifications

**Performance Configuration (v2.1):**
- `ENABLE_LLM_CODE_GENERATION`: Enable batched LLM generation (default: true)
- `MAX_PROCESSORS_PER_CHUNK`: Processors per batch (default: 20, tune 15-30)
- `LLM_SUB_BATCH_SIZE`: Sub-batch size for fallbacks (default: 10, recommended: 5)

**Optimal deployment configuration:**
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

### Development Environment Setup

```bash
# Quick setup (recommended)
./scripts/setup_dev.sh

# Manual setup
uv sync --group dev
uv run pre-commit install
```

### Running the Application

**Streamlit App:**
```bash
streamlit run streamlit_app/Dashboard.py
```

**Databricks Deployment:**
The application uses `app.yaml` for Databricks Apps deployment. Update the environment variables in `app.yaml` for your workspace.

### Testing and Validation

**Run Tests:**
```bash
# All tests
uv run pytest

# Unit tests only
uv run pytest -m unit

# Integration tests only
uv run pytest -m integration

# Exclude slow tests
uv run pytest -m "not slow"
```

**Code Quality:**
```bash
# Run all pre-commit hooks
uv run pre-commit run --all-files

# Check MyPy progress
./scripts/mypy_progress.sh

# Individual tools
uv run black .
uv run isort .
uv run flake8 .
uv run mypy .
```

### Running Migrations Programmatically

**Simplified Migration (Recommended):**
```python
from tools.migration_orchestrator import migrate_nifi_to_databricks_simplified

# Complete migration with semantic analysis pipeline
result = migrate_nifi_to_databricks_simplified(
    xml_path="nifi_pipeline_file/example.xml",
    out_dir="output_results/simplified_project",
    project="my_simplified_project",
    notebook_path="/Workspace/Users/me@company.com/project/main",
    deploy=False  # Set to True to deploy automatically
)

# Access all results
print("Migration:", result['migration_result'])
print("Analysis:", result['analysis'])
print("Configuration:", result['configuration'])
```

**Analysis Only (Fast):**
```python
from tools.migration_orchestrator import analyze_nifi_workflow_only

# Fast analysis without migration
analysis = analyze_nifi_workflow_only("path/to/workflow.xml")
```

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

# Don't use bare except: pass - this is especially problematic
try:
    with open(file_path, 'r') as f:
        content = f.read()
except:
    pass  # ❌ NEVER DO THIS - hides all errors including syntax errors
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

# Use specific exception types with meaningful handling
try:
    with open(file_path, 'r') as f:
        content = f.read()
except (IOError, OSError, UnicodeDecodeError):
    # Skip files that can't be read, but don't fail the operation
    continue
```

**Guidelines:**
- **Only use try-except when you can meaningfully handle the exception**
- **Avoid unnecessary try-except blocks** - if you control the inputs and expect success, don't wrap it
- Use specific exception types rather than broad `Exception` catching
- **NEVER use bare `except:` or `except: pass`** - this hides all errors including syntax errors
- Log warnings/errors instead of silently ignoring with `pass`
- Avoid deep nesting of try-except blocks

## MyPy Type Checking

The project uses gradual MyPy adoption with module-specific overrides:

```bash
# Check current MyPy progress
./scripts/mypy_progress.sh

# Test specific module
uv run mypy --config-file pyproject.toml tools/

# Current ignored modules (in pyproject.toml):
# - tools.*
# - agents.*
# - registry.*
# - convert_nifi_using_agent
# - utils.xml_utils
```

**MyPy graduation process**: Remove module overrides one-by-one as code is cleaned up. See `docs/mypy_strictness_guide.md` for detailed progression plan.

## Streamlit App Architecture

The Streamlit app uses a 4-page architecture with session state management:

### Page Structure
- **Dashboard.py**: Main navigation hub with file upload and processor information
- **01_Processor_Classification.py**: AI-powered processor classification and pruning
- **02_Processor_Dependencies.py**: Comprehensive dependency analysis (development branch)
- **03_Asset_Extraction.py**: Asset discovery and migration planning
- **04_Lineage_Connections.py**: Table lineage and data flow analysis

### Key Patterns
- **Session State Management**: Use `st.session_state` for caching results across page navigation
- **Navigation Protection**: Set running flags to disable UI during processing
- **Error Display**: Use `st.error()` for failures, `st.warning()` for non-critical issues, `st.success()` for completions
- **File Handling**: Always clean up temporary files in `finally` blocks
- **Progress Indication**: Use `st.spinner()` for long-running operations

### Common Issues to Avoid
- **Don't assume data types**: Always check if objects are strings vs dicts before calling `.get()`
- **Clear all caches**: Clear Results should remove both result cache AND uploaded file cache
- **Handle edge cases**: Analysis may return error strings instead of analysis objects
- **Avoid redundant UI**: Don't add back navigation buttons when sidebar navigation exists

## Branch Management

- **main**: Production branch with Dependencies functionality hidden
- **trace_data_dependency**: Development branch with full Dependencies functionality enabled

The Dependencies page exists in both branches but is only accessible via UI in the development branch.

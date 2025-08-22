# NiFi to Databricks Migration Tool

An intelligent migration tool that converts Apache NiFi workflows (Hadoop-based data pipelines) into Databricks Jobs or Delta Live Tables (DLT) pipelines using AI agents. The system leverages LangGraph agents with access to specialized tools to automate the complex process of translating NiFi processors, connections, and configurations into equivalent Databricks PySpark code and pipeline definitions.

## 🎯 Overview

This project addresses the challenge of migrating legacy NiFi workflows to modern Databricks infrastructure. Instead of manual conversion, it uses:

- **🧠 Intelligent Architecture Decision**: Automatically analyzes NiFi XML and chooses optimal Databricks architecture (Jobs for batch workflows, DLT Pipelines for streaming workflows)
- **AI-Powered Agent**: LangGraph-based conversational agent using Databricks Foundation Models
- **Chunked Processing**: Handles large NiFi workflows (50+ processors) by intelligent chunking while preserving connectivity
- **Complete Workflow Mapping**: Captures full NiFi structure including processors, connections, funnels, and controller services
- **Fresh Code Generation**: LLM-powered conversion of NiFi processors to PySpark with builtin templates for common processors
- **Job Orchestration**: Creates Databricks Jobs with precise task dependencies that mirror NiFi flow structure
- **Funnel Handling**: Intelligent detection and bypass of NiFi funnels to prevent disconnected tasks
- **Comprehensive Tooling**: Modular tools for XML parsing, code generation, job creation, and validation

## 🏗️ Architecture

```
                     🧠 Intelligent Architecture Decision
                                    │
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────────┐
│   NiFi XML     │───▶│ Architecture     │───▶│ Optimal Migration   │
│   Templates    │    │ Analyzer         │    │ Strategy Selection  │
└─────────────────┘    └──────────────────┘    └─────────────────────┘
                              │                           │
                              ▼                           ▼
                    ┌──────────────────┐    ┌──────────────────────────┐
                    │   AI Agent       │───▶│ Migration Execution      │
                    │   + Tools        │    │ (Jobs/DLT/Streaming)     │
                    └──────────────────┘    └──────────────────────────┘
                              │                           │
                              ▼                           ▼
                    ┌──────────────────┐         ┌──────────────────┐
                    │ Fresh Code Gen   │         │   Databricks     │
                    │ (LLM + Builtin)  │         │ Assets & Deploy  │
                    └──────────────────┘         └──────────────────┘
```

### 🧠 Intelligent Architecture Decision System

The tool now automatically analyzes NiFi workflows and recommends the optimal Databricks architecture:

**Architecture Options:**
- **Databricks Jobs**: Batch orchestration for file-based ETL workflows with task dependencies
- **DLT Pipelines**: Streaming ETL with automatic lineage, data quality, and bronze→silver→gold layers

**Decision Logic:**
- **Streaming Sources Detected** → DLT Pipeline (ListenHTTP, ConsumeKafka, ListenTCP)
- **Batch Sources Only** → Databricks Jobs (GetFile, ListFile, QueryDatabaseTable)
- **Universal Parsing**: Works with any NiFi workflow structure and processor dependencies
- **Sinks**: File outputs vs external systems
- **Complexity**: Workflow size and interconnection complexity

### Core Components

- **Agent System** (`agents/agent.py`): LangGraph-based conversational agent with MLflow integration
- **Intelligent Migration** (`tools/migration_tools.py`): Architecture analysis and automatic Jobs vs DLT selection
- **DLT Generation** (`tools/dlt_tools.py`): Complete Delta Live Tables pipeline generation with connection parsing
- **XML Analysis** (`tools/xml_tools.py`): NiFi template parsing and architecture requirement analysis
- **Code Generation** (`tools/generator_tools.py`): LLM-powered PySpark code generation with builtin templates
- **Job Creation** (`tools/job_tools.py`): Databricks Jobs API integration and deployment
- **Configuration** (`config/settings.py`): Environment management and logging

## 🚀 Quick Start

### Prerequisites

- Databricks workspace with Unity Catalog enabled
- Python environment with required dependencies
- NiFi XML template files for migration

### 🔥 **Performance Improvements** (v2.2)

The migration system now includes significant performance optimizations, enhanced JSON reliability, and honest job status reporting:

- **⚡ Batched LLM Generation**: Generate code for multiple processors in single requests (up to 96% fewer API calls)
- **🎯 Smart Round Limiting**: Agent completes in 1-2 rounds instead of endless loops
- **📊 Real-time Progress Tracking**: Visual progress indicators show exactly what's happening
- **🛡️ Robust Error Handling**: Graceful fallbacks prevent migration failures
- **🔧 Enhanced JSON Parsing**: Explicit JSON format enforcement prevents escape sequence errors
- **⚙️ Configurable Batch Sizes**: Tune `MAX_PROCESSORS_PER_CHUNK` and `LLM_SUB_BATCH_SIZE` for optimal performance
- **✅ Verified Job Status**: Real job execution verification with 5-second startup wait and 45-second polling
- **🔗 Direct Monitoring Links**: Databricks Jobs UI links for immediate job monitoring and debugging
- **🎯 Honest Messaging**: Clear distinction between "job triggered" vs "job running" vs "job failed"

**Example Performance**: 100-processor workflow goes from 100+ LLM calls → 4-6 batched calls
**JSON Reliability**: Eliminates "Invalid \escape" errors with explicit prompt formatting rules
**Job Status Accuracy**: Eliminates false "Job is actively running" reports for jobs that actually fail

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

# Agent Configuration
ENABLE_LLM_CODE_GENERATION=true      # Use batched LLM for high-quality code

# Batch Processing Configuration (Performance tuning)
MAX_PROCESSORS_PER_CHUNK=20          # Processors per batch (default: 20, tune 15-30)
LLM_SUB_BATCH_SIZE=5                 # Sub-batch size for fallbacks (default: 10, recommended: 5)
```

**How to configure:**

- Replace `your-databricks-personal-access-token` with your actual Databricks token
- Replace `your-workspace.cloud.databricks.com` with your actual workspace URL
- Replace `your-email@company.com` with your email address

**Required Variables:**
- `DATABRICKS_TOKEN`: Personal access token or service principal token for authentication
- `DATABRICKS_HOSTNAME`: Full URL to your Databricks workspace (include https://)
- `MODEL_ENDPOINT`: Foundation model endpoint for the AI agent (uses Llama 3.3 70B by default)

**Agent Configuration Variables:**
- `ENABLE_LLM_CODE_GENERATION`: Enable batched LLM code generation (default: true)

**Batch Processing Configuration Variables:**
- `MAX_PROCESSORS_PER_CHUNK`: Processors per batch (default: 20, tune 15-30 based on complexity)
- `LLM_SUB_BATCH_SIZE`: Sub-batch size for fallbacks (default: 10, recommended: 5 for better success rate)

**Optional Variables:**
- `NOTIFICATION_EMAIL`: Email for job failure notifications

2. **Ready to migrate!** The tool now generates fresh code each time without requiring any pattern registry setup.

## 📁 Input Files

Store your NiFi XML template files in **Databricks Catalog Volumes** for secure and organized access:

### Recommended Storage Structure
```
/Volumes/your_catalog/your_schema/nifi_files/
├── production_workflows/
│   ├── data_ingestion_pipeline.xml
│   ├── etl_processing_flow.xml
│   └── analytics_pipeline.xml
├── test_workflows/
│   ├── sample_workflow.xml
│   └── validation_pipeline.xml
└── archived/
    └── legacy_workflows/
```

### Usage Examples
```python
# Reference files in catalog volumes
xml_path = "/Volumes/my_catalog/migration/nifi_files/production_workflows/main_pipeline.xml"
out_dir = "/Workspace/Users/me@company.com/migrations/output"
```

**Benefits of Catalog Volumes:**
- ✅ **Security**: Governed access control through Unity Catalog
- ✅ **Organization**: Structured storage with proper versioning
- ✅ **Collaboration**: Team access to shared NiFi templates
- ✅ **Scalability**: No repository size limitations

## 📊 Migration Process

### Unified Chunked Migration Process (handles all workflow sizes)
1. **Complete Workflow Mapping**: Extracts full NiFi structure including processors, connections, funnels, and controller services
2. **Funnel Detection & Bypass**: Identifies NiFi funnels and creates bypass mappings to preserve connectivity
3. **Intelligent Chunking**: Splits workflow by process groups (if needed) while preserving graph relationships
4. **Chunk Processing**: Processes each chunk individually to avoid context limits
5. **Code Generation**: Creates PySpark code for processors using builtin templates and LLM generation with proper dependencies and error handling
6. **Workflow Reconstruction**: Merges chunk results into complete multi-task Databricks job using original connectivity map
7. **Asset Bundling**: Creates enhanced project structure with analysis, dependencies, and configurations

**Advantages of unified approach:**
- **Scalable**: Automatically handles both small (1-10 processors) and large (100+ processors) workflows
- **Robust**: Better error handling and context management
- **Complete**: Preserves all NiFi connectivity including complex funnel routing
- **Debuggable**: Comprehensive workflow maps and chunk analysis for troubleshooting

### Key Migration Patterns
- **GetFile/ListFile** → Auto Loader with cloudFiles format
- **PutHDFS/PutFile** → Delta Lake writes with ACID guarantees
- **ConsumeKafka/PublishKafka** → Structured Streaming with Kafka source/sink
- **RouteOnAttribute** → DataFrame filter operations with multiple outputs
- **ConvertRecord** → Format conversions using DataFrame read/write
- **ExecuteSQL** → Spark SQL operations or JDBC connections
- **Funnels** → Bypass logic with union operations in downstream processors

## 🔧 Available Tools

The agent has access to specialized tools in the `tools/` folder:

### Core Migration Tools
- **`xml_tools.py`**: NiFi XML parsing and template extraction
- **`migration_tools.py`**: Main conversion logic and orchestration (both standard and chunked)
- **`chunking_tools.py`**: Large XML file chunking, workflow mapping, and reconstruction utilities
- **`generator_tools.py`**: Code generation utilities with LLM-powered PySpark code creation

### Databricks Integration
- **`job_tools.py`**: Jobs API integration, creation, and deployment
- **`dlt_tools.py`**: Delta Live Tables pipeline generation
- **`eval_tools.py`**: Pipeline validation and data comparison utilities

### Key Functions
- **`orchestrate_intelligent_nifi_migration`**: **⭐ RECOMMENDED** - Automatically analyzes NiFi XML and chooses optimal architecture (Jobs vs DLT)
- **`orchestrate_databricks_job_migration`**: Force Databricks Jobs creation for any workflow (manual override)
- **`orchestrate_nifi_migration`**: Legacy standard migration for smaller workflows
- **`generate_dlt_pipeline_from_nifi`**: Convert NiFi workflows to Delta Live Tables with proper processor dependencies
- **`extract_complete_workflow_map`**: Captures full NiFi structure including funnels
- **`chunk_nifi_xml_by_process_groups`**: Intelligent workflow chunking
- **`reconstruct_full_workflow`**: Merges chunks with preserved connectivity

## 📋 Generated Output Structure

```
output_results/project_name/
├── src/steps/              # Processor conversions (organized by chunks for larger workflows)
├── chunks/                 # Individual chunk processing results and analysis (if chunked)
├── notebooks/              # Enhanced orchestrator with intelligent execution logic
├── jobs/                   # Multi-task job configurations with precise dependencies
│   ├── job.json            # Standard job configuration
│   └── job.chunked.json    # Enhanced multi-task job (primary output)
├── conf/                   # Comprehensive migration analysis and planning
│   ├── complete_workflow_map.json     # Full NiFi structure with funnels and connections
│   ├── chunking_result.json           # Chunking analysis (if applicable)
│   ├── reconstructed_workflow.json    # Final merged workflow with dependencies
│   └── parameter_contexts.json        # NiFi parameters and controller services
├── databricks.yml         # Asset bundle configuration for deployment
└── README.md              # Project documentation with migration statistics
```

**Key Files:**
- **`job.chunked.json`**: Primary job configuration with proper task dependencies
- **`complete_workflow_map.json`**: Full workflow analysis including funnel detection
- **`reconstructed_workflow.json`**: Final connectivity map for debugging
- **`architecture_analysis.json`**: Intelligent architecture decision analysis and reasoning

## 🧠 Architecture Decision Logic

The intelligent migration system applies these decision rules:

### **Decision Tree**

```
NiFi XML Analysis
       │
       ▼
┌─────────────────┐
│ Detect Sources  │ ──→ Batch only (GetFile, ListFile) ──→ Databricks Jobs
│ & Processors    │
└─────────────────┘
       │
       ▼
┌─────────────────┐
│ Streaming       │ ──→ + Complex transforms ──→ DLT Pipeline
│ Detected?       │ ──→ + Simple processing ──→ Structured Streaming
└─────────────────┘
       │
       ▼
┌─────────────────┐
│ Mixed Sources   │ ──→ Batch + Streaming ──→ DLT Pipeline (unified)
│ (Batch+Stream)  │
└─────────────────┘
       │
       ▼
┌─────────────────┐
│ Heavy Trans-    │ ──→ Routing + JSON + Transforms ──→ DLT Pipeline
│ formations?     │
└─────────────────┘
```

### **Architecture Recommendations**

| **NiFi Pattern** | **Detected Features** | **Recommended Architecture** | **Reasoning** |
|------------------|----------------------|------------------------------|---------------|
| **File ETL** | GetFile + PutHDFS, no streaming | **Databricks Jobs** | Simple batch orchestration sufficient |
| **Log Processing** | ListenHTTP + EvaluateJsonPath + RouteOnAttribute | **DLT Pipeline** | Streaming + JSON processing + routing logic |
| **Kafka Pipeline** | ConsumeKafka + transforms + PublishKafka | **DLT Pipeline** | Streaming ETL with transformations |
| **Simple Streaming** | ListenHTTP + basic transforms | **Structured Streaming** | Custom streaming logic preferred |
| **Mixed Workload** | GetFile + ListenHTTP + routing | **DLT Pipeline** | Unified batch/streaming processing |
| **Complex ETL** | Multiple processors + routing + JSON | **DLT Pipeline** | Declarative transformations optimal |

### **Feature Detection**

The system analyzes your NiFi XML to detect:

- **🔄 Streaming Sources**: ListenHTTP, ConsumeKafka, ListenTCP, ConsumeJMS, etc.
- **📁 Batch Sources**: GetFile, ListFile, QueryDatabaseTable, etc.
- **🔧 Transformations**: EvaluateJsonPath, UpdateAttribute, ConvertRecord, etc.
- **🔀 Routing Logic**: RouteOnAttribute, RouteOnContent, conditional processing
- **📊 JSON Processing**: EvaluateJsonPath, SplitJson, JSON transformations
- **🌐 External Sinks**: PublishKafka, InvokeHTTP, external system outputs
- **📈 Complexity Factors**: Workflow size, multiple outputs, nested process groups

## 🔍 Common Migration Patterns

| NiFi Component | Databricks Equivalent | Key Considerations |
|----------------|----------------------|-------------------|
| GetFile/ListFile | Auto Loader | Schema evolution, file format detection |
| PutHDFS/PutFile | Delta Lake | Partitioning, optimization, ACID properties |
| ConsumeKafka | Structured Streaming | Watermarking, checkpointing, rate limiting |
| PublishKafka | Kafka Sink | Serialization, topic configuration |
| RouteOnAttribute | DataFrame.filter() | Multiple output paths, condition logic |
| ConvertRecord | Format conversions | CSV↔JSON↔Parquet↔Delta transformations |
| ExecuteSQL | Spark SQL / JDBC | Connection pooling, query optimization |
| InvokeHTTP | UDF with requests | API rate limiting, error handling |
| **Funnel** | **Union operations** | **Automatic bypass, no task created, union logic in downstream** |
| Process Groups | Task grouping | Logical organization, dependency management |

## 🧪 Testing and Validation

# Pattern Registry removed - generates fresh code each time

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

Add builtin patterns to `tools/generator_tools.py` in the `_get_builtin_pattern()` function for common processors. For custom processors, the LLM will generate fresh code each time.

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
5. **Duplicate Task Keys**: Use chunked migration for large workflows to avoid conflicts
6. **Circular Dependencies**: Check `reconstructed_workflow.json` for dependency issues
7. **Disconnected Tasks**: Review `complete_workflow_map.json` for funnel bypass issues

### Performance Issues (Fixed in v2.1)

8. **Excessive LLM Calls**: Set `ENABLE_LLM_CODE_GENERATION=true` for optimal performance (single-round agent)
9. **Slow Code Generation**: The system now uses batched LLM generation (1 call per chunk vs 1 per processor)
10. **Agent Timeout**: Progress tracking shows exactly where the migration is and prevents endless loops
11. **JSON Parsing Failures**: Fixed "Invalid \escape" errors with explicit JSON format enforcement in prompts
12. **Wasteful Fallbacks**: Reduced `LLM_SUB_BATCH_SIZE=5` to minimize individual processor generation

### Batch Size Optimization

If you experience JSON parsing failures or performance issues, tune these settings:

```bash
# For complex processors with lots of properties
export MAX_PROCESSORS_PER_CHUNK=15

# For better fallback success rate
export LLM_SUB_BATCH_SIZE=5

# For simple processors
export MAX_PROCESSORS_PER_CHUNK=25
```

**Success Rate Patterns:**
- 15-20 processors: ~80-90% success rate
- 20-25 processors: ~66% success rate
- 5-8 processors (fallback): ~90% success rate

### Chunked Migration Troubleshooting

1. **Context Limit Errors**: Reduce `max_processors_per_chunk` (try 15-20)
2. **Missing Connections**: Check `funnel_bypasses` in workflow map for proper routing
3. **Job Deployment Failures**: Verify task dependencies in `job.chunked.json`
4. **Large Workflow Issues**: Use `extract_complete_workflow_map` to analyze structure

### Debug Mode

Enable detailed logging:
```python
import logging
logging.getLogger().setLevel(logging.DEBUG)
```

### Workflow Analysis

```python
# Analyze large workflows before migration
from tools.chunking_tools import extract_complete_workflow_map
result = extract_complete_workflow_map("path/to/large_workflow.xml")
print(f"Processors: {result['summary']['total_processors']}")
print(f"Funnels: {result['summary']['total_funnels']}")
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

## Development Setup

### Pre-commit Hooks

This project uses pre-commit hooks to ensure code quality. The hooks are configured to run automatically on every commit and include:

- **Black**: Code formatting
- **isort**: Import sorting
- **flake8**: Linting (with lenient settings for initial setup)
- **General checks**: Trailing whitespace, end-of-file, large files, merge conflicts, debug statements

#### Setup

**Quick setup (recommended):**
```bash
./scripts/setup_dev.sh
```

**Manual setup:**
1. Install dev dependencies:
   ```bash
   uv sync --group dev
   ```

2. Install pre-commit hooks:
   ```bash
   uv run pre-commit install
   ```

3. (Optional) Run hooks on all files:
   ```bash
   uv run pre-commit run --all-files
   ```

The hooks will now run automatically on every commit. If any hooks fail, the commit will be blocked until the issues are fixed.

#### Configuration

- **Black**: Configured with 88 character line length
- **isort**: Configured to work with Black
- **flake8**: Lenient configuration for initial setup (ignores many common issues)
- **mypy**: Enabled with lenient settings and module-specific overrides for gradual adoption
- **Exception Handling**: Non-gating checks for banned exception patterns with metrics

#### MyPy Type Checking

MyPy is configured to start lenient but provides a clear path to strictness:

- **Current**: Basic type checking with module ignores for existing code
- **Gradual**: Remove module overrides one-by-one as code is cleaned up
- **Future**: Full strict mode for maximum type safety

📖 **See [MyPy Strictness Guide](docs/mypy_strictness_guide.md)** for the complete graduation plan.

#### Exception Handling Quality

The pre-commit hooks include a custom exception handling checker that:

- **Reports** banned patterns like `except Exception:` and `except:`
- **Tracks metrics** for try/except usage, with statements, and more
- **Provides suggestions** for better error handling
- **Non-gating** - warns but doesn't block commits
- **Metrics tracking** - gates commits if exception handling metrics degrade over time

**Quick commands:**
```bash
# Check exception handling quality
python3 scripts/exception_metrics.py .

# Get help
python3 scripts/exception_metrics.py --help

# Run all pre-commit hooks
uv run pre-commit run --all-files
```

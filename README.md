# NiFi to Databricks Migration Tool

An intelligent migration tool that converts Apache NiFi workflows (Hadoop-based data pipelines) into Databricks jobs using AI agents. The system leverages LangGraph agents with access to specialized tools to automate the complex process of translating NiFi processors, connections, and configurations into equivalent Databricks PySpark code and job definitions.

## 🎯 Overview

This project addresses the challenge of migrating legacy NiFi workflows to modern Databricks infrastructure. Instead of manual conversion, it uses:

- **🧠 Intelligent Architecture Decision**: Automatically analyzes NiFi XML and recommends optimal Databricks architecture (Jobs, DLT Pipeline, or Structured Streaming)
- **AI-Powered Agent**: LangGraph-based conversational agent using Databricks Foundation Models
- **Chunked Processing**: Handles large NiFi workflows (50+ processors) by intelligent chunking while preserving connectivity
- **Complete Workflow Mapping**: Captures full NiFi structure including processors, connections, funnels, and controller services
- **Pattern Registry**: Unity Catalog-backed repository of NiFi-to-Databricks conversion patterns
- **Automated Code Generation**: Converts NiFi processors to PySpark with proper error handling and best practices
- **Job Orchestration**: Creates Databricks Jobs with precise task dependencies that mirror NiFi flow structure
- **Funnel Handling**: Intelligent detection and bypass of NiFi funnels to prevent disconnected tasks
- **Comprehensive Tooling**: Modular tools for XML parsing, pattern matching, job creation, and validation

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
                    │ Pattern Registry │         │   Databricks     │
                    │  (Unity Catalog) │         │ Assets & Deploy  │
                    └──────────────────┘         └──────────────────┘
```

### 🧠 Intelligent Architecture Decision System

The tool now automatically analyzes NiFi workflows and recommends the optimal Databricks architecture:

**Architecture Options:**
- **Databricks Jobs**: Batch orchestration for file-based ETL workflows
- **DLT Pipeline**: Streaming ETL with transformations, JSON processing, and routing
- **Structured Streaming**: Custom streaming logic for real-time data processing

**Decision Factors:**
- **Source Types**: Batch (GetFile) vs Streaming (ListenHTTP, ConsumeKafka)
- **Transformations**: JSON processing, routing logic, complex transformations
- **Sinks**: File outputs vs external systems
- **Complexity**: Workflow size and interconnection complexity

### Core Components

- **Agent System** (`agents/`, `nifi_databricks_agent.py`): LangGraph-based conversational interface
- **Migration Tools** (`tools/`): Specialized tools for each aspect of the conversion process
- **Pattern Registry** (`registry/`): UC-backed pattern storage and retrieval
  - `init_delta_tables.sql`: SQL script to create required Unity Catalog tables
- **Configuration** (`config/`): Environment management and logging
- **Utilities** (`utils/`): File operations, XML processing, and helper functions

## 🚀 Quick Start

### Prerequisites

- Databricks workspace with Unity Catalog enabled
- Python environment with required dependencies
- NiFi XML template files for migration

### 🔥 **Performance Improvements** (v2.1)

The migration system now includes significant performance optimizations and enhanced JSON reliability:

- **⚡ Batched LLM Generation**: Generate code for multiple processors in single requests (up to 96% fewer API calls)
- **🎯 Smart Round Limiting**: Agent completes in 1-2 rounds instead of endless loops
- **📊 Real-time Progress Tracking**: Visual progress indicators show exactly what's happening
- **🛡️ Robust Error Handling**: Graceful fallbacks prevent migration failures
- **🔧 Enhanced JSON Parsing**: Explicit JSON format enforcement prevents escape sequence errors
- **⚙️ Configurable Batch Sizes**: Tune `MAX_PROCESSORS_PER_CHUNK` and `LLM_SUB_BATCH_SIZE` for optimal performance

**Example Performance**: 100-processor workflow goes from 100+ LLM calls → 4-6 batched calls
**JSON Reliability**: Eliminates "Invalid \escape" errors with explicit prompt formatting rules

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

# Agent Configuration (Controls LLM call behavior)
AGENT_MAX_ROUNDS=5                    # Max agent-tool rounds (default: 10)
ENABLE_LLM_CODE_GENERATION=true      # Use batched LLM for high-quality code

# Batch Processing Configuration (Performance tuning)
MAX_PROCESSORS_PER_CHUNK=20          # Processors per batch (default: 20, tune 15-30)
LLM_SUB_BATCH_SIZE=5                 # Sub-batch size for fallbacks (default: 10, recommended: 5)

# Unity Catalog Pattern Registry (Optional - improves performance over time)
# Uncomment these to enable pattern learning and reuse across migrations
PATTERN_TABLE=eliao.nifi_to_databricks.processors
COMPLEX_TABLE=eliao.nifi_to_databricks.complex_patterns
META_TABLE=eliao.nifi_to_databricks.patterns_meta
RAW_TABLE=eliao.nifi_to_databricks.patterns_raw_snapshots
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

**Agent Configuration Variables:**
- `AGENT_MAX_ROUNDS`: Maximum agent-tool rounds (default: 10, recommended: 5 for efficiency)
- `ENABLE_LLM_CODE_GENERATION`: Enable batched LLM code generation (default: true)

**Batch Processing Configuration Variables:**
- `MAX_PROCESSORS_PER_CHUNK`: Processors per batch (default: 20, tune 15-30 based on complexity)
- `LLM_SUB_BATCH_SIZE`: Sub-batch size for fallbacks (default: 10, recommended: 5 for better success rate)

**Optional Variables:**
- `NOTIFICATION_EMAIL`: Email for job failure notifications
- `PATTERN_TABLE`: Unity Catalog table for processor patterns (enables pattern learning)
- `COMPLEX_TABLE`: Unity Catalog table for complex migration patterns
- `META_TABLE`: Unity Catalog table for pattern metadata and versioning
- `RAW_TABLE`: Unity Catalog table for raw pattern snapshots

**Unity Catalog Pattern Registry Benefits:**
- **Pattern Learning**: Saves LLM-generated patterns for reuse in future migrations
- **Performance**: Reduces LLM calls from N to 0 for known processors (cost & speed)
- **Team Sharing**: Multiple users benefit from collectively learned patterns
- **Auto-Creation**: Tables are created automatically when configured

2. **Initialize Unity Catalog Tables**:
```sql
-- Run the SQL initialization script in Databricks
%sql
%run ./init_delta_tables
```

3. **Initialize Pattern Registry**:
```python
from registry import PatternRegistryUC

# Initialize with Unity Catalog tables
reg = PatternRegistryUC()

# Patterns are managed directly in Delta tables - no JSON seeding needed
# Use reg.add_pattern() to add new processor patterns as needed
```

## 📊 Unity Catalog Table Schema

The migration tool uses four Delta tables in Unity Catalog to store patterns and track migrations. These tables are created by running `init_delta_tables.sql`.

### Table Schema Details

#### 1. `migration_patterns` Table
**Purpose**: Stores processor conversion patterns and code templates
**Location**: `nifi_migration.patterns.migration_patterns`

| Column | Type | Description |
|--------|------|-------------|
| `processor_class` | STRING (PK) | NiFi processor class name (e.g., "GetFile", "EvaluateJsonPath") |
| `databricks_equivalent` | STRING | Equivalent Databricks service/technology |
| `description` | STRING | Human-readable description of the conversion |
| `best_practices` | ARRAY<STRING> | List of recommended practices |
| `code_template` | STRING | PySpark code template with placeholders |
| `last_seen_properties` | MAP<STRING, STRING> | Sample properties from recent usage |
| `created_at` | TIMESTAMP | Pattern creation timestamp |
| `updated_at` | TIMESTAMP | Last modification timestamp |

**Example Data**:
```
processor_class: "EvaluateJsonPath"
databricks_equivalent: "JSON Functions"
description: "Extract JSON values using PySpark JSON functions"
code_template: "df.select(from_json(col('json_data'), schema).alias('parsed'))"
```

#### 2. `processor_mappings` Table
**Purpose**: Maps NiFi processors to Databricks services with complexity indicators
**Location**: `nifi_migration.patterns.processor_mappings`

| Column | Type | Description |
|--------|------|-------------|
| `nifi_processor` | STRING | NiFi processor name |
| `databricks_service` | STRING | Target Databricks service |
| `complexity_level` | STRING | Migration complexity (Low/Medium/High) |
| `migration_notes` | STRING | Special considerations for migration |
| `example_properties` | MAP<STRING, STRING> | Common property examples |
| `created_at` | TIMESTAMP | Record creation time |

**Example Data**:
```
nifi_processor: "ConsumeKafka"
databricks_service: "Structured Streaming"
complexity_level: "Medium"
migration_notes: "Requires Kafka cluster configuration and checkpointing"
```

#### 3. `migration_history` Table
**Purpose**: Tracks migration execution results and statistics
**Location**: `nifi_migration.patterns.migration_history`

| Column | Type | Description |
|--------|------|-------------|
| `migration_id` | STRING | Unique migration execution ID |
| `xml_file_path` | STRING | Source NiFi XML template path |
| `project_name` | STRING | Generated project name |
| `processor_count` | INTEGER | Total processors in workflow |
| `success_count` | INTEGER | Successfully converted processors |
| `failure_count` | INTEGER | Failed conversions |
| `migration_type` | STRING | "standard" or "chunked" |
| `started_at` | TIMESTAMP | Migration start time |
| `completed_at` | TIMESTAMP | Migration completion time |
| `errors` | ARRAY<STRING> | List of errors encountered |
| `generated_files` | ARRAY<STRING> | List of output files created |

**Example Data**:
```
migration_id: "migration_20241220_143052"
xml_file_path: "/Volumes/catalog/schema/nifi_files/workflow.xml"
processor_count: 45
success_count: 42
failure_count: 3
migration_type: "chunked"
```

#### 4. `controller_services` Table
**Purpose**: Maps NiFi controller services to Databricks configurations
**Location**: `nifi_migration.patterns.controller_services`

| Column | Type | Description |
|--------|------|-------------|
| `service_type` | STRING | NiFi controller service type |
| `databricks_equivalent` | STRING | Equivalent Databricks configuration |
| `configuration_mapping` | MAP<STRING, STRING> | Property mappings |
| `setup_instructions` | STRING | Setup guidance |
| `dependencies` | ARRAY<STRING> | Required dependencies |
| `created_at` | TIMESTAMP | Record creation time |

**Example Data**:
```
service_type: "DBCPConnectionPool"
databricks_equivalent: "JDBC Connection"
configuration_mapping: {"Database Connection URL": "spark.conf jdbc.url"}
setup_instructions: "Configure JDBC connection in cluster settings"
```

### Table Initialization

To create these tables, run the SQL script in Databricks:

```sql
%run ./init_delta_tables
```

The script creates:
- Tables with proper schemas and constraints
- Sample data for common processors (GetFile, PutFile, ConsumeKafka, etc.)
- Primary key constraints and Delta table properties
- Change data feed enablement for auditing

### Basic Usage

#### 🧠 **Intelligent Migration (Recommended)**
Automatically analyzes your NiFi workflow and chooses the best Databricks architecture:

```python
# In Databricks notebook - using the AI Agent with Intelligent Decision
from agents import AGENT
from mlflow.types.responses import ResponsesAgentRequest

req = ResponsesAgentRequest(input=[{
    "role": "user",
    "content": """
    Run orchestrate_intelligent_nifi_migration with:
    xml_path=/Volumes/catalog/schema/nifi_files/my_workflow.xml
    out_dir=/Workspace/Users/me@company.com/migrations/output
    project=my_nifi_project
    deploy=false
    """
}])

response = AGENT.predict(req)
```

**What it does:**
1. **Analyzes** your NiFi XML for processor types and complexity
2. **Recommends** optimal architecture (Jobs vs DLT vs Streaming)
3. **Executes** the appropriate migration strategy automatically
4. **Saves** architecture analysis in `conf/architecture_analysis.json`

**Progress Tracking**: You'll see real-time progress like:
```
🔧 [TOOL REQUEST] orchestrate_chunked_nifi_migration
📋 [MIGRATION] Processing 4 chunks with 87 total processors
📦 [CHUNK 1/4] Processing 25 processors...
🧠 [LLM BATCH] Generating code for 25 processors in chunk_0
✅ [CHUNK 1/4] Generated 25 tasks
🎉 [MIGRATION COMPLETE] 87 processors → 87 tasks
✅ [AGENT COMPLETE] Migration finished successfully after 1 rounds
```

#### **Manual Migration (Legacy)**
For when you want to specify the approach manually:

```python
# In Databricks notebook - using the AI Agent (Manual Choice)
from agents import AGENT
from mlflow.types.responses import ResponsesAgentRequest

req = ResponsesAgentRequest(input=[{
    "role": "user",
    "content": """
    Run orchestrate_chunked_nifi_migration with:
    xml_path=/Volumes/catalog/schema/nifi_files/my_workflow.xml
    out_dir=/Workspace/Users/me@company.com/migrations/output
    project=my_nifi_project
    job=my_migration_job
    max_processors_per_chunk=25
    existing_cluster_id=your-cluster-id
    deploy=true
    """
}])

response = AGENT.predict(req)
```

**Key Parameters:**
- `max_processors_per_chunk=25`: Optimal chunk size (adjust 15-30 based on complexity)
- `existing_cluster_id`: Reuse existing cluster or omit to create new one
- `deploy=true`: Automatically deploy the job to Databricks

**Alternative: Direct Function Call**
```python
# For programmatic usage without agent
from tools.migration_tools import orchestrate_chunked_nifi_migration

result = orchestrate_chunked_nifi_migration(
    xml_path="/Volumes/catalog/schema/nifi_files/my_workflow.xml",
    out_dir="/Workspace/Users/me@company.com/migrations/output",
    project="my_project",
    job="migration_job",
    max_processors_per_chunk=25,
    existing_cluster_id="your-cluster-id",
    deploy=True
)
```

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
5. **Pattern Matching**: Maps NiFi processors to Databricks equivalents using UC patterns
6. **Code Generation**: Creates PySpark code for processors with proper dependencies and error handling
7. **Workflow Reconstruction**: Merges chunk results into complete multi-task Databricks job using original connectivity map
8. **Asset Bundling**: Creates enhanced project structure with analysis, dependencies, and configurations

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
- **`pattern_tools.py`**: Pattern registry operations and template rendering

### Databricks Integration
- **`job_tools.py`**: Jobs API integration, creation, and deployment
- **`dlt_tools.py`**: Delta Live Tables pipeline generation
- **`eval_tools.py`**: Pipeline validation and data comparison utilities

### Key Functions
- **`orchestrate_nifi_migration`**: Standard migration for smaller workflows
- **`orchestrate_chunked_nifi_migration`**: Advanced chunked processing for large workflows
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
5. **Duplicate Task Keys**: Use chunked migration for large workflows to avoid conflicts
6. **Circular Dependencies**: Check `reconstructed_workflow.json` for dependency issues
7. **Disconnected Tasks**: Review `complete_workflow_map.json` for funnel bypass issues

### Performance Issues (Fixed in v2.1)

8. **Excessive LLM Calls**: Set `AGENT_MAX_ROUNDS=5` and `ENABLE_LLM_CODE_GENERATION=true` for optimal performance
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

#### MyPy Type Checking

MyPy is configured to start lenient but provides a clear path to strictness:

- **Current**: Basic type checking with module ignores for existing code
- **Gradual**: Remove module overrides one-by-one as code is cleaned up
- **Future**: Full strict mode for maximum type safety

📖 **See [MyPy Strictness Guide](docs/mypy_strictness_guide.md)** for the complete graduation plan.

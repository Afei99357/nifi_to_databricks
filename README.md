# NiFi to Databricks Migration Tool

An intelligent migration tool that converts Apache NiFi workflows (Hadoop-based data pipelines) into Databricks jobs using AI agents. The system leverages LangGraph agents with access to specialized tools to automate the complex process of translating NiFi processors, connections, and configurations into equivalent Databricks PySpark code and job definitions.

## 🎯 Overview

This project addresses the challenge of migrating legacy NiFi workflows to modern Databricks infrastructure. Instead of manual conversion, it uses:

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

# Patterns are managed directly in Delta tables - no JSON seeding needed
# Use reg.add_pattern() to add new processor patterns as needed
```

### Basic Usage

```python
# In Databricks notebook - using the AI Agent (Recommended)
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
# NiFi to Databricks Migration Tool

An intelligent migration tool that converts Apache NiFi workflows into Databricks pipelines using AI-powered analysis and code generation. The system provides both a **Streamlit web app** for interactive migrations and **programmatic APIs** for automated workflows.

## ✨ Key Features

- **🎯 Streamlit Web App**: Interactive UI for uploading NiFi XML files and running migrations
- **🧠 AI-Powered Analysis**: LLM-based processor classification and code generation using `databricks-langchain`
- **✂️ Smart Pruning**: Automatically identifies and focuses on essential data processing logic
- **🔄 Semantic Flow Detection**: Maps NiFi processor chains to logical data transformation flows
- **📊 Comprehensive Reports**: Detailed analysis of processors, migrations, and recommendations
- **⚙️ Databricks Native**: Uses `app.yml` configuration for seamless Databricks deployment

## 🎯 Overview

This project addresses the challenge of migrating legacy NiFi workflows to modern Databricks infrastructure. Instead of manual conversion, it uses:

- **🧠 Intelligent Architecture Decision**: Automatically analyzes NiFi XML and recommends optimal Databricks architecture (Jobs, DLT Pipeline, or Structured Streaming)
- **🔍 Smart Workflow Analysis**: Advanced processor classification system that accurately identifies data transformations vs infrastructure operations
- **🗂️ Intelligent Asset Discovery**: Comprehensive scanning of all processors to identify migration dependencies including scripts, databases, file systems, and external services
- **🚀 Simplified Pipeline**: Direct function calls without complexity - linear execution path
- **✂️ Semantic Migration**: Prune → Chain → Flow approach for business-meaningful migrations
- **Fresh Code Generation**: AI-powered conversion of NiFi processors to PySpark recommendations
- **Comprehensive Analysis**: Detailed reports on processor classification and migration strategy
- **Modular Architecture**: Specialized tools for XML parsing, analysis, and report generation

## 🏗️ How It Works

1. **📤 Upload**: Upload your NiFi XML template through the Streamlit interface
2. **🔍 Analyze**: AI-powered analysis classifies processors and identifies data flows
3. **✂️ Prune**: Automatically filters out infrastructure-only processors (logging, routing, flow control)
4. **🔄 Chain**: Detects semantic data processing chains and logical flows
5. **📊 Report**: Generates comprehensive migration analysis and recommendations

## 🗂️ Intelligent Asset Discovery

The migration tool includes a sophisticated asset discovery system that automatically identifies all external dependencies and resources that need to be migrated alongside your NiFi workflows.

### 🎯 What It Discovers

**Script Files & Executables (11 types supported):**
- Shell scripts (`.sh`)
- Python scripts (`.py`)
- SQL files (`.sql`)
- Java JARs (`.jar`)
- Perl scripts (`.pl`)
- R scripts (`.r`)
- Ruby scripts (`.rb`)
- JavaScript (`.js`)
- Scala scripts (`.scala`)
- Groovy scripts (`.groovy`)
- Windows executables (`.exe`, `.bat`)

**Database Connections:**
- Impala/Hive clusters
- MySQL, PostgreSQL, Oracle
- MongoDB, Cassandra
- SQL Server
- Custom JDBC connections

**File System Paths:**
- HDFS paths (`/user/`, `/etl/`, `/warehouse/`)
- Data warehouse locations
- Staging directories
- Temporary processing paths
- Landing zones

**Working Directories:**
- Script execution directories
- Processing workspaces
- Configuration paths
- Log directories

### 🔧 How It Works

**Pattern-Based Intelligence:**
```python
# Multi-method detection for comprehensive coverage
asset_discovery = {
    "script_detection": "Regex patterns + file extensions + semantic analysis",
    "path_discovery": "HDFS patterns + directory structures + content analysis",
    "database_extraction": "JDBC parsing + hostname patterns + connection strings",
    "directory_identification": "Property name semantics + path structure analysis"
}
```

**Real Example Output:**
```bash
# NiFi Asset Summary
- Script Files Found: 11
- HDFS Paths Found: 98
- Database Hosts Found: 1
- Working Directories: 13

## Script Files Requiring Migration
- /users/hadoop_nifi_svc/scripts/icn8/run_impala_query.sh
- /users/hadoop_nifi_svc/scripts/icn8/backup_data.py
- /scripts/icn8/process_results.sql

## Database Connections
- **Impala**: nardc02prod-impala.na-rdc02.nxp.com

## HDFS Paths for Unity Catalog Migration
- /user/hive/warehouse/mfg_data/staging_tables
- /etl/dropzone/incoming/daily_files
- /warehouse/prod/analytics_tables
```

### 🚀 Migration Planning Benefits

**Complete Dependency Map:**
- Identifies ALL external dependencies before migration starts
- Prevents "discovered during migration" surprises
- Enables accurate effort estimation

**Risk Assessment:**
- High-risk external scripts requiring manual conversion
- Database connection changes needed
- File system restructuring requirements

**Asset Migration Strategy:**
- Scripts → Convert to PySpark/Databricks SQL
- Database hosts → Databricks SQL compute clusters
- HDFS paths → Unity Catalog managed tables
- Working directories → Databricks workspace paths

## 🚀 Quick Start

### Prerequisites

- Databricks workspace with Unity Catalog enabled
- Python environment with required dependencies
- NiFi XML template files for migration

### 🔥 **Performance Improvements** (v2.2)

The migration system now includes significant performance optimizations, enhanced JSON reliability, and honest job status reporting:

- **⚡ Batched LLM Generation**: Generate code for multiple processors in single requests (up to 96% fewer API calls)
- **🎯 Simplified Pipeline**: Linear execution without orchestration complexity
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

### Databricks Deployment

The application is designed for Databricks deployment using `app.yml` configuration:

1. **Update `app.yml` with your Databricks credentials**:

```yaml
env:
  - name: "DATABRICKS_HOSTNAME"
    value: "https://your-workspace.cloud.databricks.com"
  - name: "DATABRICKS_TOKEN"
    value: "dapi-your-token-here"
  - name: "MODEL_ENDPOINT"
    value: "databricks-meta-llama-3-3-70b-instruct"
  - name: "NOTIFICATION_EMAIL"
    value: "your-email@company.com"
  - name: "ENABLE_LLM_CODE_GENERATION"
    value: "true"
  - name: "MAX_PROCESSORS_PER_CHUNK"
    value: "20"
  - name: "LLM_SUB_BATCH_SIZE"
    value: "5"
```

2. **Deploy as Databricks App**:
   - Upload the project to your Databricks workspace
   - The app will automatically read configuration from `app.yml`
   - Access the Streamlit interface through Databricks Apps

## 🖥️ Using the Streamlit App

1. **Upload NiFi XML**: Use the file uploader to select your NiFi template XML file
2. **Run Migration**: Click "Run Migration" to start the analysis and conversion process
3. **View Results**: The app will display:
   - **Essential Processors Report**: Shows the core data processing logic identified
   - **Migration Guide**: Comprehensive analysis and recommendations
   - **Unknown Processors**: Any processors that need manual review
   - **Asset Summary**: Overview of generated Databricks assets

The app automatically:
- Analyzes and classifies all NiFi processors using AI
- Prunes infrastructure-only processors (logging, routing, flow control)
- Detects semantic data transformation chains
- Generates Databricks migration recommendations
- Creates detailed reports for manual review

## 🔧 Programmatic API Usage

For automated workflows, you can use the migration functions directly:

```python
from tools.simplified_migration import migrate_nifi_to_databricks_simplified

# Complete migration with analysis and reports
result = migrate_nifi_to_databricks_simplified(
    xml_path="path/to/your/nifi_template.xml",
    out_dir="output_directory",
    project="my_migration_project",
    notebook_path="/Workspace/Users/me@company.com/project/main",
)

# Access results
print("Migration completed:", result['migration_result'])
print("Analysis summary:", result['analysis']['summary'])
print("Reports:", result.get('reports', {}))
```

**Key Functions:**
- `migrate_nifi_to_databricks_simplified()`: Complete migration pipeline
- `analyze_nifi_workflow_only()`: Analysis-only mode (fast, no migration)



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

### Simplified Migration Pipeline (Current Implementation)
1. **Workflow Analysis**: LLM-powered processor classification and business understanding
2. **Smart Pruning**: Remove infrastructure-only processors (logging, monitoring)
3. **Chain Detection**: Identify semantic data flow chains using DAG analysis
4. **Semantic Flows**: Create business-meaningful data flows
5. **Code Generation**: LLM-powered PySpark code generation with batched API calls
6. **Job Creation**: Generate **Databricks Jobs** with task dependencies and configurations

> **📋 Output**: All migrations currently produce **Databricks batch jobs**, regardless of workflow type. This provides universal compatibility and reliable execution for all NiFi patterns.


### Key Migration Patterns
- **GetFile/ListFile** → Auto Loader with cloudFiles format
- **PutHDFS/PutFile** → Delta Lake writes with ACID guarantees
- **ConsumeKafka/PublishKafka** → Structured Streaming with Kafka source/sink
- **RouteOnAttribute** → DataFrame filter operations with multiple outputs
- **ConvertRecord** → Format conversions using DataFrame read/write
- **ExecuteSQL** → Spark SQL operations or JDBC connections
- **Funnels** → Bypass logic with union operations in downstream processors

## 🔧 Available Tools

The migration system uses specialized tools in the `tools/` folder:

### Core Migration Tools
- **`xml_tools.py`**: NiFi XML parsing and template extraction
- **`migration_tools.py`**: Main conversion logic and orchestration
- **`improved_classifier.py`**: AI-powered processor classification and analysis
- **`simplified_migration.py`**: Main migration pipeline with pruning and semantic flow detection

### Databricks Integration
- **`job_tools.py`**: Jobs API integration, creation, and deployment
- **`dlt_tools.py`**: Delta Live Tables pipeline generation
- **`eval_tools.py`**: Pipeline validation and data comparison utilities

### Key Functions
- **`migrate_nifi_to_databricks_simplified()`**: Complete migration pipeline with AI analysis
- **`analyze_workflow_patterns()`**: Processor classification and workflow analysis
- **`prune_infrastructure_processors()`**: Smart filtering of infrastructure-only processors
- **`detect_data_flow_chains()`**: Semantic flow detection and chaining

## 📋 Generated Output Structure

```
output_results/project_name/
├── reports/                # Analysis and migration reports
│   ├── essential_processors_report.md  # Core data processing logic identified
│   ├── workflow_analysis.json         # Complete processor classification
│   └── migration_guide.md             # Comprehensive migration recommendations
├── assets/                 # Generated Databricks assets (when deploy=True)
│   ├── notebooks/          # PySpark notebooks for data processing
│   ├── jobs/               # Databricks job configurations
│   └── databricks.yml      # Asset bundle configuration
└── README.md              # Project-specific documentation
```

**Key Files:**
- **`essential_processors_report.md`**: Focus on core business logic requiring migration
- **`workflow_analysis.json`**: Complete AI-powered processor classification and analysis
- **`migration_guide.md`**: Detailed recommendations and migration strategy


## 🔍 Intelligent Workflow Analysis

The system includes advanced workflow analysis capabilities that provide detailed insights into NiFi workflows before and during migration:

### **Smart Processor Classification**

The tool uses a sophisticated **type-first, properties-based classification system** to accurately categorize processors:

#### **Classification Strategy:**
1. **PRIMARY**: Processor type (`org.apache.nifi.processors.*`) - defines behavior class
2. **SECONDARY**: Properties content - what processor actually does
3. **TERTIARY**: Name as hint only - user labels can be misleading

#### **Classification Categories:**
- **📊 Data Transformation**: Processors that modify, transform, or generate data content
- **📦 Data Movement**: Processors that move data without transformation
- **🔗 Infrastructure**: Routing, logging, flow control, and metadata operations
- **🌐 External Processing**: Processors that interact with external systems

#### **Advanced Detection Features:**

**UpdateAttribute Intelligence:**
- Analyzes properties for actual SQL construction vs metadata operations
- Requires SQL keywords + structure indicators + substantial content (>50 chars)
- Correctly identifies SQL-generating processors while classifying filename/counter operations as infrastructure

**ExecuteStreamCommand Smart Detection:**
- **SQL Operations**: Detects IMPALA, HIVE, REFRESH TABLE, database CLI tools
- **File Management**: Identifies rm, delete, cleanup, filesystem operations
- **Properties-first**: Analyzes actual commands rather than relying on misleading names

**Hybrid Rule-Based + LLM Approach:**
- **Rule-based classification** for obvious cases (90% of processors)
- **Batch LLM analysis** for truly ambiguous processors
- **Smart exceptions**: SQL-generating UpdateAttribute, data-generating GenerateFlowFile
- **Significant cost reduction**: ~96% fewer LLM API calls while maintaining accuracy

### **Workflow Analysis Capabilities**

```python
from utils.nifi_analysis_utils import analyze_workflow_patterns

# Comprehensive workflow analysis
result = analyze_workflow_patterns("path/to/nifi_workflow.xml")

# Returns detailed breakdown:
# - Data transformation processors (actual business logic)
# - Infrastructure processors (routing, logging, flow control)
# - Critical processors (high-impact data operations)
# - Architecture recommendations
# - Complexity analysis and migration insights
```

### **Analysis Output Examples**

**Typical Enterprise Workflow:**
```
📊 WORKFLOW OVERVIEW:
   • Total Processors: 58
   • Data Processors: 8 (14%) - Actual business logic
   • Infrastructure: 48 (83%) - Routing, logging, delays
   • Data Movement: 2 (3%) - File transfer operations

🔧 DATA TRANSFORMATION PROCESSORS (8):
   📈 Data Transformers (6):
      - Execute Impala Query (ExecuteStreamCommand): Database operation
      - Execute Hive Query (ExecuteStreamCommand): Database operation
      - Run refresh statement (ExecuteStreamCommand): Database operation
      - SWS FB - Determine query and filename (UpdateAttribute): SQL query construction
      - Determine query and filename (UpdateAttribute): SQL query construction
      - Run Impala check query - PMI DQ (ExecuteStreamCommand): Database operation

   🔌 External Processors (2):
      - PMI-View logging to table - shell script (ExecuteStreamCommand): External system interaction
      - Run beeline query (ExecuteStreamCommand): External system interaction
```

### **Migration Insights**

The analysis provides actionable insights for migration planning:

- **🎯 Focus Areas**: Identifies the ~10% of processors that contain actual business logic
- **⚡ Automation Potential**: Shows which processors can be eliminated (logging, delays, retries)
- **🏗️ Architecture Guidance**: Recommends optimal Databricks architecture based on actual data operations
- **📈 Complexity Assessment**: Evaluates workflow complexity and interconnection patterns
- **🔍 Critical Path Analysis**: Highlights high-impact processors requiring careful migration

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

1. **Authentication Errors**: Verify `DATABRICKS_TOKEN` and `DATABRICKS_HOSTNAME` in `app.yml`
2. **LLM Generation Errors**: Ensure model endpoint is accessible and token has permissions
3. **Job Creation Failures**: Ensure cluster permissions and workspace access
4. **XML Parsing Errors**: Validate NiFi template XML structure
5. **Large Workflows**: Complex workflows may require manual review of generated reports
6. **Unknown Processors**: Review the Unknown Processors report for manual migration needs

### Performance Notes

The system uses intelligent batching for LLM calls to optimize performance:
- Multiple processors are analyzed together to reduce API calls
- Automatic fallback to smaller batches if needed
- Progress tracking shows migration status in real-time


## 📚 Additional Resources

- **CLAUDE.md**: Detailed guidance for Claude Code interactions
- **Sample Workflows**: Example NiFi templates in `nifi_pipeline_file/`
- **Streamlit App**: Interactive web interface for migrations

## 🤝 Contributing

1. Improve AI processor classification rules
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

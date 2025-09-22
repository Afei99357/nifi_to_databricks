# NiFi Analyzer Tools

An intelligent migration tool that converts Apache NiFi workflows into Databricks pipelines using AI-powered analysis and code generation. The system provides both a **Streamlit web app** for interactive migrations and **programmatic APIs** for automated workflows.

## ✨ Key Features

- **🎯 Streamlit Web App**: Interactive 4-page UI for comprehensive NiFi analysis and migration planning
- **🧠 AI-Powered Analysis**: LLM-based processor classification and code generation using `databricks-langchain`
- **✂️ Smart Pruning**: Automatically identifies and focuses on essential data processing logic
- **🔗 Comprehensive Dependencies**: Complete dependency analysis across ALL processors with variable tracking, impact analysis, and circular dependency detection
- **📦 Intelligent Asset Discovery**: Automatic detection of scripts, databases, file paths, and external dependencies
- **🔄 Semantic Flow Detection**: Maps NiFi processor chains to logical data transformation flows
- **📊 Table Lineage Tracking**: End-to-end data flow analysis from source tables through processors to destination tables
- **📋 Comprehensive Reports**: Detailed analysis with interactive filtering, downloads, and session state persistence
- **⚙️ Databricks Native**: Uses `app.yaml` configuration for seamless Databricks deployment

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

1. **📤 Upload**: Upload your NiFi XML template through the Streamlit Dashboard
2. **🚀 Classify**: AI-powered processor classification separating business logic from infrastructure
3. **🔗 Dependencies**: Comprehensive dependency mapping across ALL processors (development branch)
4. **📦 Assets**: Intelligent discovery of scripts, databases, file paths, and external dependencies
5. **📊 Lineage**: End-to-end table lineage tracking and data flow analysis
6. **📋 Plan**: Comprehensive migration planning with downloadable reports and analysis

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

**Example Output:**
```bash
# NiFi Asset Summary
- Script Files Found: 8
- HDFS Paths Found: 45
- Database Hosts Found: 2
- Working Directories: 6

## Script Files Requiring Migration
- /users/etl_service/scripts/data/run_query.sh
- /users/etl_service/scripts/data/backup_tables.py
- /scripts/data/transform_results.sql

## Database Connections
- **Impala**: analytics-cluster.company.com
- **MySQL**: metadata-db.company.com

## HDFS Paths for Unity Catalog Migration
- /user/hive/warehouse/customer_data/staging
- /etl/incoming/daily_batch
- /warehouse/prod/summary_tables
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

## 🔗 Comprehensive Dependency Analysis *(Development Branch)*

The system includes a sophisticated dependency analysis engine that maps ALL processor relationships and dependencies across the entire NiFi workflow, providing complete visibility into processor interconnections.

### 🎯 **What It Analyzes**

**Variable Dependencies:**
- Track all `${variable}` definitions and usages across processors
- Identify external variables (used but not defined within workflow)
- Map variable providers to consumers with detailed context

**Connection Dependencies:**
- Complete processor-to-processor flow connection mapping
- Bidirectional relationship analysis ("depends on" + "dependents")
- Connection relationship details with source/destination context

**Property Dependencies:**
- Processors that reference other processors' outputs in configurations
- Controller service references and processor group dependencies
- Cross-processor configuration references

**Configuration Dependencies:**
- Shared configuration values across multiple processors
- Common property patterns and reused settings
- Configuration consistency analysis

### 🎯 **Advanced Analysis Features**

**Impact Analysis:**
- **High-Impact Processors**: Processors with many dependents (change affects many others)
- **Isolated Processors**: Processors with no dependencies (safe to modify independently)
- **Dependency Chains**: Sequential dependency relationships and critical paths
- **Circular Dependencies**: Problematic dependency cycles requiring attention

**Interactive Filtering:**
- Filter processors by type, dependencies, or search terms
- Detailed dependency views for each processor
- Variable usage tracking with definition sources
- Connection mapping with relationship context

### 🚀 **Use Cases & Benefits**

**Migration Planning:**
- "What are ALL the dependencies I need to consider for this processor?"
- Complete dependency map prevents migration surprises
- Identify critical processors requiring careful migration order

**Impact Analysis:**
- "If I change this processor, what else will be affected?"
- Risk assessment for processor modifications
- Change planning with comprehensive dependency awareness

**Debugging & Troubleshooting:**
- "Why is this processor not working? What does it depend on?"
- Variable resolution tracing and dependency validation
- Configuration consistency checking across processors

**Architecture Understanding:**
- Complete workflow relationship mapping
- Identify tightly coupled vs loosely coupled processor groups
- Optimize workflow design based on dependency patterns

### 📊 **Example Analysis Output**

```
📊 Dependency Overview: 87 Total Processors
- Total Dependencies: 156 relationships
- High-Impact Processors: 12 (affecting 5+ others)
- Isolated Processors: 23 (safe to modify)
- Variable Dependencies: 34 variables tracked
- Circular Dependencies: 2 cycles detected ⚠️

🎯 High-Impact Processors:
- SQL_Query_Builder: 8 dependents
- Config_Variable_Provider: 6 dependents
- Data_Router: 5 dependents

📊 Variable Dependencies:
- ${query.select}: Defined by 2 processors, used by 8 processors
- ${hdfs.path}: External variable (used by 5 processors, not defined)
- ${batch.size}: Defined by 1 processor, used by 3 processors
```

**Interactive Features:**
- 5-tab interface with comprehensive filtering
- Download options for markdown reports and raw JSON data
- Real-time dependency relationship exploration
- Impact analysis with visual dependency chains

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

The application is designed for Databricks deployment using `app.yaml` configuration:

1. **Create your `app.yaml` file** (this file is git-ignored for security):

```yaml
command: ["streamlit", "run", "streamlit_app/Dashboard.py"]

env:
  - name: "SERVING_ENDPOINT"
    valueFrom: "serving-endpoint"
  - name: "NOTIFICATION_EMAIL"
    value: "your-email@company.com"
  - name: "ENABLE_LLM_CODE_GENERATION"
    value: "true"
  - name: "MAX_PROCESSORS_PER_CHUNK"
    value: "20"
  - name: "LLM_SUB_BATCH_SIZE"
    value: "5"
  - name: "STREAMLIT_BROWSER_GATHER_USAGE_STATS"
    value: "false"
```

2. **Deploy as Databricks App**:
   - Upload the project to your Databricks workspace
   - The app will automatically read configuration from `app.yaml`
   - Dependencies will install from `requirements.txt`
   - Access the Streamlit interface through Databricks Apps

**Note**: Your `app.yaml` file with credentials is git-ignored and stays private. When you pull updates, your local credentials won't be overwritten or cause merge conflicts.

## 🖥️ Using the Streamlit App

The Streamlit app provides a comprehensive web interface for NiFi to Databricks migration analysis with four specialized pages:

### 📊 **Dashboard**
1. **Upload NiFi XML**: Upload your NiFi template XML file to begin analysis
2. **Navigation Hub**: Access all analysis tools through dedicated buttons:
   - 🚀 **Classify Processors**: Processor classification and pruning analysis
   - 📦 **Extract Assets**: Asset discovery and migration planning
   - 📊 **Lineage & Connections**: Table lineage and data flow analysis
   - 🔄 **Variable Dependencies**: Comprehensive variable analysis and flow tracking
3. **Processor Information**: View raw processor details with filtering and search
4. **Clear Results**: Reset all cached results and uploaded files

### 🚀 **Processor Classification & Pruning**
1. **Smart Classification**: AI-powered analysis of all processors using hybrid rule-based + LLM approach
2. **Essential Processors Report**: Core data processing logic requiring migration
3. **Unknown Processors Report**: Processors requiring manual review with classification reasons
4. **Auto-expand Sections**: All reports displayed by default for easy review
5. **Result Caching**: Analysis persists when switching between pages

### 🔄 **Variable Dependencies**
**Comprehensive variable analysis and flow tracking across ALL processors in the workflow:**

1. **Interactive 4-Tab Interface**:
   - **📋 Variable Details**: Complete inventory of all variables with their source processors
   - **🔄 Variable Flow Tracking**: Trace individual variables through their complete lifecycle
   - **📝 Variable Actions**: Analyze how variables are defined, modified, and used across processors
   - **🌐 Variable Flow Connections**: See variable flow paths through processor connections

2. **Tab-Specific Features**:

   **📋 Variable Details Tab:**
   - **Complete Variable Inventory**: All variables in the workflow with source processors
   - **Source Information**: Variable name, processor name, processor ID, processor type
   - **Source Classification**: DEFINES (defined in workflow) vs EXTERNAL (used but not defined)
   - **Filtering**: Filter by variable name and source type
   - **Download**: Export complete variable inventory as CSV

   **🔄 Variable Flow Tracking Tab:**
   - **Variable Selection**: Choose any variable to trace its complete lifecycle
   - **Flow Statistics**: Total processors, definitions, usages, internal/external status
   - **Processor Flow Chain**: Complete table showing all processors that interact with the variable
   - **Action Classification**: DEFINES, MODIFIES, TRANSFORMS, USES, EVALUATES, LOGS, EXECUTES
   - **Flow Visualization**: Visual flow chains showing processor connections with IDs

   **📝 Variable Actions Tab:**
   - **Summary Overview**: All variables with definition, modification, and usage counts
   - **Detailed Analysis**: Select any variable for in-depth analysis
   - **Three Analysis Tables**: Definitions, Transformations, and Usages with full processor details
   - **Complete Values**: Full expressions and transformation details without truncation
   - **Filtering**: Filter by status, minimum usage counts, and text search

   **🌐 Variable Flow Connections Tab:**
   - **Connection Flow Data**: Variable flow paths between connected processors
   - **Flow Path Analysis**: Each row represents one hop in a variable's journey
   - **Dynamic Flow Chains**: Select a variable to see its complete flow paths
   - **Connection Details**: Source/target processors, connection types, flow chains
   - **Relationship Mapping**: Shows success/failure relationship paths

3. **Advanced Capabilities**:
   - **External Variable Detection**: Identifies variables used but not defined in the workflow
   - **Flow Path Tracing**: Maps how variables move through processor connections
   - **Value Transformation Tracking**: Shows how variables are modified along their journey
   - **Processor ID Display**: Uses full processor IDs for accurate identification
   - **Plain English Descriptions**: Clear action descriptions instead of technical symbols

4. **Use Cases**:
   - **Migration Planning**: "Which processors define the variables that others depend on?"
   - **Impact Analysis**: "If I change this variable definition, what processors are affected?"
   - **Debugging**: "Where does this variable get its value and how is it transformed?"
   - **Data Flow Understanding**: "How do variables flow through my NiFi pipeline?"
   - **External Dependency Mapping**: "Which variables come from outside the workflow?"

### 📦 **Asset Extraction**
1. **Intelligent Asset Discovery**: Comprehensive scanning of all processors to identify migration dependencies
2. **Asset Categories**:
   - **Script Files**: Shell, Python, SQL, Java JARs, and other executables (11 types)
   - **Database Connections**: Impala/Hive, MySQL, PostgreSQL, Oracle, MongoDB
   - **File System Paths**: HDFS paths, data warehouse locations, staging directories
   - **Working Directories**: Script execution and processing workspaces
3. **Filtering and Search**: Dynamic filtering by asset type with text search
4. **Migration Planning**: Asset summary with download options for external analysis

### 📊 **Lineage & Connections**
1. **Table-Level Data Lineage**: Track end-to-end data flow through NiFi workflows
2. **Analysis Results**:
   - **Summary Metrics**: Processors, connections, and discovered table chains
   - **Table Lineage Chains**: Direct table-to-table data flow relationships
   - **Connection Details**: Complete processor-to-processor connection mapping
3. **Critical Tables Analysis**: Identify high-connectivity tables (important data assets)
4. **Download Options**: Export lineage results and connections as CSV for external analysis

### ✨ **Universal Features**
- **Result Caching**: All analysis results persist when switching between pages
- **Navigation Protection**: Buttons disabled during processing to prevent interruption
- **Auto-Expand Sections**: All expandable content displayed by default
- **Clean Interface**: Streamlined UI focusing on essential functionality
- **Error Handling**: Graceful handling of analysis errors with detailed feedback
- **Session Management**: Smart file and result caching across page navigation

### 🎯 **Complete Analysis Workflow**
The app provides a comprehensive analysis pipeline:
1. **Classification**: Analyze and classify all processors (infrastructure vs business logic)
2. **Assets**: Discover external dependencies requiring migration
3. **Lineage**: Trace table-to-table data flows and connections
4. **Variables**: Complete variable analysis and flow tracking across all processors

Each page focuses on a specific aspect of migration planning while maintaining shared context through session state management.

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

## 📊 Table Lineage Analysis (NEW)

The system now includes **table-level data lineage tracking** that shows end-to-end data flow through your NiFi workflows, mapping exactly how data moves from source tables/files through processors to destination tables/files.

### ✨ **Key Features**
- **🔄 Table-to-Table Lineage**: Tracks data flow chains like `source_table → [Processor1] → [Processor2] → target_table`
- **📁 File-to-Table Mapping**: Shows how files are transformed into database tables
- **🌐 Cross-Group Analysis**: Handles embedded process groups and their connections properly
- **🎯 Critical Table Identification**: Finds tables with high connectivity (important data assets)
- **🏗️ Migration Planning**: Provides specific recommendations for Databricks Unity Catalog architecture

### 🎮 **How to Use**

**In Streamlit App:**
1. Upload your NiFi XML file
2. Click **📊 Analyze Table Lineage** button (new option alongside "Run Migration")
3. View interactive results with:
   - Summary metrics (tables, files, lineage chains)
   - Data flow visualization with processor details
   - Critical tables analysis with connectivity scores
   - Complete downloadable markdown report

**Programmatic Usage:**
```python
from tools.networkx_complete_flow_analysis import (
    build_complete_nifi_graph_with_tables,
    analyze_complete_workflow_with_tables,
    generate_table_lineage_report
)

# Read XML content
with open("your_nifi_file.xml", 'r') as f:
    xml_content = f.read()

# Build enhanced graph with table lineage
G = build_complete_nifi_graph_with_tables(xml_content)

# Run analysis
analysis = analyze_complete_workflow_with_tables(G, k=10)

# Generate report
report = generate_table_lineage_report(analysis)
```

### 📋 **Example Output**
```
📊 End-to-End Data Flow Chains
==============================

Chain 1: /raw/input_data.csv → staging.transactions
Data Flow Path: /raw/input_data.csv → **[GetFile]** → **[ConvertRecord]** → staging.transactions
Complexity: 2 processing steps

Chain 2: staging.transactions → curated.clean_data
Data Flow Path: staging.transactions → **[ExecuteSQL]** → curated.clean_data
Complexity: 1 processing steps

Chain 3: curated.clean_data → analytics.summary
Data Flow Path: curated.clean_data → **[AggregateSQL]** → **[PutHDFS]** → analytics.summary
Complexity: 2 processing steps

🎯 Critical Tables (High Connectivity)
=====================================
- **staging.transactions**: 4 connections (2 readers, 2 writers)
- **curated.clean_data**: 3 connections (1 reader, 2 writers)
```

### 🔧 **Table Extraction Logic**

**Smart Processor Analysis:**
- **GetFile/ListFile**: Input files → `file:/path/to/input`
- **PutHDFS/PutFile**: Output files → `file:/path/to/output`
- **ExecuteSQL**: SQL parsing → `FROM table_name` (input), `INSERT INTO table_name` (output)
- **Property Analysis**: Table name properties → `database.table_name`
- **Path Inference**: Inferred database/table from file paths

**Graph Enhancement:**
- **Original nodes**: NiFi processors, input/output ports, funnels
- **New nodes**: `table:database.table_name`, `file:/path/to/file`
- **Smart edges**: `reads_from`, `writes_to`, `reads_file`, `writes_file`

### 🎯 **Migration Benefits**

**Architecture Planning:**
- **Simple Chains**: Standard Databricks Jobs with table dependencies
- **Complex Chains**: Delta Live Tables for multi-stage pipelines
- **High-connectivity Tables**: Central data assets requiring careful Unity Catalog schema design

**Migration Priorities:**
1. **Critical Tables** → Design Unity Catalog schemas for high-connectivity tables first
2. **Source Tables** → Plan data ingestion architecture (Auto Loader, etc.)
3. **Sink Tables** → Design output data architecture (Delta Lake, etc.)
4. **Complex Chains** → Multi-stage DLT pipeline dependencies

**Databricks Patterns:**
- **GetFile → ConvertRecord → PutHDFS**: Auto Loader → DataFrame transformations → Delta Lake
- **ExecuteSQL chains**: Spark SQL operations with explicit table dependencies
- **Multi-processor chains**: DLT pipeline stages with proper lineage

### ⚠️ **Important Notes**

**What It CAN Extract:**
- ✅ Processor connection topology from XML
- ✅ Table references from processor properties and SQL queries
- ✅ File input/output paths and directory structures
- ✅ Cross-group data flow patterns through embedded process groups

**What It CANNOT Extract:**
- ❌ Bronze/Silver/Gold semantic layers (business concepts not in XML)
- ❌ Data quality rules or business logic details
- ❌ Runtime data volumes or performance characteristics
- ❌ Complex SQL transformation details beyond table names

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

1. **Authentication Errors**: Verify `SERVING_ENDPOINT` configuration in `app.yaml`
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

### Declarative Classification

Processor triage now comes from editable rules stored in `classification_rules.yaml`. The Streamlit dashboard reads these rules through `tools/classification/rules_engine.py`, classifying each NiFi processor into migration-centric categories (Business Logic, Source Adapter, Sink Adapter, Orchestration / Monitoring, Infrastructure Only, Ambiguous). Add or tweak rules via YAML—no code change required—and commit manual decisions for specific processor IDs in `classification_overrides.yaml`. See `docs/classification_rules.md` for the rule schema and contributor tips.

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

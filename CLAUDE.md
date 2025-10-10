# CLAUDE.md

This file provides guidance to Claude Code when working with code in this repository.

## Project Overview

**NiFi to Databricks Migration Tool** - An AI-powered system that analyzes Apache NiFi workflows and generates migration plans for Databricks. The system provides both a Streamlit web UI and programmatic APIs.

**Core Architecture**: Declarative classification engine → Smart pruning → Semantic flow detection → Migration recommendations

## Development Commands

### Environment Setup
```bash
# Install dependencies with uv
uv sync

# Install dev dependencies (pre-commit hooks, testing tools)
uv sync --group dev

# Setup pre-commit hooks
uv run pre-commit install
# OR use the convenience script:
./scripts/setup_dev.sh
```

### Running the Application

**Streamlit Web App (Primary Interface):**
```bash
# Local development
streamlit run streamlit_app/Dashboard.py

# Databricks deployment uses app.yaml configuration
# The app reads SERVING_ENDPOINT, NOTIFICATION_EMAIL, etc. from app.yaml
```

**Note**: `app.yaml` is git-ignored (contains credentials). Create your own from the template in README.md.

### Code Quality

```bash
# Run all pre-commit hooks manually
uv run pre-commit run --all-files

# Run specific checks
uv run black .                    # Format code
uv run isort .                    # Sort imports
uv run flake8 .                   # Linting
uv run mypy .                     # Type checking

# Check exception handling quality (non-gating)
python3 scripts/exception_metrics.py .
```

### Testing

```bash
# Run all tests
uv run pytest

# Run specific test file
uv run pytest tests/test_declarative_classifier.py

# Run with markers
uv run pytest -m unit           # Unit tests only
uv run pytest -m "not slow"     # Skip slow tests
uv run pytest -v                # Verbose output
```

### Schema Migration Tool (Hive DDL → Databricks DDL)

Located in `tools/schema_migration_tool/`:

```bash
# Basic rule-based conversion (fast, free, no LLM)
python tools/schema_migration_tool/convert_ddl.py -i hive.sql -o databricks.sql

# With optional Databricks LLM enhancement
python tools/schema_migration_tool/convert_ddl_with_llm.py -i hive.sql -o databricks.sql --llm-full

# Test the converter with built-in example
python -m tools.schema_migration_tool.databricks_ddl_generator
```

See `tools/schema_migration_tool/QUICKSTART.md` for detailed usage.

## Architecture Deep Dive

### Declarative Classification System

**Core Principle**: Processor classification is driven by YAML rules, not hardcoded logic.

**Files**:
- `classification_rules.yaml` - Reusable rules applied to all workflows
- `classification_overrides.yaml` - Manual corrections for specific processor IDs
- `tools/classification/rules_engine.py` - Rule evaluation engine
- `tools/classification/processor_features.py` - Feature extraction from processors

**Rule Structure**:
```yaml
- name: "Impala Query Execution"
  processor_types:
    - org.apache.nifi.processors.standard.ExecuteStreamCommand
  conditions:
    - feature: scripts.has_database_cli
      operator: equals
      value: true
  migration_category: "Business Logic"
  databricks_target: "Databricks SQL Warehouse"
  confidence: 0.9
  notes: "Impala/Hive queries should use Databricks SQL"
```

**Migration Categories**:
1. **Business Logic** - Core data transformations requiring migration
2. **Source Adapter** - Data ingestion (GetFile, ConsumeKafka, etc.)
3. **Sink Adapter** - Data output (PutHDFS, PublishKafka, etc.)
4. **Orchestration / Monitoring** - Flow control, monitoring (support processors)
5. **Infrastructure Only** - Logging, delays, routing (can be eliminated)
6. **Ambiguous** - Requires manual review

**Classification Workflow**:
```
NiFi XML → parse_nifi_template_impl() → extract processor features
         → rules_engine.py evaluates rules → categorize processors
         → migration_orchestrator.py groups by category → generate reports
```

### Migration Pipeline Flow

**Primary Entry Point**: `tools/migration_orchestrator.py`

```python
migrate_nifi_to_databricks_simplified()
  ↓
  1. Parse NiFi XML (xml_tools.py)
  2. Classify processors (classification/rules_engine.py)
  3. Categorize into essentials/support/infrastructure/ambiguous
  4. Detect data flow chains (improved_pruning.py)
  5. Generate migration recommendations
  6. Return structured result
```

**Key Functions**:
- `classify_workflow()` - Apply declarative rules to all processors
- `_categorise_processors()` - Group by migration category
- `detect_data_flow_chains()` - Identify semantic data flows using DAG analysis
- `analyze_nifi_workflow_only()` - Analysis-only mode (no migration)

### Streamlit Architecture

**Multi-page App Structure**:
```
streamlit_app/
├── Dashboard.py                      # Main entry, file upload, navigation
└── pages/
    ├── 01_Processor_Classification.py  # AI-powered processor analysis
    ├── 02_Table_Extraction.py          # Database table references
    ├── 03_Script_Extraction.py         # External scripts/assets
    ├── 04_Lineage_Connections.py       # Table-to-table data flow
    ├── 05_Variable_Dependencies.py     # Variable tracking across processors
    └── 06_AI_Assist.py                 # LLM-powered code generation
```

**Session State Management**:
- `st.session_state.xml_path` - Uploaded NiFi XML file path
- `st.session_state.classification_result` - Cached processor classifications
- `st.session_state.lineage_result` - Cached lineage analysis
- Results persist across page navigation

**Key Pattern**: Each page checks for cached results in session state, only re-runs analysis if missing.

### Asset Discovery System

**Purpose**: Identify ALL external dependencies requiring migration (scripts, databases, HDFS paths, etc.)

**Implementation**: `tools/script_extraction.py` and asset extraction helpers

**Discovered Assets**:
- Script files (`.sh`, `.py`, `.sql`, `.jar`, etc.) - 11 types supported
- Database connections (Impala, Hive, MySQL, PostgreSQL, etc.)
- HDFS paths (`/user/`, `/warehouse/`, `/etl/`)
- Working directories and execution paths

**Pattern-based Detection**:
- Regex patterns for file paths and extensions
- JDBC URL parsing for database hosts
- Property name semantics (e.g., "Command Path", "Working Directory")
- SQL content analysis

### Table Lineage Analysis

**Purpose**: Track end-to-end data flow from source tables → processors → destination tables

**Implementation**: `tools/nifi_table_lineage.py`

**Capabilities**:
- Builds NetworkX graph with processors + table nodes
- Extracts table references from ExecuteSQL, properties, file paths
- Traces data flow chains: `source_table → [Proc1] → [Proc2] → target_table`
- Identifies high-connectivity tables (critical data assets)
- Generates migration recommendations (Unity Catalog, DLT pipelines)

**Graph Structure**:
- **Nodes**: Processors, tables (`table:db.name`), files (`file:/path`)
- **Edges**: `reads_from`, `writes_to`, `reads_file`, `writes_file`

### Variable Flow Tracking

**Purpose**: Complete variable lifecycle analysis across the entire NiFi workflow

**Implementation**: `tools/variable_extraction.py`

**Capabilities**:
- Track `${variable}` definitions and usages
- Identify external variables (used but not defined in workflow)
- Map variable flow through processor connections
- Classify actions: DEFINES, MODIFIES, TRANSFORMS, USES, EVALUATES

**Streamlit Interface**: 4-tab UI with filtering, flow visualization, and CSV export

### Schema Migration Tool (DDL Converter)

**Purpose**: Convert Hive/Impala DDL to Databricks Delta Lake DDL

**Location**: `tools/schema_migration_tool/`

**Architecture**: Hybrid rule-based + optional LLM enhancement

**Components**:
1. **Rule-based Converter** (always run, fast, free):
   - `hive_ddl_parser.py` - Regex-based DDL parsing
   - `databricks_ddl_generator.py` - Databricks DDL generation
   - Handles: table structure, partitions, HDFS → DBFS paths, STRING → TIMESTAMP optimization

2. **Optional LLM Enhancement**:
   - `databricks_llm_enhancer.py` - Uses Databricks Foundation Model API
   - `convert_ddl_with_llm.py` - CLI with `--validate`, `--suggest`, `--enhance` flags
   - Same pattern as AI assistant: `get_deploy_client("databricks").predict()`

**Design Rationale**: Rule-based handles 95% of cases perfectly. LLM adds validation/suggestions for edge cases.

## Configuration Files

### classification_rules.yaml
Declarative rules for processor classification. Each rule specifies processor types, feature conditions, migration category, and Databricks target.

**When to edit**: Add rules when encountering new processor patterns that should be reusable across workflows.

### classification_overrides.yaml
Manual corrections keyed by processor ID. Overrides always win over rules.

**When to edit**: One-off exceptions, custom scripts, temporary migrations. Prefer rules for reusable patterns.

### app.yaml (git-ignored)
Databricks App configuration containing credentials and environment variables.

**Required variables**:
- `SERVING_ENDPOINT` - Databricks LLM endpoint name
- `NOTIFICATION_EMAIL` - Email for job notifications
- `ENABLE_LLM_CODE_GENERATION` - Toggle LLM features
- `MAX_PROCESSORS_PER_CHUNK` - Batch size for LLM calls (default: 20)
- `LLM_SUB_BATCH_SIZE` - Sub-batch size (default: 5)

## Key Integration Patterns

### Databricks LLM Integration

**Pattern** (used in AI Assist page and DDL enhancement):
```python
from mlflow.deployments import get_deploy_client

messages = [{"role": "user", "content": prompt}]

response = get_deploy_client("databricks").predict(
    endpoint=endpoint_name,
    inputs={"messages": messages, "max_tokens": max_tokens}
)

# Extract response content (handles different response formats)
if "choices" in response:
    content = response["choices"][0]["message"]["content"]
```

**Files using this pattern**:
- `model_serving_utils.py` - AI assistant utilities
- `tools/schema_migration_tool/databricks_llm_enhancer.py` - DDL validation
- Streamlit page: `pages/06_AI_Assist.py`

### NiFi XML Parsing

**Primary function**: `parse_nifi_template_impl()` in `tools/xml_tools.py`

**Returns**:
```python
{
    "processors": [
        {
            "id": "proc-id",
            "name": "processor name",
            "type": "org.apache.nifi.processors.standard.GetFile",
            "properties": {"key": "value", ...},
            # ... other metadata
        }
    ],
    "connections": [...],
    "process_groups": [...]
}
```

**Pattern for new tools**:
```python
from tools.xml_tools import parse_nifi_template_impl

with open(xml_path, 'r') as f:
    xml_content = f.read()

template_data = parse_nifi_template_impl(xml_content)
processors = template_data.get("processors", [])

for processor in processors:
    # Access processor["type"], processor["properties"], etc.
```

### Processor Feature Extraction

**Function**: `extract_processor_features()` in `tools/classification/processor_features.py`

**Extracted Features**:
- `sql.*` - SQL-related flags (has_dml, has_ddl, has_select, table_count)
- `scripts.*` - Script detection (inline_count, external_count, has_database_cli)
- `variables.*` - Variable usage counts (defines, uses, transforms)
- `graph.*` - Connectivity (upstream_count, downstream_count, is_source, is_sink)
- `controller_services` - Referenced services
- `processor_type` - Fully-qualified NiFi type

**Usage in rules**:
```yaml
conditions:
  - feature: sql.has_dml
    operator: equals
    value: true
  - feature: sql.table_count
    operator: gt
    value: 0
```

## Common Development Tasks

### Adding a New Processor Classification Rule

1. Upload NiFi XML to Streamlit Dashboard
2. Review "Ambiguous" processors in Processor Classification page
3. Check processor features (expanded row shows SQL flags, scripts, etc.)
4. Decide on migration category
5. Add rule to `classification_rules.yaml`:
   ```yaml
   - name: "My New Pattern"
     processor_types:
       - org.apache.nifi.processors.MyProcessor
     conditions:
       - feature: sql.has_dml
         operator: equals
         value: true
     migration_category: "Business Logic"
     databricks_target: "Databricks SQL Task"
     notes: "Why this rule exists"
   ```
6. Refresh Streamlit page to verify
7. Add test case to `tests/test_declarative_classifier.py`

### Adding a New Streamlit Analysis Page

1. Create `streamlit_app/pages/NN_Page_Name.py` (NN = 2-digit sequence)
2. Import analysis functions from `tools/`
3. Use session state for file path: `st.session_state.xml_path`
4. Cache results in session state for cross-page persistence
5. Follow existing page patterns (tabs, filtering, download buttons)
6. Add navigation button in `Dashboard.py` if needed

### Extending Asset Discovery

1. Add pattern detection to `tools/script_extraction.py` or create new helper
2. Update `extract_assets_from_properties()` to recognize new asset types
3. Modify Streamlit page `03_Script_Extraction.py` to display new assets
4. Update `extract_nifi_assets_only()` in `migration_orchestrator.py` to include new asset counts

### Adding Schema Migration Features

Located in `tools/schema_migration_tool/`:

1. **Rule-based conversion**: Edit `databricks_ddl_generator.py`
   - Add type mappings, path conversions, table properties
2. **LLM enhancement**: Edit `databricks_llm_enhancer.py`
   - Modify prompts for validation, suggestions, or comments
3. **CLI options**: Edit `convert_ddl_with_llm.py`
   - Add argument parsing and workflow logic

See `tools/schema_migration_tool/LLM_ENHANCEMENT.md` for architecture details.

## Testing Strategy

### Test Files
- `tests/test_declarative_classifier.py` - Classification rule validation
- `tests/test_*.py` - Unit tests for individual modules

### Running Tests
```bash
# All tests
uv run pytest

# Specific test file
uv run pytest tests/test_declarative_classifier.py

# With coverage (if configured)
uv run pytest --cov=tools --cov-report=html
```

### Adding Classification Tests

When adding new rules to `classification_rules.yaml`, add corresponding test:

```python
def test_new_processor_pattern():
    """Test that MyProcessor with SQL is classified as Business Logic."""
    features = {
        "processor_id": "test-id",
        "processor_type": "org.apache.nifi.processors.MyProcessor",
        "sql": {"has_dml": True, "table_count": 1},
        # ... other features
    }

    result = apply_rules_to_processor(features)

    assert result["migration_category"] == "Business Logic"
    assert result["databricks_target"] == "Databricks SQL Task"
```

## Code Quality Standards

### Pre-commit Hooks (Enforced)
- **Black**: Code formatting (88 char line length)
- **isort**: Import sorting (Black-compatible)
- **flake8**: Linting (lenient settings for initial setup)
- **mypy**: Type checking (gradual adoption, see `docs/mypy_strictness_guide.md`)
- **Exception metrics**: Non-gating warnings for bare except blocks

### Type Checking (MyPy)

**Current state**: Lenient mode with module ignores
**Path to strictness**: See `docs/mypy_strictness_guide.md`

**Module overrides** (gradually remove these):
- `tools.*` - Ignored for now
- `agents.*` - Ignored for now
- `registry.*` - Ignored for now
- `convert_nifi_using_agent` - Databricks-specific (dbutils)

### Exception Handling Quality

**Non-gating checker** tracks:
- Bare `except:` usage (banned pattern)
- `except Exception:` usage (discouraged)
- Metrics: try/except ratio, with statement usage

**Command**: `python3 scripts/exception_metrics.py .`

## Important Notes for Claude Code

### When Working with Classification Rules

**DO**:
- Add reusable patterns to `classification_rules.yaml`
- Use `classification_overrides.yaml` for one-off exceptions
- Test rules in Streamlit before committing
- Add test cases for new rules

**DON'T**:
- Hardcode processor classifications in Python code
- Bypass the declarative system
- Skip testing rules with actual NiFi XML

### When Working with Databricks Integration

**Authentication**: Assumes Databricks environment with configured credentials
- LLM features require `SERVING_ENDPOINT` in `app.yaml`
- Jobs API requires workspace permissions
- Unity Catalog operations require catalog permissions

**LLM Pattern**: Always use `get_deploy_client("databricks").predict()` pattern from `model_serving_utils.py`

### When Working with NiFi XML

**Parser limitations**:
- XML structure is parsed, NOT runtime behavior
- Cannot extract business logic semantics (Bronze/Silver/Gold)
- Cannot determine data volumes or performance characteristics
- SQL queries are extracted as strings, not semantically analyzed

**Best practice**: Combine XML parsing with LLM analysis for deeper insights

### Schema Migration Tool Context

**Design philosophy**: Hybrid approach
1. **Always** run rule-based conversion first (fast, deterministic, free)
2. **Optionally** enhance with LLM validation/suggestions

**When to recommend LLM enhancement**:
- Critical production tables
- Non-standard DDL patterns
- Need for optimization suggestions
- Want human-readable comments

**When rule-based is sufficient**:
- Standard Hive DDL
- Batch processing many tables
- Development/testing environments

## File Locations Quick Reference

### Core Analysis Tools
- `tools/xml_tools.py` - NiFi XML parsing
- `tools/classification/rules_engine.py` - Declarative classification engine
- `tools/classification/processor_features.py` - Feature extraction
- `tools/migration_orchestrator.py` - Main migration pipeline
- `tools/improved_pruning.py` - Data flow chain detection

### Asset Discovery
- `tools/script_extraction.py` - Script and asset detection
- `tools/table_extraction.py` - Database table references
- `tools/variable_extraction.py` - Variable flow tracking
- `tools/nifi_table_lineage.py` - Table-to-table lineage

### Schema Migration
- `tools/schema_migration_tool/` - Hive DDL → Databricks DDL converter
  - `convert_ddl.py` - Rule-based CLI
  - `convert_ddl_with_llm.py` - Hybrid CLI with LLM options
  - `databricks_llm_enhancer.py` - LLM validation/enhancement
  - `QUICKSTART.md` - Usage guide

### Streamlit UI
- `streamlit_app/Dashboard.py` - Main dashboard
- `streamlit_app/pages/` - Analysis pages (6 specialized pages)

### Configuration
- `classification_rules.yaml` - Declarative processor rules
- `classification_overrides.yaml` - Manual processor overrides
- `app.yaml` - Databricks App config (git-ignored, create from README template)
- `pyproject.toml` - Python dependencies and tool configuration

### Documentation
- `README.md` - Comprehensive project documentation
- `docs/classification_rules.md` - Rule system guide
- `docs/mypy_strictness_guide.md` - Type checking adoption plan
- `tools/schema_migration_tool/LLM_ENHANCEMENT.md` - DDL converter architecture

## Branch Context

**Current branch**: `schema_ai_migration` (as of last session)
**Main branch**: `main`

Recent work included adding Databricks LLM enhancement to the schema migration tool.

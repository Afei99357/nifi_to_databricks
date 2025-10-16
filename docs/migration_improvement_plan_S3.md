# NiFi to Databricks Migration Tool - Improvement Plan (S3 Architecture)

**Date:** 2025-01-XX
**Architecture:** Source data in AWS S3 → Databricks Delta Tables
**Goal:** Move from 6.5/10 to 9/10 migration quality
**Timeline:** 4 weeks (reduced from 6 weeks)

---

## Executive Summary

Based on **S3-based architecture** (no JDBC source connections), we can **skip** connection resolution and Sqoop translation phases. This reduces the implementation from 6 weeks to **4 weeks** while addressing all critical gaps.

### **Simplified Architecture:**

```
AWS S3 Bucket (Parquet/CSV/JSON)
    ↓
  Auto Loader / spark.read
    ↓
Databricks Delta Tables
    ↓
Transformations (from extracted SQL)
```

### **What We Skip:**
- ❌ ~~Phase 2: Connection Service Resolution~~ - Not needed (no JDBC)
- ❌ ~~Phase 5: Sqoop Translation~~ - Not needed (no Sqoop imports)

### **What We Keep (Critical):**
- ✅ **Phase 1: SQL Extraction** - Extract schemas and transformations from NiFi
- ✅ **Phase 3: Parallel Flow Detection** - Detect multi-site/multi-table patterns
- ✅ **Phase 4: Code Generation** - Generate S3-aware notebooks
- ✅ **Phase 6: Validation** - Ensure quality improvements

---

## Phase 1: SQL Extraction Foundation (Week 1-2)

### **Goal:** Extract CREATE TABLE schemas and INSERT OVERWRITE transformations

**Why Critical:** Without this, notebooks use generic schemas (id, log_timestamp) instead of actual business schemas (mid, ts_state_start, seq_num, state, prev_state, etc.)

### **Implementation:**

#### **Task 1.1: ExecuteStreamCommand SQL Parser**

```python
# New file: tools/sql_extraction.py

def extract_sql_from_nifi_workflow(xml_path: str) -> Dict[str, Any]:
    """Extract all SQL statements from ExecuteStreamCommand processors.

    Returns:
        {
            "schemas": {
                "emt_log_new": {
                    "columns": [
                        {"name": "mid", "type": "STRING", "comment": "Machine ID"},
                        {"name": "ts_state_start", "type": "STRING"},
                        {"name": "seq_num", "type": "INT"},
                        ...
                    ],
                    "partition_columns": [
                        {"name": "site", "type": "STRING"},
                        {"name": "year", "type": "SMALLINT"},
                        ...
                    ],
                    "stored_as": "PARQUET",
                    "sort_by": ["mid"],
                    "comment": "Backend EI - EMT Log table"
                }
            },
            "transformations": {
                "emt_log_new": {
                    "column_mappings": [
                        {"source": "mid", "target": "mid", "transform": None},
                        {"source": "ts_state_start", "target": "ts_state_start",
                         "transform": "TRIM(ts_state_start)"},
                        {"source": "seq_num", "target": "seq_num",
                         "transform": "CAST(seq_num AS INT)"},
                        ...
                    ],
                    "order_by": ["mid", "ts_state_start", "event_id"],
                    "partition_spec": {
                        "site": "${fab}",
                        "year": "${year}",
                        "month": "${month}",
                        "day": "${day}"
                    }
                }
            }
        }
    """
```

#### **Key Functions:**

**1. Parse SQL from Command Arguments:**
```python
def parse_sql_statements(command_args: str) -> List[Dict[str, Any]]:
    """Extract SQL from shell script wrapper in Command Arguments.

    Handles:
    - Impala beeline commands: beeline -u ${IMPALA_SERVER} -e "SQL HERE"
    - Multi-statement SQL separated by ;
    - Variable interpolation ${var}
    """
    # Example command_args:
    # beeline -u ${IMPALA_SERVER} -e "USE be_ei; CREATE TABLE emt_log_new (...); INSERT OVERWRITE ..."

    # Extract SQL from beeline -e "..." wrapper
    sql_match = re.search(r'-e\s+["\'](.+?)["\']', command_args, re.DOTALL)
    if sql_match:
        sql_content = sql_match.group(1)
    else:
        sql_content = command_args

    # Split by semicolon (but not within strings)
    statements = split_sql_statements(sql_content)

    return [classify_statement(stmt) for stmt in statements]
```

**2. Extract Schema from CREATE TABLE:**
```python
def extract_schema_from_create_table(sql: str) -> Dict[str, Any]:
    """Parse CREATE TABLE DDL to extract schema.

    Handles:
    - Column definitions with types and comments
    - PARTITIONED BY clause
    - SORT BY clause
    - STORED AS format
    - Table comments
    """
    # Use sqlparse library + custom regex for Hive-specific syntax

    # Example SQL:
    # CREATE TABLE be_ei.emt_log_new (
    #    mid STRING,
    #    ts_state_start STRING,
    #    seq_num INTEGER,
    #    ...
    # )
    # PARTITIONED BY (site STRING, year SMALLINT, month TINYINT, day TINYINT)
    # SORT BY (mid)
    # STORED AS PARQUET

    # Parse and return structured schema
```

**3. Extract Transformations from INSERT:**
```python
def extract_transformations_from_insert(sql: str) -> Dict[str, Any]:
    """Parse INSERT OVERWRITE to extract column transformations.

    Handles:
    - TRIM() function calls
    - CAST() type conversions
    - ORDER BY clause
    - PARTITION specification
    - Column aliasing
    """
    # Example SQL:
    # INSERT OVERWRITE TABLE emt_log_new
    # PARTITION(site='${fab}', year=${year}, month=${month}, day=${day})
    # SELECT
    #    mid,
    #    TRIM(ts_state_start) AS ts_state_start,
    #    CAST(seq_num AS INT),
    #    ...
    # FROM ${tempTable}
    # ORDER BY mid, ts_state_start, event_id

    # Parse and return transformation mappings
```

### **Deliverables:**
- [ ] `tools/sql_extraction.py` - SQL parser module
- [ ] `tests/test_sql_extraction.py` - Unit tests with EI.xml examples
- [ ] Integration with migration_orchestrator.py
- [ ] Documentation: `docs/sql_extraction_guide.md`

**Estimated Effort:** 2 weeks

---

## Phase 2: Parallel Flow Detection (Week 3)

### **Goal:** Detect 4 parallel flows (ATTJ/ATKL × emt_log/emt_log_attributes)

**Why Critical:** Without this, notebook generates single parameterized function instead of proper multi-task job with parallel execution.

### **Implementation:**

#### **Task 2.1: Flow Pattern Detector**

```python
# New file: tools/parallel_flow_detector.py

def detect_parallel_flows(workflow: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Identify parallel data pipelines with similar structure.

    Algorithm:
    1. Build processor DAG from connections
    2. Find independent subgraphs (no shared processors)
    3. Calculate structural similarity (processor types, property patterns)
    4. Cluster flows with >90% similarity
    5. Extract differentiating parameters (site, table, s3_path)

    Returns:
        [
            {
                "flow_id": "flow_ATTJ_emt_log",
                "pattern": "s3_read_transform_write",
                "site": "ATTJ",
                "table": "emt_log",
                "s3_path": "/landing-zone/emt_log/site=ATTJ/",
                "processors": [...]
            },
            {
                "flow_id": "flow_ATTJ_emt_log_attributes",
                "pattern": "s3_read_transform_write",
                "site": "ATTJ",
                "table": "emt_log_attributes",
                "s3_path": "/landing-zone/emt_log_attributes/site=ATTJ/",
                "processors": [...]
            },
            ...
        ]
    """
```

#### **Key Functions:**

**1. Build Flow Graph:**
```python
def build_processor_graph(processors: List[Dict], connections: List[Dict]) -> nx.DiGraph:
    """Build directed graph of processor flow."""
    G = nx.DiGraph()

    for proc in processors:
        G.add_node(proc["id"], **proc)

    for conn in connections:
        G.add_edge(conn["source"]["id"], conn["destination"]["id"])

    return G
```

**2. Find Similar Flows:**
```python
def cluster_similar_flows(subgraphs: List[nx.DiGraph]) -> List[List[nx.DiGraph]]:
    """Group flows with similar structure (>90% processor type match)."""
    clusters = []

    for subgraph in subgraphs:
        # Compute flow signature (ordered processor types)
        signature = compute_flow_signature(subgraph)

        # Find matching cluster
        matched = find_matching_cluster(clusters, signature, threshold=0.9)
        if matched:
            matched["flows"].append(subgraph)
        else:
            clusters.append({"signature": signature, "flows": [subgraph]})

    return clusters
```

**3. Extract Parameters:**
```python
def extract_flow_parameters(flow_cluster: List[nx.DiGraph]) -> Dict[str, List[str]]:
    """Find parameters that differ between similar flows.

    Example:
        Flow 1: UpdateAttribute sets site=ATTJ, table=emt_log
        Flow 2: UpdateAttribute sets site=ATKL, table=emt_log

        Returns: {"site": ["ATTJ", "ATKL"], "table": ["emt_log", "emt_log_attributes"]}
    """
    all_params = defaultdict(set)

    for flow in flow_cluster:
        for node_id in flow.nodes():
            node = flow.nodes[node_id]
            if node["type"] == "UpdateAttribute":
                for key, value in node["properties"].items():
                    if not is_variable(value):  # Static value
                        all_params[key].add(value)

    # Return parameters with multiple values (differentiators)
    return {k: list(v) for k, v in all_params.items() if len(v) > 1}
```

### **Deliverables:**
- [ ] `tools/parallel_flow_detector.py` - Flow clustering module
- [ ] `tests/test_parallel_flow_detector.py` - Unit tests with EI.xml
- [ ] Integration with migration_orchestrator.py
- [ ] Documentation: `docs/parallel_flow_detection.md`

**Estimated Effort:** 1 week

---

## Phase 3: S3-Aware Code Generation (Week 4)

### **Goal:** Generate notebooks that read from S3 instead of JDBC

**Why Critical:** Original notebooks assumed JDBC reads. With S3 architecture, we need different code patterns (Auto Loader, spark.read.parquet, etc.)

### **Implementation:**

#### **Task 3.1: S3 Data Ingestion Generator**

```python
# Enhanced: tools/notebook_generator.py

def generate_s3_ingestion_code(flow: Dict[str, Any], schema: Dict[str, Any]) -> str:
    """Generate S3 read code based on flow parameters.

    Options:
    1. Auto Loader (incremental, production-grade)
    2. spark.read.parquet/csv (batch, simpler)
    """

    if USE_AUTO_LOADER:
        return f"""
def ingest_{flow['table']}_from_s3(site: str, year: str, month: str, day: str):
    \"\"\"
    Ingest {flow['table']} data from S3 using Auto Loader (incremental).
    \"\"\"
    s3_path = f"s3://{{bucket}}/{flow['s3_path_pattern']}/site={{site}}/year={{year}}/month={{month}}/day={{day}}/"

    df = spark.readStream \\
        .format("cloudFiles") \\
        .option("cloudFiles.format", "parquet") \\  # or csv, json
        .option("cloudFiles.schemaLocation", f"s3://{{bucket}}/schemas/{flow['table']}") \\
        .option("cloudFiles.inferColumnTypes", "true") \\
        .load(s3_path)

    # Apply schema if needed
    df = df.select({', '.join(f'col("{c["name"]}").cast("{c["type"]}")' for c in schema['columns'])})

    return df
"""
    else:
        return f"""
def ingest_{flow['table']}_from_s3(site: str, year: str, month: str, day: str):
    \"\"\"
    Ingest {flow['table']} data from S3 (batch read).
    \"\"\"
    s3_path = f"s3://{{bucket}}/{flow['s3_path_pattern']}/site={{site}}/year={{year}}/month={{month}}/day={{day}}/"

    df = spark.read \\
        .format("parquet") \\  # or csv, json
        .load(s3_path)

    # Validate schema
    expected_columns = {[c['name'] for c in schema['columns']]}
    actual_columns = set(df.columns)
    if expected_columns != actual_columns:
        logger.warning(f"Schema mismatch: expected {{expected_columns}}, got {{actual_columns}}")

    return df
"""
```

#### **Task 3.2: Schema-Aware CREATE TABLE Generator**

```python
def generate_create_table_code(schema: Dict[str, Any], catalog: str, schema_name: str) -> str:
    """Generate Databricks CREATE TABLE from extracted Hive schema."""

    columns_ddl = []
    for col in schema["columns"]:
        col_type = map_hive_to_spark_type(col["type"])
        col_def = f"    {col['name']} {col_type}"
        if col.get("comment"):
            col_def += f" COMMENT '{col['comment']}'"
        columns_ddl.append(col_def)

    partitions_ddl = []
    for col in schema["partition_columns"]:
        part_type = map_hive_to_spark_type(col["type"])
        partitions_ddl.append(f"{col['name']} {part_type}")

    sort_by = schema.get("sort_by", [])[0] if schema.get("sort_by") else "id"

    return f"""
def create_{schema['table']}_table():
    \"\"\"
    Create {schema['table']} table in Unity Catalog with Delta Lake.

    Original Hive schema: {schema['database']}.{schema['table']}
    Migrated to: {catalog}.{schema_name}.{schema['table']}
    \"\"\"
    create_sql = f\"\"\"
    CREATE TABLE IF NOT EXISTS {catalog}.{schema_name}.{schema['table']} (
{chr(10).join(columns_ddl)}
    )
    USING DELTA
    PARTITIONED BY ({', '.join(partitions_ddl)})
    CLUSTER BY ({sort_by})
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true',
        'source_table' = '{schema['database']}.{schema['table']}',
        'migration_date' = '{{{{current_date()}}}}'
    )
    COMMENT '{schema.get('comment', '')}'
    \"\"\"

    logger.info(f"Creating table {catalog}.{schema_name}.{schema['table']}")
    spark.sql(create_sql)
    logger.info("Table created successfully")
"""
```

#### **Task 3.3: Transformation-Aware Data Processing Generator**

```python
def generate_transformation_code(transform: Dict[str, Any], schema: Dict[str, Any]) -> str:
    """Generate data transformation code from extracted INSERT OVERWRITE logic."""

    # Build SELECT expressions with TRIM, CAST, etc.
    select_exprs = []
    for mapping in transform["column_mappings"]:
        if mapping["transform"]:
            # Has transformation (TRIM, CAST, etc.)
            select_exprs.append(f'        {convert_sql_to_pyspark(mapping["transform"])}.alias("{mapping["target"]}")')
        else:
            # Direct mapping
            select_exprs.append(f'        col("{mapping["source"]}")')

    # Build ORDER BY if present
    order_by_code = ""
    if transform.get("order_by"):
        order_by_cols = ', '.join(f'"{col}"' for col in transform["order_by"])
        order_by_code = f".orderBy({order_by_cols})"

    return f"""
def transform_{schema['table']}_data(df):
    \"\"\"
    Apply transformations to {schema['table']} data.

    Original transformations:
    {chr(10).join(f'    - {m["source"]} → {m["target"]}: {m["transform"] or "direct"}' for m in transform["column_mappings"])}
    \"\"\"
    from pyspark.sql.functions import col, trim, current_timestamp

    transformed_df = df.select(
{chr(10).join(select_exprs)}
    ){order_by_code}

    return transformed_df
"""

def convert_sql_to_pyspark(sql_expr: str) -> str:
    """Convert SQL expression to PySpark expression.

    Examples:
        TRIM(ts_state_start) → trim(col("ts_state_start"))
        CAST(seq_num AS INT) → col("seq_num").cast("int")
        now() → current_timestamp()
    """
    # Simple pattern matching for common transformations
    if sql_expr.startswith("TRIM(") and sql_expr.endswith(")"):
        col_name = sql_expr[5:-1]
        return f'trim(col("{col_name}"))'

    if "CAST(" in sql_expr and " AS " in sql_expr:
        match = re.match(r"CAST\((\w+) AS (\w+)\)", sql_expr)
        if match:
            col_name, type_name = match.groups()
            return f'col("{col_name}").cast("{type_name.lower()}")'

    if sql_expr == "now()":
        return "current_timestamp()"

    # Fallback: use spark.sql() for complex expressions
    return f'expr("{sql_expr}")'
```

#### **Task 3.4: Multi-Task Job Generator**

```python
def generate_multi_task_job_definition(flows: List[Dict[str, Any]],
                                       notebook_path: str) -> Dict[str, Any]:
    """Generate Databricks multi-task job JSON definition.

    Creates parallel tasks for each flow (ATTJ/ATKL × emt_log/emt_log_attributes).
    """
    tasks = []

    for flow in flows:
        task = {
            "task_key": f"{flow['table']}_{flow['site']}",
            "description": f"Process {flow['table']} data for site {flow['site']}",
            "notebook_task": {
                "notebook_path": notebook_path,
                "base_parameters": {
                    "site": flow["site"],
                    "table": flow["table"],
                    "s3_path": flow["s3_path"],
                    "year": "{{job.start_time.year}}",
                    "month": "{{job.start_time.month}}",
                    "day": "{{job.start_time.day}}"
                }
            },
            "depends_on": [],  # Parallel execution
            "timeout_seconds": 3600,
            "max_retries": 3,
            "min_retry_interval_millis": 180000,  # 3 minutes (from NiFi retry logic)
            "retry_on_timeout": True
        }
        tasks.append(task)

    job_definition = {
        "name": "EMT_Log_Pipeline",
        "tasks": tasks,
        "schedule": {
            "quartz_cron_expression": "0 30 3 * * ?",  # 3:30 AM daily (from NiFi)
            "timezone_id": "UTC"
        },
        "max_concurrent_runs": 1,
        "timeout_seconds": 7200,
        "email_notifications": {
            "on_failure": ["data-engineering@company.com"]
        }
    }

    return job_definition
```

### **Deliverables:**
- [ ] Enhanced `tools/notebook_generator.py` with S3 ingestion
- [ ] Schema-aware CREATE TABLE generation
- [ ] Transformation-aware processing code
- [ ] Multi-task job JSON generator
- [ ] `tests/test_notebook_generator_s3.py` - Unit tests
- [ ] Documentation: `docs/s3_code_generation.md`

**Estimated Effort:** 1 week

---

## Phase 4: Integration & Validation (Week 5)

### **Goal:** Wire everything together and measure quality improvements

### **Task 4.1: Enhanced Migration Pipeline**

```python
# Updated: tools/migration_orchestrator.py

def migrate_nifi_to_databricks_s3_architecture(xml_path: str,
                                                s3_bucket: str,
                                                catalog: str,
                                                schema: str) -> Dict[str, Any]:
    """Enhanced migration pipeline for S3-based architecture.

    Args:
        xml_path: Path to NiFi XML template
        s3_bucket: S3 bucket name for source data
        catalog: Unity Catalog name
        schema: Schema/database name in catalog

    Returns:
        {
            "notebook": Generated notebook content,
            "job_definition": Multi-task job JSON,
            "schemas": Extracted table schemas,
            "transformations": Extracted transformations,
            "parallel_flows": Detected flow patterns,
            "validation_report": Quality metrics
        }
    """

    # Step 1: Parse NiFi XML (existing)
    with open(xml_path, 'r') as f:
        xml_content = f.read()
    template_data = parse_nifi_template_impl(xml_content)

    # Step 2: Extract SQL statements (NEW)
    logger.info("Extracting SQL schemas and transformations...")
    sql_extraction = extract_sql_from_nifi_workflow(xml_path)
    schemas = sql_extraction["schemas"]
    transformations = sql_extraction["transformations"]

    # Step 3: Detect parallel flows (NEW)
    logger.info("Detecting parallel flow patterns...")
    parallel_flows = detect_parallel_flows(template_data)
    logger.info(f"Detected {len(parallel_flows)} parallel flows")

    # Step 4: Classify processors (existing)
    logger.info("Classifying processors...")
    classification_result = classify_workflow(xml_path)

    # Step 5: Generate S3-aware notebook (NEW)
    logger.info("Generating Databricks notebook...")
    notebook = generate_s3_notebook(
        schemas=schemas,
        transformations=transformations,
        parallel_flows=parallel_flows,
        classifications=classification_result,
        s3_bucket=s3_bucket,
        catalog=catalog,
        schema=schema
    )

    # Step 6: Generate multi-task job definition (NEW)
    logger.info("Generating multi-task job definition...")
    job_definition = generate_multi_task_job_definition(
        flows=parallel_flows,
        notebook_path=f"/Workspace/Migrations/{Path(xml_path).stem}"
    )

    # Step 7: Validate migration quality (NEW)
    logger.info("Validating migration quality...")
    validation_report = validate_migration_quality(
        nifi_schemas=schemas,
        nifi_transforms=transformations,
        generated_notebook=notebook
    )

    return {
        "notebook": notebook,
        "job_definition": job_definition,
        "schemas": schemas,
        "transformations": transformations,
        "parallel_flows": parallel_flows,
        "validation_report": validation_report,
        "metadata": {
            "source_file": xml_path,
            "migration_version": "2.0_s3",
            "timestamp": datetime.now().isoformat()
        }
    }
```

### **Task 4.2: Validation Framework**

```python
# New: tools/validation_framework.py

def validate_migration_quality(nifi_schemas: Dict[str, Any],
                               nifi_transforms: Dict[str, Any],
                               generated_notebook: str) -> Dict[str, Any]:
    """Validate generated notebook against source NiFi workflow.

    Returns quality score and detailed findings.
    """

    findings = {
        "schema_validation": validate_schemas(nifi_schemas, generated_notebook),
        "transformation_validation": validate_transformations(nifi_transforms, generated_notebook),
        "code_quality": validate_code_quality(generated_notebook),
    }

    # Calculate overall score
    schema_score = findings["schema_validation"]["score"]
    transform_score = findings["transformation_validation"]["score"]
    code_score = findings["code_quality"]["score"]

    overall_score = (schema_score * 0.4 + transform_score * 0.4 + code_score * 0.2)

    return {
        "overall_score": overall_score,
        "overall_grade": score_to_grade(overall_score),
        "findings": findings,
        "recommendation": generate_recommendation(overall_score, findings)
    }


def validate_schemas(nifi_schemas: Dict[str, Any], notebook: str) -> Dict[str, Any]:
    """Validate that all columns from NiFi schemas are present in notebook."""

    findings = []
    total_columns = 0
    matched_columns = 0

    for table_name, schema in nifi_schemas.items():
        total_columns += len(schema["columns"])

        for col in schema["columns"]:
            # Check if column appears in CREATE TABLE or DataFrame operations
            if f'"{col["name"]}"' in notebook or f"'{col["name"]}'" in notebook:
                matched_columns += 1
            else:
                findings.append({
                    "severity": "ERROR",
                    "table": table_name,
                    "message": f"Column '{col['name']}' not found in notebook"
                })

    score = matched_columns / total_columns if total_columns > 0 else 0

    return {
        "score": score,
        "matched_columns": matched_columns,
        "total_columns": total_columns,
        "findings": findings
    }


def validate_transformations(nifi_transforms: Dict[str, Any], notebook: str) -> Dict[str, Any]:
    """Validate that transformations (TRIM, CAST, ORDER BY) are present."""

    findings = []
    total_transforms = 0
    matched_transforms = 0

    for table_name, transform in nifi_transforms.items():
        for mapping in transform["column_mappings"]:
            if mapping["transform"]:
                total_transforms += 1

                # Check if transformation appears in notebook
                # Look for PySpark equivalent: trim(...), cast(...), etc.
                if "TRIM" in mapping["transform"] and "trim(" in notebook:
                    matched_transforms += 1
                elif "CAST" in mapping["transform"] and ".cast(" in notebook:
                    matched_transforms += 1
                elif mapping["transform"] in notebook:
                    matched_transforms += 1
                else:
                    findings.append({
                        "severity": "ERROR",
                        "table": table_name,
                        "column": mapping["target"],
                        "message": f"Transformation '{mapping['transform']}' not found in notebook"
                    })

        # Check ORDER BY
        if transform.get("order_by"):
            total_transforms += 1
            if "orderBy(" in notebook or "ORDER BY" in notebook:
                matched_transforms += 1
            else:
                findings.append({
                    "severity": "ERROR",
                    "table": table_name,
                    "message": f"ORDER BY clause not found in notebook"
                })

    score = matched_transforms / total_transforms if total_transforms > 0 else 1.0

    return {
        "score": score,
        "matched_transforms": matched_transforms,
        "total_transforms": total_transforms,
        "findings": findings
    }


def score_to_grade(score: float) -> str:
    """Convert numeric score to letter grade."""
    if score >= 0.9:
        return "A (Excellent)"
    elif score >= 0.8:
        return "B (Good)"
    elif score >= 0.7:
        return "C (Fair)"
    elif score >= 0.6:
        return "D (Poor)"
    else:
        return "F (Failing)"
```

### **Task 4.3: Streamlit UI Updates**

```python
# Update: streamlit_app/Dashboard.py

# Add new button for S3-based migration
if st.button("🚀 Generate S3-Based Migration", use_container_width=True):
    with st.spinner("Generating enhanced migration with S3 architecture..."):
        # Get configuration
        s3_bucket = st.text_input("S3 Bucket Name", value="your-bucket-name")
        catalog = st.text_input("Unity Catalog", value="main")
        schema = st.text_input("Schema Name", value="default")

        # Run enhanced migration
        result = migrate_nifi_to_databricks_s3_architecture(
            xml_path=xml_path,
            s3_bucket=s3_bucket,
            catalog=catalog,
            schema=schema
        )

        # Store results
        st.session_state["enhanced_migration_result"] = result

        # Display validation report
        validation = result["validation_report"]
        st.success(f"Migration Complete! Quality Score: {validation['overall_score']:.1%} ({validation['overall_grade']})")

        # Show findings
        with st.expander("📊 Validation Report"):
            st.json(validation["findings"])

        # Download buttons
        col1, col2 = st.columns(2)
        with col1:
            st.download_button(
                "📥 Download Notebook",
                data=result["notebook"],
                file_name=f"{Path(xml_path).stem}_migrated.py",
                mime="text/x-python"
            )
        with col2:
            st.download_button(
                "📥 Download Job Definition",
                data=json.dumps(result["job_definition"], indent=2),
                file_name=f"{Path(xml_path).stem}_job.json",
                mime="application/json"
            )
```

### **Deliverables:**
- [ ] Enhanced migration pipeline in `migration_orchestrator.py`
- [ ] `tools/validation_framework.py` - Quality validation
- [ ] Updated Streamlit UI with S3 configuration
- [ ] Regression test suite comparing old vs. new generation
- [ ] Before/after comparison reports
- [ ] Documentation: `docs/validation_framework.md`

**Estimated Effort:** 1 week

---

## Success Metrics (S3 Architecture)

### **Before (Current State)**
- Schema Accuracy: 30% (generic templates)
- Transformation Completeness: 40% (missing TRIM/CAST/ORDER BY)
- Parallel Flow Detection: 0% (not detected)
- Code Quality: Generic placeholders, TODOs
- **Overall Quality: 6.5/10**

### **After (Target State)**
- Schema Accuracy: 95% (extracted from DDL)
- Transformation Completeness: 90% (extracted from DML)
- Parallel Flow Detection: 95% (detected and generated)
- Code Quality: S3-aware, production-ready patterns
- **Overall Quality: 9/10**

### **Key Improvements**
1. ✅ **Correct table schemas** - mid, ts_state_start, seq_num, state, prev_state (not id, log_timestamp, message)
2. ✅ **Complete transformations** - TRIM, CAST, ORDER BY all preserved
3. ✅ **Parallel execution** - 4 tasks run concurrently (ATTJ/ATKL × 2 tables)
4. ✅ **S3-native ingestion** - Auto Loader or spark.read with proper paths
5. ✅ **Production-ready** - 80% of code works without manual fixes

---

## Implementation Timeline (4 Weeks)

### **Week 1-2: SQL Extraction** ⚡ HIGH IMPACT
- [x] Design SQL parser architecture
- [ ] Implement statement parser
- [ ] Implement DDL schema extractor
- [ ] Implement DML transformation extractor
- [ ] Unit tests with EI.xml
- [ ] Integration with migration_orchestrator

### **Week 3: Parallel Flow Detection** ⚡ HIGH IMPACT
- [ ] Implement flow clustering algorithm
- [ ] Implement parameter extraction
- [ ] Implement multi-task job generator
- [ ] Unit tests with EI.xml (4 flows)
- [ ] Integration with migration_orchestrator

### **Week 4: S3 Code Generation** ⚡ MEDIUM-HIGH IMPACT
- [ ] Implement S3 ingestion code generator
- [ ] Implement schema-aware CREATE TABLE generator
- [ ] Implement transformation-aware processing generator
- [ ] Implement multi-task job JSON generator
- [ ] Unit tests

### **Week 5: Integration & Validation** ⚡ MEDIUM IMPACT
- [ ] Enhanced migration pipeline
- [ ] Validation framework
- [ ] Streamlit UI updates
- [ ] Regression testing
- [ ] Documentation

**Total: 4 weeks (vs. 6 weeks in original plan)**

---

## What We're NOT Building (and Why)

### ❌ Connection Service Resolution
**Why:** No JDBC connections needed - data comes from S3
**Savings:** 1 week

### ❌ Sqoop Configuration Translation
**Why:** No Sqoop imports - using Auto Loader or spark.read from S3
**Savings:** <1 week

### ❌ Kerberos Authentication Handling
**Why:** S3 uses AWS IAM roles, not Kerberos
**Savings:** Complexity reduction

---

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                         NiFi Workflow (XML)                      │
│                                                                  │
│  ┌────────────┐   ┌────────────┐   ┌────────────┐             │
│  │ GetFile /  │→│ Transform  │→│ PutHDFS    │             │
│  │ Consume   │   │ Process    │   │            │             │
│  └────────────┘   └────────────┘   └────────────┘             │
│                                                                  │
│  ┌──────────────────────────────┐                              │
│  │ ExecuteStreamCommand (SQL)   │                              │
│  │ - CREATE TABLE emt_log_new   │  ← Extract schemas          │
│  │ - INSERT OVERWRITE ...       │  ← Extract transformations  │
│  └──────────────────────────────┘                              │
└─────────────────────────────────────────────────────────────────┘
                              ↓
                    ┌─────────────────────┐
                    │  Migration Tool     │
                    │  (Enhanced)         │
                    │                     │
                    │  1. SQL Extraction  │
                    │  2. Flow Detection  │
                    │  3. Code Generation │
                    └─────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                    Databricks Notebook (Python)                  │
│                                                                  │
│  def ingest_emt_log_from_s3(site, year, month, day):           │
│      s3_path = f"s3://bucket/emt_log/site={site}/..."          │
│      df = spark.read.parquet(s3_path)  # or Auto Loader        │
│      return df                                                   │
│                                                                  │
│  def create_emt_log_table():                                    │
│      # Actual schema from NiFi DDL:                             │
│      CREATE TABLE catalog.schema.emt_log_new (                  │
│          mid STRING,                                             │
│          ts_state_start STRING,                                  │
│          seq_num INT,        ← Extracted from NiFi              │
│          ...                                                     │
│      ) USING DELTA ...                                          │
│                                                                  │
│  def transform_emt_log_data(df):                                │
│      # Actual transformations from NiFi SQL:                    │
│      return df.select(                                          │
│          col("mid"),                                             │
│          trim(col("ts_state_start")),  ← From TRIM()            │
│          col("seq_num").cast("int"),   ← From CAST()            │
│          ...                                                     │
│      ).orderBy("mid", "ts_state_start")  ← From ORDER BY       │
│                                                                  │
│  # Main execution with parallel flows                            │
│  with ThreadPoolExecutor(max_workers=4) as executor:            │
│      futures = [                                                 │
│          executor.submit(process_site_table, 'ATTJ', 'emt_log'),│
│          executor.submit(process_site_table, 'ATTJ', 'emt_..'),│
│          executor.submit(process_site_table, 'ATKL', 'emt_log'),│
│          executor.submit(process_site_table, 'ATKL', 'emt_..'),│
│      ]                                                           │
└─────────────────────────────────────────────────────────────────┘
```

---

## Risk Assessment

### **Technical Risks**

**Risk 1: SQL Parsing Complexity**
- **Impact:** High
- **Probability:** Medium
- **Mitigation:** Use `sqlparse` library + custom regex for Hive syntax
- **Fallback:** Document unparseable SQL with manual review flag

**Risk 2: Variable Resolution in S3 Paths**
- **Impact:** Medium
- **Probability:** Low
- **Mitigation:** Extract UpdateAttribute variables and map to S3 path patterns
- **Fallback:** Generate parameterized paths with clear TODO comments

**Risk 3: Transformation Translation (SQL→PySpark)**
- **Impact:** Medium
- **Probability:** Medium
- **Mitigation:** Cover common cases (TRIM, CAST, now()), fallback to `expr()` for complex
- **Fallback:** Use Spark SQL string with extracted transformations

---

## Conclusion

With **S3-based architecture**, we can achieve **9/10 migration quality** in **4 weeks** (vs. 6 weeks) by focusing on:

1. ✅ **SQL Extraction** - Schemas and transformations from ExecuteStreamCommand
2. ✅ **Parallel Flow Detection** - Multi-site/multi-table patterns
3. ✅ **S3-Aware Code Generation** - Auto Loader, proper schemas, transformations
4. ✅ **Validation Framework** - Measure quality improvements

**Benefits:**
- ✅ 95% schema accuracy (vs. 30%)
- ✅ 90% transformation completeness (vs. 40%)
- ✅ 95% parallel flow detection (vs. 0%)
- ✅ 80% reduction in manual fixes (6 hours → 1 hour)
- ✅ 2 weeks faster implementation (no JDBC/Sqoop complexity)

**Ready to proceed with Phase 1 (SQL Extraction)?**

---

**Document Version:** 2.0 (S3 Architecture)
**Last Updated:** 2025-01-XX
**Status:** READY FOR IMPLEMENTATION

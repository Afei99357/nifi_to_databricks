# 📘 Complete Implementation Plan: Binary Decision + Mixed-Mode + Extended Coverage

## 🎯 Core Strategy

Binary architecture decision with hybrid support:
- **Databricks Job** → for batch workflows
- **DLT Pipeline** → for streaming workflows
- **Hybrid Split** → for workflows where batch feeds streaming

## 🔑 Enhanced Decision Strategy

### Step 1: Detect Entry Points
Parse NiFi XML for source processors (no upstream connections).
Classify into:
- **Streaming sources**: ListenHTTP, ConsumeKafka, ListenTCP, etc.
- **Batch sources**: GetFile, ListFile, QueryDatabaseTable, etc.

### Step 2: Apply Rules
```python
if has_streaming_sources:
    return "dlt_pipeline"   # even if sinks are batch
elif has_batch_sources:
    if has_streaming_sinks:  # PublishKafka, PutMQTT, etc.
        return "hybrid_split"
    else:
        return "databricks_job"
```

### Step 3: Hybrid Split Handling
When batch feeds streaming:
- **Part A**: Batch Job → writes Delta table
- **Part B**: DLT Pipeline → reads that Delta as streaming source and outputs to Kafka/MQTT
- Auto-generate both configs + orchestration notes

## 🔧 Implementation Phases

### Phase 1: Simplify Architecture Decision Logic
**File**: `tools/xml_tools.py` → `recommend_databricks_architecture()`

**Changes**:
- Remove `structured_streaming` option entirely
- Add `"hybrid_split"` option
- Implement entry point + sink detection
- Detect multiple entry points and classify each separately

### Phase 2: Enhanced DLT Pipeline Generation
**File**: `tools/dlt_tools.py`

**Add functions**:
- `create_dlt_sql_from_processors()` → chain processors into DLT SQL
- `generate_dlt_pipeline_from_nifi()` → complete XML → DLT conversion
- `detect_workflow_entry_points()` → multi-source detection

**Processor SQL mappings**:
- **Sources**: ListenHTTP, ConsumeKafka → `@dlt.table` + `readStream`
- **Transforms**: EvaluateJsonPath, RouteOnAttribute → SQL select/filter on `STREAM(LIVE.upstream)`
- **Sinks**: PutHDFS, PutDelta → final DLT table

**Error handling**:
- RouteOnAttribute → extra dead-letter table (`dlt.table(name="errors")`)

**Schema management**:
Use Auto Loader options:
```python
.option("cloudFiles.inferColumnTypes", "true")
.option("cloudFiles.schemaEvolutionMode", "addNewColumns")
```

### Phase 3: Job Migration with Handoff Tables
**File**: `tools/migration_tools.py`

**Enhancements**:
- Add persistent Delta tables as handoff points between tasks
- Each task reads/writes Delta tables instead of in-memory flowfiles
- Handle error branches → `temp.error_table`

**Example**:
- Task1: GetFile → `dev.temp_stage1`
- Task2: EvaluateJsonPath → `dev.temp_stage2`
- Task3: PutHDFS → final table

### Phase 4: Hybrid Split Implementation
**File**: `migration_tools.py` → `orchestrate_intelligent_nifi_migration()`

**New logic** when recommendation = `"hybrid_split"`:
- Generate Job spec for batch portion → writes Delta output
- Generate DLT spec for streaming portion → reads Delta as stream → publishes to sink
- Auto-generate deployment notes: "Run Job first, then start DLT pipeline"
- Optionally generate DAG metadata linking the two

### Phase 5: Metadata & Lineage
**File**: `tools/lineage_tools.py`

**Add**:
- Auto-generate JSON/YAML lineage file:
  ```
  GetFile → stage1_delta
  EvaluateJsonPath → stage2_delta
  PutHDFS → final_table
  ```
- Include mapping from NiFi processor ID → Databricks artifact

### Phase 6: Testing & Validation
- **Unit Tests**: feed sample JSON/CSV into generated Job/DLT
- **Integration Tests**: simulate streaming with Auto Loader (drop files into directory)
- **Golden Dataset Validation**:
  - Run NiFi pipeline + migrated Databricks pipeline
  - Compare outputs with EXCEPT queries

### Phase 7: Deployment Packaging
- **Jobs** → export as `.json` usable by `databricks jobs create`
- **DLT Pipelines** → export as `.yaml` for `databricks pipelines create`
- **Hybrid** → export both + orchestration metadata

## 📊 Example Outcomes

### A. Streaming → Batch (DLT Only)
```
ListenHTTP → EvaluateJsonPath → PutHDFS
```
→ One DLT pipeline with bronze → silver → gold pattern

### B. Batch → Batch (Job Only)
```
GetFile → EvaluateJsonPath → PutHDFS
```
→ One Databricks Job with Delta handoff tables

### C. Batch → Stream (Hybrid Split)
```
GetFile → EvaluateJsonPath → PublishKafka
```
→ **Job**: GetFile → Delta
→ **DLT**: readStream(Delta) → PublishKafka
→ **Orchestration doc** generated

### D. Multi-Entry
```
GetFile branch + ConsumeKafka branch in same XML
```
→ Generates 2 independent pipelines (1 Job, 1 DLT)

## 🎯 Success Metrics

✅ Binary decision: streaming → DLT, batch → Job
✅ Hybrid support: batch-to-streaming handled cleanly
✅ Workflow continuity: Delta handoffs instead of broken flowfiles
✅ Multi-entry detection supported
✅ Error handling mapped to dead-letter tables
✅ Lineage preserved in JSON/YAML
✅ Deployable artifacts: Job JSON + DLT YAML

---

## 🚀 Phase 1 Implementation (Current Sprint)

**Simplified for Testing**:
- Binary decision only: streaming → DLT, batch → Job
- No hybrid support yet
- Focus on basic workflow continuity

**Files to modify**:
1. `tools/xml_tools.py` - simplify decision logic
2. `tools/dlt_tools.py` - add basic DLT SQL generation
3. Test with simple workflows

**Next**: Add hybrid support in Phase 2

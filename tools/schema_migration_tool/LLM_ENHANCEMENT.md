# LLM Enhancement for DDL Converter

## Overview

This enhancement adds **optional** Databricks LLM validation and optimization to the rule-based DDL converter. It uses the same Databricks Foundation Model API as the AI assistant page.

## Architecture

### Hybrid Approach
1. **Rule-based conversion** (always run, fast & free)
   - Deterministic parsing and conversion
   - No API costs or latency
   - Works offline

2. **LLM enhancement** (optional)
   - Validates conversion correctness
   - Suggests optimizations
   - Adds helpful comments
   - Requires Databricks authentication

## Files

### `databricks_llm_enhancer.py`
Core LLM integration using Databricks Foundation Model API.

**Key Functions:**
- `query_databricks_llm()` - Query Databricks LLM endpoint (same pattern as AI assistant)
- `validate_ddl_conversion()` - Validates Hive to Databricks conversion
- `suggest_optimizations()` - Gets optimization suggestions
- `enhance_ddl_with_comments()` - Adds helpful comments to DDL
- `convert_with_llm_validation()` - Complete workflow combining all features

**Example:**
```python
from databricks_llm_enhancer import validate_ddl_conversion

validation = validate_ddl_conversion(
    hive_ddl="CREATE EXTERNAL TABLE...",
    databricks_ddl="CREATE TABLE...",
    endpoint_name="databricks-meta-llama-3-1-70b-instruct"
)

print(validation)
# {
#   "status": "PASS",
#   "issues": [],
#   "suggestions": ["Consider partitioning by date for better performance"],
#   "confidence": "HIGH"
# }
```

### `convert_ddl_with_llm.py`
CLI tool combining rule-based conversion with optional LLM enhancement.

**Usage:**

```bash
# Rule-based only (default - fast, free, no API calls)
python convert_ddl_with_llm.py -i hive.sql -o databricks.sql

# With LLM validation
python convert_ddl_with_llm.py -i hive.sql -o databricks.sql --validate

# With optimization suggestions
python convert_ddl_with_llm.py -i hive.sql -o databricks.sql --suggest

# With enhanced comments
python convert_ddl_with_llm.py -i hive.sql -o databricks.sql --enhance

# Full LLM enhancement (validate + suggest + enhance)
python convert_ddl_with_llm.py -i hive.sql -o databricks.sql --llm-full
```

**Options:**
- `-i, --input`: Input Hive DDL file
- `-o, --output`: Output Databricks DDL file
- `-t, --type`: Migration type (`delta` or `external`)
- `-s, --storage`: Target storage (`dbfs`, `azure`, `aws`, `unity_catalog`)
- `--optimize`: Convert `_ts` columns to TIMESTAMP (default: on)
- `--no-optimize`: Keep original types
- `--validate`: Validate conversion with LLM
- `--suggest`: Get optimization suggestions
- `--enhance`: Add LLM-generated comments
- `--llm-full`: Full LLM enhancement
- `--endpoint`: Databricks serving endpoint (default: `databricks-meta-llama-3-1-70b-instruct`)

### `llm_enhancer.py`
Placeholder for future generic LLM integration (non-Databricks).

## How It Works

### 1. Rule-Based Conversion (Always Run)
```python
databricks_ddl = convert_hive_to_databricks(
    hive_ddl,
    target_storage="dbfs",
    migration_type="delta",
    optimize_types=True
)
```

### 2. LLM Enhancement (Optional)
```python
if args.llm_full:
    result = convert_with_llm_validation(
        hive_ddl,
        databricks_ddl,
        endpoint_name="databricks-meta-llama-3-1-70b-instruct",
        verbose=True
    )

    # Returns:
    # {
    #   "validation": {...},
    #   "suggestions": [...],
    #   "original_ddl": "..."
    # }
```

## LLM Integration Pattern

This implementation uses the **exact same pattern** as the AI assistant in the Streamlit app:

```python
from mlflow.deployments import get_deploy_client

messages = [{"role": "user", "content": prompt}]

response = get_deploy_client("databricks").predict(
    endpoint=endpoint_name,
    inputs={"messages": messages, "max_tokens": max_tokens},
)
```

## When to Use LLM Enhancement

### Use Rule-Based Only (Default)
- Standard Hive DDL conversions
- Batch processing many tables
- Need fast, deterministic results
- Working offline or without Databricks access

### Add LLM Enhancement
- Validating critical table conversions
- Getting optimization suggestions for production tables
- Non-standard or complex DDL patterns
- Want human-readable comments in output

## Requirements

### Rule-Based Only
```bash
# No additional dependencies
python convert_ddl_with_llm.py -i hive.sql -o databricks.sql
```

### With LLM Enhancement
```bash
pip install databricks-sdk mlflow

# Requires Databricks authentication (inside Databricks or with credentials configured)
python convert_ddl_with_llm.py -i hive.sql -o databricks.sql --llm-full
```

## Example Output

### Without LLM (default)
```
🔄 Converting with rule-based converter...
✅ Rule-based conversion complete
✅ Converted DDL written to: databricks.sql
```

### With LLM Validation
```
🔄 Converting with rule-based converter...
✅ Rule-based conversion complete

🤖 Enhancing with Databricks LLM...
🤖 Validating conversion with Databricks LLM...
✅ Validation: PASS (Confidence: HIGH)
💡 Getting optimization suggestions...
   Found 3 suggestions:
   1. Consider using Z-ORDER for frequently queried columns
   2. Enable auto-compact for tables with frequent updates
   3. Add COMMENT properties for better documentation
   ... and 0 more

================================================================================
VALIDATION RESULTS
================================================================================
{
  "status": "PASS",
  "issues": [],
  "suggestions": ["Consider Z-ORDER for better query performance"],
  "confidence": "HIGH"
}

✅ Converted DDL written to: databricks.sql
```

## Design Rationale

### Why Hybrid Approach?

1. **Rule-based is sufficient for 95% of cases**
   - Hive DDL is well-structured and follows standard patterns
   - Regex parsing is deterministic and reliable
   - Fast (milliseconds vs seconds)
   - Free (no API costs)
   - Works offline

2. **LLM adds value for edge cases**
   - Validation gives confidence for critical tables
   - Suggestions provide optimization insights
   - Comments help team understanding
   - But not needed for every conversion

3. **Best of both worlds**
   - Always get correct conversion (rule-based)
   - Optionally add AI insights (LLM)
   - User chooses based on their needs

## Future Enhancements

- [ ] Support for custom LLM providers (OpenAI, Anthropic, etc.)
- [ ] Batch validation of multiple tables
- [ ] Interactive mode for reviewing suggestions
- [ ] Learning from user feedback to improve rules
- [ ] Integration with Unity Catalog metadata

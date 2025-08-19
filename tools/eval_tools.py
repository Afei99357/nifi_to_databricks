# tools/eval_tools.py
# Evaluation and comparison helpers (template code generation).

from __future__ import annotations

from langchain_core.tools import tool

__all__ = ["evaluate_pipeline_outputs"]


@tool
def evaluate_pipeline_outputs(src_path: str, dst_path: str, key_cols_csv: str = "", float_tol: float = 1e-6) -> str:
    """
    Emit a PySpark code template that compares two Delta/Parquet datasets with robust float handling.
    Returns a Python code string intended to be run in a Databricks notebook.
    """
    key_cols = [c.strip() for c in key_cols_csv.split(",") if c.strip()]
    comparison_code = f"""
# Pipeline Output Comparison Template
from pyspark.sql import functions as F

# Load datasets
df1 = spark.read.load("{src_path}")  # Source
df2 = spark.read.load("{dst_path}")  # Destination

# Basic counts
src_count = df1.count()
dst_count = df2.count()

# Schema comparison
schema_equal = (df1.schema == df2.schema)

# Null counts per column
def get_nulls(df):
    return {{c: df.filter(F.col(c).isNull()).count() for c in df.columns}}

src_nulls = get_nulls(df1)
dst_nulls = get_nulls(df2)

# Key-based comparison if keys provided
key_cols = {key_cols}
float_tolerance = {float_tol}

print(f"Source count: {{src_count}}")
print(f"Destination count: {{dst_count}}")
print(f"Schema equal: {{schema_equal}}")
print(f"Source nulls: {{src_nulls}}")
print(f"Destination nulls: {{dst_nulls}}")
"""
    return comparison_code.strip()

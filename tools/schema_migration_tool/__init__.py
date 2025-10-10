"""
Schema Migration Tool

Convert Hive/Impala DDL to Databricks DDL for table migration.
"""

from .databricks_ddl_generator import DatabricksDDLGenerator, convert_hive_to_databricks
from .hive_ddl_parser import HiveDDLParser, parse_hive_ddl

__all__ = [
    "HiveDDLParser",
    "parse_hive_ddl",
    "DatabricksDDLGenerator",
    "convert_hive_to_databricks",
]

__version__ = "1.0.0"

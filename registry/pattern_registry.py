# pattern_registry.py
# ------------------------------------------------------------------------------
# Unity Catalog–backed pattern registry for NiFi→Databricks migrations.
# Stores processor patterns and complex patterns in Delta tables only.
#
# Usage:
#   1) Set env vars (or pass table names to ctor):
#        PATTERN_TABLE="eliao.nifi_to_databricks.processors"
#        COMPLEX_TABLE="eliao.nifi_to_databricks.complex_patterns"
#        META_TABLE="eliao.nifi_to_databricks.patterns_meta"          (optional)
#        RAW_TABLE="eliao.nifi_to_databricks.patterns_raw_snapshots"  (optional)
#   2) In your agent code:
#        from pattern_registry_uc import PatternRegistryUC
#        pattern_registry = PatternRegistryUC()
# ------------------------------------------------------------------------------

from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Dict, Optional, Any

# --------------------------
# Environment configuration
# --------------------------
PATTERN_TABLE = os.environ.get("PATTERN_TABLE", "eliao.nifi_to_databricks.processors")
COMPLEX_TABLE = os.environ.get("COMPLEX_TABLE", "eliao.nifi_to_databricks.complex_patterns")
META_TABLE = os.environ.get("META_TABLE", "eliao.nifi_to_databricks.patterns_meta")               # optional
RAW_TABLE = os.environ.get("RAW_TABLE", "eliao.nifi_to_databricks.patterns_raw_snapshots")        # optional


def _spark():
    """Return active SparkSession if running on Databricks / PySpark context, else None."""
    try:
        from pyspark.sql import SparkSession  # type: ignore
        return SparkSession.getActiveSession()
    except Exception:
        return None


class PatternRegistryUC:
    """
    Unity Catalog–backed registry for NiFi→Databricks migrations.
    Persists processor and complex patterns in Delta tables.
    """

    def __init__(
        self,
        processors_table: Optional[str] = None,
        complex_table: Optional[str] = None,
        meta_table: Optional[str] = None,
        raw_snapshots_table: Optional[str] = None,
    ):
        # Spark / UC wiring
        self.spark = _spark()
        if not self.spark:
            raise RuntimeError("SparkSession not available. PatternRegistryUC requires Databricks / Spark.")

        self.processors_table = processors_table or PATTERN_TABLE
        self.complex_table = complex_table or COMPLEX_TABLE
        self.meta_table = meta_table or META_TABLE
        self.raw_snapshots_table = raw_snapshots_table or RAW_TABLE

        # runtime/cache fields
        self.cache: Dict[str, dict] = {}
        self.usage_stats: Dict[str, dict] = {}

        # bootstrap UC tables
        self._ensure_tables()
        self.patterns = self._load_processors_uc()
        self.complex = self._load_complex_uc()

    # ---------- UC table bootstrap ----------
    def _ensure_tables(self) -> None:
        self.spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {self.processors_table} (
              processor STRING,
              pattern_json STRING,
              updated_at TIMESTAMP
            ) USING DELTA
            """
        )
        self.spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {self.complex_table} (
              pattern_name STRING,
              pattern_json STRING,
              updated_at TIMESTAMP
            ) USING DELTA
            """
        )
        if self.meta_table:
            self.spark.sql(
                f"""
                CREATE TABLE IF NOT EXISTS {self.meta_table} (
                  version STRING,
                  last_updated TIMESTAMP
                ) USING DELTA
                """
            )
        if self.raw_snapshots_table:
            self.spark.sql(
                f"""
                CREATE TABLE IF NOT EXISTS {self.raw_snapshots_table} (
                  snapshot_ts TIMESTAMP,
                  raw_json STRING
                ) USING DELTA
                """
            )

    # ---------- Loads ----------
    def _load_processors_uc(self) -> dict:
        rows = (
            self.spark.table(self.processors_table)
            .select("processor", "pattern_json")
            .collect()
        )
        processors: Dict[str, dict] = {}
        for r in rows:
            if r["pattern_json"]:
                try:
                    processors[r["processor"]] = json.loads(r["pattern_json"])
                except Exception:
                    continue
        return {"processors": processors}

    def _load_complex_uc(self) -> dict:
        rows = (
            self.spark.table(self.complex_table)
            .select("pattern_name", "pattern_json")
            .collect()
        )
        out: Dict[str, dict] = {}
        for r in rows:
            if r["pattern_json"]:
                try:
                    out[r["pattern_name"]] = json.loads(r["pattern_json"])
                except Exception:
                    continue
        return out

    # ---------- Upserts ----------
    def _upsert_processor_uc(self, processor: str, pattern: dict) -> None:
        data = [(processor, json.dumps(pattern, ensure_ascii=False), datetime.utcnow())]
        sdf = self.spark.createDataFrame(data, ["processor", "pattern_json", "updated_at"])
        sdf.createOrReplaceTempView("_nifi_proc_upsert")
        self.spark.sql(
            f"""
            MERGE INTO {self.processors_table} t
            USING _nifi_proc_upsert s
              ON t.processor = s.processor
            WHEN MATCHED THEN UPDATE SET
              t.pattern_json = s.pattern_json,
              t.updated_at   = s.updated_at
            WHEN NOT MATCHED THEN INSERT (processor, pattern_json, updated_at)
              VALUES (s.processor, s.pattern_json, s.updated_at)
            """
        )

    def _upsert_complex_uc(self, name: str, pattern: dict) -> None:
        data = [(name, json.dumps(pattern, ensure_ascii=False), datetime.utcnow())]
        sdf = self.spark.createDataFrame(data, ["pattern_name", "pattern_json", "updated_at"])
        sdf.createOrReplaceTempView("_nifi_complex_upsert")
        self.spark.sql(
            f"""
            MERGE INTO {self.complex_table} t
            USING _nifi_complex_upsert s
              ON t.pattern_name = s.pattern_name
            WHEN MATCHED THEN UPDATE SET
              t.pattern_json = s.pattern_json,
              t.updated_at   = s.updated_at
            WHEN NOT MATCHED THEN INSERT (pattern_name, pattern_json, updated_at)
              VALUES (s.pattern_name, s.pattern_json, s.updated_at)
            """
        )

    # ---------- Public API ----------
    def add_pattern(self, processor: str, pattern: dict) -> None:
        if "processors" not in self.patterns:
            self.patterns["processors"] = {}
        existing = self.patterns["processors"].get(processor, {})
        merged = {**existing, **pattern}
        self.patterns["processors"][processor] = merged
        self._upsert_processor_uc(processor, merged)

    def get_pattern(self, processor: str, context: Optional[dict] = None) -> Optional[dict]:
        cache_key = f"{processor}_{str(context)}"
        if cache_key in self.cache:
            return self.cache[cache_key]
        pat = self.patterns.get("processors", {}).get(processor)
        if pat:
            self.cache[cache_key] = pat
            self.track_usage(processor)
        return pat

    def get_complex_pattern(self, name: str) -> Optional[dict]:
        return self.complex.get(name)

    def add_complex_pattern(self, name: str, pattern: dict) -> None:
        self.complex[name] = pattern
        self._upsert_complex_uc(name, pattern)

    # ---------- Operational helpers ----------
    def refresh_from_uc(self) -> None:
        self.patterns = self._load_processors_uc()
        self.complex = self._load_complex_uc()
        self.cache.clear()

    def snapshot_raw_json(self, raw_json: str) -> None:
        if not self.raw_snapshots_table:
            return
        self.spark.createDataFrame([(datetime.utcnow(), raw_json)], ["snapshot_ts", "raw_json"]) \
            .write.mode("append").saveAsTable(self.raw_snapshots_table)

    def seed_from_blob(self, blob: Dict[str, Any]) -> None:
        # processors
        proc_rows = [
            (name, json.dumps(patt, ensure_ascii=False), datetime.utcnow())
            for name, patt in (blob.get("processors", {}) or {}).items()
        ]
        if proc_rows:
            self.spark.createDataFrame(proc_rows, ["processor", "pattern_json", "updated_at"]) \
                .createOrReplaceTempView("_seed_processors")
            self.spark.sql(
                f"""
                MERGE INTO {self.processors_table} t
                USING _seed_processors s
                ON t.processor = s.processor
                WHEN MATCHED THEN UPDATE SET
                  t.pattern_json = s.pattern_json,
                  t.updated_at   = s.updated_at
                WHEN NOT MATCHED THEN INSERT (processor, pattern_json, updated_at)
                  VALUES (s.processor, s.pattern_json, s.updated_at)
                """
            )
        # complex
        complex_rows = [
            (name, json.dumps(patt, ensure_ascii=False), datetime.utcnow())
            for name, patt in (blob.get("complex_patterns", {}) or {}).items()
        ]
        if complex_rows:
            self.spark.createDataFrame(complex_rows, ["pattern_name", "pattern_json", "updated_at"]) \
                .createOrReplaceTempView("_seed_complex")
            self.spark.sql(
                f"""
                MERGE INTO {self.complex_table} t
                USING _seed_complex s
                ON t.pattern_name = s.pattern_name
                WHEN MATCHED THEN UPDATE SET
                  t.pattern_json = s.pattern_json,
                  t.updated_at   = s.updated_at
                WHEN NOT MATCHED THEN INSERT (pattern_name, pattern_json, updated_at)
                  VALUES (s.pattern_name, s.pattern_json, s.updated_at)
                """
            )
        # optional meta
        if self.meta_table:
            version = blob.get("version")
            last_updated = blob.get("last_updated")
            if version and last_updated:
                if isinstance(last_updated, str) and "T" not in last_updated:
                    last_updated = f"{last_updated}T00:00:00"
                self.spark.createDataFrame(
                    [(version, datetime.fromisoformat(last_updated))],
                    ["version", "last_updated"],
                ).write.mode("overwrite").saveAsTable(self.meta_table)

        self.refresh_from_uc()

    def seed_from_file(self, json_file_path: str) -> None:
        with open(json_file_path, "r", encoding="utf-8") as f:
            raw = f.read()
        blob = json.loads(raw)
        self.snapshot_raw_json(raw)
        self.seed_from_blob(blob)

    def track_usage(self, processor: str) -> None:
        stats = self.usage_stats.get(processor, {"count": 0})
        stats["count"] += 1
        stats["last_used"] = datetime.utcnow().isoformat()
        self.usage_stats[processor] = stats


__all__ = ["PatternRegistryUC"]

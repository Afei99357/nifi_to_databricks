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
from typing import Any, Dict, Optional

# --------------------------
# Environment configuration
# --------------------------
PATTERN_TABLE = os.environ.get("PATTERN_TABLE", "eliao.nifi_to_databricks.processors")
COMPLEX_TABLE = os.environ.get(
    "COMPLEX_TABLE", "eliao.nifi_to_databricks.complex_patterns"
)
META_TABLE = os.environ.get(
    "META_TABLE", "eliao.nifi_to_databricks.patterns_meta"
)  # optional
RAW_TABLE = os.environ.get(
    "RAW_TABLE", "eliao.nifi_to_databricks.patterns_raw_snapshots"
)  # optional


def _spark():
    """Return active SparkSession if running on Databricks / PySpark context, else None."""
    try:
        from pyspark.sql import SparkSession  # type: ignore

        # Try to get existing active session first
        spark = SparkSession.getActiveSession()
        if spark:
            return spark

        # If no active session, try to create one (for Databricks environment)
        try:
            spark = SparkSession.builder.appName(
                "NiFi_Migration_Pattern_Registry"
            ).getOrCreate()
            # Avoid sparkContext.appName in Spark Connect - just check if session is valid
            try:
                app_name = spark.sparkContext.appName
                print(f"🔧 [SPARK] Created new SparkSession: {app_name}")
            except Exception:
                # Spark Connect mode - can't access sparkContext
                print(f"🔧 [SPARK] Created new SparkSession (Spark Connect mode)")
            return spark
        except Exception as create_error:
            print(f"⚠️  [SPARK] Could not create SparkSession: {create_error}")
            return None

    except Exception as import_error:
        print(f"⚠️  [SPARK] PySpark not available: {import_error}")
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
        print(f"🔧 [UC INIT] Initializing Unity Catalog pattern registry...")
        self.spark = _spark()
        if not self.spark:
            raise RuntimeError(
                "SparkSession not available. PatternRegistryUC requires Databricks / Spark."
            )

        self.processors_table = processors_table or PATTERN_TABLE
        self.complex_table = complex_table or COMPLEX_TABLE
        self.meta_table = meta_table or META_TABLE
        self.raw_snapshots_table = raw_snapshots_table or RAW_TABLE

        print(f"🔧 [UC INIT] Target tables:")
        print(f"  📊 Processors: {self.processors_table}")
        print(f"  🔗 Complex: {self.complex_table}")
        print(f"  📋 Meta: {self.meta_table}")
        print(f"  📸 Snapshots: {self.raw_snapshots_table}")

        # runtime/cache fields
        self.cache: Dict[str, dict] = {}
        self.usage_stats: Dict[str, dict] = {}

        # bootstrap UC tables
        self._ensure_tables()
        self.patterns = self._load_processors_uc()
        self.complex = self._load_complex_uc()

    # ---------- UC table bootstrap ----------
    def _ensure_tables(self) -> None:
        # 1. Main processor patterns table with comprehensive schema
        self.spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {self.processors_table} (
              processor STRING NOT NULL COMMENT 'NiFi processor class name (e.g., ControlRate, GetFile)',
              category STRING COMMENT 'Pattern category (builtin, llm_generated, custom)',
              databricks_equivalent STRING COMMENT 'Databricks service equivalent (e.g., Auto Loader, Delta Lake)',
              description STRING COMMENT 'Description of what this processor does',
              code_template STRING COMMENT 'PySpark code template with placeholders',
              best_practices ARRAY<STRING> COMMENT 'Array of best practice recommendations',
              generated_from_properties MAP<STRING, STRING> COMMENT 'Properties that generated this pattern',
              generation_source STRING COMMENT 'Source of generation (llm_hybrid_approach, manual, etc)',
              pattern_json STRING COMMENT 'Full pattern JSON for backward compatibility',
              usage_count BIGINT COMMENT 'Number of times this pattern has been used',
              last_used TIMESTAMP COMMENT 'Last time this pattern was used in migration',
              created_at TIMESTAMP COMMENT 'When pattern was first created',
              updated_at TIMESTAMP COMMENT 'When pattern was last updated',
              created_by STRING COMMENT 'User who created this pattern'
            ) USING DELTA
            TBLPROPERTIES (
              'delta.enableChangeDataFeed' = 'true',
              'delta.autoOptimize.optimizeWrite' = 'true',
              'delta.autoOptimize.autoCompact' = 'true'
            )
            COMMENT 'NiFi processor to Databricks migration patterns with usage tracking'
            """
        )

        # Add constraints and indexes for better performance
        try:
            self.spark.sql(
                f"ALTER TABLE {self.processors_table} ADD CONSTRAINT pk_processors PRIMARY KEY (processor)"
            )
        except Exception:
            pass  # Constraint might already exist

        # 2. Complex migration patterns table
        self.spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {self.complex_table} (
              pattern_name STRING NOT NULL COMMENT 'Unique name for complex pattern (e.g., kafka_to_delta_streaming)',
              pattern_type STRING COMMENT 'Type of complex pattern (workflow, integration, etc)',
              description STRING COMMENT 'Description of the complex migration pattern',
              involved_processors ARRAY<STRING> COMMENT 'List of NiFi processors involved',
              databricks_services ARRAY<STRING> COMMENT 'List of Databricks services used',
              complexity_level STRING COMMENT 'LOW, MEDIUM, HIGH complexity rating',
              migration_strategy STRING COMMENT 'Strategy used (chunked, streaming, batch, etc)',
              code_snippets MAP<STRING, STRING> COMMENT 'Code snippets by processor type',
              configuration MAP<STRING, STRING> COMMENT 'Configuration parameters needed',
              dependencies ARRAY<STRING> COMMENT 'External dependencies required',
              pattern_json STRING COMMENT 'Full complex pattern JSON',
              success_rate DOUBLE COMMENT 'Success rate of this pattern (0.0-1.0)',
              usage_count BIGINT COMMENT 'Number of times pattern was used',
              last_used TIMESTAMP COMMENT 'Last usage timestamp',
              created_at TIMESTAMP COMMENT 'Creation timestamp',
              updated_at TIMESTAMP COMMENT 'Last update timestamp',
              created_by STRING COMMENT 'User who created this pattern'
            ) USING DELTA
            TBLPROPERTIES (
              'delta.enableChangeDataFeed' = 'true',
              'delta.autoOptimize.optimizeWrite' = 'true'
            )
            COMMENT 'Complex multi-processor migration patterns and workflows'
            """
        )

        # 3. Optional metadata table for registry versioning
        if self.meta_table:
            self.spark.sql(
                f"""
                CREATE TABLE IF NOT EXISTS {self.meta_table} (
                  key STRING NOT NULL COMMENT 'Metadata key (version, last_migration, stats, etc)',
                  value STRING COMMENT 'Metadata value as JSON string',
                  category STRING COMMENT 'Metadata category (system, user, migration)',
                  description STRING COMMENT 'Description of this metadata entry',
                  created_at TIMESTAMP COMMENT 'Creation timestamp',
                  updated_at TIMESTAMP COMMENT 'Last update timestamp'
                ) USING DELTA
                TBLPROPERTIES (
                  'delta.enableChangeDataFeed' = 'true'
                )
                COMMENT 'Registry metadata and versioning information'
                """
            )

        # 4. Optional raw snapshots table for debugging and rollback
        if self.raw_snapshots_table:
            self.spark.sql(
                f"""
                CREATE TABLE IF NOT EXISTS {self.raw_snapshots_table} (
                  snapshot_id STRING NOT NULL COMMENT 'Unique snapshot identifier',
                  snapshot_type STRING COMMENT 'Type: migration_run, pattern_backup, etc',
                  migration_id STRING COMMENT 'Associated migration run ID',
                  processor_count INT COMMENT 'Number of processors in snapshot',
                  patterns_count INT COMMENT 'Number of patterns captured',
                  nifi_xml_hash STRING COMMENT 'Hash of source NiFi XML for deduplication',
                  raw_json STRING COMMENT 'Complete raw JSON snapshot data',
                  file_size_bytes BIGINT COMMENT 'Size of the snapshot in bytes',
                  compression STRING COMMENT 'Compression method used',
                  created_at TIMESTAMP COMMENT 'Snapshot creation time',
                  created_by STRING COMMENT 'User who created this snapshot',
                  tags MAP<STRING, STRING> COMMENT 'Key-value tags for categorization'
                ) USING DELTA
                TBLPROPERTIES (
                  'delta.enableChangeDataFeed' = 'true',
                  'delta.deletedFileRetentionDuration' = 'interval 30 days',
                  'delta.logRetentionDuration' = 'interval 90 days'
                )
                COMMENT 'Raw snapshots for debugging, rollback, and historical analysis'
                """
            )

        print(f"🔧 [UC SETUP] Created Delta tables with comprehensive schema:")
        print(f"  📊 Processors: {self.processors_table}")
        print(f"  🔗 Complex patterns: {self.complex_table}")
        if self.meta_table:
            print(f"  📋 Metadata: {self.meta_table}")
        if self.raw_snapshots_table:
            print(f"  📸 Snapshots: {self.raw_snapshots_table}")

    # ---------- Loads ----------
    def _load_processors_uc(self) -> dict:
        try:
            rows = (
                self.spark.table(self.processors_table)
                .select(
                    "processor",
                    "category",
                    "databricks_equivalent",
                    "description",
                    "code_template",
                    "best_practices",
                    "pattern_json",
                    "usage_count",
                )
                .collect()
            )
            processors: Dict[str, dict] = {}
            for r in rows:
                # Try to use structured fields first, fall back to pattern_json
                if r["code_template"] and r["databricks_equivalent"]:
                    # Use structured data
                    pattern = {
                        "category": r["category"] or "unknown",
                        "databricks_equivalent": r["databricks_equivalent"],
                        "description": r["description"] or "",
                        "code_template": r["code_template"],
                        "best_practices": r["best_practices"] or [],
                        "usage_count": r["usage_count"] or 0,
                    }
                    processors[r["processor"]] = pattern
                elif r["pattern_json"]:
                    # Fall back to JSON field for backward compatibility
                    try:
                        processors[r["processor"]] = json.loads(r["pattern_json"])
                    except Exception:
                        continue
            print(
                f"📚 [UC LOAD] Loaded {len(processors)} processor patterns from UC table"
            )
            return {"processors": processors}
        except Exception as e:
            print(f"⚠️  [UC LOAD] Failed to load patterns: {e}")
            return {"processors": {}}

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
        import os
        from datetime import datetime

        current_time = datetime.utcnow()
        current_user = os.environ.get("USER_EMAIL", os.environ.get("USER", "system"))

        # Extract fields from pattern dict
        category = pattern.get("category", "llm_generated")
        databricks_equivalent = pattern.get("databricks_equivalent", "Unknown")
        description = pattern.get("description", f"Pattern for {processor}")
        code_template = pattern.get("code_template", "")
        best_practices = pattern.get("best_practices", [])
        generated_from_properties = pattern.get("generated_from_properties", {})
        generation_source = pattern.get("generation_source", "manual")
        pattern_json = json.dumps(pattern, ensure_ascii=False)

        # Create DataFrame with comprehensive schema
        data = [
            (
                processor,
                category,
                databricks_equivalent,
                description,
                code_template,
                best_practices,
                generated_from_properties,
                generation_source,
                pattern_json,
                1,  # usage_count - start with 1 since we're using it
                current_time,  # last_used
                current_time,  # created_at
                current_time,  # updated_at
                current_user,  # created_by
            )
        ]

        schema = [
            "processor",
            "category",
            "databricks_equivalent",
            "description",
            "code_template",
            "best_practices",
            "generated_from_properties",
            "generation_source",
            "pattern_json",
            "usage_count",
            "last_used",
            "created_at",
            "updated_at",
            "created_by",
        ]

        sdf = self.spark.createDataFrame(data, schema)
        sdf.createOrReplaceTempView("_nifi_proc_upsert")

        self.spark.sql(
            f"""
            MERGE INTO {self.processors_table} t
            USING _nifi_proc_upsert s
              ON t.processor = s.processor
            WHEN MATCHED THEN UPDATE SET
              t.category = s.category,
              t.databricks_equivalent = s.databricks_equivalent,
              t.description = s.description,
              t.code_template = s.code_template,
              t.best_practices = s.best_practices,
              t.generated_from_properties = s.generated_from_properties,
              t.generation_source = s.generation_source,
              t.pattern_json = s.pattern_json,
              t.usage_count = t.usage_count + 1,
              t.last_used = s.last_used,
              t.updated_at = s.updated_at
            WHEN NOT MATCHED THEN INSERT (
              processor, category, databricks_equivalent, description,
              code_template, best_practices, generated_from_properties,
              generation_source, pattern_json, usage_count, last_used,
              created_at, updated_at, created_by
            ) VALUES (
              s.processor, s.category, s.databricks_equivalent, s.description,
              s.code_template, s.best_practices, s.generated_from_properties,
              s.generation_source, s.pattern_json, s.usage_count, s.last_used,
              s.created_at, s.updated_at, s.created_by
            )
            """
        )

    def _upsert_complex_uc(self, name: str, pattern: dict) -> None:
        data = [(name, json.dumps(pattern, ensure_ascii=False), datetime.utcnow())]
        sdf = self.spark.createDataFrame(
            data, ["pattern_name", "pattern_json", "updated_at"]
        )
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

    def _upsert_processors_bulk_uc(self, patterns: Dict[str, dict]) -> None:
        import os
        from datetime import datetime

        current_time = datetime.utcnow()
        current_user = os.environ.get("USER_EMAIL", os.environ.get("USER", "system"))

        rows = []
        for processor, pattern in patterns.items():
            rows.append(
                (
                    processor,
                    pattern.get("category", "llm_generated"),
                    pattern.get("databricks_equivalent", "Unknown"),
                    pattern.get("description", f"Pattern for {processor}"),
                    pattern.get("code_template", ""),
                    pattern.get("best_practices", []),
                    pattern.get("generated_from_properties", {}),
                    pattern.get("generation_source", "manual"),
                    json.dumps(pattern, ensure_ascii=False),
                    1,
                    current_time,
                    current_time,
                    current_time,
                    current_user,
                )
            )

        schema = [
            "processor",
            "category",
            "databricks_equivalent",
            "description",
            "code_template",
            "best_practices",
            "generated_from_properties",
            "generation_source",
            "pattern_json",
            "usage_count",
            "last_used",
            "created_at",
            "updated_at",
            "created_by",
        ]

        sdf = self.spark.createDataFrame(rows, schema)
        sdf.createOrReplaceTempView("_nifi_proc_upsert_bulk")

        self.spark.sql(
            f"""
            MERGE INTO {self.processors_table} t
            USING _nifi_proc_upsert_bulk s
              ON t.processor = s.processor
            WHEN MATCHED THEN UPDATE SET
              t.category = s.category,
              t.databricks_equivalent = s.databricks_equivalent,
              t.description = s.description,
              t.code_template = s.code_template,
              t.best_practices = s.best_practices,
              t.generated_from_properties = s.generated_from_properties,
              t.generation_source = s.generation_source,
              t.pattern_json = s.pattern_json,
              t.usage_count = t.usage_count + 1,
              t.last_used = s.last_used,
              t.updated_at = s.updated_at
            WHEN NOT MATCHED THEN INSERT (
              processor, category, databricks_equivalent, description,
              code_template, best_practices, generated_from_properties,
              generation_source, pattern_json, usage_count, last_used,
              created_at, updated_at, created_by
            ) VALUES (
              s.processor, s.category, s.databricks_equivalent, s.description,
              s.code_template, s.best_practices, s.generated_from_properties,
              s.generation_source, s.pattern_json, s.usage_count, s.last_used,
              s.created_at, s.updated_at, s.created_by
            )
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

    def get_pattern(
        self, processor: str, context: Optional[dict] = None
    ) -> Optional[dict]:
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

    def add_patterns_bulk(self, patterns: Dict[str, dict]) -> None:
        if not patterns:
            return
        # Update in-memory cache
        for processor, pattern in patterns.items():
            if "processors" not in self.patterns:
                self.patterns["processors"] = {}
            existing = self.patterns["processors"].get(processor, {})
            merged = {**existing, **pattern}
            self.patterns["processors"][processor] = merged
        # Persist in one MERGE
        self._upsert_processors_bulk_uc(patterns)

    def add_raw_snapshot(self, snapshot: Dict[str, Any]) -> None:
        """Insert a raw snapshot record into the snapshots table."""
        if not self.raw_snapshots_table:
            return
        # Prepare one-row DataFrame and insert via SQL for UC compatibility
        row = (
            snapshot.get("snapshot_id"),
            snapshot.get("snapshot_type", "pattern_buffer"),
            snapshot.get("migration_id"),
            int(snapshot.get("processor_count", 0)),
            int(snapshot.get("patterns_count", 0)),
            snapshot.get("nifi_xml_hash"),
            snapshot.get("raw_json"),
            int(snapshot.get("file_size_bytes", 0)),
            snapshot.get("compression", "none"),
            snapshot.get("created_at", datetime.utcnow()),
            snapshot.get("created_by"),
            snapshot.get("tags", {}),
        )
        sdf = self.spark.createDataFrame(
            [row],
            [
                "snapshot_id",
                "snapshot_type",
                "migration_id",
                "processor_count",
                "patterns_count",
                "nifi_xml_hash",
                "raw_json",
                "file_size_bytes",
                "compression",
                "created_at",
                "created_by",
                "tags",
            ],
        )
        sdf.createOrReplaceTempView("_nifi_snapshot_upsert")
        self.spark.sql(
            f"""
            INSERT INTO {self.raw_snapshots_table}
            SELECT snapshot_id, snapshot_type, migration_id, processor_count,
                   patterns_count, nifi_xml_hash, raw_json, file_size_bytes,
                   compression, created_at, created_by, tags
            FROM _nifi_snapshot_upsert
            """
        )

    def upsert_meta(
        self, key: str, value: str, category: str = "system", description: str = ""
    ) -> None:
        """Upsert a metadata key/value into the meta table."""
        if not self.meta_table:
            return
        now = datetime.utcnow()
        data = [(key, value, category, description, now, now)]
        sdf = self.spark.createDataFrame(
            data,
            ["key", "value", "category", "description", "created_at", "updated_at"],
        )
        sdf.createOrReplaceTempView("_nifi_meta_upsert")
        self.spark.sql(
            f"""
            MERGE INTO {self.meta_table} t
            USING _nifi_meta_upsert s
              ON t.key = s.key
            WHEN MATCHED THEN UPDATE SET
              t.value = s.value,
              t.category = s.category,
              t.description = s.description,
              t.updated_at = s.updated_at
            WHEN NOT MATCHED THEN INSERT (key, value, category, description, created_at, updated_at)
              VALUES (s.key, s.value, s.category, s.description, s.created_at, s.updated_at)
            """
        )

    # ---------- Operational helpers ----------
    def refresh_from_uc(self) -> None:
        self.patterns = self._load_processors_uc()
        self.complex = self._load_complex_uc()
        self.cache.clear()

    # JSON seeding functions removed - use Delta table operations only

    def track_usage(self, processor: str) -> None:
        stats = self.usage_stats.get(processor, {"count": 0})
        stats["count"] += 1
        stats["last_used"] = datetime.utcnow().isoformat()
        self.usage_stats[processor] = stats


__all__ = ["PatternRegistryUC"]

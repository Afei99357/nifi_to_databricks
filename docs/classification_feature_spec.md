# Processor Classification Feature Spec

## Overview
Define a reusable feature schema for machine-learning classification of NiFi processors.
Each processor row should yield consistent numeric/boolean/categorical columns derived from
existing `feature_evidence`. All fields must be computable without post-classification
labels to avoid leakage.

## Core Identifiers
- `processor_id` (string) — opaque identifier used to join back to evidence (not fed to ML).
- `short_type` (categorical) — trimmed type (e.g., `PutFile`).

## SQL Signals
- `sql_has_sql` (bool) — true if any SQL text detected.
- `sql_has_dml` (bool) — true if INSERT/UPDATE/MERGE/DELETE detected.
- `sql_has_metadata_only` (bool) — true if metadata/diagnostic SQL and no DML.

## Script Signals
- `scripts_inline_count` (int) — count of inline scripts.
- `scripts_external_count` (int) — count of external scripts or commands.
- `scripts_has_inline` (bool) — `scripts_inline_count > 0`.
- `scripts_has_external` (bool) — `scripts_external_count > 0`.
- `scripts_external_hosts_count` (int) — number of distinct external hosts referenced.

## Table & Property Interaction
- `table_count` (int) — number of distinct tables referenced (`len(tables.tables)`).
- `table_property_reference_count` (int) — number of NiFi processor properties that reference tables (`len(tables.properties)`).

## Variable Usage
- `uses_variables` (bool) — direct flag from evidence.
- `variables_define_count` (int) — number of variables defined.
- `variables_use_count` (int) — number of variables referenced.
- `uses_expression_language` (bool) — true if any usage expression contains NiFi EL syntax (`${...}`).

## Controller Services
- `controller_service_count` (int) — total bound services.
- `uses_dbcp` (bool) — any controller service class contains `dbcp`.
- `uses_kafka` (bool) — any controller service class contains `kafka`.
- `uses_jms` (bool) — any controller service class contains `jms`.
- `uses_hive` (bool) — any controller service class or property references Hive (e.g., `hive` keyword, Hive JDBC).
- `uses_impala` (bool) — any controller service or property references Impala.
- `uses_kudu` (bool) — any controller service or property references Kudu.

## Connection Topology
- `incoming_count` (int) — number of incoming connections.
- `outgoing_count` (int) — number of outgoing connections.
- `is_fanin` (bool) — `incoming_count > 1`.
- `is_fanout` (bool) — `outgoing_count > 1`.
- `is_terminal` (bool) — `outgoing_count == 0`.
- `incoming_type_flags` (dict of bool) — curated set of flags for notable upstream types
  (e.g., `incoming_has_ExecuteSQL`, `incoming_has_ListFile`).
- `outgoing_type_flags` (dict of bool) — curated set of flags for notable downstream types.

## Textual Hints
- `name_has_log` (bool) — processor name contains `log` or `logging`.
- `name_has_retry` (bool) — name contains `retry` or `requeue`.
- `name_has_sftp` (bool) — name contains `sftp`.
- `group_has_alert` (bool) — parent group name contains `alert`, `monitor`.

## Target Labels
Train the initial model on five migration categories (collapsed to avoid sparse classes):
- `Business Logic`
- `Source Adapter`
- `Sink Adapter`
- `Orchestration / Monitoring`
- `Infrastructure Only`

Record original `Source / Sink Adapter` instances and map them into Source or Sink
categories until sufficient examples exist to reintroduce the combined class. Optionally
exclude `Ambiguous` rows or treat them as a separate class once labels stabilise.

## Notes
- Missing values should default to `0` or `False`.
- Categorical encoding can happen during model training (one-hot, target encoding).
- The curated incoming/outgoing type flags should focus on high-signal processors and can grow over time.
- Additional derived features (e.g., hashed property names) can be appended after baseline evaluation.

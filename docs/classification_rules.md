# Declarative Classification Rules

The Streamlit dashboard now relies on a declarative rulebook to map NiFi processors to migration actions. Rules live in the repository root as `classification_rules.yaml`; each entry describes:

- `name`: human-friendly label for the rule.
- `processor_types`: optional list of fully-qualified NiFi processor types the rule applies to. If omitted, the rule can apply to any processor.
- `conditions`: list of predicates evaluated against extracted processor features (e.g., `sql.has_dml`, `scripts.inline_count`). Supported operators are `equals`, `not_equals`, `gt`, `lt`, `contains`, and `contains_any`.
- `migration_category`: one of the migration-focused buckets (Business Logic, Source Adapter, Sink Adapter, Orchestration / Monitoring, Infrastructure Only, Ambiguous).
- `databricks_target`: recommended Databricks primitive or workflow task.
- `confidence`: numeric confidence (0.0–1.0) used by the UI to surface low-confidence matches.
- `notes`: short rationale or guidance for contributors.

Processor features are created by `tools/processor_features.py`. They include counts of inline/external scripts, SQL behaviour flags, table references, variable usage, controller services, and graph connectivity.

Manual corrections belong in `classification_overrides.yaml`, where each processor ID can override the inferred category/target/confidence. Overrides win over rules and are recorded in the Streamlit interface as “override” sources.

When expanding coverage, prefer adding/updating rules rather than patching code. Run `pytest tests/test_declarative_classifier.py` to validate new rules and keep a sample NiFi template handy to sanity-check the Streamlit experience.

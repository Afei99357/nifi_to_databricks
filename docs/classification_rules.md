# Declarative Classification Rules

The Streamlit dashboard now relies on a declarative rulebook to map NiFi processors to migration actions. Rules live in the repository root as `classification_rules.yaml`; each entry describes:

- `name`: human-friendly label for the rule.
- `processor_types`: optional list of fully-qualified NiFi processor types the rule applies to. If omitted, the rule can apply to any processor.
- `conditions`: list of predicates evaluated against extracted processor features (e.g., `sql.has_dml`, `scripts.inline_count`). Supported operators are `equals`, `not_equals`, `gt`, `lt`, `contains`, and `contains_any`.
- `migration_category`: one of the migration-focused buckets (Business Logic, Source Adapter, Sink Adapter, Orchestration / Monitoring, Infrastructure Only, Ambiguous).
- `databricks_target`: recommended Databricks primitive or workflow task.
- `confidence`: optional numeric confidence (0.0–1.0). Until the automated scorer lands, leave this unset or pick a coarse value (e.g., `0.6` for tentative, `0.9` for certain); the UI mainly relies on the `classification_source` flag.
- `notes`: short rationale or guidance for contributors.

Processor features are created by `tools/processor_features.py`. They include counts of inline/external scripts, SQL behaviour flags, table references, variable usage, controller services, and graph connectivity.

Manual corrections belong in `classification_overrides.yaml`, where each processor ID can override the inferred category/target/confidence. Overrides win over rules and are recorded in the Streamlit interface as “override” sources.

When expanding coverage, prefer adding/updating rules rather than patching code. Run `pytest tests/test_declarative_classifier.py` to validate new rules and keep a sample NiFi template handy to sanity-check the Streamlit experience.

## Managing New Workflows

When new NiFi templates arrive, follow this loop to keep the rulebook healthy:

1. **Run a classification pass.** Upload the XML in the Streamlit dashboard (Dashboard → Processor Classification). Let the declarative engine process the file and review the “Ambiguous” table for any processors that lack a rule.
2. **Inspect evidence.** Expand an ambiguous row, check the feature summary (SQL flags, script counts, downstream targets), and decide whether the processor should map to Business Logic, Adapter, Support, or Infrastructure.
3. **Update the rulebook.** If the pattern is reusable, add a YAML entry in `classification_rules.yaml` (copy an existing rule and tweak `processor_types` / `conditions`). If it is a one-off, record the outcome in `classification_overrides.yaml` keyed by the processor ID.
4. **Re-run the page.** Refresh Streamlit to confirm the new rule/override removes the processor from the ambiguous list. Iterate until the workflow classifies cleanly.
5. **Add coverage tests.** When a new rule stabilises, add a fixture to `tests/test_declarative_classifier.py` so future edits do not regress the behaviour.
6. **Document notable patterns.** Mention processor-specific tips or gotchas in this file so future contributors understand why a rule exists.

_Tip:_ If multiple workflows introduce the same processor type, prefer a general rule over per-workflow overrides. Reserve overrides for genuine exceptions (custom scripts, temporary migrations, etc.).

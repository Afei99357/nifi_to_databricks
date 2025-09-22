# NiFi → Databricks Classification Overhaul

## Goals

- Automatically triage processors from all NiFi workflows into migration-focused categories (Business Logic, Source Adapter, Sink Adapter, Orchestration / Monitoring, Infrastructure Only, Ambiguous).
- Minimise manual rule maintenance by leaning on automated suggestions, Streamlit overrides, and forthcoming model assistance.
- Ensure metadata-heavy processors that feed downstream business sinks are preserved, while pure infrastructure plumbing is dropped or mapped to Databricks workflow controls.
- Maintain an auditable pipeline (rules, overrides, tests, artefacts) that scales across hundreds of workflows.

## Current Assets

- Declarative rule engine: `tools/classification/rules_engine.py`
- Feature extraction: `tools/classification/processor_features.py`
- Batch CLI + JSON/CSV export: `tools/classification/classify_all_workflows.py`
- Rule suggestion CLI: `tools/classification/suggest_rules.py`
- Streamlit classification & review UI: `streamlit_app/pages/01_Processor_Classification.py`
- Rule/config files: `classification_rules.yaml`, `classification_overrides.yaml`, `classification_rules.suggestions.yaml`
- Regression tests: `tests/test_declarative_classifier.py`

## Phase 1 – Batch every workflow (Complete)

**Deliverable:** `tools/classification/classify_all_workflows.py`

Status: ✅ Implemented. CLI walks NiFi templates, writes per-template JSON and `derived_classification/summary.csv`, and the Streamlit app consumes saved artefacts.

## Phase 2 – Rule suggestion pipeline (Complete)

**Deliverable:** `tools/classification/suggest_rules.py`

Status: ✅ Implemented. Aggregates `summary.csv`, emits draft rules with evidence into `classification_rules.suggestions.yaml`. Manual review and merge still required.

> Optional sanity check: run a t-SNE projection over the processor feature vectors (after normalising numeric fields and one-hot encoding categoricals) to visualise whether similar processors cluster together. Use this strictly for exploration—take note of clusters/outliers to refine rules in `classification_rules.yaml`, but do not treat the embedding as a classifier.

## Phase 3 – Metadata promotion logic (Complete)

Status: ✅ Implemented inside `rules_engine._apply_metadata_promotions` with regression coverage. Infrastructure processors that define variables consumed by Business Logic/Sink Adapters are auto-promoted with audit notes.

## Phase 4 – Streamlit enhancements (In progress)

Status: 🚧 Quick-action overrides, cached results, and saved-run review flow are live. Remaining work:

1. Replace raw JSON evidence block with structured panels showing downstream processors, variable lineage, script counts, SQL flags, and controller services.
2. Trigger reclassification/refresh automatically after a quick-action override writes to `classification_overrides.yaml`.
3. Surface rule suggestions in the UI so reviewers can accept/reject entries without editing YAML manually.

## Phase 5 – Confidence automation (Future)

### Lightweight ML scoring (preferred)
- **High priority:** procure a sufficiently balanced labelled dataset by aggregating processors across workflows, with particular focus on sparse classes (Source/Sink adapters). Treat manual review of the ambiguous queue as ongoing training data collection.
- Train a model (e.g., gradient boosting or logistic regression) on accumulated features and final categories once each class has a baseline sample (≈100 instances).
- Use calibrated probabilities as confidence scores in place of static rule values.
- Surface model insights during review (top features, probability).

### Optional LLM assist
- Batch ambiguous processors into prompts with feature context.
- Parse category/target/confidence suggestions; record accepted results with `source = "llm"`.
- Compare performance vs ML model for coverage and governance readiness.

## Regression & Documentation Checklist

- Extend `tests/test_declarative_classifier.py` with fixtures for new rules and promotion edge cases.
- After substantive changes:
  - `python tools/classification/classify_all_workflows.py ...`
  - `uv run pytest tests/test_declarative_classifier.py`
- Update `docs/classification_rules.md` with new rule patterns, promotion logic, and evidence UX once Phase 4 is finalised.
- Keep `classification_rules.suggestions.yaml` under review so accepted rules migrate into `classification_rules.yaml` promptly.

## Metrics & Operational Questions

- Track ambiguous processor count, overrides per template, and promotion rate across runs to gauge manual workload reduction.
- Decide retention/location for `derived_classification_results/` artefacts (repo vs external storage).
- Determine cadence (nightly/weekly) for rerunning the batch classifier as new NiFi templates appear.

## Next Focus

1. Deliver Phase 4 enhancements: structured evidence UI, auto-refresh after overrides, and in-app rule acceptance.
2. Evaluate lightweight ML prototype for Phase 5 and define feature set + labeling pipeline.
3. Establish review workflow for integrating accepted suggestions into `classification_rules.yaml` without manual YAML edits.

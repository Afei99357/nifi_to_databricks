# NiFi → Databricks Classification Overhaul

## Goals

- Automatically triage processors from all NiFi workflows into migration-focused categories (Business Logic, Source Adapter, Sink Adapter, Orchestration / Monitoring, Infrastructure Only, Ambiguous).
- Minimise manual rule maintenance by leaning on automated suggestions, Streamlit overrides, and optional model assistance.
- Ensure metadata-heavy processors that feed downstream business sinks are preserved, while pure infrastructure plumbing is dropped or mapped to Databricks workflow controls.
- Maintain an auditable pipeline (rules, overrides, tests, artefacts) that scales across hundreds of workflows.

## Current Assets (Baseline)

- Declarative rule engine: `tools/declarative_classifier.py`
- Feature extraction: `tools/processor_features.py`
- Streamlit classification page: `streamlit_app/pages/01_Processor_Classification.py`
- Rule/config files: `classification_rules.yaml`, `classification_overrides.yaml`
- Documentation: `docs/classification_rules.md`
- Regression test scaffold: `tests/test_declarative_classifier.py`

## Phase 1 – Batch every workflow (Day 1)

**Deliverable:** `tools/classify_all_workflows.py`

1. Walk `nifi_pipeline_file/**.xml` (support glob path input).
2. For each template, call `classify_workflow(xml_path)`.
3. Write JSON result to `derived_classification/{template_stem}.json`.
4. Append rows (template, processor_id, short_type, migration_category, key feature hints) to `derived_classification/summary.csv`.
5. Emit run stats (total processors, ambiguous count, duration) to stdout/log.

_Command prototype:_ `python tools/classify_all_workflows.py --input nifi_pipeline_file --output derived_classification`

## Phase 2 – Rule suggestion pipeline (Day 1–2)

**Deliverable:** `tools/suggest_rules.py`

1. Load `derived_classification/summary.csv` into Pandas.
2. Build the set of processor types already covered in `classification_rules.yaml`.
3. Group the summary by **fully-qualified `processor_type`**.
4. For each uncovered type:
   - Aggregate feature hints (means for boolean/numeric fields like `sql.has_dml`, `sql.has_metadata_only`, `scripts.inline_count`, `downstream_sink_fraction`; counts for occurrences, average existing rule confidence, etc.).
   - Optionally capture representative processor IDs (top N by confidence) when `--include-evidence` is passed.
   - Use simple heuristics on the aggregates to infer a draft category/target. Examples:
     - `sql.has_dml_mean > 0.2` ⇒ Business Logic.
     - High downstream sink fraction with no scripts/sql ⇒ Sink Adapter.
     - High external script count ⇒ Source/Sink Adapter.
5. Emit YAML stubs like:
   ```yaml
   - name: Auto: PutFile sink
     processor_types:
       - org.apache.nifi.processors.standard.PutFile
     conditions:
       - field: scripts.inline_count
         equals: 0
     migration_category: Sink Adapter
     databricks_target: Delta write
     confidence: TBD
     notes: "Auto-suggested from 84 instances; no scripts/SQL; downstream sink fraction 0.92"
   ```
   > **Confidence placeholder:** remove the hand-assigned confidence column for now; Phase 2 will replace it with a data-driven score derived from aggregate metrics or a simple model.
6. Save suggestions to `classification_rules.suggestions.yaml`.
7. CLI flags:
   - `--only-new`: ignore processor types already represented in `classification_rules.yaml`.
   - `--include-evidence`: attach sample processor IDs / downstream counts in the notes field for reviewer context.

Workflow: run Phase 1 → generate suggestions → manually review / merge accepted entries → rerun Phase 1 to validate ambiguity reduction.

## Phase 3 – Metadata promotion logic (Day 2–3)

**Objective:** Promote processors currently tagged as infrastructure when they define variables/attributes consumed by essential sinks.

Implementation outline (within `tools.declarative_classifier` or a post-process helper):

1. After base classification, iterate all processor features again.
2. For processors marked `Infrastructure Only` or `Orchestration / Monitoring`:
   - Inspect `variables.defines` and `feature_evidence.connections`.
   - If any defined variable flows to a downstream processor categorised as `Business Logic` or `Sink Adapter`, reclassify current processor to `Business Logic` (or `Support`) and add `promotion_reason` note.
3. Guard against overrides (overrides always win).
4. Ensure promotions appear in Streamlit outputs and exported CSVs.

## Phase 4 – Streamlit enhancements (Day 3–4)

1. For low-confidence / ambiguous rows, add quick-action buttons (Business Logic / Support / Infrastructure). On click, append or update entry in `classification_overrides.yaml`, set default `confidence = 0.95`, and rerun the page.
2. Render evidence panel beside each record:
   - Downstream processor IDs/names.
   - Variables defined/used.
   - Inline/external script counts.
   - SQL snippets (truncated) or flags.
   - Controller services referenced.
3. Optionally expose raw feature JSON in an expandable block for deep dives.

Outcome: reviewers resolve dozens of processors without editing YAML manually.

## Phase 5 – Optional fallback (future)

### LLM assistance
- Batch ambiguous processors into prompts (include feature snapshot + downstream context).
- Call Databricks/Claude endpoint, parse suggested category/target/confidence.
- Write decisions to `classification_overrides.yaml` with `source = "llm"` and, once implemented, replace the placeholder confidence metric with the model’s score.

### ML assistance
- Train a light model (e.g., gradient boosting, nearest neighbour) on accumulated features + final categories.
- Use its probability output to populate the confidence column going forward.

## Regression & Documentation checklist

- Extend `tests/test_declarative_classifier.py` as new rules stabilise (fixtures per processor type).
- After each change set:
  - `python tools/classify_all_workflows.py ...`
  - `uv run pytest tests/test_declarative_classifier.py`
- Update `docs/classification_rules.md` with new rule patterns, promotion heuristics, UI workflows, and the eventual confidence calculation formula once automated.
- Consider ignoring large derived artefacts (`derived_classification/*.json`, `.csv`) via `.gitignore` unless needed for audit.

## Suggested timeline (flexible)

| Day | Focus                                      |
|-----|--------------------------------------------|
| 1   | Phase 1 (batch CLI) + Phase 2 (rule suggestions draft) |
| 2   | Merge accepted rules, rerun batch, start metadata promotion logic |
| 3   | Finalise metadata promotion, implement Streamlit quick actions |
| 4   | Polish UI evidence, document process, prep for optional LLM/ML work |
| Later | Integrate LLM/ML fallback, introduce data-driven confidence |

## Open questions

- Owner for reviewing auto-generated rule suggestions?
- Storage strategy for `derived_classification` artefacts (repo vs external)?
- Threshold or alternative signal to drive the manual review queue now that confidence is suspended?
- Do we want a nightly job to re-run the batch classifier as new templates land?

---

Pick up Monday with Phase 1: build/run the batch classifier, inspect `summary.csv`, and feed the results into the rule suggestion script.

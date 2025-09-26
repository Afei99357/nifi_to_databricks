# Processor Batch Conversion & Segmentation Outline

This document captures the phased plan for the new migration pipeline that converts NiFi processors in manageable batches and then composes Databricks artefacts by segment.

## Phase 1 – Processor Batch Conversion

1. **Batch builder**
   - Iterate processors in deterministic order (template name, parent group path, processor_id).
   - Slice into configurable batches (default 4 processors) while tracking total estimated token size.
2. **Prompt payload**
   - For each processor expose: metadata (name/type/category), feature evidence (SQL flags, scripts, variables, upstream/downstream ids), and existing overrides.
   - Serialise as compact JSON ready for the LLM request.
3. **LLM response contract**
   - Require JSON array with, per processor: proposed Databricks code stub, inputs/outputs, dependencies, risk flags.
   - Validate response, retry with smaller batch if parsing fails.
4. **Caching**
   - Hash the processor evidence; skip LLM calls when unchanged.
   - Persist batch outputs under `derived_processor_snippets/` for downstream composition.

## Phase 2 – Segment Assembly

1. **Segmentation modes**
   - *Group mode*: default to `parent_group_path`, but allow manual regrouping and synthetic "Ungrouped" buckets for orphan processors.
   - *Path mode*: optional traversal following connections from chosen entry processors to sinks.
2. **Segment payload builder**
   - Collect processor snippets for the segment, plus connection metadata linking to upstream/downstream segments.
   - Generate minimal context JSON for the composer LLM: ordered snippet list, shared variables/tables, controller services.
3. **Composer prompt**
   - Produce a Databricks-ready notebook or workflow task containing ordered snippets, deduplicated imports, TODO markers for unknowns, and lineage comments.
   - Separate template for SQL-only segments that should become DBSQL scripts.
4. **Output management**
   - Save composed artefacts under `derived_segment_notebooks/` with metadata (segment path, dependencies, source batch hashes).
   - Provide download + preview in Streamlit.

## Phase 3 – UI Integration

1. Add a "Processor Snippets" page to run Phase 1 and inspect/correct per-processor outputs.
2. Extend the classification page (or a new "Segment Composer" page) to preview segments, trigger composition, and download notebooks/workflow definitions.
3. Expose diagnostics (processors skipped, batches retried, segments exceeding token limits) so users can act before rerunning.

## Phase 4 – Automation & CI Hooks

1. Command-line entry points for batch conversion and segment composition, so we can script nightly runs.
2. Unit tests covering batch slicing, caching behaviour, and composer payload validation.
3. Optional regression tests comparing generated notebooks to stored golden outputs for key templates.

# Processor Embedding (t-SNE) Workflow

This checklist describes how to regenerate the flattened feature matrix and
produce t-SNE plots for the current NiFi templates. All commands run from the
project root.

## 1. Rebuild classification artefacts

Classify every NiFi XML under `nifi_pipeline_file/`:

```bash
uv run python tools/classification/classify_all_workflows.py \
  --input nifi_pipeline_file \
  --output derived_classification_results
```

This emits per-template JSON files and a consolidated `summary.csv` inside
`derived_classification_results/`.

## 2. Export flattened feature matrix

Convert the JSON artefacts into a single CSV enriched with catalog features and
other signals:

```bash
uv run python -m tools.classification.export_feature_matrix \
  --input derived_classification_results \
  --output derived_classification_features_all.csv \
  --include-ambiguous
```

- `--include-ambiguous` keeps low-confidence processors so the plot reflects the
  full review set. Omit the flag to focus on high-confidence labels.

## 3. Generate t-SNE plots

Produce color-blind-friendly scatter plots grouped by migration category and by
processor short type:

```bash
uv run python tools.classification.run_tsne \
  --input derived_classification_features_all.csv \
  --output derived_classification_tsne_by_category.png \
  --color-by migration_category

uv run python tools.classification.run_tsne \
  --input derived_classification_features_all.csv \
  --output derived_classification_tsne_by_short_type.png \
  --color-by short_type
```

Adjust `--perplexity`, `--learning-rate`, or `--seed` to explore alternate
projections.

## 4. Inspect and share

Open the generated PNG files in the repo root. They are ignored by Git via
`.gitignore` (`derived_classification_tsne*.png`), so share them manually if
needed.

## Optional: Small subset run

For a specific template, run the pipeline against a single XML:

```bash
uv run python tools/classification.classify_all_workflows \
  --input nifi_pipeline_file/nxp_example/ICN8_Track-out_time_based_loading.xml \
  --output derived_classification_results/icn8_only

uv run python -m tools.classification.export_feature_matrix \
  --input derived_classification_results/icn8_only \
  --output derived_classification_features_icn8.csv
```

Then pass that CSV into `run_tsne.py` as above.

## Notes

- The feature exporter now adds catalog-derived columns (`catalog_category`,
  binary `catalog_is_*` flags). These improve clustering and feed ML.
- Regenerate the matrix whenever classification rules change or new NiFi
  templates are added.
- Always sanity-check class counts in the CSV before interpreting the plots.

# Processor Catalog

The processor catalog centralises knowledge about common NiFi processor short
names. The data lives in `tools/resources/processor_catalog.yaml` and follows the
schema below:

```yaml
version: YYYY-MM-DD
categories:
  ingestion:
    - GetFile
    - ...
aliases:
  PublishKafka_2_0: PutKafka
metadata:
  PutFile:
    default_migration_category: Sink Adapter
    notes: "Writes to filesystem; map to Delta or Files API"
```

## Field definitions

- **categories**: Maps catalog buckets (ingestion, egress, transform, routing,
  utility, security) to lists of NiFi short types. These buckets are used by
  rule suggestion, feature export, and readiness scoring.
- **aliases**: Normalises alternate short names by pointing them at a canonical
  entry. Aliases are resolved before category or metadata lookups.
- **metadata**: Optional per-processor annotations. The catalog currently records
  the default migration category and descriptive notes that feed rule suggestion
  outputs.

## Usage

The loader in `tools/catalog/__init__.py` exposes:

- `load_catalog()` – returns a `ProcessorCatalog` dataclass
- `category_for(short_type)` – returns the catalog bucket for a short type
- `metadata_for(short_type)` – returns metadata dict entries

These helpers power:

- Rule suggestion defaults (`tools/classification/suggest_rules.py`)
- Feature export columns (`tools/classification/export_feature_matrix.py`)
- Streamlit catalog coverage displays

## Updating the catalog

1. Edit `tools/resources/processor_catalog.yaml`.
2. Keep short types in canonical form (no package names).
3. Add aliases for versioned variants (e.g. `PublishKafka_2_0`).
4. Update the metadata section when default migration categories change.
5. Run `pytest tests/test_processor_catalog.py tests/test_catalog_validation.py`.
6. Commit the YAML and tests together.

Tracking changes in a shared YAML keeps classification logic, analytics, and UI
in sync without duplicating processor lists across scripts.

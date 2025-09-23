import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from tools.catalog import load_catalog  # noqa: E402

ALLOWED_MIGRATION_CATEGORIES = {
    "Business Logic",
    "Source Adapter",
    "Sink Adapter",
    "Orchestration / Monitoring",
    "Infrastructure Only",
}


def test_catalog_aliases_resolve_to_known_members() -> None:
    catalog = load_catalog()
    for alias, target in catalog.aliases.items():
        assert catalog.category_for(target) or target in catalog.metadata, (
            f"Alias target '{target}' is not present in catalog categories or metadata"
        )


def test_catalog_metadata_defaults_are_valid() -> None:
    catalog = load_catalog()
    for short_type, metadata in catalog.metadata.items():
        default_category = metadata.get("default_migration_category")
        if default_category is not None:
            assert default_category in ALLOWED_MIGRATION_CATEGORIES, (
                f"Unexpected default migration category '{default_category}' for {short_type}"
            )

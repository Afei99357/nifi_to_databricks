import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from tools.catalog import load_catalog, short_type_from_fqn


def test_catalog_load_and_lookup(tmp_path: Path) -> None:
    catalog = load_catalog()

    assert catalog.category_for("GetFile") == "ingestion"
    assert catalog.category_for("PublishKafka_2_0") == "egress"
    assert catalog.metadata_for("ExecuteScript").get("default_migration_category") == "Business Logic"
    assert catalog.category_for("UnknownProcessor") is None
    assert catalog.is_category("PutFile", "egress")


def test_short_type_from_fully_qualified_name() -> None:
    assert short_type_from_fqn("org.apache.nifi.processors.standard.GetFile") == "GetFile"
    assert short_type_from_fqn("GetFile") == "GetFile"
    assert short_type_from_fqn("") == ""

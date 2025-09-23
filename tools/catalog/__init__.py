"""Utilities for working with the NiFi processor catalog."""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Dict, Optional, Set

import yaml

RESOURCES_DIR = Path(__file__).resolve().parents[1] / "resources"
DEFAULT_CATALOG_PATH = RESOURCES_DIR / "processor_catalog.yaml"


@dataclass(frozen=True)
class ProcessorCatalog:
    """Lookup structure describing known NiFi processor short types."""

    categories: Dict[str, Set[str]]
    aliases: Dict[str, str]
    metadata: Dict[str, Dict[str, object]]

    def resolve_alias(self, short_type: str) -> str:
        """Return normalized short type using alias map."""
        return self.aliases.get(short_type, short_type)

    def category_for(self, short_type: str) -> Optional[str]:
        """Return the catalog category for ``short_type`` if known."""
        normalised = self.resolve_alias(short_type)
        for category, members in self.categories.items():
            if normalised in members:
                return category
        return None

    def metadata_for(self, short_type: str) -> Dict[str, object]:
        """Return metadata for ``short_type`` if present."""
        normalised = self.resolve_alias(short_type)
        return self.metadata.get(normalised, {})

    def is_category(self, short_type: str, category: str) -> bool:
        """Check if ``short_type`` belongs to ``category``."""
        return self.category_for(short_type) == category


def _load_raw_catalog(path: Path) -> Dict[str, object]:
    with path.open("r", encoding="utf-8") as fh:
        payload = yaml.safe_load(fh)
    if not isinstance(payload, dict):  # pragma: no cover - defensive
        raise ValueError(f"Invalid catalog payload in {path}")
    return payload


def _normalise_entries(payload: Dict[str, object]) -> ProcessorCatalog:
    categories_payload = payload.get("categories", {})
    categories: Dict[str, Set[str]] = {}
    for category, members in categories_payload.items():
        categories[category] = {str(item) for item in members or []}

    metadata_payload = payload.get("metadata", {})
    metadata: Dict[str, Dict[str, object]] = {}
    for short_type, meta in metadata_payload.items():
        metadata[short_type] = dict(meta or {})

    alias_payload = payload.get("aliases", {})
    aliases = {str(alias): str(target) for alias, target in alias_payload.items()}

    return ProcessorCatalog(categories=categories, aliases=aliases, metadata=metadata)


@lru_cache(maxsize=1)
def load_catalog(path: Path = DEFAULT_CATALOG_PATH) -> ProcessorCatalog:
    """Load the processor catalog from ``path`` (cached)."""
    raw = _load_raw_catalog(path)
    return _normalise_entries(raw)


def short_type_from_fqn(processor_type: str) -> str:
    """Extract the NiFi short type from a fully-qualified class name."""
    if not processor_type:
        return ""
    return processor_type.split(".")[-1]


__all__ = [
    "ProcessorCatalog",
    "load_catalog",
    "short_type_from_fqn",
]

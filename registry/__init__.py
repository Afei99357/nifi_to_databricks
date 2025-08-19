# registry/__init__.py
# ------------------------------------------------------------------------------
# Registry package init
# Exposes the Unity Catalog–backed PatternRegistryUC class.
# ------------------------------------------------------------------------------

from .pattern_registry import PatternRegistryUC

__all__ = ["PatternRegistryUC"]
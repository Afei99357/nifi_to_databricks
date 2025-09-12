# tools/__init__.py
# Import submodules for direct function access (no agent complexity)

# Import basic tools without external dependencies
from . import (
    simple_table_lineage,
    xml_tools,
)

# Conditionally import tools with external dependencies
try:
    from . import (
        improved_classifier,
        improved_pruning,
        migration_orchestrator,
    )

    # Export main migration functions for easy access
    from .migration_orchestrator import (
        analyze_nifi_workflow_only,
        migrate_nifi_to_databricks_simplified,
    )

    _databricks_tools_available = True
except ImportError:
    _databricks_tools_available = False

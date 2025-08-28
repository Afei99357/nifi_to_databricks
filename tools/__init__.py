# tools/__init__.py
# Collect all @tool objects from submodules and expose:
# - TOOLS: list of tool objects (what the agent needs)
# - Individual tool names at top-level (optional convenience)
#
# Add new submodules to SUBMODULES; no need to list each tool twice.

from langchain_core.tools import BaseTool

# Import submodules explicitly (reliable on Databricks)
from . import (
    analysis_tools,
    chunking_tools,
    dlt_tools,
    eval_tools,
    generator_tools,
    job_tools,
    migration_tools,
    nifi_intelligence,
    xml_tools,
)

SUBMODULES = [
    analysis_tools,
    xml_tools,
    migration_tools,
    job_tools,
    generator_tools,
    dlt_tools,
    eval_tools,
    chunking_tools,
    nifi_intelligence,
]

# Build TOOLS and optionally export names
TOOLS = []
_seen_tools = set()  # Track tool names to avoid duplicates
__all__ = ["TOOLS"]

for m in SUBMODULES:
    for name, obj in vars(m).items():
        if isinstance(obj, BaseTool):
            # Only add if we haven't seen this tool name before
            if obj.name not in _seen_tools:
                _seen_tools.add(obj.name)
                # Re-export tool at package level (optional)
                globals()[name] = obj
                __all__.append(name)
                TOOLS.append(obj)

# tools/__init__.py
# Collect all @tool objects from submodules and expose:
# - TOOLS: list of tool objects (what the agent needs)
# - Individual tool names at top-level (optional convenience)
#
# Add new submodules to SUBMODULES; no need to list each tool twice.

from langchain_core.tools import BaseTool

# Import submodules explicitly (reliable on Databricks)
from . import xml_tools, migration_tools, job_tools, pattern_tools, dlt_tools, eval_tools

SUBMODULES = [xml_tools, migration_tools, job_tools, pattern_tools, dlt_tools, eval_tools]

# Build TOOLS and optionally export names
TOOLS = []
__all__ = ["TOOLS"]

for m in SUBMODULES:
    for name, obj in vars(m).items():
        if isinstance(obj, BaseTool):
            # Re-export tool at package level (optional)
            globals()[name] = obj
            __all__.append(name)
            TOOLS.append(obj)

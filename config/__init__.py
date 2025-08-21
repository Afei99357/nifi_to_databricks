"""
Config package for global settings and environment variables.
"""

from .settings import (
    DATABRICKS_HOSTNAME,
    DATABRICKS_TOKEN,
    MODEL_ENDPOINT,
    NOTIFICATION_EMAIL,
    logger,
)

__all__ = [
    "logger",
    "NOTIFICATION_EMAIL",
    "DATABRICKS_HOSTNAME",
    "DATABRICKS_TOKEN",
    "MODEL_ENDPOINT",
]

"""
Config package for global settings and environment variables.
"""

from .settings import (
    logger,
    NOTIFICATION_EMAIL,
    DATABRICKS_HOSTNAME,
    DATABRICKS_TOKEN,
    MODEL_ENDPOINT,
)

__all__ = [
    "logger",
    "NOTIFICATION_EMAIL",
    "DATABRICKS_HOSTNAME",
    "DATABRICKS_TOKEN",
    "MODEL_ENDPOINT",
]

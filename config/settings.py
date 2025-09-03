import logging
import os
from pathlib import Path

try:
    from dotenv import load_dotenv

    DOTENV_AVAILABLE = True
except ImportError:
    DOTENV_AVAILABLE = False

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load .env if present and dotenv available
if DOTENV_AVAILABLE:
    env_path = Path(__file__).parent.parent / ".env"
    if env_path.exists():
        load_dotenv(env_path)
        logger.info(f"Loaded .env from {env_path}")
    else:
        logger.info("No .env file found - using environment variables")
else:
    logger.info("python-dotenv not available - using environment variables")

# Env variables
NOTIFICATION_EMAIL = os.getenv("NOTIFICATION_EMAIL")
DATABRICKS_HOSTNAME = os.getenv("DATABRICKS_HOSTNAME")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
MODEL_ENDPOINT = os.getenv("MODEL_ENDPOINT", "databricks-meta-llama-3-3-70b-instruct")

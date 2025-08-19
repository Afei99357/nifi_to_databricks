import os, logging
from pathlib import Path
from dotenv import load_dotenv

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load .env if present
env_path = (Path(__file__).parent.parent / ".env")
if env_path.exists():
    load_dotenv(env_path)
    logger.info(f"Loaded .env from {env_path}")
else:
    logger.info("No .env file found")

# Env variables
NOTIFICATION_EMAIL = os.getenv("NOTIFICATION_EMAIL")
DATABRICKS_HOSTNAME = os.getenv("DATABRICKS_HOSTNAME")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
MODEL_ENDPOINT = os.getenv("MODEL_ENDPOINT", "databricks-meta-llama-3-3-70b-instruct")

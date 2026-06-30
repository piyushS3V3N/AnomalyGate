import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


class Config:
    PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    DATA_FILE = os.path.join(PROJECT_ROOT, "sample_logs.json")
    MODEL_PATH = os.path.join(PROJECT_ROOT, "models", "log_filter_rf_model")

    KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
    KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "raw-security-logs")
    KAFKA_OUTPUT_TOPIC = os.getenv("KAFKA_OUTPUT_TOPIC", "siem-critical-logs")

    ES_HOST = os.getenv("ES_HOST", "localhost")
    ES_PORT = os.getenv("ES_PORT", "9200")
    ES_INDEX = os.getenv("ES_INDEX", "siem-critical-logs")

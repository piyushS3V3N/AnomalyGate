import os

class Config:
    PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    DATA_FILE = os.path.join(PROJECT_ROOT, "sample_logs.json")
    MODEL_PATH = os.path.join(PROJECT_ROOT, "models", "log_filter_rf_model")
    
    KAFKA_BROKER = "localhost:9092"
    KAFKA_TOPIC = "raw-security-logs"
    
    ES_HOST = "localhost"
    ES_PORT = "9200"
    ES_INDEX = "siem-critical-logs"

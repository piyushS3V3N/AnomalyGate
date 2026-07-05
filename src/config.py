import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Fix PySpark compatibility with Java 16+ (jdk.internal.ref.Cleaner ClassNotFoundException)
java_opens = (
    "-XX:+IgnoreUnrecognizedVMOptions "
    "--add-opens=java.base/java.lang=ALL-UNNAMED "
    "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED "
    "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED "
    "--add-opens=java.base/java.io=ALL-UNNAMED "
    "--add-opens=java.base/java.net=ALL-UNNAMED "
    "--add-opens=java.base/java.nio=ALL-UNNAMED "
    "--add-opens=java.base/java.util=ALL-UNNAMED "
    "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED "
    "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED "
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
    "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED "
    "--add-opens=java.base/sun.security.action=ALL-UNNAMED "
    "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED "
    "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"
)
os.environ["JDK_JAVA_OPTIONS"] = os.environ.get("JDK_JAVA_OPTIONS", "") + " " + java_opens


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

import json
from kafka import KafkaConsumer
from src.config import Config
from src.utils.logger import get_logger

logger = get_logger("AnomalyGate.AlertWorker")

def start_alerting_worker():
    logger.info(f"Connecting to SIEM security topic: {Config.KAFKA_OUTPUT_TOPIC}...")
    
    # 1. Initialize the Kafka Consumer targeting the filtered output stream
    consumer = KafkaConsumer(
        Config.KAFKA_OUTPUT_TOPIC,
        bootstrap_servers=[Config.KAFKA_BROKER],
        auto_offset_reset='latest',  # Listen for brand new anomalies
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    logger.info("Alert Worker successfully initialized. Listening for threats...")
    print("-" * 80)
    print(" >>> REAL-TIME ANOMALYGATE ALERT ENGINE ACTIVE <<< ")
    print("-" * 80)

    # 2. Infinite Loop: Wait and catch live logs flagged by your PySpark system
    for record in consumer:
        payload = record.value
        
        # Pull key properties out of the Spring Boot Logstash formatting structure
        timestamp = payload.get("@timestamp")
        level = payload.get("level", "UNKNOWN")
        logger_name = payload.get("logger", "UNKNOWN")
        message = payload.get("message", "")
        context = payload.get("context", {})
        client_ip = context.get("client_ip", "UNKNOWN")
        
        # 3. Format visual terminal alerts depending on the threat severity
        print(f"\n🚨 [ALERT] Anomaly Detected at {timestamp}")
        print(f"   ├─ Severity Level:  {level}")
        print(f"   ├─ Attacker Source: {client_ip}")
        print(f"   ├─ Java Class Hook: {logger_name}")
        print(f"   └─ Raw Payload Msg: {message}")
        print("-" * 50)

if __name__ == "__main__":
    start_alerting_worker()

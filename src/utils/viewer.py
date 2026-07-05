import json
import logging
from kafka import KafkaConsumer

def run_viewer(topic="siem-critical-logs", bootstrap_servers="localhost:9092"):
    print(f"[*] Starting AnomalyGate Live Viewer...")
    print(f"[*] Listening to Kafka topic: {topic}")
    print(f"{'-'*80}")
    
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        for message in consumer:
            log = message.value
            context = log.get('context', {})
            
            level = log.get('level', 'UNKNOWN')
            logger_name = log.get('logger', 'UNKNOWN').split('.')[-1]
            message = log.get('message', 'UNKNOWN')
            client_ip = context.get('client_ip', 'UNKNOWN')
            bytes_sent = context.get('bytes_sent', 0)
            anomaly_score = log.get('anomaly_score', 0.0)
            
            # Heuristic to explain the ML model's decision
            threat_reason = "Anomalous Traffic Pattern (Unsupervised Cluster)"
            if "status 401" in message or "Login attempt" in message:
                threat_reason = "Brute Force / Credential Stuffing Attack"
            elif "SQL" in message or "UNION" in message or "SELECT" in message:
                threat_reason = "SQL Injection (SQLi) Attempt"
            elif bytes_sent > 1000000:
                threat_reason = "Suspicious Data Exfiltration (Large Payload)"
            elif "denied" in message.lower() or "blocked" in message.lower():
                threat_reason = "Probing / Reconnaissance Activity"
            
            # Use ANSI escape codes for red coloring on the threat reason
            RED = '\033[91m'
            BOLD = '\033[1m'
            RESET = '\033[0m'
            
            # Format nicely for the terminal
            print(f"{RED}{BOLD}[CRITICAL ALERT] Threat: {threat_reason}{RESET}")
            print(f"    Mathematical Anomaly Score: {anomaly_score:.2f} (Threshold crossed)")
            print(f"    Target IP: {client_ip:<15} | Size: {bytes_sent:>8}B | Source: {logger_name}")
            print(f"    Raw Log:   {message}")
            print(f"{'-'*80}")
    except KeyboardInterrupt:
        print("\n[*] Exiting live viewer...")
    except Exception as e:
        print(f"[!] Error connecting to Kafka: {e}")

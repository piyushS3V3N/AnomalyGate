import json
import random
from datetime import datetime
from src.config import Config

def generate_log():
    is_noise = random.random() < 0.90
    
    if is_noise:
        log = {
            "timestamp": datetime.now().isoformat(),
            "source_ip": f"192.168.1.{random.randint(1, 255)}",
            "event_type": "Network Traffic",
            "action": "allowed",
            "bytes_transferred": random.randint(100, 5000),
            "severity": "low",
            "is_anomaly": 0
        }
    else:
        log = {
            "timestamp": datetime.now().isoformat(),
            "source_ip": f"{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}",
            "event_type": random.choice(["Failed Login", "Port Scan", "SQL Injection Attempt"]),
            "action": "blocked",
            "bytes_transferred": random.randint(5000, 100000),
            "severity": random.choice(["medium", "high", "critical"]),
            "is_anomaly": 1
        }
    return log

def run_generation(num_logs: int = 10000):
    with open(Config.DATA_FILE, "w") as f:
        for _ in range(num_logs):
            log = generate_log()
            f.write(json.dumps(log) + "\n")
    print(f"Generated {num_logs} sample logs in {Config.DATA_FILE}")

import json
import random
from datetime import datetime
from src.config import Config


def generate_log():
    # 90% benign traffic, 10% anomalies/attacks
    is_anomaly = random.random() < 0.10

    if not is_anomaly:
        # Benign Profiles
        profile = random.choice(["web_browsing", "user_login", "system_check"])
        if profile == "web_browsing":
            action = "allowed" if random.random() < 0.95 else "denied"
            bytes_transferred = random.randint(200, 8000)
            severity = "low"
            event_type = random.choice(["Network Traffic", "API Call"])
        elif profile == "user_login":
            action = "allowed" if random.random() < 0.85 else "denied"
            bytes_transferred = random.randint(50, 300)
            # Failed logins have medium severity, successful have low severity
            severity = "low" if action == "allowed" else "medium"
            event_type = "User Login"
        else:  # system_check / file_access
            action = "allowed" if random.random() < 0.99 else "denied"
            bytes_transferred = random.randint(1000, 25000)
            severity = "low"
            event_type = random.choice(["System Check", "File Access"])

        # Private IP space
        if random.random() < 0.8:
            source_ip = f"192.168.1.{random.randint(1, 255)}"
        else:
            source_ip = f"10.0.{random.randint(1, 254)}.{random.randint(1, 254)}"
        is_anomaly_val = 0
    else:
        # Anomaly / Attack Profiles
        profile = random.choice(
            ["sql_injection", "brute_force", "data_exfil", "port_scan"]
        )
        if profile == "sql_injection":
            action = "blocked" if random.random() < 0.70 else "allowed"
            bytes_transferred = random.randint(15000, 85000)
            severity = random.choice(["high", "critical"])
            event_type = random.choice(["API Call", "SQL Injection Attempt"])
        elif profile == "brute_force":
            action = "denied" if random.random() < 0.90 else "allowed"
            bytes_transferred = random.randint(100, 1200)
            severity = "high"
            event_type = "Failed Login"
        elif profile == "data_exfil":
            action = "allowed"  # undetected data exfiltration
            bytes_transferred = random.randint(500000, 10000000)
            severity = "high"
            event_type = "File Access"
        else:  # port_scan
            action = "denied" if random.random() < 0.80 else "blocked"
            bytes_transferred = random.randint(0, 150)
            severity = "medium"
            event_type = "Port Scan"

        # External IP space
        source_ip = (
            f"{random.randint(1, 255)}.{random.randint(1, 255)}."
            f"{random.randint(1, 255)}.{random.randint(1, 255)}"
        )
        is_anomaly_val = 1

    return {
        "timestamp": datetime.now().isoformat(),
        "source_ip": source_ip,
        "event_type": event_type,
        "action": action,
        "bytes_transferred": bytes_transferred,
        "severity": severity,
        "is_anomaly": is_anomaly_val,
    }


def run_generation(num_logs: int = 10000):
    with open(Config.DATA_FILE, "w") as f:
        for _ in range(num_logs):
            log = generate_log()
            f.write(json.dumps(log) + "\n")
    print(f"Generated {num_logs} sample logs in {Config.DATA_FILE}")

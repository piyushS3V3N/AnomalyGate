import json
import random
from datetime import datetime
from src.config import Config

# Real Java classes common in enterprise Spring Boot applications
SPRING_LOGGERS = {
    "web_browsing": "org.springframework.web.servlet.DispatcherServlet",
    "user_login": "org.springframework.security.web.FilterChainProxy",
    "system_check": "org.springframework.boot.actuate.health.HealthEndpoint",
    "sql_injection": "org.hibernate.engine.jdbc.spi.SqlStatementLogger",
    "data_exfil": "com.example.gateway.service.FileDownloadService",
    "port_scan": "org.apache.catalina.core.StandardWrapperValve"
}

def generate_log():
    """Generates a pure Spring Boot log and a separate evaluation flag."""
    is_anomaly = random.random() < 0.10
    thread_id = f"http-nio-8080-exec-{random.randint(1, 10)}"
    timestamp = datetime.now().isoformat()

    if not is_anomaly:
        # --- BENIGN PROFILES ---
        profile = random.choice(["web_browsing", "user_login", "system_check"])
        logger = SPRING_LOGGERS[profile]
        
        if profile == "web_browsing":
            status = 200 if random.random() < 0.95 else 403
            level = "INFO" if status == 200 else "WARN"
            bytes_sent = random.randint(200, 8000)
            message = f"GET /api/v1/assets/resource HTTP/1.1 - Response status {status}"
        elif profile == "user_login":
            status = 200 if random.random() < 0.85 else 401
            level = "INFO" if status == 200 else "WARN"
            bytes_sent = random.randint(50, 300)
            message = f"POST /login HTTP/1.1 - Login attempt status {status}"
        else:  # system_check
            level = "INFO"
            bytes_sent = random.randint(1000, 25000)
            message = f"GET /actuator/health HTTP/1.1 - Components status UP"

        # Private IP space
        if random.random() < 0.8:
            source_ip = f"192.168.1.{random.randint(1, 255)}"
        else:
            source_ip = f"10.0.{random.randint(1, 254)}.{random.randint(1, 254)}"
        is_anomaly_val = 0
    
    else:
        # --- ANOMALY / ATTACK PROFILES ---
        profile = random.choice(["sql_injection", "brute_force", "data_exfil", "port_scan"])
        logger = SPRING_LOGGERS.get(profile, "org.springframework.web.Log")
        
        if profile == "sql_injection":
            level = "ERROR"
            bytes_sent = random.randint(15000, 85000)
            # Realistic SQL pattern inside raw URL query parameter
            message = "GET /api/users?id=1%20OR%201=1%20UNION%20SELECT%20null,username,password%20FROM%20users HTTP/1.1 - SQLException Failure"
        elif profile == "brute_force":
            level = "WARN"
            bytes_sent = random.randint(100, 1200)
            message = "POST /login HTTP/1.1 - Authentication failed - Bad credentials for principal: admin"
        elif profile == "data_exfil":
            level = "INFO"  # High-volume download hiding as clean traffic
            bytes_sent = random.randint(500000, 10000000)
            message = f"GET /api/v1/reports/download-all HTTP/1.1 - Streaming payload chunks"
        else:  # port_scan
            level = "WARN"
            bytes_sent = random.randint(0, 150)
            message = "Invalid request format: raw connection dropped or handshake timed out"

        # External IP space
        source_ip = f"{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}"
        is_anomaly_val = 1

    # Standard corporate Logstash Logback JSON layout
    pure_spring_log = {
        "@timestamp": timestamp,
        "level": level,
        "thread": thread_id,
        "logger": logger,
        "message": message,
        "context": {
            "client_ip": source_ip,
            "bytes_sent": bytes_sent
        }
    }

    # We return the log and the tag separately! The tag NEVER touches the production JSON payload.
    return pure_spring_log, is_anomaly_val


def run_generation(num_logs: int = 10000):
    # Optional: You can save evaluation tags to a hidden file if your test scripts need to check accuracy later
    labels_file = Config.DATA_FILE.replace(".json", "_labels.txt")
    
    with open(Config.DATA_FILE, "w") as f_log, open(labels_file, "w") as f_label:
        for _ in range(num_logs):
            log_payload, is_anomaly_val = generate_log()
            
            # Write out pure, unlabeled Spring Boot data
            f_log.write(json.dumps(log_payload) + "\n")
            # Write label sidecar record separately
            f_label.write(f"{is_anomaly_val}\n")
            
    print(f"Generated {num_logs} pure Spring Boot logs in {Config.DATA_FILE}")
    print(f"Generated validation labels in {labels_file}")

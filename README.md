# AnomalyGate (Automated Security Log Noise Filters for SIEM)

This project provides a pre-ingestion pipeline built with PySpark and ML to filter out benign background noise from high-risk anomalies, routing only critical data to a SIEM (like Splunk or Datadog) to save on ingestion costs.

## Concept
Enterprise SIEMs charge by data volume, and 90% of logs are often benign noise. This pipeline acts as a tollbooth:
1. **Ingests** millions of raw system logs (e.g., via Kafka).
2. **Processes** them using PySpark.
3. **Classifies** them using a lightweight ML model (Random Forest) into "Noise" or "Critical".
4. **Routes** only "Critical" logs to the SIEM.

## Setup & Running

1. **Start Kafka & Elasticsearch Infrastructure**:
   You must start the Docker containers first so that PySpark can connect to the Kafka broker.
   ```bash
   docker compose up -d
   ```

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Generate Sample Data**:
   Generates a JSON file with 10,000 logs (~90% noise, 10% anomalies).
   ```bash
   python main.py generate 10000
   ```

4. **Train the ML Model**:
   Trains a PySpark ML Pipeline model and saves it to the `models/` directory.
   ```bash
   python main.py train
   ```

5. **Publish Sample Data to Kafka**:
   Since the streaming pipeline reads from Kafka, you can use the Kafka CLI to pipe the generated logs into the topic:
   ```bash
   docker exec -i aslnfs-kafka-1 kafka-console-producer --broker-list localhost:9092 --topic raw-security-logs < sample_logs.json
   ```
   *(Note: Adjust the container name `aslnfs-kafka-1` if your Docker Compose project name is different.)*

6. **Run the Filtering Pipeline**:
   Simulates streaming the logs, applying the ML model, and filtering out the noise.
   ```bash
   python main.py run
   ```

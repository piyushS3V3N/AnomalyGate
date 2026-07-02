import os
import json
import numpy as np
import pyspark
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.sql.functions import col, from_json, length, udf, when, window, count, avg, max
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from src.config import Config
from src.utils.logger import get_logger

logger = get_logger("AnomalyGate.Stream")


def ensure_kafka_topics_exist(spark, bootstrap_servers, topics):
    logger.info("Checking if Kafka topics exist...")
    try:
        jvm = spark.sparkContext._gateway.jvm
        props = jvm.java.util.Properties()
        props.put("bootstrap.servers", bootstrap_servers)
        admin_client = jvm.org.apache.kafka.clients.admin.AdminClient.create(props)
        try:
            existing_topics = admin_client.listTopics().names().get()
            topics_to_create = []
            for topic in topics:
                if not existing_topics.contains(topic):
                    logger.info(f"Topic '{topic}' does not exist. Creating it...")
                    rep_factor = jvm.java.lang.Short.valueOf(1)
                    new_topic = jvm.org.apache.kafka.clients.admin.NewTopic(
                        topic, 1, rep_factor
                    )
                    topics_to_create.append(new_topic)

            if topics_to_create:
                java_list = jvm.java.util.ArrayList()
                for nt in topics_to_create:
                    java_list.add(nt)
                admin_client.createTopics(java_list).all().get()
                logger.info("Kafka topics verified/created successfully.")
            else:
                logger.info("All required Kafka topics already exist.")
        finally:
            admin_client.close()
    except Exception as e:
        logger.warning(
            f"Could not automatically verify/create Kafka topics via AdminClient: {e}. "
            "Ensure they exist in the Kafka broker manually."
        )


def run_pipeline(once=False):
    spark_version = pyspark.__version__
    scala_version = "2.13" if int(spark_version.split(".")[0]) >= 4 else "2.12"
    kafka_pkg = f"org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}"

    spark = (
        SparkSession.builder.appName("AnomalyGate_Pipeline")
        .config("spark.jars.packages", kafka_pkg)
        .getOrCreate()
    )

    ensure_kafka_topics_exist(
        spark, Config.KAFKA_BROKER, [Config.KAFKA_TOPIC, Config.KAFKA_OUTPUT_TOPIC]
    )

    logger.info(f"Loading pre-trained ML model from {Config.MODEL_PATH}...")
    try:
        model = PipelineModel.load(Config.MODEL_PATH)
    except Exception as e:
        logger.error(f"Failed to load model. Did you run 'make train'? Error: {e}")
        return

    # Schema definition for raw JSON strings from Kafka
    schema = StructType(
        [
            StructField("@timestamp", StringType(), True),
            StructField("level", StringType(), True),
            StructField("thread", StringType(), True),
            StructField("logger", StringType(), True),
            StructField("message", StringType(), True),
            StructField(
                "context", 
                StructType([
                    StructField("client_ip", StringType(), True),
                    StructField("bytes_sent", IntegerType(), True)
                ]), 
                True
            )
        ]
    )

    logger.info(
        f"Connecting to Kafka stream at {Config.KAFKA_BROKER} on topic {Config.KAFKA_TOPIC}..."
    )
    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", Config.KAFKA_BROKER)
        .option("subscribe", Config.KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # 1. Parse raw bytes into string values
    parsed_json_df = (
        kafka_df.selectExpr("CAST(value AS STRING) as json_string")
        .select(col("json_string"), from_json(col("json_string"), schema).alias("data"))
        .select("json_string", "data.*")
    )

    # 2. Add baseline metrics and convert string timestamp to true event_time
    base_df = parsed_json_df.withColumn("bytes_sent", col("context.bytes_sent").cast("double")) \
                            .withColumn("msg_len", length(col("message")).cast("double")) \
                            .withColumn("event_time", col("@timestamp").cast("timestamp"))

    # 3. LIVE WINDOW AGGREGATION
    # Groups data into 10-second blocks, checking for sudden traffic bursts or volume spikes per IP
    logger.info("Initializing 10-second traffic tracking windows...")
    parsed_df = base_df \
        .withWatermark("event_time", "1 minute") \
        .groupBy(
            window(col("event_time"), "10 seconds", "5 seconds"), # 10s windows, sliding every 5s
            col("context.client_ip").alias("client_ip"),
            col("level")
        ).agg(
            count("message").alias("log_count"),
            avg("bytes_sent").alias("avg_bytes_transferred"),
            max("msg_len").alias("max_message_length"),
            max("json_string").alias("json_string"), # Pass along one raw string for SIEM output
            max("@timestamp").alias("@timestamp")     # Keep latest timestamp value
        )

    # Run the unsupervised ML model on our aggregated metrics
    clustered_stream_df = model.transform(parsed_df)

    # Extract cluster centers to calculate live mathematical distance deviations
    kmeans_stage = model.stages[-1]
    cluster_centers = kmeans_stage.clusterCenters()

    def calculate_live_distance(features, cluster_prediction):
        center = cluster_centers[cluster_prediction]
        return float(np.linalg.norm(np.array(features.toArray()) - np.array(center)))

    distance_udf = udf(calculate_live_distance, DoubleType())

    # Safely load the static boundary threshold computed during model training
    threshold_path = Config.MODEL_PATH + "_threshold.txt"
    try:
        with open(threshold_path, "r") as f:
            content = f.read().strip()
            anomaly_threshold = float(content) if content else 3.5
    except Exception:
        logger.warning(f"Could not load threshold from {threshold_path}. Using fallback default (3.5).")
        anomaly_threshold = 3.5
    
    logger.info(f"Applying real-time anomaly distance threshold: {anomaly_threshold}")

    # Calculate distance and flag windows that cross our threshold boundary line
    stream_with_distances = clustered_stream_df.withColumn(
        "distance_from_center", 
        distance_udf(col("features"), col("cluster_prediction"))
    )

    predictions = stream_with_distances.withColumn(
        "prediction",
        when(col("distance_from_center") > anomaly_threshold, 1.0).otherwise(0.0)
    )

    # Filter out normal groups, keeping only structural threats
    filtered_df = predictions.filter(col("prediction") == 1.0)

    # Route original raw JSON to preserve all fields for downstream SIEM parsing
    siem_payload_df = filtered_df.select(
        col("@timestamp").alias("key"), col("json_string").alias("value")
    )

    logger.info(f"Routing critical logs to SIEM topic: {Config.KAFKA_OUTPUT_TOPIC}...")

    checkpoint_dir = os.path.join(Config.PROJECT_ROOT, "checkpoints", "kafka_sink")

    write_query = (
        siem_payload_df.writeStream.format("kafka")
        .option("kafka.bootstrap.servers", Config.KAFKA_BROKER)
        .option("topic", Config.KAFKA_OUTPUT_TOPIC)
        .option("checkpointLocation", checkpoint_dir)
        .outputMode("update") 
    )


    if once:
        logger.info("Processing currently available data and exiting...")
        query = write_query.trigger(availableNow=True).start()
    else:
        logger.info("-" * 80)
        logger.info("Stream processing started successfully and is now running in the foreground.")
        logger.info("It is actively listening for new logs on 'raw-security-logs' topic.")
        logger.info("To simulate streaming logs while this runs, open a new terminal and run: make feed")
        logger.info("To stop the streaming job, press Ctrl+C.")
        logger.info("-" * 80)
        query = write_query.start()

    query.awaitTermination()

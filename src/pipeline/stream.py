from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from src.config import Config
from src.utils.logger import get_logger
import os

import pyspark

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

    # Ensure Kafka topics exist before starting the stream
    ensure_kafka_topics_exist(
        spark, Config.KAFKA_BROKER, [Config.KAFKA_TOPIC, Config.KAFKA_OUTPUT_TOPIC]
    )

    logger.info(f"Loading pre-trained ML model from {Config.MODEL_PATH}...")
    try:
        model = PipelineModel.load(Config.MODEL_PATH)
    except Exception as e:
        logger.error(f"Failed to load model. Did you run 'make train'? Error: {e}")
        return

    schema = StructType(
        [
            StructField("timestamp", StringType(), True),
            StructField("source_ip", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("action", StringType(), True),
            StructField("bytes_transferred", IntegerType(), True),
            StructField("severity", StringType(), True),
        ]
    )

    logger.info(
        f"Connecting to Kafka stream at {Config.KAFKA_BROKER} "
        f"on topic {Config.KAFKA_TOPIC}..."
    )
    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", Config.KAFKA_BROKER)
        .option("subscribe", Config.KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed_df = (
        kafka_df.selectExpr("CAST(value AS STRING) as json_string")
        .select(col("json_string"), from_json(col("json_string"), schema).alias("data"))
        .select("json_string", "data.*")
    )

    # Run the ML model to filter out noise
    predictions = model.transform(parsed_df)
    filtered_df = predictions.filter(col("prediction") == 1.0)

    # Route original raw JSON to preserve all fields for downstream SIEM parsing
    siem_payload_df = filtered_df.select(
        col("timestamp").alias("key"), col("json_string").alias("value")
    )

    logger.info(f"Routing critical logs to SIEM topic: {Config.KAFKA_OUTPUT_TOPIC}...")

    # Production Sink: Write to Output Kafka Topic
    checkpoint_dir = os.path.join(Config.PROJECT_ROOT, "checkpoints", "kafka_sink")

    write_query = (
        siem_payload_df.writeStream.format("kafka")
        .option("kafka.bootstrap.servers", Config.KAFKA_BROKER)
        .option("topic", Config.KAFKA_OUTPUT_TOPIC)
        .option("checkpointLocation", checkpoint_dir)
    )

    if once:
        logger.info("Processing currently available data and exiting...")
        query = write_query.trigger(availableNow=True).start()
    else:
        logger.info("-" * 80)
        logger.info(
            "Stream processing started successfully and is now running in the "
            "foreground."
        )
        logger.info(
            "It is actively listening for new logs on 'raw-security-logs' " "topic."
        )
        logger.info(
            "To simulate streaming logs while this runs, open a new terminal "
            "and run:"
        )
        logger.info("  make feed")
        logger.info("To stop the streaming job, press Ctrl+C.")
        logger.info("-" * 80)
        query = write_query.start()

    query.awaitTermination()

from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.sql.functions import col, from_json, to_json, struct
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from src.config import Config
from src.utils.logger import get_logger
import os

import pyspark

logger = get_logger("AnomalyGate.Stream")

def run_pipeline():
    spark_version = pyspark.__version__
    scala_version = "2.13" if int(spark_version.split('.')[0]) >= 4 else "2.12"
    kafka_pkg = f"org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}"

    spark = SparkSession.builder \
        .appName("AnomalyGate_Pipeline") \
        .config("spark.jars.packages", kafka_pkg) \
        .getOrCreate()
        
    logger.info(f"Loading pre-trained ML model from {Config.MODEL_PATH}...")
    try:
        model = PipelineModel.load(Config.MODEL_PATH)
    except Exception as e:
        logger.error(f"Failed to load model. Did you run 'make train'? Error: {e}")
        return

    schema = StructType([
        StructField("timestamp", StringType(), True),
        StructField("source_ip", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("action", StringType(), True),
        StructField("bytes_transferred", IntegerType(), True),
        StructField("severity", StringType(), True)
    ])

    logger.info(f"Connecting to Kafka stream at {Config.KAFKA_BROKER} on topic {Config.KAFKA_TOPIC}...")
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", Config.KAFKA_BROKER) \
        .option("subscribe", Config.KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()
        
    parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as json_string") \
        .select(from_json(col("json_string"), schema).alias("data")) \
        .select("data.*")
    
    # Run the ML model to filter out noise
    predictions = model.transform(parsed_df)
    filtered_df = predictions.filter(col("prediction") == 1.0)
    
    siem_payload_df = filtered_df.select(
        "timestamp", "source_ip", "event_type", "action", "bytes_transferred", "severity"
    )
    
    logger.info(f"Routing critical logs to SIEM topic: {Config.KAFKA_OUTPUT_TOPIC}...")
    
    # Production Sink: Write to Output Kafka Topic
    checkpoint_dir = os.path.join(Config.PROJECT_ROOT, "checkpoints", "kafka_sink")
    
    query = siem_payload_df.select(
        col("timestamp").alias("key"),
        to_json(struct("*")).alias("value")
    ) \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", Config.KAFKA_BROKER) \
    .option("topic", Config.KAFKA_OUTPUT_TOPIC) \
    .option("checkpointLocation", checkpoint_dir) \
    .start()

    logger.info("Stream processing started. Awaiting termination...")
    query.awaitTermination()

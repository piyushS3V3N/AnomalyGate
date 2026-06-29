from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from src.config import Config

import pyspark

def run_pipeline():
    spark_version = pyspark.__version__
    scala_version = "2.13" if int(spark_version.split('.')[0]) >= 4 else "2.12"
    kafka_pkg = f"org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}"

    spark = SparkSession.builder \
        .appName("SecurityLogNoiseFilter_Pipeline") \
        .config("spark.jars.packages", kafka_pkg) \
        .getOrCreate()
        
    print(f"Loading pre-trained ML model from {Config.MODEL_PATH}...")
    try:
        model = PipelineModel.load(Config.MODEL_PATH)
    except Exception as e:
        print(f"Failed to load model. Error: {e}")
        return

    schema = StructType([
        StructField("timestamp", StringType(), True),
        StructField("source_ip", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("action", StringType(), True),
        StructField("bytes_transferred", IntegerType(), True),
        StructField("severity", StringType(), True)
    ])

    print(f"Connecting to Kafka stream at {Config.KAFKA_BROKER} on topic {Config.KAFKA_TOPIC}...")
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", Config.KAFKA_BROKER) \
        .option("subscribe", Config.KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()
        
    parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as json_string") \
        .select(from_json(col("json_string"), schema).alias("data")) \
        .select("data.*")
    
    predictions = model.transform(parsed_df)
    filtered_df = predictions.filter(col("prediction") == 1.0)
    
    siem_payload_df = filtered_df.select(
        "timestamp", "source_ip", "event_type", "action", "bytes_transferred", "severity"
    )
    
    print(f"Starting stream to output... (Skipping direct ES Connector due to Scala ClassLoader conflicts)")
    
    # Writing to Console for local verification
    query = siem_payload_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .start()
        
    # In production, to avoid Elasticsearch JAR conflicts inside PySpark, 
    # it is best practice to output to a new Kafka topic and use Logstash or Kafka Connect to sink to ES.
    # kafka_query = siem_payload_df.selectExpr("CAST(timestamp AS STRING) AS key", "to_json(struct(*)) AS value") \
    #    .writeStream \
    #    .format("kafka") \
    #    .option("kafka.bootstrap.servers", Config.KAFKA_BROKER) \
    #    .option("topic", "siem-critical-logs") \
    #    .option("checkpointLocation", "/tmp/spark_checkpoints/kafka_sink") \
    #    .start()

    query.awaitTermination()

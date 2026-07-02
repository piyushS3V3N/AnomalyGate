import os
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length, window, count, avg, max, monotonically_increasing_id, when, udf
from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql.types import DoubleType
from src.config import Config
from src.utils.logger import get_logger

logger = get_logger("AnomalyGate.Trainer")


def train_model():
    # 1. Initialize Spark Session optimized for data aggregations
    spark = SparkSession.builder \
        .appName("TrainLogNoiseFilter") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()

    labels_file = Config.DATA_FILE.replace(".json", "_labels.txt")

    logger.info(f"Loading raw Spring Boot data from {Config.DATA_FILE}...")
    if not os.path.exists(Config.DATA_FILE) or not os.path.exists(labels_file):
        logger.error("Data file or validation labels sidecar missing. Generate data first.")
        spark.stop()
        return

    # 2. Ingest data and join with labels immediately before windowing
    raw_df = spark.read.json(Config.DATA_FILE)
    labels_raw_df = spark.read.text(labels_file).withColumnRenamed("value", "is_anomaly_str")
    
    labels_df = labels_raw_df.withColumn("is_anomaly_label", col("is_anomaly_str").cast("double")) \
                             .withColumn("label_id", monotonically_increasing_id())
                             
    raw_indexed_df = raw_df.withColumn("raw_id", monotonically_increasing_id())
    full_labeled_df = raw_indexed_df.join(labels_df, col("raw_id") == col("label_id")).drop("raw_id", "label_id", "is_anomaly_str")

    # 3. FEATURE ENGINEERING & TIME WINDOW AGGREGATION
    # Cast timestamp strings to true dates so Spark can use time-series windows
    time_df = full_labeled_df.withColumn("event_time", col("@timestamp").cast("timestamp")) \
                             .withColumn("msg_len", length(col("message")).cast("double")) \
                             .withColumn("bytes_sent", col("context.bytes_sent").cast("double"))

    logger.info("Aggregating traffic logs into 10-second behavioral windows per IP...")
    
    # Group logs by IP and window blocks to measure velocity metrics [3]
    windowed_df = time_df.groupBy(
        window(col("event_time"), "10 seconds"),
        col("context.client_ip").alias("client_ip"),
        col("level")
    ).agg(
        count("message").alias("log_count"),                # Total requests in 10 seconds
        avg("bytes_sent").alias("avg_bytes_transferred"),   # Average payload size
        max("msg_len").alias("max_message_length"),         # Longest log text (catches SQL injection)
        max("is_anomaly_label").alias("is_anomaly")         # Flag window as an anomaly if any attack occurred inside it
    )

    # 4. TRANSFORMATION PIPELINE
    indexer_level = StringIndexer(inputCol="level", outputCol="level_idx", handleInvalid="skip")

    # Feed the structural velocity metrics directly into the assembler matrix
    assembler = VectorAssembler(
        inputCols=["level_idx", "log_count", "avg_bytes_transferred", "max_message_length"],
        outputCol="raw_features",
    )

    # Normalize feature distributions so 'avg_bytes_transferred' doesn't override 'log_count'
    scaler = StandardScaler(inputCol="raw_features", outputCol="features", withStd=True, withMean=False)

    # KMeans builds behavior groups. Normal windows go to one group; fast attacks form their own clusters.
    kmeans = KMeans(featuresCol="features", predictionCol="cluster_prediction", k=4, seed=42)

    pipeline = Pipeline(stages=[indexer_level, assembler, scaler, kmeans])

    logger.info("Training session-aware Unsupervised Clustering Model...")
    model = pipeline.fit(windowed_df)

    # 5. ANOMALY DETECTION THRESHOLD LOGIC
    kmeans_stage = model.stages[-1]
    cluster_centers = kmeans_stage.clusterCenters()

    def calculate_distance(features, cluster_id):
        center = cluster_centers[cluster_id]
        return float(np.linalg.norm(np.array(features.toArray()) - np.array(center)))

    distance_udf = udf(calculate_distance, DoubleType())
    
    clustered_df = model.transform(windowed_df)
    df_with_distances = clustered_df.withColumn(
        "distance_from_center", 
        distance_udf(col("features"), col("cluster_prediction"))
    )

    # Dynamically extract the 90th percentile boundary line
    threshold_value = df_with_distances.approxQuantile("distance_from_center", [0.90], 0.01)[0]
    logger.info(f"Calculated Dynamic Session Anomaly Cutoff Distance: {threshold_value:.4f}")

    # Flag records crossing our mathematical limit boundary
    final_predictions_df = df_with_distances.withColumn(
        "prediction", 
        when(col("distance_from_center") > threshold_value, 1.0).otherwise(0.0)
    )

    # 6. EVALUATION
    evaluator_acc = MulticlassClassificationEvaluator(labelCol="is_anomaly", predictionCol="prediction", metricName="accuracy")
    evaluator_f1 = MulticlassClassificationEvaluator(labelCol="is_anomaly", predictionCol="prediction", metricName="f1")

    accuracy = evaluator_acc.evaluate(final_predictions_df)
    f1_score = evaluator_f1.evaluate(final_predictions_df)

    logger.info(f"Model Evaluation -> Accuracy: {accuracy:.4f}, F1-Score: {f1_score:.4f}")

    # 7. EXPORT MODEL AND THRESHOLD METADATA
    logger.info(f"Saving unsupervised session pipeline to {Config.MODEL_PATH}...")
    model.write().overwrite().save(Config.MODEL_PATH)
    
    threshold_path = Config.MODEL_PATH + "_threshold.txt"
    with open(threshold_path, "w") as f:
        f.write(str(threshold_value))
    logger.info(f"Saved threshold metadata to {threshold_path}")

    spark.stop()

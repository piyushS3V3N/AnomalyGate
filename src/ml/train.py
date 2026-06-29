from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import Pipeline
from src.config import Config
import os

def train_model():
    spark = SparkSession.builder \
        .appName("TrainLogNoiseFilter") \
        .getOrCreate()

    print(f"Loading data from {Config.DATA_FILE}...")
    if not os.path.exists(Config.DATA_FILE):
        print(f"Error: {Config.DATA_FILE} does not exist. Please generate data first.")
        spark.stop()
        return

    df = spark.read.json(Config.DATA_FILE)
    
    indexer1 = StringIndexer(inputCol="event_type", outputCol="event_type_idx", handleInvalid="keep")
    indexer2 = StringIndexer(inputCol="action", outputCol="action_idx", handleInvalid="keep")
    indexer3 = StringIndexer(inputCol="severity", outputCol="severity_idx", handleInvalid="keep")
    
    assembler = VectorAssembler(
        inputCols=["event_type_idx", "action_idx", "severity_idx", "bytes_transferred"],
        outputCol="features"
    )
    
    rf = RandomForestClassifier(labelCol="is_anomaly", featuresCol="features", numTrees=10)
    pipeline = Pipeline(stages=[indexer1, indexer2, indexer3, assembler, rf])
    
    print("Training model...")
    model = pipeline.fit(df)
    
    print(f"Saving model to {Config.MODEL_PATH}...")
    model.write().overwrite().save(Config.MODEL_PATH)
    print("Model trained and saved successfully.")
    
    spark.stop()

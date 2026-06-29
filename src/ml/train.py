from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from src.config import Config
from src.utils.logger import get_logger
import os

logger = get_logger("AnomalyGate.Trainer")

def train_model():
    spark = SparkSession.builder \
        .appName("TrainLogNoiseFilter") \
        .getOrCreate()

    logger.info(f"Loading data from {Config.DATA_FILE}...")
    if not os.path.exists(Config.DATA_FILE):
        logger.error(f"{Config.DATA_FILE} does not exist. Please generate data first.")
        spark.stop()
        return

    df = spark.read.json(Config.DATA_FILE)
    
    # Split data into training and test sets
    train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
    logger.info(f"Data split: {train_df.count()} training rows, {test_df.count()} test rows.")
    
    indexer1 = StringIndexer(inputCol="event_type", outputCol="event_type_idx", handleInvalid="keep")
    indexer2 = StringIndexer(inputCol="action", outputCol="action_idx", handleInvalid="keep")
    indexer3 = StringIndexer(inputCol="severity", outputCol="severity_idx", handleInvalid="keep")
    
    assembler = VectorAssembler(
        inputCols=["event_type_idx", "action_idx", "severity_idx", "bytes_transferred"],
        outputCol="features"
    )
    
    rf = RandomForestClassifier(labelCol="is_anomaly", featuresCol="features", numTrees=20, maxDepth=5)
    pipeline = Pipeline(stages=[indexer1, indexer2, indexer3, assembler, rf])
    
    logger.info("Training Random Forest model...")
    model = pipeline.fit(train_df)
    
    logger.info("Evaluating model on test data...")
    predictions = model.transform(test_df)
    
    evaluator_acc = MulticlassClassificationEvaluator(labelCol="is_anomaly", predictionCol="prediction", metricName="accuracy")
    evaluator_f1 = MulticlassClassificationEvaluator(labelCol="is_anomaly", predictionCol="prediction", metricName="f1")
    
    accuracy = evaluator_acc.evaluate(predictions)
    f1_score = evaluator_f1.evaluate(predictions)
    
    logger.info(f"Model Evaluation -> Accuracy: {accuracy:.4f}, F1-Score: {f1_score:.4f}")
    
    logger.info(f"Saving model to {Config.MODEL_PATH}...")
    model.write().overwrite().save(Config.MODEL_PATH)
    logger.info("Model trained and saved successfully.")
    
    spark.stop()

from pyspark.sql import SparkSession
from src.common.config import Config
from src.common.logger import log_instance as logger

spark = SparkSession.builder.appName("employment_jumpscare").getOrCreate()


def train_model():
    df = spark.read.parquet(f"{Config.data_path}/ml_ready.parquet")
    logger.info("mock for machine learning model training")
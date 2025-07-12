import os
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from pyspark.ml.feature import StringIndexer, VectorAssembler
from src.common.logger import log_instance as logger
from src.common.config import Config


def transform_data_for_analytics(df):
    analytic_df = df.withColumn("AgeGroup", F.when(F.col("Age") < 30, "<30").when(F.col("Age") < 50, "30-49").otherwise("50+")) \
        .withColumn("SleepGroup", F.when(F.col("SleepHours") < 6, "<6").when(F.col("SleepHours") < 8, "6-7.9").otherwise("8+"))

    agg_df = analytic_df.groupBy("Country", "RemoteWork", "AgeGroup", "SleepGroup").agg(
        F.count("*").alias("num_employees"),
        F.avg("BurnoutLevel").alias("avg_burnout"),
        F.avg("StressLevel").alias("avg_stress"),
        F.avg("WorkHoursPerWeek").alias("avg_hours"),
        F.avg("JobSatisfaction").alias("avg_satisfaction"),
        F.avg("ManagerSupportScore").alias("avg_mgr_support"),
    )

    agg_df.write.parquet(f"{Config.data_path}/analytics.parquet", mode="overwrite")
    logger.info("dataset for analytics saved.")
    
def transform_data_for_ml(df):
    categoricals = [
        "Gender", "Country", "JobRole", "Department", "RemoteWork",
        "HasMentalHealthSupport", "HasTherapyAccess", "SalaryRange"
    ]
    numericals = [
        c for c, t in df.dtypes
        if t in ("int", "double") and c not in ("EmployeeID", "BurnoutRisk")
    ]
    os.makedirs(Config.data_path, exist_ok=True)
    df.select(*categoricals, *numericals, "BurnoutRisk") \
      .write.parquet(f"{Config.data_path}/ml_ready.parquet", mode="overwrite")
    logger.info("dataset for ml saved.")
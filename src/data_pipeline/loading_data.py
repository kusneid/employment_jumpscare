from pyspark.sql import SparkSession
from extracting_from_hf import get_df
from transforming import transform_data_for_analytics, transform_data_for_ml

def load_data():
    spark = SparkSession.builder.appName("employment_jumpscare").master("local[2]").getOrCreate()

    df = spark.createDataFrame(get_df())

    transform_data_for_analytics(df)
    transform_data_for_ml(df)

if __name__ == "__main__":
    load_data()
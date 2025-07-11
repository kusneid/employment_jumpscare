from pyspark.sql import SparkSession
from extracting_from_hf import get_df

spark = SparkSession.builder.appName("employment_jumpscare").master("local[2]").getOrCreate()

df = spark.createDataFrame(get_df())

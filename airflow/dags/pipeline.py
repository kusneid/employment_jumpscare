from datetime import datetime
import os
import sys
sys.path.insert(
    0,
    os.path.abspath(
        os.path.join(os.path.dirname(__file__), os.pardir, os.pardir)
    )
)

from airflow import DAG
from airflow.operators.python import PythonOperator
from pyspark.sql import SparkSession
from src.data_pipeline.extracting_from_hf import get_df
from src.data_pipeline.transforming import transform_data_for_ml,transform_data_for_analytics
from src.ml.burnout_prediction_train import train_model
from src.common.logger import log_instance as logger

DEFAULT_ARGS = {
    "owner": "Vladislav Baydakov",
    "depends_on_past": False,
    "start_date": datetime(2025, 7, 12),
    "retries": 0,
}

dag = DAG(
    "employment_jumpscare_pipeline",
    default_args=DEFAULT_ARGS,
    catchup=False
)


def _transform_ml():
    spark = (
        SparkSession.builder
        .appName("employment_jumpscare")
        .getOrCreate()
    )
    pdf = get_df()
    df = spark.createDataFrame(pdf)
    transform_data_for_ml(df)


def _transform_analytics():
    spark = (
        SparkSession.builder
        .appName("employment_jumpscare")
        .getOrCreate()
    )
    pdf = get_df()
    df = spark.createDataFrame(pdf)
    transform_data_for_analytics(df)


def _train_model():
    train_model()


t1 = PythonOperator(
    task_id="transform_data_for_ml",
    python_callable=_transform_ml,
    dag=dag,
)

t2 = PythonOperator(
    task_id="transform_data_for_analytics",
    python_callable=_transform_analytics,
    dag=dag,
)

t3 = PythonOperator(
    task_id="train_model",
    python_callable=_train_model,
    dag=dag,
)

[t1, t2] >> t3

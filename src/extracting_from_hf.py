import kagglehub
from kagglehub import KaggleDatasetAdapter
import pandas as pd
from logger import log_instance as logger
import io

def get_df() -> pd.DataFrame:
  df= kagglehub.load_dataset(
  KaggleDatasetAdapter.PANDAS,
  "khushikyad001/mental-health-and-burnout-in-the-workplace",
  "mental_health_workplace_survey.csv"
  )
  logger.info("dataset loaded, current schema:\n%s", df.info())
  return df

df= get_df()
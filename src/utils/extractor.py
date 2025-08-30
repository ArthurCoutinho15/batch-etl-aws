from typing import Optional
import requests
import logging
import json
from typing import Dict

from pyspark.sql import SparkSession, DataFrame


class DataExtractor():
    def __init__(self, spark_session: SparkSession, api_key: str) -> None:
        self.spark_session = spark_session
        self.api_key = api_key

    def extract_data(self, api_url: str, params: Optional[dict] = None) -> Dict:

        if params is None:
            params = {}

        try:
            response = requests.get(api_url, params=params)
            data = response.json()
        except Exception as e:
            logging.error(f"Erro ao extrair dados da API: {str(e)}")
        return data

    def generate_spark_dataframe(self, data) -> DataFrame:
        json_str = json.dumps(data)

        rdd = self.spark_session.sparkContext.parallelize([json_str])

        df = self.spark_session.read.json(rdd)
        return df

    def save_data(self, spark_dataframe: DataFrame, path: str) -> None:
        spark_dataframe.write.mode('overwrite').json(path)

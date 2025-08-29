import requests
import json
from typing import Dict

from pyspark.sql import SparkSession, DataFrame

class DataExtractor():
    def __init__(self, url: str, spark_session: SparkSession)-> None:
        self.api_url: str = url
        self.spark_session = spark_session 
        
        
    def extract_data(self) -> DataFrame:
        response = requests.get(self.api_url)        
        data = response.json() 
        
        json_str = json.dumps(data)
        
        rdd = self.spark_session.sparkContext.parallelize([json_str])
        
        df = self.spark_session.read.json(rdd)
        return df
    
    def save_data(self, spark_dataframe: DataFrame, path: str) -> None:
        spark_dataframe.write.mode('overwrite').json(path)
        
        
        
        
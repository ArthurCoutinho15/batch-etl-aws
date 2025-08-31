from typing import List

import pyspark.sql.functions as F 
from pyspark.sql import DataFrame



class SparkCleanData():
    
    @staticmethod
    def explode_json(spark_dataframe: DataFrame, columns_explode: List[str]) -> DataFrame:
        for column in columns_explode:
            spark_dataframe = spark_dataframe.withColumn(column, F.explode(F.col(column)))
            
        return spark_dataframe
    
    @staticmethod
    def select_exploded_columns(spark_dataframe: DataFrame, select_columns: list = None) -> DataFrame:
        
        if select_columns:
            spark_dataframe = spark_dataframe.select(*select_columns)
        return spark_dataframe
    
    @staticmethod
    def union_dfs(spark_dataframe_list: List[DataFrame]) -> DataFrame:
        if not spark_dataframe_list:
            return None
        
        spark_dataframe = spark_dataframe_list[0]
        for df in spark_dataframe_list[1:]:
            spark_dataframe = spark_dataframe.unionByName(df, allowMissingColumns=True)
    
        return spark_dataframe
    
    
    @staticmethod
    def save_bronze(spark_dataframe: DataFrame, ingestion_path = str):
        spark_dataframe.write.mode("overwrite").parquet(ingestion_path)
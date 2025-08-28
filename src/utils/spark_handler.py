from pyspark.sql import SparkSession, DataFrame

class SparkHandler():
    def __init__(self, app_name: str):
        self.app_name = app_name
        
    
    def spark_session_instance(self) -> SparkSession:
        spark = SparkSession.builder\
            .appName(f"{self.app_name}")\
            .master("local[*]")\
            .getOrCreate() 
        return spark
from utils.spark_handler import SparkHandler

spark = SparkHandler(app_name="teste").spark_session_instance()

spark
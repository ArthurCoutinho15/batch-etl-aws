import os 
import logging
from typing import List

from dotenv import load_dotenv
import pandas as pd
from pyspark.sql import DataFrame

import utils.configs as configs
from utils.spark_handler import SparkHandler
from utils.extractor import DataExtractor
from utils.spark_cleaner import SparkCleanData
from utils.s3_handler import S3Handler
from utils.data_ingestion import RdsIngestion

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def config(api_key):
    spark = SparkHandler(app_name="teste").spark_session_instance()
    data_extractor = DataExtractor(spark_session=spark, api_key=api_key)
    
    return data_extractor

def s3_config():
    s3_handler = S3Handler()
    return s3_handler

def get_s3_client(s3_handler: S3Handler):
    s3_client = s3_handler.s3_connection()
    
    return s3_client


def api_extraction(data_extractor: DataExtractor, api_url: str, params: dict, ingestion_path: str):
    try:
        data = data_extractor.extract_data(api_url, params)
        spark_dataframe = data_extractor.generate_spark_dataframe(data)
        # data_extractor.save_data(spark_dataframe, ingestion_path)
        # logging.info(f"Json salvo com sucesso em: {ingestion_path}")
        return spark_dataframe

    except Exception as e:
        logging.error(f"Erro ao salvar arquivo: {str(e)}")
        return None
    
if __name__ == "__main__":
    
    spark_cleaner = SparkCleanData()
    
    rds_handler = RdsIngestion()
    
    rds_engine = rds_handler.create_engine()
 
    s3_handler = s3_config()
    s3_client = get_s3_client(s3_handler)
    
    data_extractor = config(configs.API_KEY)

    
    dolar_df = api_extraction(data_extractor, api_url= configs.DOLAR_URL, params={}, ingestion_path="/home/arthur/Projetos/data_batch_etl/data/dolar")
    
    
    apple_df = api_extraction(data_extractor, configs.AAPL_URL, params={"symbol": "AAPL", "interval": "1day", "apikey": configs.API_KEY}, ingestion_path="/home/arthur/Projetos/data_batch_etl/data/apple")
    amazon_df = api_extraction(data_extractor, configs.AAPL_URL, params={"symbol": "AMZN", "interval": "1day", "apikey": configs.API_KEY}, ingestion_path="/home/arthur/Projetos/data_batch_etl/data/amazon")
    bitcoin_df = api_extraction(data_extractor, configs.AAPL_URL, params={"symbol": "BTC/USD", "interval": "1day", "apikey": configs.API_KEY}, ingestion_path="/home/arthur/Projetos/data_batch_etl/data/bitcoin")
    ethereum_df = api_extraction(data_extractor, configs.AAPL_URL, params={"symbol": "BTC/USD", "interval": "1day", "apikey": configs.API_KEY}, ingestion_path="/home/arthur/Projetos/data_batch_etl/data/bitcoin")
    
    stocks: List[DataFrame] = [apple_df, amazon_df]
    crypto: List[DataFrame] = [bitcoin_df, ethereum_df]
    
    stocks_df = spark_cleaner.union_dfs(stocks)
    crypto_df = spark_cleaner.union_dfs(crypto)
    
    
    dolar_df = spark_cleaner.explode_json(dolar_df, ['value'])
    stocks_df = spark_cleaner.explode_json(stocks_df, ['values'])
    crypto_df = spark_cleaner.explode_json(crypto_df, ['values'])
    
    dolar_df = spark_cleaner.select_exploded_columns(dolar_df, select_columns=["value.cotacaoCompra", "value.cotacaoVenda", "value.dataHoraCotacao"])
    stocks_df = spark_cleaner.select_exploded_columns(stocks_df, select_columns=["meta.currency", "meta.exchange", "meta.exchange_timezone", "meta.interval", "meta.symbol", "meta.type", "status", "values.close", "values.datetime", "values.high", "values.low", "values.open", "values.volume"])
    crypto_df = spark_cleaner.select_exploded_columns(crypto_df, select_columns=["meta.currency_base", "meta.currency_quote", "meta.exchange", "meta.interval", "meta.symbol", "meta.type", "status", "values.close", "values.datetime", "values.high", "values.low", "values.open"])
    
    load_dict = {"stocks": stocks_df, "dolar": dolar_df, "crypto": crypto_df}
    
    for path, dataframe in load_dict.items():
        spark_cleaner.save_bronze(dataframe, ingestion_path=f"/home/arthur/Projetos/data_batch_etl/data/bronze/{path}")
        s3_handler.s3_upload_files(s3_client, folder_path=f"/home/arthur/Projetos/data_batch_etl/data/bronze/{path}", bucket_name=configs.AWS_BUCKET, s3_prefix=f"raw-data/{path}")
        
    
    df_stocks = s3_handler.s3_get_data(s3_path="s3://arthur-datalake/raw-data/stocks/")
    df_dolar = s3_handler.s3_get_data(s3_path="s3://arthur-datalake/raw-data/dolar/")
    df_crypto = s3_handler.s3_get_data(s3_path="s3://arthur-datalake/raw-data/crypto/")
    
    rds_handler.load_data(df_dolar, table_name="bronze_dolar", engine=rds_engine)        
    rds_handler.load_data(df_stocks, table_name="bronze_stocks", engine=rds_engine)        
    rds_handler.load_data(df_crypto, table_name="bronze_crypto", engine=rds_engine)        
import os 
from dotenv import load_dotenv

import logging

from utils.spark_handler import SparkHandler
from utils.extractor import DataExtractor

load_dotenv()


def config(api_key):
    spark = SparkHandler(app_name="teste").spark_session_instance()
    data_extractor = DataExtractor(spark_session=spark, api_key=api_key)
    
    return data_extractor

def api_extraction(data_extractor: DataExtractor, api_url: str, params: dict, ingestion_path: str):
    try:
        data = data_extractor.extract_data(api_url, params)
        spark_dataframe = data_extractor.generate_spark_dataframe(data)
        data_extractor.save_data(spark_dataframe, ingestion_path)
        logging.info(f"Json salvo com sucesso em: {ingestion_path}")
        return spark_dataframe

    except Exception as e:
        logging.error(f"Erro ao salvar arquivo: {str(e)}")
        return None
    
if __name__ == "__main__":
    
    API_KEY = str(os.getenv('API_KEY'))
    DOLAR_URL = "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/CotacaoDolarPeriodo(dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)?%40dataInicial='01-02-2025'&%40dataFinalCotacao='08-29-2025'&$format=json"
    AAPL_URL = "https://api.twelvedata.com/time_series"
    
    data_extractor = config(API_KEY)
    dolar_df = api_extraction(data_extractor, api_url= DOLAR_URL, params={}, ingestion_path="/home/arthur/Projetos/data_batch_etl/data/dolar")
    apple_df = api_extraction(data_extractor, AAPL_URL, params={"symbol": "AAPL", "interval": "1day", "apikey": API_KEY}, ingestion_path="/home/arthur/Projetos/data_batch_etl/data/apple")
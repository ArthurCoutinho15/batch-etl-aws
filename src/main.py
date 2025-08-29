from utils.spark_handler import SparkHandler
from utils.extractor import DataExtractor

DOLAR_URL = "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/CotacaoDolarPeriodo(dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)?%40dataInicial='01-02-2025'&%40dataFinalCotacao='08-29-2025'&$format=json"

spark = SparkHandler(app_name="teste").spark_session_instance()
data_extractor = DataExtractor(url = DOLAR_URL, spark_session=spark)

spark

data = data_extractor.extract_data()

data_extractor.save_data(data, path="/home/arthur/Projetos/data_batch_etl/data/dolar")
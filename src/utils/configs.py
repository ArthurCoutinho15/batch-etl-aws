import os 
from dotenv import load_dotenv

load_dotenv()

API_KEY = str(os.getenv("API_KEY"))
DOLAR_URL = "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/CotacaoDolarPeriodo(dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)?%40dataInicial='01-02-2025'&%40dataFinalCotacao='08-29-2025'&$format=json"
AAPL_URL = "https://api.twelvedata.com/time_series"

AWS_KEY = str(os.getenv("aws_access_key_id"))
AWS_PSWD = str(os.getenv("aws_secret_access_key"))
AWS_REGION = str(os.getenv("region_name")) 
AWS_BUCKET = str(os.getenv("bucket_name"))
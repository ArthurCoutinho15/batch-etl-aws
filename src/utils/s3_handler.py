import logging
import os 

import boto3
import pandas as pd 
from dotenv import load_dotenv

load_dotenv()
class S3Handler():
    def __init__(self):
        self._aws_access_key_id = str(os.getenv("aws_access_key_id")) 
        self._aws_secret_access_key = str(os.getenv("aws_secret_access_key")) 
        self._region_name = str(os.getenv("region_name"))
    
    def s3_connection(self):
        try:
            boto3.setup_default_session(
                aws_access_key_id = self._aws_access_key_id,
                aws_secret_access_key = self._aws_secret_access_key,
                region_name = self._region_name
            )
            
            s3 = boto3.client("s3")
            logging.info("Sucesso na conexão com s3")
            
            return s3
        except Exception as e:
            logging.error(f"Erro ao fazer conexão com s3: {str(e)}")
    
    def s3_upload_files(self, s3, folder_path: str, bucket_name: str, s3_prefix):
        try:
            parquet_files = [f for f in os.listdir(folder_path) if f.endswith(".parquet")]
            for file in parquet_files:
                local_file_path = os.path.join(folder_path, file)
                s3_key = f"{s3_prefix}/{file}"
                with open(local_file_path, "rb") as data:
                    s3.put_object(
                        Bucket=bucket_name,
                        Key=s3_key,
                        Body=data
                    )
                logging.info("Sucesso ao carregar dados para o s3")
        except Exception as e:
            logging.error(f"Erro ao carregar dados para o s3: {str(e)}")
            
    def s3_get_data(self, s3_path) -> pd.DataFrame:
            
        df_pandas = pd.read_parquet(s3_path, storage_options={
            "key": self._aws_access_key_id,
            "secret": self._aws_secret_access_key
        })
        
        return df_pandas
import os
import logging

from dotenv import load_dotenv
from sqlalchemy import create_engine

import pandas as pd
load_dotenv()


class RdsIngestion():
    def __init__(self):
        self._rds_user = str(os.getenv("RDS_USER"))
        self._rds_psw = str(os.getenv("RDS_PASSWORD"))
        self._host = str(os.getenv("RDS_HOST"))
        self._port = str(os.getenv("RDS_PORT"))
        self._db_name = str(os.getenv("RDS_DB_NAME"))
        
    
    def create_engine(self):
        engine = create_engine(
            f"mysql+pymysql://{self._rds_user}:{self._rds_psw}@{self._host}:{self._port}/{self._db_name}"
        )
        return engine
    
    def load_data(self, dataframe: pd.DataFrame, table_name: str, engine):
        try:
            dataframe.to_sql(
                name=table_name,
                con=engine,
                index=False,
                if_exists="replace"
            )
            logging.info("Dados inseridos com sucesso!")
        except Exception as e:
            logging.error(f"Erro ao inserir dados: {e}")
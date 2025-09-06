import os 
import sys
 
sys.path.append('airflow')

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

ETL_VENV = "/home/arthur/Projetos/data_batch_etl/venv/bin/activate"
ETL_SCRIPT = "/home/arthur/Projetos/data_batch_etl/src/main.py"

default_args = {
    'owner': 'Arthur',
    'start_date': datetime(2025, 1, 1),
    'retries': 1
}

with DAG(
    dag_id='finance_pipeline',
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=['etl']
) as dag: 
    
    run_etl = BashOperator(
        task_id="run_etl",
        bash_command=f"source {ETL_VENV} && python {ETL_SCRIPT}"
    )
    
    run_etl
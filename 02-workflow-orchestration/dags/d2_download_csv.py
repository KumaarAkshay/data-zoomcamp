from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

# Instantiate the DAG
dag = DAG(
    'download_csv',
    default_args=default_args,
    description='A DAG to download CSV data, parse schema, and insert data into PostgreSQL',
    schedule_interval=None,  # Run on-demand
    catchup=False,
)

# Task to download CSV data using wget
download_csv_task = BashOperator(
    task_id='download_csv',
    bash_command='wget -qO- https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz | gunzip > /opt/airflow/output_files/dataunzip.csv',
    dag=dag,
)

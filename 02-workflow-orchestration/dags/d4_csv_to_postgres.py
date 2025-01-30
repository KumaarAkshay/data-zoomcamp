from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime
import pandas as pd

conn_name = 'posgress_conn'
table_name = 'color_table'

def load_csv_to_postgres(file_path, table_name, postgres_conn_id):
    postgres_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    with open(file_path, 'r') as f:
        cursor.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV HEADER", f)
    conn.commit()

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

# Instantiate the DAG
dag = DAG(
    'csv_to_postgres',
    default_args=default_args,
    description='Upload data to postgres db PostgreSQL',
    schedule_interval=None,  # Run on-demand
    catchup=False,
)

# Task to create the PostgreSQL table based on schema file
create_table_task = PostgresOperator(
    task_id='create_table',
    postgres_conn_id=conn_name,
    sql=f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        Name TEXT,
        HEX TEXT,
        RGB TEXT
    );
    """,
    dag=dag,
)

# Task to load CSV data into PostgreSQL
load_data_task = PythonOperator(
    task_id='load_csv_to_postgres',
    python_callable=load_csv_to_postgres,
    op_kwargs={
        'file_path': '/opt/airflow/output_files/color.csv',
        'table_name': table_name,
        'postgres_conn_id': conn_name,
    },
    dag=dag,
)

create_table_task >> load_data_task
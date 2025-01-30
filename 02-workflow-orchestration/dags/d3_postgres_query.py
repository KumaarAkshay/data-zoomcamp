from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

dag = DAG(
    'postgres_query',
    default_args=default_args,
    description='A simple DAG to create a log table and insert dummy data',
    schedule_interval=None,  # Run on-demand
    catchup=False,
)

# Task to create the log table
create_table_task = PostgresOperator(
    task_id='create_log_table',
    postgres_conn_id='posgress_conn',  # Connection ID configured in Airflow
    sql="""
    CREATE TABLE IF NOT EXISTS cars (
    brand VARCHAR(255),
    model VARCHAR(255),
    year INT
    );
    """,
    dag=dag,
)

# Task to insert dummy data into the log table
insert_data_task = PostgresOperator(
    task_id='insert_dummy_data',
    postgres_conn_id='posgress_conn',
    sql="""
    INSERT INTO cars (brand, model, year)
    VALUES ('Ford', 'Mustang', 1964);
    """,
    dag=dag,
)

# Set task dependencies
create_table_task >> insert_data_task
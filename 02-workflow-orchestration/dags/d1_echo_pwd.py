from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import os

# Default arguments for the DAG
default_args = {
    'owner': 'akshay',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

# Instantiate the DAG
dag = DAG(
    'echo_pwd',
    default_args=default_args,
    description='A DAG to echo the current working directory',
    schedule_interval=None,  # Run on-demand
    catchup=False,
)

# BashOperator to echo the current working directory
# echo_pwd = BashOperator(
#     task_id="echo_pwd_task",
#     bash_command="pwd",
#     dag=dag
# )


def print_working_directory():
    print("Current Working Directory:",os.getcwd())

task = PythonOperator(task_id='print_cwd', python_callable=print_working_directory, dag=dag)

# Define task dependencies if needed (in this case, there are none)

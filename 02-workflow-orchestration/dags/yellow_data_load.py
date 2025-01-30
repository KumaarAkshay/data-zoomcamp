from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime

# Constants
conn_name = 'posgress_conn'
table_name = 'yellow_staging'
output_location = '/opt/airflow/output_files/'

def load_csv_to_postgres(file_path, table_name, postgres_conn_id):
    postgres_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()
    with open(file_path, 'r') as f:
        cursor.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV HEADER", f)
    conn.commit()

default_args = {
    'owner': 'airflow_akshay',
    'depends_on_past': True,       # True if task depends on the success of the same task in the previous DAG run
    'start_date': datetime(2019, 1, 1),
    'end_date': datetime(2021, 7, 31),
}

dag = DAG(
    'yellow_alldata',
    default_args=default_args,
    description='Upload data to PostgreSQL',
    schedule_interval='@monthly',
    catchup=True,       # causes Airflow to "catch up" on all missed DAG runs between the start_date and current date
    max_active_runs=1,  # No of active running at at time -- if skipped max no of dag run at a time
)

# Fixed BashOperator with direct URL templating
download_csv_task = BashOperator(
    task_id='download_csv',
    bash_command=(
        "wget -qO- 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/"
        "yellow_tripdata_{{ execution_date.strftime('%Y-%m') }}.csv.gz' "
        "| gunzip > {{ params.output_location }}/yellow_tripdata_{{ execution_date.strftime('%Y-%m') }}.csv"
    ),
    params={'output_location': output_location},
    dag=dag,
)

# Fixed create_table_task 
create_table_task = PostgresOperator(
    task_id='create_staging_table',
    postgres_conn_id=conn_name,
    sql="""
    CREATE TABLE IF NOT EXISTS yellow_staging (
        vendorid               text,
        tpep_pickup_datetime   timestamp,
        tpep_dropoff_datetime  timestamp,
        passenger_count        integer,
        trip_distance          double precision,
        ratecodeid             text,
        store_and_fwd_flag     text,
        pulocationid           text,
        dolocationid           text,
        payment_type           integer,
        fare_amount            double precision,
        extra                  double precision,
        mta_tax                double precision,
        tip_amount             double precision,
        tolls_amount           double precision,
        improvement_surcharge  double precision,
        total_amount           double precision,
        congestion_surcharge   double precision
    );

    TRUNCATE TABLE yellow_staging;
    """,
    dag=dag,
)

load_data_staging = PythonOperator(
    task_id='load_to_postgres',
    python_callable=load_csv_to_postgres,
    op_kwargs={
        'file_path': "{{ params.output_location }}/yellow_tripdata_{{ execution_date.strftime('%Y-%m') }}.csv",
        'table_name': 'yellow_staging',
        'postgres_conn_id': conn_name,
    },
    params={'output_location': output_location},
    dag=dag,
)

# Added temp table per execution to avoid concurrency issues
load_data_final_tbl = PostgresOperator(
    task_id='inserting_to_final_tbl',
    postgres_conn_id=conn_name,
    sql="""
    -- Insert into final table from staging table
    INSERT INTO yellow_final_all (
        filename, vendorid, tpep_pickup_datetime, tpep_dropoff_datetime,
        passenger_count, trip_distance, ratecodeid,store_and_fwd_flag, pulocationid,
        dolocationid, payment_type, fare_amount,extra, mta_tax, tip_amount, tolls_amount,
        improvement_surcharge, total_amount, congestion_surcharge      
    )
    SELECT 
        '{{ execution_date.strftime('%Y-%m') }}',
        vendorid::text, 
        tpep_pickup_datetime::timestamp, 
        tpep_dropoff_datetime::timestamp, 
        passenger_count::integer,
        trip_distance::double precision,
        ratecodeid::text,
        store_and_fwd_flag::text, 
        pulocationid::text,
        dolocationid::text,
        payment_type::integer,
        fare_amount::double precision,
        extra::double precision,
        mta_tax::double precision,
        tip_amount::double precision,
        tolls_amount::double precision,
        improvement_surcharge::double precision,
        total_amount::double precision,
        congestion_surcharge::double precision
    FROM yellow_staging ;

    -- Log execution
    INSERT INTO input_logs (dag_name, file_name, run_time)
    VALUES ('yellow_all', '{{ execution_date.strftime('%Y-%m-%d') }}', NOW());
    """,
    dag=dag,
)

cleanup_task = BashOperator(
    task_id='cleanup_files',
    bash_command="rm -f {{ params.output_location }}/yellow_tripdata_{{ execution_date.strftime('%Y-%m') }}.csv",
    params={'output_location': output_location},
    dag=dag,
)

download_csv_task >> create_table_task >> load_data_staging >> load_data_final_tbl >> cleanup_task
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime

# Constants
conn_name = 'posgress_conn'
table_name = 'green_staging'
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
    'start_date': datetime(2021, 1, 1),
    'end_date': datetime(2021, 7, 31),
}

dag = DAG(
    'simple_green_monthly',
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
        "wget -qO- 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/"
        "green_tripdata_{{ execution_date.strftime('%Y-%m') }}.csv.gz' "
        "| gunzip > {{ params.output_location }}/green_tripdata_{{ execution_date.strftime('%Y-%m') }}.csv"
    ),
    params={'output_location': output_location},
    dag=dag,
)

# Fixed create_table_task 
create_table_task = PostgresOperator(
    task_id='create_staging_table',
    postgres_conn_id=conn_name,
    sql="""
    CREATE TABLE IF NOT EXISTS green_staging (
        VendorID               text,
        lpep_pickup_datetime   text,
        lpep_dropoff_datetime  text,
        store_and_fwd_flag     text,
        RatecodeID             text,
        PULocationID           text,
        DOLocationID           text,
        passenger_count        text,
        trip_distance          text,
        fare_amount            text,
        extra                  text,
        mta_tax                text,
        tip_amount             text,
        tolls_amount           text,
        ehail_fee              text,
        improvement_surcharge  text,
        total_amount           text,
        payment_type           text,
        trip_type              text,
        congestion_surcharge   text
    );

    TRUNCATE TABLE green_staging;
    """,
    dag=dag,
)

load_data_staging = PythonOperator(
    task_id='load_to_postgres',
    python_callable=load_csv_to_postgres,
    op_kwargs={
        'file_path': "{{ params.output_location }}/green_tripdata_{{ execution_date.strftime('%Y-%m') }}.csv",
        'table_name': 'green_staging',
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
    -- Insert into final table from temp table
    INSERT INTO green_final (
        unique_row_id, filename, vendorid, lpep_pickup_datetime, 
        lpep_dropoff_datetime, store_and_fwd_flag, ratecodeid, 
        pulocationid, dolocationid, passenger_count, trip_distance, 
        fare_amount, extra, mta_tax, tip_amount, tolls_amount, 
        ehail_fee, improvement_surcharge, total_amount, payment_type, 
        trip_type, congestion_surcharge
    )
    SELECT 
        md5(
            coalesce(vendorid::text, '') ||
            coalesce(lpep_pickup_datetime::text, '') || 
            coalesce(lpep_dropoff_datetime::text, '') || 
            coalesce(pulocationid::text, '') || 
            coalesce(dolocationid::text, '') || 
            coalesce(fare_amount::text, '') || 
            coalesce(trip_distance::text, '') ||
            coalesce('{{ execution_date.strftime('%Y-%m') }}','')
        ),
        '{{ execution_date.strftime('%Y-%m') }}',
        vendorid::text, 
        lpep_pickup_datetime::timestamp, 
        lpep_dropoff_datetime::timestamp, 
        store_and_fwd_flag::text, 
        ratecodeid::text, 
        pulocationid::text, 
        dolocationid::text, 
        passenger_count::integer, 
        trip_distance::double precision, 
        fare_amount::double precision, 
        extra::double precision, 
        mta_tax::double precision, 
        tip_amount::double precision, 
        tolls_amount::double precision, 
        ehail_fee::double precision, 
        improvement_surcharge::double precision, 
        total_amount::double precision, 
        payment_type::integer, 
        trip_type::integer, 
        congestion_surcharge::double precision
    FROM green_staging
    ON CONFLICT (unique_row_id) DO NOTHING;

    -- Log execution
    INSERT INTO input_logs (dag_name, file_name, run_time)
    VALUES ('green_staging', '{{ execution_date.strftime('%Y-%m-%d') }}', NOW());
    """,
    dag=dag,
)

cleanup_task = BashOperator(
    task_id='cleanup_files',
    bash_command="rm -f {{ params.output_location }}/green_tripdata_{{ execution_date.strftime('%Y-%m') }}.csv",
    params={'output_location': output_location},
    dag=dag,
)

download_csv_task >> create_table_task >> load_data_staging >> load_data_final_tbl >> cleanup_task
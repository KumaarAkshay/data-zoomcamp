from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime

# Constants
conn_name = 'posgress_conn'
table_name = 'green_staging'  # Updated to match the table name in the SQL query
url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz'
output_location = '/opt/airflow/output_files/'
filename = url.split("/")[-1].split(".")[0]

# Function to load CSV data into PostgreSQL
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
    'start_date': datetime(2025, 1, 1),  # Future date to avoid immediate execution
}

# Instantiate the DAG
dag = DAG(
    'upload_csv_to_postgres',
    default_args=default_args,
    description='Upload data to PostgreSQL',
    schedule_interval=None,  # Run on-demand
    catchup=False,
)

# Task to download CSV data using wget
download_csv_task = BashOperator(
    task_id='download_csv',
    bash_command=f"wget -qO- {url} | gunzip > {output_location}{filename}.csv",
    dag=dag,
)

# Task to create the PostgreSQL table
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

    COMMIT;

    TRUNCATE TABLE green_staging;
    """,
    dag=dag,
)

# Task to load CSV data into PostgreSQL
load_data_task = PythonOperator(
    task_id='load_to_postgres',
    python_callable=load_csv_to_postgres,
    op_kwargs={
        'file_path': f"{output_location}{filename}.csv",
        'table_name': table_name,
        'postgres_conn_id': conn_name,
    },
    dag=dag,
)

# Task to create the PostgreSQL table
load_data_final_tbl = PostgresOperator(
    task_id='inserting_to_final_tbl',
    postgres_conn_id=conn_name,
    sql=f"""
    INSERT INTO green_final (
        unique_row_id, filename, vendorid, lpep_pickup_datetime, lpep_dropoff_datetime, 
        store_and_fwd_flag, ratecodeid, pulocationid, dolocationid, 
        passenger_count, trip_distance, fare_amount, extra, mta_tax, 
        tip_amount, tolls_amount, ehail_fee, improvement_surcharge, 
        total_amount, payment_type, trip_type, congestion_surcharge
    )
    SELECT 
        md5(
            coalesce(cast(vendorid as text), '') ||
            coalesce(cast(lpep_pickup_datetime as text), '') || 
            coalesce(cast(lpep_dropoff_datetime as text), '') || 
            coalesce(cast(pulocationid as text), '') || 
            coalesce(cast(dolocationid as text), '') || 
            coalesce(cast(fare_amount as text), '') || 
            coalesce(cast(trip_distance as text), '')
        ) AS unique_row_id,
        '{filename}' AS filename,
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

    COMMIT;

    INSERT INTO input_logs (dag_name, file_name, run_time)
    VALUES ('Green_single', '{filename}',CURRENT_DATE);
    """,
    dag=dag,
)

# Define task dependencies
download_csv_task >> create_table_task >> load_data_task >> load_data_final_tbl
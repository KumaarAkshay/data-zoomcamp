from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime

# Constants
CONN_NAME = 'postgres_conn'  # Fixed typo in connection name
OUTPUT_LOCATION = '/opt/airflow/output_files'

default_args = {
    'owner': 'airflow_akshay',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 1),
    'end_date': datetime(2019, 3, 30),
    # 'retries': 2,
    # 'retry_delay': datetime.timedelta(minutes=5),
}

def load_csv_to_postgres(file_path, postgres_conn_id):
    """Load CSV data directly into temporary staging table"""
    postgres_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()
    
    # Use execution date in temp table name
    temp_table = f"tmp_green_{{{{ execution_date.strftime('%Y_%m') }}}}"
    
    try:
        # Create temporary table with same schema as final table
        cursor.execute(f"""
            CREATE TEMP TABLE {temp_table} (
                VendorID               text,
                lpep_pickup_datetime   timestamp,
                lpep_dropoff_datetime  timestamp,
                store_and_fwd_flag     text,
                RatecodeID             text,
                PULocationID           text,
                DOLocationID           text,
                passenger_count        integer,
                trip_distance          double precision,
                fare_amount            double precision,
                extra                  double precision,
                mta_tax                double precision,
                tip_amount             double precision,
                tolls_amount           double precision,
                ehail_fee              double precision,
                improvement_surcharge  double precision,
                total_amount           double precision,
                payment_type           integer,
                trip_type              integer,
                congestion_surcharge   double precision
            );
        """)
        
        # Load data directly into temp table
        with open(file_path, 'r') as f:
            cursor.copy_expert(f"""
                COPY {temp_table} FROM STDIN WITH CSV HEADER
            """, f)
        
        conn.commit()
    finally:
        cursor.close()
        conn.close()

with DAG(
    'upload_green_monthly_optimized',
    default_args=default_args,
    description='Optimized monthly green taxi data pipeline',
    schedule_interval='@monthly',
    catchup=True,
    max_active_runs=3,  # Control concurrent executions
) as dag:

    download_csv_task = BashOperator(
        task_id='download_csv',
        bash_command=(
            'wget -qO- "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/'
            'green_tripdata_{{ execution_date.strftime("%Y-%m") }}.csv.gz" '
            '| gunzip > "{}/green_tripdata_{{ execution_date.strftime("%Y-%m") }}.csv"'.format(OUTPUT_LOCATION)
        ),
        dag=dag,
    )

    load_data_task = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_csv_to_postgres,
        op_kwargs={
            'file_path': f"{OUTPUT_LOCATION}/green_tripdata_{{ execution_date.strftime('%Y-%m') }}.csv",
            'postgres_conn_id': CONN_NAME,
        },
        dag=dag,
    )

    load_data_final_tbl = PostgresOperator(
        task_id='upsert_final_table',
        postgres_conn_id=CONN_NAME,
        sql="""
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
                    coalesce(trip_distance::text, '')
                ),
                '{{ execution_date.strftime("%Y-%m") }}',
                vendorid, 
                lpep_pickup_datetime, 
                lpep_dropoff_datetime, 
                store_and_fwd_flag, 
                ratecodeid, 
                pulocationid, 
                dolocationid, 
                passenger_count, 
                trip_distance, 
                fare_amount, 
                extra, 
                mta_tax, 
                tip_amount, 
                tolls_amount, 
                ehail_fee, 
                improvement_surcharge, 
                total_amount, 
                payment_type, 
                trip_type, 
                congestion_surcharge
            FROM tmp_green_{{ execution_date.strftime('%Y_%m') }}
            ON CONFLICT (unique_row_id) DO NOTHING;

            -- Add execution log
            INSERT INTO input_logs (dag_name, file_name, run_time)
            VALUES (
                'green_monthly', 
                '{{ execution_date.strftime("%Y-%m") }}', 
                NOW()
            );
        """,
        dag=dag,
    )

    cleanup_task = BashOperator(
        task_id='cleanup_files',
        bash_command=f"rm -f {OUTPUT_LOCATION}/green_tripdata_{{ execution_date.strftime('%Y-%m') }}.csv",
        dag=dag,
    )

    download_csv_task >> load_data_task >> load_data_final_tbl >> cleanup_task
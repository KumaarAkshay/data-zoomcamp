-- Description: This file contains the SQL commands to create the tables in the database.

--green_staging
CREATE TABLE IF NOT EXISTS green_staging (
    vendorid               text,
    lpep_pickup_datetime   timestamp,
    lpep_dropoff_datetime  timestamp,
    store_and_fwd_flag     text,
    ratecodeid             text,
    pulocationid           text,
    dolocationid           text,
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

--green_final
CREATE TABLE IF NOT EXISTS green_final (
    unique_row_id          text PRIMARY KEY,
    filename               text,
    vendorid               text,
    lpep_pickup_datetime   timestamp,
    lpep_dropoff_datetime  timestamp,
    store_and_fwd_flag     text,
    ratecodeid             text,
    pulocationid           text,
    dolocationid           text,
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


--yellow_staging
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

--yellow_final
CREATE TABLE IF NOT EXISTS yellow_final (
    unique_row_id          text PRIMARY KEY,
    filename               text,
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

--input-log
CREATE TABLE IF NOT EXISTS input_logs (
    dag_name        text,
    file_name       text,
    run_time        timestamp
);


---------------------------------------
--All data tables

CREATE TABLE IF NOT EXISTS green_final_all (
    filename               text,
    vendorid               text,
    lpep_pickup_datetime   timestamp,
    lpep_dropoff_datetime  timestamp,
    store_and_fwd_flag     text,
    ratecodeid             text,
    pulocationid           text,
    dolocationid           text,
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


CREATE TABLE IF NOT EXISTS yellow_final_all (
    filename               text,
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
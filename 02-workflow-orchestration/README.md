# Airflow Setup

## Configure Codespace

### Build Custom Airflow Image
To create a custom Apache Airflow Docker image with `wget` installed, run the following command:
```sh
docker build -t custom-airflow:latest .
```

### Set Folder Permissions
Grant read/write permissions to the `output_files` directory:
```sh
sudo chmod -R 777 output_files
```

### Start Docker Containers
Run the Docker Compose file to start the necessary services:
```sh
docker compose up -d
```

## Configure Database

### Create Database Connection in pgAdmin
```plaintext
Hostname: localhost
Port: 5432
Username: airflow
```

### Run Table Creation Script
Execute the following SQL script to create staging, final, and log tables:
```sh
psql -U airflow -d your_database -f table_schema.sql
```

## Add Connection in Airflow

### Go to Admin > connections > add connection
```plaintext
connection name : posgress_conn
connection type : postgres
database        : taxi_db
hostname        : postgres
username        : airflow
password        : airflow
Port            : 5432
```

## Stop Containers

### Stop Docker Containers
To stop and remove the running containers, use:
```sh
docker compose down
```

## Dag File Details

```plaintext
f1_green_all/f1_green_all    -> load data from 2019-01 to 2021-07 simple Insert Into Select
f1_green_monthly_simple/f1_yellow_monthly_simple    -> load from 2021-01 to 2021-07 unique records

d1 to d4    -> sample dags
```
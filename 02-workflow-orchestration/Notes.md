# Airflow Datacamp Notes

## Linux Commands

### File and Folder Permissions
- **Grant full access to a folder and its subfolders:**
  ```sh
  sudo chmod -R 777 data_postgres
  ```
- **Grant full access to a specific file or folder:**
  ```sh
  sudo chmod 777 data_postgres
  ```

### System Information
- **View command history:**
  ```sh
  history
  ```
- **Check OS details:**
  ```sh
  cat /etc/os-release
  ```
- **View all environment variables:**
  ```sh
  env
  ```
- **Search for a specific environment variable:**
  ```sh
  env | grep search_value
  ```

### File Download
- **Download a file using `wget`:**
  ```sh
  wget -O sample_files/data.csv "https://cdn.wsform.com/wp-content/uploads/2020/06/color_srgb.csv"
  ```
- **Difference between `-O` and `-o`:**
  - `-O` is used to save the downloaded file in the specified path.
  - `-o` saves logs to a file.
- **Difference between absolute and relative paths:**
  - `/folder/file.csv` → Absolute path from root.
  - `folder/file.csv` → Relative path from the current working directory.
- **Download using `curl`:**
  ```sh
  curl -o sample_files/datacurl.csv "https://cdn.wsform.com/wp-content/uploads/2020/06/color_srgb.csv"
  ```
  - Equivalent to `wget -O`.
- **Download and unzip file:**
  ```sh
  wget -qO- 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-12.csv.gz' | gunzip > output.csv
  ```

---

## Airflow Docker

### Container Access
- **SSH into an Airflow container:**
  ```sh
  docker exec -it --user root 6147014504d9 /bin/bash
  ```

### File Management
- **Airflow file directory:**
  ```sh
  /opt/airflow/output_files
  ```
- **Copy a file from a container to the local machine:**
  ```sh
  docker cp 6147014504d9:/opt/airflow/airflow.cfg ./data/airflow.cfg
  docker cp 6147014504d9:/opt/airflow/airflow.cfg airflow.cfg
  ```

### Building and Downloading
- **Build a new Airflow Docker image:**
  ```sh
  docker build -t custom-airflow:latest .
  ```
- **Download and extract a CSV file from a zip archive:**
  ```sh
  wget -qO- https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz | gunzip > /opt/airflow/output_files/dataunzip.csv
  ```


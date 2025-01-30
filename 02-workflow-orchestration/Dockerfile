FROM apache/airflow:latest

# Switch to the root user to install packages
USER root

# Update package lists and install wget
RUN apt-get update && apt-get install -y wget && apt-get clean

# Switch back to the airflow user
USER airflow

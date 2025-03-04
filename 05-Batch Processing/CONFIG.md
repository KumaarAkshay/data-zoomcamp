## Configuring pyspark in cloud dataproc

Deploy Dataproc cluster from `gcloud cli`

```sh
gcloud dataproc clusters create cluster-zoom \
    --enable-component-gateway \
    --region us-central1 \
    --bucket zoom-batch-storage \
    --master-machine-type n2-standard-2 \
    --master-boot-disk-type pd-balanced \
    --master-boot-disk-size 100 \
    --num-workers 2 \
    --worker-machine-type n2-standard-2 \
    --worker-boot-disk-type pd-balanced \
    --worker-boot-disk-size 50 \
    --image-version 2.0-debian10 \
    --scopes 'https://www.googleapis.com/auth/cloud-platform' \
    --optional-components JUPYTER \
    --initialization-actions gs://goog-dataproc-initialization-actions-asia-south2/connectors/connectors.sh \
    --metadata bigquery-connector-version=1.2.0 \
    --metadata spark-bigquery-connector-version=0.21.0 
```

Explaination

```plaintext
gs://goog-dataproc-initialization-actions-asia-south2/connectors/connectors.sh path provide the google's buckat path to download the BigQuery connector

initialization-actions - used for initialization script to run on the cluster
metadata - used to provide the BigQuery connector version
scopes - make cluster access to all gcp services
optional-components - Jupyter notebook is enabled
bucket - bucket name to store the cluster data
no-address - to disable the external IP address
single-node - to create a single node cluster
enable-component-gateway required to run Jupyter notebook
```

Go to Dataproc >> Web Interface >> Jyputer to run jupyter in cluster

Go to Dataproc >> Jobs to run pyspark script 

### Extra Notes

Run single note in dataproc not recomended

```sh
gcloud dataproc clusters create cluster-single-node \
    --enable-component-gateway \
    --region us-central1 \
    --bucket zoom-batch-storage \
    --single-node \
    --master-machine-type n2-standard-2 \
    --master-boot-disk-type pd-balanced \
    --master-boot-disk-size 100 \
    --image-version 2.0-debian10 \
    --scopes 'https://www.googleapis.com/auth/cloud-platform' \
    --optional-components JUPYTER \
    --initialization-actions gs://goog-dataproc-initialization-actions-asia-south2/connectors/connectors.sh \
    --metadata bigquery-connector-version=1.2.0 \
    --metadata spark-bigquery-connector-version=0.21.0 
```
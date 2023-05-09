import os
import glob
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq

PROJECT_ID = Variable.get("PROJECT_ID")
BUCKET = Variable.get("BUCKET_NAME")

#dataset_file = "yellow_tripdata_2022.csv"
#dataset_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-*.parquet"
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
# path_to_local_home = "/opt/airflow/"
#parquet_file = dataset_file.replace('.csv', '.parquet')
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", "cloudflow")


def format_to_parquet(src_file):
    if not src_file.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace('.csv', '.parquet'))


# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(bucket, object_dir, local_glob):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """

    # logging.info(f"bucket nama: {bucket}")
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    for ifile in glob.glob(local_glob):
        filename = os.path.basename(ifile)
        blob = bucket.blob(f"{object_dir}/{filename}")
        blob.upload_from_filename(ifile)


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="data_ingestion_gcs_dag",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:

    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        #bash_command=f"curl -sSL {dataset_url} > {path_to_local_home}/{dataset_file}"
        bash_command=f"for m in {{01..3}}; do wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-$m.parquet -P {path_to_local_home}; done"
    )

    # format_to_parquet_task = PythonOperator(
    #     task_id="format_to_parquet_task",
    #     python_callable=format_to_parquet,
    #     op_kwargs={
    #         "src_file": f"{path_to_local_home}/{dataset_file}",
    #     },
    # )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_dir": f"raw",
            "local_glob": f"{path_to_local_home}/*.parquet",
        },
    )

    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
         task_id="bigquery_external_table_task",
         table_resource={
             "tableReference": {
                 "projectId": PROJECT_ID,
                 "datasetId": BIGQUERY_DATASET,
                 "tableId": "external_table",
             },
             "externalDataConfiguration": {
                 "sourceFormat": "PARQUET",
                 "sourceUris": [f"gs://{BUCKET}/raw/yellow_tripdata_2022-01.parquet"],
             },
         },
     )

    download_dataset_task >> local_to_gcs_task >> bigquery_external_table_task


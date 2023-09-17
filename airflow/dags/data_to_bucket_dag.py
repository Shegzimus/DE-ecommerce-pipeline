import os

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models.baseoperator import chain

from google.cloud import storage
import pyarrow.csv as pv
import pyarrow.parquet as pq

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

kaggle_dataset_user = "olistbr"
kaggle_dataset_name = "brazilian-ecommerce"
kaggle_dataset_download_ref = f"{kaggle_dataset_user}/{kaggle_dataset_name}"

path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

csv_files = ["olist_orders_dataset.csv", "olist_customers_dataset.csv", "olist_order_payments_dataset.csv",
             "olist_order_items_dataset.csv", "olist_products_dataset.csv", "olist_order_reviews_dataset.csv",
             "olist_sellers_dataset.csv", "olist_geolocation_dataset.csv", "product_category_name_translation.csv"]

def format_to_parquet(individual_file):
    table = pv.read_csv(individual_file)
    pq.write_table(table, individual_file.replace('.csv', '.parquet'))

def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="data_to_bucket_dag",
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['de_project'],
) as dag:

    download_task = BashOperator(
        task_id=f"download_data_task",
        bash_command=f"kaggle datasets download {kaggle_dataset_download_ref} -p {path_to_local_home}/data && \
                       unzip {path_to_local_home}/data/{kaggle_dataset_name}.zip -d {path_to_local_home}/data"
    )

    def format_to_parquet_task(file):
        task = PythonOperator(
            task_id=f"format_to_parquet_task_{file}",
            python_callable=format_to_parquet,
            op_kwargs={
                "individual_file": f"{path_to_local_home}/data/{file}",
            },
        )
        return task

    def file_to_gcs_task(file):
        task = PythonOperator(
            task_id=f"{file}_to_gcs_task",
            python_callable=upload_to_gcs,
            op_kwargs={
                "bucket": BUCKET,
                "object_name": f"raw/{file}",
                "local_file": f"{path_to_local_home}/data/{file}",
            },
        )
        return task

    chain(download_task,
          [format_to_parquet_task(file) for file in csv_files],
          [file_to_gcs_task(file.replace('.csv','.parquet')) for file in csv_files])

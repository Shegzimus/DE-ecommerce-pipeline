import os
from pathlib import Path
from datetime import timedelta

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryDeleteDatasetOperator,
    BigQueryCreateEmptyTableOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.providers.google.cloud.transfers.bigquery_to_bigquery import (
    BigQueryToBigQueryOperator,
)
from great_expectations_provider.operators.great_expectations import (
    GreatExpectationsOperator,
)
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator


base_path = Path(__file__).parents[1]
ge_root_dir = os.path.join(base_path, "config", "ge")

# In a production DAG, the global variables below should be stored as Airflow
# or Environment variables.
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

bq_testing_dataset = "de_dataset_testing"

bq_orders_table = "olist_orders_testing"
bq_customers_table = "olist_customers_testing"
bq_payments_table = "olist_payments_testing"

gcp_orders_data_dest = "raw/olist_orders_dataset.parquet"
gcp_customers_data_dest = "raw/olist_customers_dataset.parquet"
gcp_payments_data_dest = "raw/olist_order_payments_dataset.parquet"

def check_validation_result(ti, table_validated):
    result = ti.xcom_pull(task_ids=f"{table_validated}_table_validation")
    if result['success']:
        return f'copy_{table_validated}_table_to_staging_if_pass'
    else:
        return f'{table_validated}_validation_fails'


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    dag_id="data_quality_check_with_ge_dag",
    description="Check data quality for the olist orders, customers & payments tables.",
    doc_md=__doc__,
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
) as dag:

    """
    #### BigQuery dataset creation
    Create the dataset to store the sample data tables.
    """
    create_testing_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_testing_dataset", dataset_id=bq_testing_dataset, location="europe-west3"
    )

    """
    #### Create Temp Table for GE in BigQuery
    """
    create_orders_testing_table = BigQueryCreateEmptyTableOperator(
        task_id=f"create_{bq_orders_table}_table",
        dataset_id=bq_testing_dataset,
        table_id=bq_orders_table,
        schema_fields=[
            {"name": "order_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "customer_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "order_status", "type": "STRING", "mode": "NULLABLE"},
            {"name": "order_purchase_timestamp", "type": "DATETIME", "mode": "NULLABLE"},
            {"name": "order_approved_at", "type": "DATETIME", "mode": "NULLABLE"},
            {"name": "order_delivered_carrier_date", "type": "DATETIME", "mode": "NULLABLE"},
            {"name": "order_delivered_customer_date", "type": "DATETIME", "mode": "NULLABLE"},
            {"name": "order_estimated_delivery_date", "type": "DATETIME", "mode": "NULLABLE"},
        ],
    )

    create_customers_testing_table = BigQueryCreateEmptyTableOperator(
        task_id=f"create_{bq_customers_table}_table",
        dataset_id=bq_testing_dataset,
        table_id=bq_customers_table,
        schema_fields=[
            {"name": "customer_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "customer_unique_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "customer_zip_code_prefix", "type": "INT64", "mode": "REQUIRED"},
            {"name": "customer_city", "type": "STRING", "mode": "NULLABLE"},
            {"name": "customer_state", "type": "STRING", "mode": "NULLABLE"},
        ],
    )

    create_payments_testing_table = BigQueryCreateEmptyTableOperator(
        task_id=f"create_{bq_payments_table}_table",
        dataset_id=bq_testing_dataset,
        table_id=bq_payments_table,
        schema_fields=[
            {"name": "order_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "payment_sequential", "type": "INT64", "mode": "REQUIRED"},
            {"name": "payment_type", "type": "STRING", "mode": "NULLABLE"},
            {"name": "payment_installments", "type": "INT64", "mode": "REQUIRED"},
            {"name": "payment_value", "type": "FLOAT64", "mode": "NULLABLE"},
        ],
    )


    """
    #### Transfer data from GCS to BigQuery
    Moves the data uploaded to GCS in the previous step to BigQuery, where
    Great Expectations can run a test suite against it.
    """
    transfer_orders_data_to_BQ = GCSToBigQueryOperator(
        task_id=f"{bq_orders_table}_gcs_to_bigquery",
        bucket=BUCKET,
        source_objects=[gcp_orders_data_dest],
        skip_leading_rows=1,
        destination_project_dataset_table="{}.{}".format(bq_testing_dataset, bq_orders_table),
        schema_fields=[
            {"name": "order_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "customer_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "order_status", "type": "STRING", "mode": "NULLABLE"},
            {"name": "order_purchase_timestamp", "type": "DATETIME", "mode": "NULLABLE"},
            {"name": "order_approved_at", "type": "DATETIME", "mode": "NULLABLE"},
            {"name": "order_delivered_carrier_date", "type": "DATETIME", "mode": "NULLABLE"},
            {"name": "order_delivered_customer_date", "type": "DATETIME", "mode": "NULLABLE"},
            {"name": "order_estimated_delivery_date", "type": "DATETIME", "mode": "NULLABLE"},
        ],
        source_format="PARQUET",
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        allow_jagged_rows=True,
    )

    transfer_customers_data_to_BQ = GCSToBigQueryOperator(
        task_id=f"{bq_customers_table}_gcs_to_bigquery",
        bucket=BUCKET,
        source_objects=[gcp_customers_data_dest],
        skip_leading_rows=1,
        destination_project_dataset_table="{}.{}".format(bq_testing_dataset, bq_customers_table),
        schema_fields=[
            {"name": "customer_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "customer_unique_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "customer_zip_code_prefix", "type": "INT64", "mode": "REQUIRED"},
            {"name": "customer_city", "type": "STRING", "mode": "NULLABLE"},
            {"name": "customer_state", "type": "STRING", "mode": "NULLABLE"},
        ],
        source_format="PARQUET",
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        allow_jagged_rows=True,
    )

    transfer_payments_data_to_BQ = GCSToBigQueryOperator(
        task_id=f"{bq_payments_table}_gcs_to_bigquery",
        bucket=BUCKET,
        source_objects=[gcp_payments_data_dest],
        skip_leading_rows=1,
        destination_project_dataset_table="{}.{}".format(bq_testing_dataset, bq_payments_table),
        schema_fields=[
            {"name": "order_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "payment_sequential", "type": "INT64", "mode": "REQUIRED"},
            {"name": "payment_type", "type": "STRING", "mode": "NULLABLE"},
            {"name": "payment_installments", "type": "INT64", "mode": "REQUIRED"},
            {"name": "payment_value", "type": "FLOAT64", "mode": "NULLABLE"},
        ],
        source_format="PARQUET",
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        allow_jagged_rows=True,
    )


    """
    #### Great Expectations suite
    Run the Great Expectations suite on the table.
    """
    ge_orders_validation = GreatExpectationsOperator(
        task_id=f"{bq_orders_table}_table_validation",
        data_context_root_dir=ge_root_dir,
        checkpoint_name='olist_orders_chk',
        return_json_dict=True
    )

    ge_customers_validation = GreatExpectationsOperator(
        task_id=f"{bq_customers_table}_table_validation",
        data_context_root_dir=ge_root_dir,
        checkpoint_name='olist_customers_chk',
        return_json_dict=True
    )

    ge_payments_validation = GreatExpectationsOperator(
        task_id=f"{bq_payments_table}_table_validation",
        data_context_root_dir=ge_root_dir,
        checkpoint_name='olist_payments_chk',
        return_json_dict=True
    )

    check_orders_validation_result = BranchPythonOperator(
        task_id=f'check_{bq_orders_table}_validation_result',
        python_callable=check_validation_result,
        op_kwargs = {"table_validated": bq_orders_table},
        trigger_rule='all_done',
        provide_context=True
    )

    check_customers_validation_result = BranchPythonOperator(
        task_id=f'check_{bq_customers_table}_validation_result',
        python_callable=check_validation_result,
        op_kwargs = {"table_validated": bq_customers_table},
        trigger_rule='all_done',
        provide_context=True
    )

    check_payments_validation_result = BranchPythonOperator(
        task_id=f'check_{bq_payments_table}_validation_result',
        python_callable=check_validation_result,
        op_kwargs = {"table_validated": bq_payments_table},
        trigger_rule='all_done',
        provide_context=True
    )

    copy_olist_orders_testing_table_to_staging_if_pass = BigQueryToBigQueryOperator(
        task_id=f'copy_{bq_orders_table}_table_to_staging_if_pass',
        source_project_dataset_tables=f'{bq_testing_dataset}.{bq_orders_table}',  # Specify the source table
        destination_project_dataset_table='the-data-engineering-project.de_dataset_staging.olist_orders_staging',  # Specify the destination table
        write_disposition='WRITE_TRUNCATE',  # Options: WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY
        create_disposition='CREATE_IF_NEEDED',  # Options: CREATE_IF_NEEDED, CREATE_NEVER
        trigger_rule='all_success'
    )

    copy_olist_customers_testing_table_to_staging_if_pass = BigQueryToBigQueryOperator(
        task_id=f'copy_{bq_customers_table}_table_to_staging_if_pass',
        source_project_dataset_tables=f'{bq_testing_dataset}.{bq_customers_table}',  # Specify the source table
        destination_project_dataset_table='the-data-engineering-project.de_dataset_staging.olist_customers_staging',  # Specify the destination table
        write_disposition='WRITE_TRUNCATE',  # Options: WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY
        create_disposition='CREATE_IF_NEEDED',  # Options: CREATE_IF_NEEDED, CREATE_NEVER
        trigger_rule='all_success'
    )

    copy_olist_payments_testing_table_to_staging_if_pass = BigQueryToBigQueryOperator(
        task_id=f'copy_{bq_payments_table}_table_to_staging_if_pass',
        source_project_dataset_tables=f'{bq_testing_dataset}.{bq_payments_table}',  # Specify the source table
        destination_project_dataset_table='the-data-engineering-project.de_dataset_staging.olist_payments_staging',  # Specify the destination table
        write_disposition='WRITE_TRUNCATE',  # Options: WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY
        create_disposition='CREATE_IF_NEEDED',  # Options: CREATE_IF_NEEDED, CREATE_NEVER
        trigger_rule='all_success'
    )

    """
    #### Delete test dataset and table
    Clean up the dataset and table created for the example.
    """
    delete_dataset = BigQueryDeleteDatasetOperator(
        task_id="delete_dataset",
        project_id=PROJECT_ID,
        dataset_id=bq_testing_dataset,
        delete_contents=True,
        trigger_rule="all_done"
    )

    begin = DummyOperator(task_id="begin")
    end = DummyOperator(task_id="end")
    validation_fails = DummyOperator(task_id="validation_fails")

    begin >> create_testing_dataset >> [create_orders_testing_table,create_customers_testing_table,create_payments_testing_table]
    create_orders_testing_table >> transfer_orders_data_to_BQ >> ge_orders_validation >> check_orders_validation_result
    check_orders_validation_result >> [copy_olist_orders_testing_table_to_staging_if_pass, validation_fails] >> delete_dataset
    create_customers_testing_table >> transfer_customers_data_to_BQ >> ge_customers_validation >> check_customers_validation_result
    check_customers_validation_result >> [copy_olist_customers_testing_table_to_staging_if_pass, validation_fails] >> delete_dataset
    create_payments_testing_table >> transfer_payments_data_to_BQ >> ge_payments_validation >> check_payments_validation_result
    check_payments_validation_result >> [copy_olist_payments_testing_table_to_staging_if_pass, validation_fails] >> delete_dataset
    delete_dataset >> end

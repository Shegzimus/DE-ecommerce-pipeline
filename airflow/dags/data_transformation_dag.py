from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator

from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="data_transformation_dag",
    description="Perform transformation and consolidation for downstream analytical use case.",
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
) as dag:

    transformation_and_consolidation_task = BigQueryExecuteQueryOperator(
        task_id="transformation_and_consolidation_task",
        sql="""
WITH order_value_table AS (SELECT order_id, SUM(payment_value) AS total_order_value
                           FROM `de_dataset_staging.olist_payments_staging`
                           GROUP BY 1)

SELECT o.order_id, o.customer_id, o.order_purchase_timestamp, FORMAT_TIMESTAMP('%Y-%m', o.order_purchase_timestamp) AS order_year_month,
       c.customer_unique_id, c.customer_state, v.total_order_value
FROM `de_dataset_staging.olist_orders_staging` o
JOIN `de_dataset_staging.olist_customers_staging` c
ON o.customer_id = c.customer_id
AND o.order_status = "delivered"
JOIN order_value_table v
ON o.order_id = v.order_id;
    """,

    destination_dataset_table="de_dataset_warehouse.order_customer_value_data",
    write_disposition="WRITE_TRUNCATE",
    use_legacy_sql=False,
    time_partitioning={
     'field': 'order_purchase_timestamp',
     'type': 'YEAR'
}
)

    begin = DummyOperator(task_id="begin")
    end = DummyOperator(task_id="end")

    begin >> transformation_and_consolidation_task >> end

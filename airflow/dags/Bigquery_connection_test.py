from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator, 
    BigQueryGetDatasetOperator
)
from datetime import datetime

# Define the DAG
with DAG(
    dag_id="bgtest",  # Removed the trailing space in DAG ID
    schedule_interval="@once",  # Correct parameter name
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "bigquery"],
) as dag:

    # Task to create a BigQuery dataset
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset",
        dataset_id="new_dataset",
        #google_cloud_conn_id="my_gcp_conn"  # Specify your GCP connection here
    )

    # Task to get metadata for the dataset
    get_dataset = BigQueryGetDatasetOperator(
        task_id="get_dataset",  # Changed task_id to use an underscore
        dataset_id="new_dataset",
        #google_cloud_conn_id="my_gcp_conn"  # Ensure the same connection is used
    )

    # Set task dependencies
    create_dataset >> get_dataset

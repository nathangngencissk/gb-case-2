from datetime import datetime

from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)

DAG_ID = "import_base_files_to_bq"
TABLE_NAME = "base_vendas"
DATASET_NAME = "base_vendas_dataset"
PROJECT_ID = "cool-academy-356419"

with DAG(
        dag_id=DAG_ID,
        schedule_interval="@once",
        start_date=datetime(2022, 1, 1),
        description="Full load of csv files DAG",
        catchup=False,
) as dag:
    load_csv = GCSToBigQueryOperator(
        task_id="load_csv",
        bucket="gb-case-2",
        source_objects=["base2017.csv", "base2018.csv", "base2019.csv"],
        destination_project_dataset_table=f"{DATASET_NAME}.{TABLE_NAME}",
        skip_leading_rows=1,
        location="us-east4",
        write_disposition="WRITE_TRUNCATE",
    )

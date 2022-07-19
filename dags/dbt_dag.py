from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import datetime

DBT_PROJECT_DIR = "/opt/airflow/dags/dbt"

with DAG(
    dag_id='dbt_dag',
    start_date=datetime(2022, 1, 1),
    description='An Airflow DAG to invoke simple dbt commands',
    schedule_interval="0 3 * * *",
    catchup=False
) as dag:

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"dbt run --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"dbt test --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
    )

    dbt_run >> dbt_test
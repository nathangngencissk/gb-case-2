from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.secret_manager import SecretsManagerHook

DAG_ID = "scrap_twitter"
TABLE_ID = "cool-academy-356419.base_vendas_dataset.tweets_linha_dez_2019"
SECRET_ID = "TWITTER_TOKEN"


def get_twitter_token(**kwargs):
    secrets_hook = SecretsManagerHook(delegate_to=None)
    return secrets_hook.get_secret(secret_id=SECRET_ID)


def load_data(**kwargs):
    import json

    from google.cloud import bigquery
    import pandas as pd

    ti = kwargs["ti"]
    tweets = ti.xcom_pull(task_ids="consume_api")

    hook = BigQueryHook(delegate_to=None, use_legacy_sql=False, location="us-east4")
    client = hook.get_client()

    dataframe = pd.DataFrame(json.loads(tweets)["data"])

    job_config = bigquery.job.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

    job = client.load_table_from_dataframe(dataframe, TABLE_ID, job_config=job_config)
    job.result()


def get_linha():
    hook = BigQueryHook(delegate_to=None, use_legacy_sql=False, location="us-east4")
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT    LINHA
        FROM      `base_vendas_dataset.agg_vendas_linha_ano_mes`
        WHERE     MONTH = '12'
        AND       YEAR = '2019'
        ORDER BY  TOTAL_SALES DESC
        LIMIT     1;
    """
    )
    result = cursor.fetchall()
    return result[0][0]


with DAG(
        dag_id=DAG_ID,
        schedule_interval="@once",
        start_date=datetime(2022, 1, 1),
        description="Scrap twitter DAG",
        catchup=False,
) as dag:
    get_linha_task = PythonOperator(task_id="get_linha", python_callable=get_linha)

    get_twitter_token_task = PythonOperator(
        task_id="get_twitter_token", python_callable=get_twitter_token
    )

    consume_api_task = SimpleHttpOperator(
        task_id="consume_api",
        http_conn_id="twitter_api_conn",
        data={
            "query": "Boticário {{ ti.xcom_pull(task_ids='get_linha') }} lang:pt",
            "max_results": 50,
            "tweet.fields": "author_id,created_at",
        },
        method="GET",
        endpoint="/2/tweets/search/recent",
        headers={
            "Authorization": "Bearer {{ ti.xcom_pull(task_ids='get_twitter_token') }}"
        },
    )

    load_data_task = PythonOperator(
        task_id="load_data", python_callable=load_data, provide_context=True
    )

    [get_linha_task, get_twitter_token_task] >> consume_api_task >> load_data_task

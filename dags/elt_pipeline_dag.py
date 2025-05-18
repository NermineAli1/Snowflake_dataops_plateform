from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from etl_pipeline.extract.extract_google_ads import extract_google_ads_data
from etl_pipeline.transform.transform_data import transform_data
from etl_pipeline.load.load_to_snowflake import load_to_snowflake

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    'marketing_etl_pipeline',
    default_args=default_args,
    description='Pipeline ETL vers Snowflake avec données Google Ads',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    def extract():
        return extract_google_ads_data()

    def transform(ti):
        raw_data = ti.xcom_pull(task_ids='extract_data')
        return transform_data(raw_data)

    def load(ti):
        transformed_df = ti.xcom_pull(task_ids='transform_data')
        load_to_snowflake(transformed_df)

    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract,
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform,
    )

    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load,
    )

    extract_task >> transform_task >> load_task

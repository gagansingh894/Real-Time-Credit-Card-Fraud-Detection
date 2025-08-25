from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from transactions_producer import main

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    'retries': 2,
}

with DAG(
    dag_id="kafka_transactions_producer",
    default_args=default_args,
    start_date=datetime(2025, 8, 24),
    schedule_interval=None,
    catchup=False,
) as dag:

    kafka_producer_task = PythonOperator(
        task_id="run_kafka_producer",
        python_callable=main.run
    )
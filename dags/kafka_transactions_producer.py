from datetime import datetime

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    'retries': 1,
}

with DAG(
    dag_id="spark_transactions_producer",
    default_args=default_args,
    start_date=datetime(2025, 8, 24),
    schedule_interval=None,
    catchup=False,
) as dag:
    spark_transactions_producer_task = SparkSubmitOperator(
        task_id="spark_submit_transactions_producer",
        application="/opt/airflow/spark_jobs/etl/main.py",
        conn_id="spark_default",
        name="spark_transactions_producer",
        executor_memory='512m',
        driver_memory='512m',
        total_executor_cores=2,
        verbose=True,
        conf={
            'spark.master': 'spark://spark-master:7077',
            'spark.submit.deployMode': 'client'
        }
    )



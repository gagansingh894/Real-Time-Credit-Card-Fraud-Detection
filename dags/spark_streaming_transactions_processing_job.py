from datetime import datetime

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    dag_id="spark_transactions_processor",
    schedule_interval=None,
    catchup=False,
) as dag:
    spark_stream_processing_task = SparkSubmitOperator(
        task_id="spark_submit_transactions_processor",
        conn_id="spark_master",
        name="spark_transactions_processor",
        application="/opt/bitnami/spark/transactions_processor/main.py",
        verbose=True,
    )



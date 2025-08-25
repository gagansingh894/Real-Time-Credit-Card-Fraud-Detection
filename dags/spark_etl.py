from datetime import datetime

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    dag_id="spark_etl",
    schedule_interval=None,
    catchup=False,
) as dag:
    spark_etl_task = SparkSubmitOperator(
        task_id="spark_submit_etl",
        conn_id="spark_master",
        name="spark_cassandra_etl",
        application="/opt/bitnami/spark/spark_jobs/etl/main.py",
        verbose=True,
    )



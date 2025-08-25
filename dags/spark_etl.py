from datetime import datetime

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    'retries': 2,
}

with DAG(
    dag_id="spark_etl",
    default_args=default_args,
    start_date=datetime(2025, 8, 24),
    schedule_interval=None,
    catchup=False,
) as dag:
    spark_etl_task = SparkSubmitOperator(
        task_id="spark_submit_etl",
        conn_id="spark_master",
        name="spark_cassandra_etl",
        application="/opt/bitnami/spark/spark_jobs/etl/main.py",
        verbose=True,
        conf={
            'spark.master': 'spark://spark-master:7077',
            'spark.submit.deployMode': 'cluster'
        },
    )



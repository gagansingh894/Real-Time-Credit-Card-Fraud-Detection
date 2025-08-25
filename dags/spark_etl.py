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
        application="./spark_jobs/etl/main.py",
        packages="com.datastax.spark:spark-cassandra-connector_2.12:3.5.0",
        start_date=datetime.now(),
        verbose=True,
    )



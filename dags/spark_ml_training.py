from datetime import datetime

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    dag_id="spark_ml_training",
    schedule_interval=None,
    catchup=False,
) as dag:
    spark_ml_training_task = SparkSubmitOperator(
        task_id="spark_submit_ml_training",
        conn_id="spark_master",
        name="spark_random_forest_training",
        application="./spark_jobs/ml_training/main.py",
        packages="com.datastax.spark:spark-cassandra-connector_2.12:3.5.0",
        start_date=datetime.now(),
        verbose=True,
    )



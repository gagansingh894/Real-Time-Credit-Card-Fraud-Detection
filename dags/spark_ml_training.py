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
        application="/opt/bitnami/spark/spark_jobs/ml_training/main.py",
        name="spark_random_forest_training",
        verbose=True,
    )



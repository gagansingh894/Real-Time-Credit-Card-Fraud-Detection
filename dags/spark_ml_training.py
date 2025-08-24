from datetime import datetime

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    'retries': 1,
}

with DAG(
    dag_id="spark_ml_training",
    default_args=default_args,
    start_date=datetime(2025, 8, 24),
    schedule_interval=None,
    catchup=False,
) as dag:
    spark_ml_training_task = SparkSubmitOperator(
        task_id="spark_submit_ml_training",
        conn_id="spark_default",
        name="spark_random_forest_training",
        verbose=True,
        conf={
            'spark.master': 'spark://spark-master:7077',
            'spark.submit.deployMode': 'client'
        },
        java_class=None,
        packages=None,
        application_args=[],
        executor_cores=None,
        executor_memory=None,
        driver_memory=None,
        driver_class_path=None,
        py_executable="python3 -m spark_jobs.ml_training.main"
    )



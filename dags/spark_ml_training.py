from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    'retries': 1,
}

with DAG(
    dag_display_name="spark_ml_training",
    dag_id="spark_ml_training",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:
    spark_ml_training_task = SparkSubmitOperator(
        task_id="spark_submit_ml_training",
        application="/opt/airflow/spark_jobs/ml_training/main.py",
        conn_id="spark_default",
        name="spark_random_forest_training",
        executor_memory='512m',
        driver_memory='512m',
        total_executor_cores=2,
        verbose=True,
        conf={
            'spark.master': 'spark://spark-master:7077',
            'spark.submit.deployMode': 'cluster'  # driver runs on cluster
        }
    )



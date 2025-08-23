from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    'retries': 1,
}

with DAG(
    dag_id="spark_etl",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:
    spark_etl_task = SparkSubmitOperator(
        task_id="spark_submit_etl",
        application="/opt/airflow/spark_jobs/etl/main.py",
        conn_id="spark_default",
        name="spark_cassandra_etl",
        executor_memory='512m',
        driver_memory='512m',
        total_executor_cores=2,
        verbose=True,
        conf={
            'spark.master': 'spark://spark-master:7077',
            'spark.submit.deployMode': 'cluster'  # driver runs on cluster
        }
    )



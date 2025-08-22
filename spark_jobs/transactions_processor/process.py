import mlflow
from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col, to_timestamp, broadcast, round
from spark_jobs.schemas import transaction_schema
from spark_jobs.utils import haversine_distance
from spark_jobs.utils.config import (
    CASSANDRA_KEYSPACE,
    KAFKA_TOPIC,
    PROCESSED_DF_COLUMNS,
    MLFLOW_TRACKING_URI,
    MLFLOW_EXPERIMENT_NAME
)


def get_artefacts() -> (PipelineModel, PipelineModel):
    """
    Retrieves the spark pipeline models from MLFlow - preprocessor and model
    """
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(MLFLOW_EXPERIMENT_NAME)
    runs = mlflow.search_runs(experiment_names=[MLFLOW_EXPERIMENT_NAME])
    run_id = runs[0].info.run_id


    preprocessor = mlflow.spark.load_model(f"runs:/{run_id}/preprocessor")
    model = mlflow.spark.load_model(f"runs:/{run_id}/model")
    return preprocessor, model


def read_and_cache_customer_data(spark: SparkSession) -> DataFrame:
    """
    Load customer data from Cassandra and cache for joins
    """
    customer_df = (
        spark.read
        .format("org.apache.spark.sql.cassandra")
        .options(table_name="customers", keyspace=CASSANDRA_KEYSPACE)
        .load()
        .select("cc_num", "age", "lat", "long")
    )
    customer_df.cache()
    return customer_df


def read_kafka_stream(spark: SparkSession) -> DataFrame:
    """
    Read Kafka stream and parse to structured DataFrame
    """
    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed_df = raw_df.selectExpr("CAST(value AS STRING) AS json", "partition", "offset")
    return (
        parsed_df
        .select(from_json(col("json"), transaction_schema).alias("txn"), "partition", "offset")
        .select("txn.*", "partition", "offset")
        .withColumn("trans_time", to_timestamp(col("trans_time"), "yyyy-MM-dd HH:mm:ss"))
    )


def write_to_cassandra(batch_df: DataFrame, _: int):
    """
    Writes predictions to Cassandra
    """
    fraud_df = batch_df.filter(col("is_fraud") == 1.0)
    non_fraud_df = batch_df.filter(col("is_fraud") != 1.0)

    fraud_df.write.format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table="fraud_transactions", keyspace=CASSANDRA_KEYSPACE).save()

    non_fraud_df.write.format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table="non_fraud_transactions", keyspace=CASSANDRA_KEYSPACE).save()


def preprocess_predict_persist(customer_df: DataFrame, parsed_df: DataFrame, preprocessor: PipelineModel, model: PipelineModel):
    """
    Preprocess, predict and persist streaming data
    """
    processed_df = (
        parsed_df
        .join(broadcast(customer_df), on="cc_num", how="inner")
        .withColumn(
            "distance",
            round(haversine_distance(col("lat"), col("long"), col("merch_lat"), col("merch_long")), 2)
        )
        .select(PROCESSED_DF_COLUMNS)
    )

    predictions_df = model.transform(preprocessor.transform(processed_df)).withColumnRenamed("prediction", "is_fraud")

    # Use foreachBatch for custom Cassandra writes
    query = (
        predictions_df.writeStream
        .foreachBatch(write_to_cassandra)
        .outputMode("append")
        .option("checkpointLocation", "/tmp/spark_checkpoint_fraud")  # required for streaming
        .start()
    )

    query.awaitTermination()


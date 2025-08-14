from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import split, col, concat_ws, to_timestamp, date_diff, current_date, broadcast, round
from pyspark.sql.types import IntegerType

from spark_jobs.etl.config import CASSANDRA_KEYSPACE
from spark_jobs.schemas import *
from spark_jobs.utils import haversine_distance

PROCESSED_DF_COLUMNS = ["cc_num", "trans_num", "trans_time", "category", "merchant", "amt", "merch_lat", "merch_long", "distance", "age", "is_fraud"]

def load_customer_data(spark: SparkSession, customer_data_path: str) -> DataFrame:
    """
    Function to load customer data from csv file to spark SQL DataFrame.
    :param spark:
    :param customer_data_path:
    :return: DataFrame
    """
    return (
        spark.read
        .format("csv")
        .schema(customer_schema)
        .option("header", "true")
        .load(customer_data_path)
    )

def load_transaction_data(spark: SparkSession, transactions_data_path: str) -> DataFrame:
    """
    Function to load transaction data from csv file to spark SQL DataFrame.
    :param spark:
    :param transactions_data_path:
    :return: DataFrame
    """
    return (
        spark.read
        .format("csv")
        .schema(transaction_schema)
        .option("header", "true")
        .load(transactions_data_path)
    )

def transform_transaction_dates(transaction_df: DataFrame) -> DataFrame:
    """
    Function to transform transaction date columns to transaction time column
    :param transaction_df:
    :return: DataFrame
    """
    return (
        transaction_df
            .withColumn("trans_date", split(col("trans_date"), "T").getItem(0))
            .withColumn("trans_time", concat_ws(" ", col("trans_date"), col("trans_time")))
            .withColumn("trans_time", to_timestamp(col("trans_time"), "yyyy-MM-dd HH:mm:ss"))
    )

def calculate_customer_age(customer_df: DataFrame) -> DataFrame:
    """
    Function to calculate customer age and return a new DataFrame
    :param customer_df:
    :return: DataFrame
    """
    return (
        customer_df
            .withColumn("age", (date_diff(current_date(), col("dob")) / 365).cast(IntegerType()))
    )

def process_data(customer_age_df: DataFrame, transaction_df: DataFrame) -> (DataFrame, DataFrame):
    """
    Function to join customer and transaction data into a single DataFrame, create a new column distance and split data
    into fraudulent and non fraudulent transactions.
    :param customer_age_df:
    :param transaction_df:
    :return: (DataFrame, DataFrame)
    """
    processed_df = transaction_df.join(broadcast(customer_age_df), ["cc_num"]) \
        .withColumn("distance",
                    round(haversine_distance(col("lat"), col("long"), col("merch_lat"), col("merch_long")), 2)) \
        .select(PROCESSED_DF_COLUMNS)

    processed_df.cache()

    # split fraud and non fraud data
    fraud_df = processed_df.filter(col("is_fraud") == 1)
    non_fraud_df = processed_df.filter(col("is_fraud") == 0)

    return fraud_df, non_fraud_df

def write_to_cassandra(customer_df:DataFrame, fraud_df:DataFrame, non_fraud_df: DataFrame) -> None:
    """
    Function to write to cassandra tables
    :param customer_df:
    :param fraud_df:
    :param non_fraud_df:
    :return:
    """
    customer_df.drop("age") \
        .write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table="customers", keyspace=CASSANDRA_KEYSPACE) \
        .save()

    fraud_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table="fraud_transactions", keyspace=CASSANDRA_KEYSPACE) \
        .save()

    non_fraud_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table="non_fraud_transactions", keyspace=CASSANDRA_KEYSPACE) \
        .save()

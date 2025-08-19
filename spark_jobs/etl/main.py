import logging
from spark_jobs.utils import config
from spark_jobs.etl.ingest import load_customer_data, load_transaction_data, transform_transaction_dates, \
    calculate_customer_age, process_data, write_to_cassandra
from spark_jobs.utils.session import get_spark_session

logger = logging.getLogger(__name__)

def ingest_data_job():
    # create spark session
    spark = get_spark_session("cassandra_data_ingest")

    # load data from file system
    customer_df = load_customer_data(spark, config.CUSTOMER_DATA_PATH)
    transaction_df = load_transaction_data(spark, config.TRANSACTION_DATA_PATH)

    # process data
    customer_df = calculate_customer_age(customer_df)
    transaction_df = transform_transaction_dates(transaction_df)
    fraud_df, non_fraud_df = process_data(customer_df, transaction_df)

    # write data to cassandra
    write_to_cassandra(customer_df, fraud_df, non_fraud_df)


if __name__ == "__main__":
    logger.info('Starting ingest job')
    ingest_data_job()
    logger.info('Finished ingest job')
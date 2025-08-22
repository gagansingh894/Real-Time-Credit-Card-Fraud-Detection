import logging

from spark_jobs.transactions_processor.process import get_artefact, read_and_cache_customer_data, \
    read_data_from_kafka, preprocess_predict_persist

from spark_jobs.utils.session import get_spark_session

logger = logging.getLogger(__name__)

def process_and_predict_job():
    # load model artefacts
    pipeline = get_artefact()

    # create spark session
    session = get_spark_session("transactions_processor")

    # load customer data
    customer_df = read_and_cache_customer_data(session)

    # read data from kafka stream
    parsed_df = read_data_from_kafka(session)

    preprocess_predict_persist(customer_df, parsed_df, pipeline)

if __name__ == "__main__":
    logger.info("Starting transactions processor")
    process_and_predict_job()
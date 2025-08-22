import logging

from spark_jobs.transactions_processor.process import get_artefacts, read_and_cache_customer_data, \
    read_kafka_stream, preprocess_predict_persist

from spark_jobs.utils.session import get_spark_session

logger = logging.getLogger(__name__)

def process_and_predict_job():
    # load model artefacts
    preprocessor, model = get_artefacts()

    # create spark session
    session = get_spark_session("transactions_processor")

    # load customer data
    customer_df = read_and_cache_customer_data(session)

    # read data from kafka stream
    parsed_df = read_kafka_stream(session)

    preprocess_predict_persist(customer_df, parsed_df, preprocessor, model)

if __name__ == "__main__":
    logger.info("Starting transactions processor")
    process_and_predict_job()
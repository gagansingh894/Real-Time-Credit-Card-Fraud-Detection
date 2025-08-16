import logging

from spark_jobs.ml_training.train import preprocessing_pipeline, balance_features_dataframe, train_and_save_model
from spark_jobs.utils.config import CASSANDRA_KEYSPACE
from spark_jobs.utils.session import get_spark_session

logger = logging.getLogger(__name__)

def train_model_job():
    # create spark session
    spark = get_spark_session("random_forest_model_training")

    # preprocessing pipeline
    preprocessed_df = preprocessing_pipeline(spark, CASSANDRA_KEYSPACE)

    # balance dataframe since the ratio of fraudulent transactions is small compared to valid transactions
    train_df = balance_features_dataframe(preprocessed_df)

    # train and persist model
    train_and_save_model(train_df)

if __name__ == "__main__":
    logger.info("Starting ML training job")
    train_model_job()
    logger.info("ML training job completed")



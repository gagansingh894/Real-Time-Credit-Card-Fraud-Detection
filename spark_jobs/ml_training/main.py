import logging

from spark_jobs.ml_training.train import preprocessing_pipeline, balance_features_dataframe, train_model, \
    persist_artefacts
from spark_jobs.utils.config import CASSANDRA_KEYSPACE
from spark_jobs.utils.session import get_spark_session

logger = logging.getLogger(__name__)

def train_model_job():
    # create spark session
    spark = get_spark_session("random_forest_model_training")

    # preprocessing pipeline
    preprocessed_df, preprocessor = preprocessing_pipeline(spark, CASSANDRA_KEYSPACE)

    # balance dataframe since the ratio of fraudulent transactions is small compared to valid transactions
    train_df = balance_features_dataframe(preprocessed_df)

    # train model - we pass preprocessor to create a single pipeline for transformation and prediction
    trained_model = train_model(train_df, preprocessor)

    # persist to mlflow
    persist_artefacts(preprocessor_pipeline_model, trained_model)

if __name__ == "__main__":
    logger.info("Starting ML training job")
    train_model_job()
    logger.info("ML training job completed")



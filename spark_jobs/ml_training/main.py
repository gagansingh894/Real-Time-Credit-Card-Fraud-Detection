import logging

from spark_jobs.ml_training.artefacts import Artefacts
from spark_jobs.ml_training.train import preprocessing_pipeline, balance_features_dataframe, train_model, \
    persist_artefacts, evaluate
from spark_jobs.utils.config import CASSANDRA_KEYSPACE
from spark_jobs.utils.session import get_spark_session

logger = logging.getLogger(__name__)

def train_model_job():
    # create spark session
    spark = get_spark_session("random_forest_model_training")

    # preprocessing pipeline
    preprocessed_df, preprocessor_pipeline_model = preprocessing_pipeline(spark, CASSANDRA_KEYSPACE)

    # balance dataframe since the ratio of fraudulent transactions is small compared to valid transactions by adding weight column
    train_df = balance_features_dataframe(preprocessed_df)

    # train/test split and train model
    trained_model, test_df = train_model(train_df)

    # evaluation metrics
    metrics = evaluate(trained_model, test_df)

    # persist to mlflow
    persist_artefacts(preprocessor_pipeline_model, trained_model, metrics)

if __name__ == "__main__":
    logger.info("Starting ML training job")
    train_model_job()
    logger.info("ML training job completed")
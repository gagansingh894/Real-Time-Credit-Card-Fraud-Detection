import uuid

import mlflow
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.sql import SparkSession, DataFrame

from spark_jobs.utils.session import get_spark_session
from spark_jobs.utils.config import *

spark_session = get_spark_session("ml_training")

# read data from cassandra
def _read_data_from_cassandra(spark: SparkSession, keyspace:str, table_name: str) -> DataFrame:
    return (spark.read
            .format("org.apache.spark.sql.cassandra")
            .options(table=table_name, keyspace=keyspace)
            .load()
            .select("cc_num", "category", "merchant", "distance", "amt", "age", "is_fraud")
    )

def preprocessing_pipeline(spark: SparkSession, keyspace:str) -> tuple[DataFrame, PipelineModel]:
    # prepare training data
    fraud_df = _read_data_from_cassandra(spark, keyspace, "fraud_transactions")
    non_fraud_df = _read_data_from_cassandra(spark, keyspace, table_name="non_fraud_transactions")
    transactions_df = fraud_df.union(non_fraud_df)

    # build the preprocessing pip   eline
    categorical_columns = ["category", "merchant"]
    numerical_columns = ["distance", "amt", "age"]

    indexers = [StringIndexer(inputCol=col, outputCol=f"{col}_idx") for col in categorical_columns]
    encoders = [OneHotEncoder(inputCol=f"{col}_idx", outputCol=f"{col}_ohe") for col in categorical_columns]
    assembler = VectorAssembler(
        inputCols=[f"{col}_ohe" for col in categorical_columns] + numerical_columns,
        outputCol="features",
    )

    pipeline = Pipeline(stages=indexers + encoders + [assembler])

    preprocessor_pipeline_model = pipeline.fit(transactions_df)
    # preprocessor_pipeline.write().overwrite().save(PREPROCESSOR_PATH)

    features_df = preprocessor_pipeline_model.transform(transactions_df)
    return features_df, preprocessor_pipeline_model

def balance_features_dataframe(features_df: DataFrame) -> DataFrame:
    fraud_features_with_label_df = features_df.filter(features_df.is_fraud == 1) \
        .withColumnRenamed("is_fraud", "label") \
        .select("features", "label")

    non_fraud_features_with_label_df = features_df.filter(features_df.is_fraud == 0)
    fraud_count = fraud_features_with_label_df.count()
    fraction = fraud_count / non_fraud_features_with_label_df.count()
    non_fraud_features_with_label_balanced_df = non_fraud_features_with_label_df.sample(withReplacement=False, fraction=fraction) \
        .withColumnRenamed("is_fraud", "label") \
        .select("features", "label")

    final_df = fraud_features_with_label_df.union(non_fraud_features_with_label_balanced_df)
    return final_df

def train_model(train_df: DataFrame) -> PipelineModel:
    random_forest = RandomForestClassifier(featuresCol="features", labelCol="label", numTrees=10)
    pipeline = Pipeline(stages=[random_forest])
    pipeline_model = pipeline.fit(train_df)

    return pipeline_model

def persist_artefacts(preprocessor_pipeline: PipelineModel, trained_model: PipelineModel) -> None:
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(MLFLOW_EXPERIMENT_NAME)

    with mlflow.start_run():
        version = uuid.uuid4()
        mlflow.spark.log_model(spark_model=preprocessor_pipeline, artifact_path=f"preprocessor_pipeline_{version}")
        mlflow.spark.log_model(spark_model=trained_model, artifact_path=f"model_{version}")

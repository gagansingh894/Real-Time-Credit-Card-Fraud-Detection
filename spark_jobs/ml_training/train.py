import mlflow

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import when

from spark_jobs.ml_training.metrics import Metrics
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
    """
    Adds class weights to the features dataframe to avoid overfitting in the training process.
    :param features_df: Dataframe containing features to balance.
    :return: DataFrame with weight column
    """
    total = features_df.count()
    fraud_count = features_df.filter(features_df.is_fraud == 1).count()
    non_fraud_count = total - fraud_count
    fraud_weight = total / (2.0 * fraud_count)
    non_fraud_weight = total / (2.0 * non_fraud_count)

    weighted_df = features_df \
        .withColumn("class_weight",
                    when(features_df.is_fraud == 1, fraud_weight) \
                    .otherwise(non_fraud_weight)
                    ) \
        .withColumnRenamed("is_fraud", "label") \
        .select("features", "label", "class_weight")
    return weighted_df

def train_model(df: DataFrame) -> (PipelineModel, DataFrame):
    """
    Splits the data into training and testing sets and performs training using random forest classifier.
    :param df: Input dataframe for splitting into training and testing sets and performs training using random forest classifier.
    :return: tuple of Pipeline model and test dataframe
    """
    # train test split
    train_df, test_df = df.randomSplit([0.7, 0.3], seed=42)

    rf = RandomForestClassifier(featuresCol="features", labelCol="label", weightCol="class_weight", numTrees=100, seed=42)
    pipeline = Pipeline(stages=[rf])
    pipeline_model = pipeline.fit(train_df)

    return pipeline_model, test_df

def evaluate(trained_model: PipelineModel, test_df: DataFrame) -> Metrics:
    predictions = trained_model.transform(test_df)

    # binary metrics
    binary_evaluator = BinaryClassificationEvaluator(
        labelCol="label",
        rawPredictionCol="rawPrediction",
        metricName="areaUnderROC"
    )
    auc = binary_evaluator.evaluate(predictions)

    precision_evaluator = MulticlassClassificationEvaluator(labelCol="label", metricName="precisionByLabel")
    recall_evaluator = MulticlassClassificationEvaluator(labelCol="label", metricName="recallByLabel")
    f1_evaluator = MulticlassClassificationEvaluator(labelCol="label", metricName="f1")
    precision = precision_evaluator.evaluate(predictions)
    recall = recall_evaluator.evaluate(predictions)
    f1 = f1_evaluator.evaluate(predictions)

    metrics = Metrics(precision=precision, recall=recall, f1_score=f1, auc=auc)
    return metrics

def persist_artefacts(preprocessor_pipeline: PipelineModel, trained_model: PipelineModel, metrics: Metrics) -> None:
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(MLFLOW_EXPERIMENT_NAME)

    with mlflow.start_run():
        mlflow.log_metric("auc", metrics.auc)
        mlflow.log_metric("precision", metrics.precision)
        mlflow.log_metric("recall", metrics.recall)
        mlflow.log_metric("f1_score", metrics.f1_score)

        mlflow.spark.log_model(spark_model=preprocessor_pipeline, artifact_path=f"preprocessor")
        mlflow.spark.log_model(spark_model=trained_model, artifact_path=f"model")
CASSANDRA_HOST = 'cassandra'
CASSANDRA_PORT = 9042

CUSTOMER_DATA_PATH = 'spark_jobs/data/customer.csv'
TRANSACTION_DATA_PATH = 'spark_jobs/data/transactions.csv'

PROCESSED_DF_COLUMNS = ["cc_num", "trans_num", "trans_time", "category", "merchant", "amt", "merch_lat", "merch_long", "distance", "age", "is_fraud"]

CASSANDRA_KEYSPACE = 'creditcard'
CASSANDRA_CUSTOMERS_TABLE = 'customers'
CASSANDRA_FRAUD_TRANSACTIONS_TABLE = 'fraud_transactions'
CASSANDRA_NON_FRAUD_TRANSACTIONS_TABLE = 'non_fraud_transactions'

KAFKA_TOPIC = 'transactions'

PREPROCESSOR_PATH = 'spark_jobs/ml_training/artefacts/preprocessing'
RANDOM_FOREST_MODEL_PATH = 'spark_jobs/ml_training/artefacts/randomforest'

MLFLOW_TRACKING_URI = 'http:mlflow:5005'
MLFLOW_EXPERIMENT_NAME = 'fraud-detection'

SPARK_MASTER = 'spark://spark-master:7077'
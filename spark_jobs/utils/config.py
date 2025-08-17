CASSANDRA_HOST = '0.0.0.0'
CASSANDRA_PORT = 9042

CUSTOMER_DATA_PATH = 'spark_jobs/data/customer.csv'
TRANSACTION_DATA_PATH = 'spark_jobs/data/transactions.csv'

CASSANDRA_KEYSPACE = 'creditcard'
CASSANDRA_CUSTOMERS_TABLE = 'customers'
CASSANDRA_FRAUD_TRANSACTIONS_TABLE = 'fraud_transactions'
CASSANDRA_NON_FRAUD_TRANSACTIONS_TABLE = 'non_fraud_transactions'

PREPROCESSOR_PATH = 'spark_jobs/ml_training/artefacts/preprocessing'
RANDOM_FOREST_MODEL_PATH = 'spark_jobs/ml_training/artefacts/randomforest'

MLFLOW_TRACKING_URI = 'http://0.0.0.0:5000'
MLFLOW_EXPERIMENT_NAME = 'fraud-detection'
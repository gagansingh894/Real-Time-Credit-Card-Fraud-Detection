from pyspark.sql.types import StructType, StringType, StructField, LongType, DoubleType

# transaction_schema defines the common columns for the transactions data
transaction_schema = StructType([
    StructField("cc_num", StringType(), True),
    StructField("first", StringType(), True),
    StructField("last", StringType(), True),
    StructField("trans_num", StringType(), True),
    StructField("trans_date", StringType(), True),
    StructField("trans_time", StringType(), True),
    StructField("unix_time", LongType(), True),
    StructField("category", StringType(), True),
    StructField("merchant", StringType(), True),
    StructField("amt", DoubleType(), True),
    StructField("merch_lat", DoubleType(), True),
    StructField("merch_long", DoubleType(), True)
])

# fraud_checked_transaction_schema has an additional column is_fraud in addition to transaction common columns
fraud_checked_transaction_schema = transaction_schema.add("is_fraud", DoubleType(), True)


from pyspark.sql.types import StructType, StringType, StructField, TimestampType, DoubleType


# customer_schema defines the columns for customer data
customer_schema = StructType([
    StructField("cc_num", StringType(), True),
    StructField("first", StringType(), True),
    StructField("last", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("street", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("zip", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("long", DoubleType(), True),
    StructField("job", StringType(), True),
    StructField("dob", TimestampType(), True)
])
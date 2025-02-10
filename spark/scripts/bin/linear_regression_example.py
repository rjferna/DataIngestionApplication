import json
from encryption_decryption_common import Prpcrypt
from gcp_common import gcp_execute_query

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_timestamp, col, hour, dayofweek, dayofmonth, month, year
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DoubleType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator


# Initialize Spark session
spark = SparkSession.builder \
    .appName("BitcoinHistoryLinearRegression") \
    .config("spark.jars", "/path/to/postgresql-42.3.1.jar") \
    .getOrCreate()

# Postgres Database connection parameters
jdbc_url = "jdbc:postgresql://data-ingestion-postgres-app-container:5432/metadata_utilities"
connection_properties = {
    "user": "metadata_user",
    "password": "admin",
    "driver": "org.postgresql.Driver"
}

# Load Encrypted GCP Keyfile Credentials from Postgres Database and load into Spark DataFrame
df = spark.read.jdbc(url=jdbc_url, table="metadata_utilities.ingestion_connection_info", properties=connection_properties)

# Struct For Security Column
security_token_schema = StructType([
    StructField("token", StringType(), True),
    StructField("access", StringType(), True)
    ])

# Load Security Token column as Json and alias as 'token_access'
credentials_parsed_df = df.withColumn("token_access", from_json(col("security_token"), security_token_schema))\
    .filter(df.connection_name == 'GCS_COINCAP')

# Parse token_access for 'token' and 'access' columns
gcp_df = credentials_parsed_df.withColumn("token", col("token_access").getItem("token"))\
    .withColumn("access", col("token_access").getItem("access"))

# Collect data and assign to variables
connection_name = gcp_df.select("connection_name").collect()[0][0]
connection_url = gcp_df.select("connection_url").collect()[0][0]
user_name = gcp_df.select("user_name").collect()[0][0]
password_encrypted = gcp_df.select("password_encrypted").collect()[0][0]
token = gcp_df.select("token").collect()[0][0]
access = gcp_df.select("access").collect()[0][0]


#-- Decrypt GCP Credentials  --#
pc = Prpcrypt(token)

decrypted_credentials = json.loads(pc.decrypt(password_encrypted))

pc = Prpcrypt(access)

gcp_creds = {}
for name, val in decrypted_credentials.items():
    gcp_creds.update({name: pc.decrypt(val)})



#-- Query GCP and convert response data to DataFrame --#
query = '''
        SELECT time, price_usd, circulating_supply FROM `coincap-data-hub.ref_coincap_data.bitcoin_history`;
        '''

response = gcp_execute_query(
                query=query,
                keyfile_contents=gcp_creds
            )

spark_schema = StructType([
    StructField("time", StringType(), True),
    StructField("price_usd", FloatType(), True),
    StructField("circulating_supply", DoubleType(), True)
    ])

spark_df = spark.createDataFrame(response, schema=spark_schema)

#-- Begin: Linear Regression --#

#-- Preprocess data --#
data = spark_df.withColumn("hour", hour(to_timestamp(col("time") / 1000000))) \
           .withColumn("dayofweek", dayofweek(to_timestamp(col("time") / 1000000))) \
           .withColumn("dayofmonth", dayofmonth(to_timestamp(col("time") / 1000000))) \
           .withColumn("month", month(to_timestamp(col("time") / 1000000))) \
           .withColumn("year", year(to_timestamp(col("time") / 1000000))) \
           .withColumn("price_usd", col("price_usd")) \
           .withColumn("circulating_supply", col("circulating_supply"))


# Features
assembler = VectorAssembler(inputCols=["hour", "dayofweek", "dayofmonth", "month", "year", "circulating_supply"], outputCol="features")
data = assembler.transform(data)


# Split data into training & test sets
train_data, test_data = data.randomSplit([0.8, 0.2], seed=1234)


# Train linear regression model
lr = LinearRegression(featuresCol="features", labelCol="price_usd")
lr_model = lr.fit(train_data)


# Evaluate model
predictions = lr_model.transform(test_data)

evaluator = RegressionEvaluator(labelCol="price_usd", predictionCol="prediction", metricName="rmse")

rmse = evaluator.evaluate(predictions)

print('\n\n\n')

print(f"Root Mean Squared Error (RMSE): {rmse}")

print('\n\n\n')

spark.stop()
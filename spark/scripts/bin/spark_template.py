import json
from encryption_decryption_common import Prpcrypt
from gcp_common import gcp_execute_query

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DoubleType


# Initialize Spark session
spark = SparkSession.builder \
    .appName("Template-Application") \
    .config("spark.jars", "/path/to/postgresql-42.3.1.jar") \
    .getOrCreate()

# Postgres Database connection parameters
jdbc_url = "jdbc:postgresql://data-ingestion-postgres-app-container:5432/metadata_utilities"
connection_properties = {
    "user": "metadata_user",
    "password": "admin",
    "driver": "org.postgresql.Driver"
}

# Load credentials from PostgreSQL table into Spark DataFrame
df = spark.read.jdbc(url=jdbc_url, table="metadata_utilities.ingestion_connection_info", properties=connection_properties)

# Struct For Security Column
security_token_schema = StructType([
    StructField("token", StringType(), True),
    StructField("access", StringType(), True)
    ])

# Reac Security Token column as Json & Load to DataFrame
credentials_parsed_df = df.withColumn("token_access", from_json(col("security_token"), security_token_schema))\
    .filter(df.connection_name == 'GCS_COINCAP')

# Parse token_access for 'token' and 'access' columns
gcp_df = credentials_parsed_df.withColumn("token", col("token_access").getItem("token"))\
    .withColumn("access", col("token_access").getItem("access"))

# Assign values to variables
connection_name = gcp_df.select("connection_name").collect()[0][0]
connection_url = gcp_df.select("connection_url").collect()[0][0]
user_name = gcp_df.select("user_name").collect()[0][0]
password_encrypted = gcp_df.select("password_encrypted").collect()[0][0]
token = gcp_df.select("token").collect()[0][0]
access = gcp_df.select("access").collect()[0][0]


#-- Decrypt GCP Credentials to access --#
pc = Prpcrypt(token)

decrypted_credentials = json.loads(pc.decrypt(password_encrypted))

pc = Prpcrypt(access)

gcp_creds = {}
for name, val in decrypted_credentials.items():
    gcp_creds.update({name: pc.decrypt(val)})



#-- Query GCP and Load Data to DataFrame --#
query = '''
        SELECT time, date, price_usd, circulating_supply FROM `coincap-data-hub.ref_coincap_data.bitcoin_history` limit 10;
        '''

response = gcp_execute_query(
                query=query,
                keyfile_contents=gcp_creds
            )

# Example Schema for Spark DataFrame
spark_schema = StructType([
    StructField("time", StringType(), True),
    StructField("price_usd", FloatType(), True),
    StructField("circulating_supply", DoubleType(), True)
    ])

spark_df = spark.createDataFrame(response, schema=spark_schema)

spark_df.show(10)

spark.stop()
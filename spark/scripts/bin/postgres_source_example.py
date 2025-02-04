from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("PostgresConnection") \
    .config("spark.jars", "/path/to/postgresql-42.3.1.jar") \
    .getOrCreate()

# Database connection parameters
jdbc_url = "jdbc:postgresql://data-ingestion-postgres-app-container:5432/metadata_utilities"
connection_properties = {
    "user": "metadata_user",
    "password": "admin",
    "driver": "org.postgresql.Driver"
}

# Load data from PostgreSQL table into Spark DataFrame
df = spark.read.jdbc(url=jdbc_url, table="metadata_utilities.workflow_audit_details", properties=connection_properties)

# Print out 10 values in the DataFrame
df.show(10)

# Stop Spark session
spark.stop()

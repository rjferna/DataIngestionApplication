import os
import sys
import json
from gcp_common import get_gcp_storage
from dotenv import load_dotenv
from encryption_decryption_common import Prpcrypt
from postgres_common import PostgresDB

from pyspark.sql import SparkSession

try: 
    connection = sys.argv[1]
    asset = sys.argv[2]

    # Load environment variables from .env file
    load_dotenv()

    # Access the environment variables
    db_host = os.getenv("DB_HOST")
    db_database = os.getenv("DB_DATABASE")
    db_port = os.getenv("DB_PORT")
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    
    db = PostgresDB(
        host=db_host,
        database=db_database,
        user=db_user,
        password=db_password
    )

    db.open_connection()
        
    print("Postgres Connection Open...")

    query = f"""
            SELECT 
            connection_id, connection_url, connection_name, password_encrypted, security_token, user_name
            FROM metadata_utilities.ingestion_connection_info
            WHERE UPPER(connection_name) = '{connection.upper()}';
            """

    results = db.execute_query(query)
    
    # Assign Connection Details
    connection_id = results.get("connection_id")
    connection_url = results.get("connection_url")
    connection_name = results.get("connection_name")
    password_encrypted = results.get("password_encrypted")
    security_token = json.loads(results.get("security_token"))
    user_name  = results.get("user_name")

    
    #-- Decrypt GCP Credentials to access --#
    pc = Prpcrypt(security_token["token"])

    decrypted_credentials = json.loads(pc.decrypt(password_encrypted))

    pc = Prpcrypt(security_token["access"])

    gcp_creds = {}
    for name, val in decrypted_credentials.items():
        gcp_creds.update({name: pc.decrypt(val)})


    #-- Spark Configurations --#
    print("Get Spark Configuration...")

    query = f"""
            SELECT             
                application_name,
                executor_instances,
                executor_memory,
                executor_cores,
                driver_memory,
                driver_cores,
                auto_broadcast_join_threshold,
                shuffle_partitions,
                broadcast_timeout,
                bucket,
                source_file
            FROM metadata_utilities.spark_config
            WHERE UPPER(application_name) = '{asset.upper()}';
            """

    results = db.execute_query(query)

    executor_instances = results.get("executor_instances")
    executor_memory = results.get("executor_memory")
    executor_cores = results.get("executor_cores")
    driver_memory = results.get("driver_memory")
    driver_cores = results.get("driver_cores")
    auto_broadcast_join_threshold = results.get("auto_broadcast_join_threshold")
    shuffle_partitions = results.get("shuffle_partitions")
    broadcast_timeout = results.get("broadcast_timeout")
    bucket = results.get("bucket")
    source_file = results.get("source_file")    

    db.close_connection()

    # Load script contents from GCP to variable
    contents = get_gcp_storage(
        bucket_name=bucket,
        prefix_path=source_file,
        keyfile_contents=gcp_creds
        )

    # Simulate command line argument to pass GCP Credentials
    sys.argv = ['controller.py', gcp_creds] 

    # Initialize SparkSession with configurations
    spark = SparkSession.builder \
        .appName("Template-Application") \
        .config("spark.executor.instances", f"{executor_instances}") \
        .config("spark.executor.memory", f"{executor_memory}") \
        .config("spark.executor.cores", f"{executor_cores}") \
        .config("spark.driver.memory", f"{driver_memory}") \
        .config("spark.driver.cores", f"{driver_cores}") \
        .config("spark.sql.autoBroadcastJoinThreshold", f"{auto_broadcast_join_threshold}") \
        .config("spark.sql.shuffle.partitions", f"{shuffle_partitions}") \
        .config("spark.sql.broadcastTimeout", f"{broadcast_timeout}") \
        .getOrCreate()

    # Pass the Spark session or any configurations to script contents
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

    # Run the script 
    exec(contents)

    spark.stop()
except Exception as e:
    print(f"{e}")
#-------------------------------------------------------------------------------------------------------------------#
#-- Notes: Spark Scripts should be placed in GCP Bucket.                                                          --#
#--                                                                                                               --#
#-- Description: The spark controller script will take two arguements (Connection Name, Spark Application Name),  --#
#--     query Postgres database for GCP credentails and Spark Application Configurations. After collecting        --#
#--     configuration data the controller will build the required Spark Session Connfiguration and read in the    --#
#--     from this script from a GCP bucket and execute the spark code.                                            --#
#--                                                                                                               --#
#--     This script specifically will query the bitcoin_history table, train the data on a linear regression      --#
#--     model and write the output back into a BigQuery table.                                                    --#
#--                                                                                                               --#
#--    * Please note the target destination table must exists in BigQuery to write data *                         --#
#-------------------------------------------------------------------------------------------------------------------#

import sys
from gcp_common import gcp_execute_query, gcp_write_dataframe
from datetime import datetime, timezone
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, col, hour, dayofweek, dayofmonth, month, year
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DoubleType, TimestampType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator


try:
    if len(sys.argv[1]) == 0 or type(sys.argv[1]) != dict:
        print("Unexpected System argument")
        sys.exit(1)
    else:
        key_contents = sys.argv[1]
        print("Recieved Contents")


    # Get the existing SparkSession
    spark = SparkSession.builder.getOrCreate()

    # Retrieve Spark configuration settings
    configurations = spark.sparkContext.getConf().getAll()

    # Validate Configuration settings
    for config in configurations:
        print(config)

    #------------------------------------------------#
    #--         Query: GCP & Build DataFrame       --#
    #------------------------------------------------#

    #-- Query GCP and convert response data to DataFrame --#
    query = '''
            SELECT time, price_usd, circulating_supply FROM `coincap-data-hub.ref_coincap_data.bitcoin_history`;
            '''

    response = gcp_execute_query(
                    query=query,
                    keyfile_contents=key_contents
                )

    spark_schema = StructType([
        StructField("time", StringType(), True),
        StructField("price_usd", FloatType(), True),
        StructField("circulating_supply", DoubleType(), True)
        ])

    spark_df = spark.createDataFrame(response, schema=spark_schema)

    #------------------------------------------------#
    #--         Begin: Linear Regression           --#
    #------------------------------------------------#
    
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


    #------------------------------------------------#
    #--         Begin: Data Load Process           --#
    #------------------------------------------------#

    # Current Timestamp
    utc_timestamp_string = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")
    utc_timestamp = datetime.strptime(utc_timestamp_string, "%Y-%m-%dT%H:%M:%S.%f")


    # Prepare Output Spark DataFrame
    data = [(utc_timestamp,rmse)]
    columns = ['created_date', 'rmse']

    output_schema = StructType([
        StructField("created_date", TimestampType(), True),
        StructField("rmse", FloatType(), True)
        ])

    output_df = spark.createDataFrame(data, schema=output_schema)

    output_df.show()

    # convert to pandas DataFrame
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    pandas_df = output_df.toPandas()

    # Ensure the 'created_date' column is in the correct datetime format for BigQuery by 
    # Converting Spark TimestampType to Pandas datetime64[ns] with UTC timezone
    pandas_df['created_date'] = pd.to_datetime(pandas_df['created_date'], utc=True, errors='coerce').astype('datetime64[ns, UTC]')


    # Write to BigQuery
    response = gcp_write_dataframe(
        pandas_dataframe=pandas_df,
        project_id="coincap-data-hub",
        dataset_id="ref_coincap_data",
        table_id="bitcoin_history_linear_regression",
        keyfile_contents=key_contents,
        if_exists="append",
        table_schema=[
                {'name': 'created_date', 'type': 'DATETIME'},
                {'name': 'rmse', 'type': 'FLOAT'}
            ]
    )

    print(response)
    
    if "SUCCESS" in response:
        print("Spark Job Completed Successfully")
    elif "ERROR" in response:
        print(f"{response}") 

except Exception as e:
    print(f"{e}")    
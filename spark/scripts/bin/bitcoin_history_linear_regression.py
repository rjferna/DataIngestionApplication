#-- This File should be placed in GCP Bucket --#

import sys
from gcp_common import gcp_execute_query

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, col, hour, dayofweek, dayofmonth, month, year
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DoubleType
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

    #-- Query GCP and convert response data to DataFrame --#
    query = f'''
            INSERT INTO `coincap-data-hub.ref_coincap_data.bitcoin_history_linear_regression`
            (created_date, rmse) VALUES
            (CURRENT_DATETIME, SAFE_CAST("{rmse}" AS FLOAT64));
            '''

    response = gcp_execute_query(
                    query=query,
                    keyfile_contents=key_contents
                )
    
    if "SUCCESS" in response:
        print("Spark Job Completed Successfully")
    elif "ERROR" in response:
        print(f"{response}") 

    spark_df.show()
except Exception as e:
    print(f"Error: {e}")
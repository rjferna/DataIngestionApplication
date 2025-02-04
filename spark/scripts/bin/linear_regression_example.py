from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, col, hour, dayofweek, dayofmonth, month, year
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

# Initialize Spark session
spark = SparkSession.builder.appName("CurrencyLinearRegression").getOrCreate()


# Load dataset
data = spark.read.csv("/data/bitcoin_history.csv", header=True, inferSchema=True)


# Preprocess data
data = data.withColumn("hour", hour(to_timestamp(col("time") / 1000000))) \
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

print(f"Root Mean Squared Error (RMSE): {rmse}")

# Stop Spark session
spark.stop()

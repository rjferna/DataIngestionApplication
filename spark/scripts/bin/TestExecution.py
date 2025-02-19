#-------------------------------------------------------------------------------------------------------------------#
#-- Notes: Spark Scripts should be placed in GCP Bucket.                                                          --#
#--                                                                                                               --#
#-- Description: The spark controller script will take two arguements (Connection Name, Spark Application Name),  --#
#--     query Postgres database for GCP credentails and Spark Application Configurations. After collecting        --#
#--     configuration data the controller will build the required Spark Session Connfiguration and read in the    --#
#--     from this script from a GCP bucket and execute the spark code.                                            --#
#--                                                                                                               --#
#--     This script specifically will query the assets table build a DataFrame as show the DataFrame contents.    --#
#--                                                                                                               --#
#-------------------------------------------------------------------------------------------------------------------#

import sys
from gcp_common import gcp_execute_query

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DoubleType


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
            SELECT id, price_usd, supply FROM `coincap-data-hub.ref_coincap_data.assets` limit 10;
            '''

    response = gcp_execute_query(
                    query=query,
                    keyfile_contents=key_contents
                )

    spark_schema = StructType([
        StructField("id", StringType(), True),
        StructField("price_usd", FloatType(), True),
        StructField("supply", DoubleType(), True)
        ])

    spark_df = spark.createDataFrame(response, schema=spark_schema)

    spark_df.show()
except Exception as e:
    print(f"Error: {e}")
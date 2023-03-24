# import findspark
# findspark.init()

from pyspark.sql import SparkSession 
import pyspark.sql.functions as F
from pyspark.sql.types import *
    

spark = (SparkSession.builder.appName("shopping").master("spark://namenode:7077").getOrCreate())


review_schema = StructType([StructField("review_id", StringType(), False),
      StructField("user_id", StringType(), False),
      StructField("business_id", StringType(), False),
      StructField("stars", StringType(), False), 
      StructField("useful", IntegerType(), False),
      StructField("funny", IntegerType(), False),
      StructField("cool", IntegerType(), False),
      StructField("text", StringType(), False),
      StructField("date", StringType(), False),])
review_df = spark.read.csv("hdfs://namenode:9000/project_data/review.csv", sep = '|', header = False, schema = review_schema)


business_schema = StructType([
    StructField("business_id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("postal_code", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("stars", DoubleType(), True),
    StructField("review_count", IntegerType(), True),
    StructField("is_open", IntegerType(), True),
    StructField("attributes", StringType(), True),
    StructField("categories", StringType(), True),
    StructField("hours", StringType(), True)
])
business_df = spark.read.csv("hdfs://namenode:9000/project_data/business.csv", sep = '|', header = False, schema = business_schema)

# DONT RUN THIS CODE
review_df.write.parquet("hdfs://namenode:9000/project_data/data/reviews")
business_df.write.parquet("hdfs://namenode:9000/project_data/data/business")

# Read it like this:
# business=spark.read.parquet('hdfs://namenode:9000/project_data/data/business/')



import findspark
findspark.init("/usr/local/spark")

from pyspark.sql import SparkSession 
import pyspark.sql.functions as F 

from delta import *

builder = SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

business=spark.read.format("parquet").load('hdfs://namenode:9000/project_data/data/business/')
print(type(business))
business.write.format("delta").mode("overwrite").save("/temp/business")

# df = spark.read.format("delta").load("/temp/business")
# df.show(5)
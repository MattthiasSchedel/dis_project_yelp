import findspark
findspark.init("/usr/local/spark")

from pyspark.sql import SparkSession 
import pyspark.sql.functions as F 
from pyspark.ml import Pipeline
from pyspark.sql.types import *

                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
from delta import *

# Import Spark NLP
import sys
import sparknlp
from sparknlp.base import *
from sparknlp.annotator import *
from sparknlp.pretrained import PretrainedPipeline

builder = SparkSession.builder.appName(f"Sentiment Analysis - storage (6gb) instance - cores -> 4 - cache-> memory configPartition -> {sys.argv[1]}") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.jars.packages", "com.johnsnowlabs.nlp:spark-nlp_2.12:4.4.0")\
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
    .master("spark://namenode:7077")\
    .config("spark.executor.cores", 4) \
    .config("spark.sql.shuffle.partitions", int(sys.argv[1])) \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")\
    .config("spark.executor.memory", "6g") \
    #.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")\
    # .config("spark.sql.shuffle.partitions", 250) \
    
    # .config("spark.executor.instances", 10) \
    
    
    #.config("spark.executor.instances", "1")\
    #.config("spark.executor.cores", "2")

    
spark = configure_spark_with_delta_pip(builder).getOrCreate()


# Prepare Dataframes
# Review data
spark.sparkContext.setJobDescription('load review dataset')
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
review_df.createOrReplaceTempView("reviews")

#review_df.show()

# Business data
spark.sparkContext.setJobDescription('load business dataset')
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
# business_df.createOrReplaceTempView("business")

# first filter then combine

# Filter for only restaurant reviews
spark.sparkContext.setJobDescription('filter reviews')
business_df = business_df.where(F.col('categories').contains("Restaurants"))
business_df.createOrReplaceTempView("business")

# Combine data on common restaurant id
spark.sparkContext.setJobDescription('combine datasets')
restaurant_reviews = spark.sql("SELECT r.review_id, b.business_id, r.text, r.date, b.categories, r.stars FROM reviews AS r INNER JOIN business AS b ON b.business_id = r.business_id ")

#restaurant_reviews.show()

spark.sparkContext.setJobDescription('pipeline builder')
# Build a pipeline
# Document Assembler
documentAssembler = DocumentAssembler() \
    .setInputCol("text") \
    .setOutputCol("document")

spark.sparkContext.setJobDescription('pipeline -tokenizer')
tokenizer = Tokenizer() \
    .setInputCols(["document"]) \
    .setOutputCol("token")#\
    #.fit(restaurant_reviews) # apperantly works without this

spark.sparkContext.setJobDescription('pipeline -normalizer')
normalizer = Normalizer() \
    .setInputCols(["token"]) \
    .setOutputCol("normal")

spark.sparkContext.setJobDescription('pipeline -model')
vivekn = ViveknSentimentModel.pretrained() \
    .setInputCols(["document", "normal"]) \
    .setOutputCol("result_sentiment")

spark.sparkContext.setJobDescription('pipeline -model=finish')
finisher = Finisher() \
    .setInputCols(["result_sentiment"]) \
    .setOutputCols("final_sentiment")

spark.sparkContext.setJobDescription('pipeline -pipeline executor')
pipeline = Pipeline().setStages([documentAssembler, tokenizer, normalizer, vivekn, finisher])

spark.sparkContext.setJobDescription('pipeline -pipeline fit')
pipelineModel = pipeline.fit(restaurant_reviews)

spark.sparkContext.setJobDescription('pipeline -pipeline transform')
# Calculate sentiment for all restaurant reviews
result = pipelineModel.transform(restaurant_reviews)

spark.sparkContext.setJobDescription('pipeline -result check')
# Check accuracy
result = result.withColumn("right_prediction", 
                   F.when(((F.array_contains(F.col("final_sentiment"),"positive")) & (F.col("stars").isin(["5.0", "4.0", "3.0"]))) |
                        ((F.array_contains(F.col("final_sentiment"),"negative")) & (F.col("stars").isin(["3.0", "2.0", "1.0"]))), 
                        1).otherwise(0))

# result.persist()

# spark.sparkContext.setJobDescription('count right ones')
# count_ones = result.agg(F.sum("right_prediction")).collect()[0][0]

# total_count = result.count()

# print(f"Prediction accuracy for sentiment: {count_ones/total_count}")
#result.unpersist()

# spark.sparkContext.setJobDescription('write to disk')
# # delta_table_path = "/temp/sentiment_predicted_restaurant_reviews"
# delta_table_path = "/temp/sentiment_predicted_restaurant_reviews"
# # Check if the Delta table exists
# if DeltaTable.isDeltaTable(spark, delta_table_path):
#     print("Updating delta")
#     # If the Delta table exists, load it into a DataFrame
#     deltaTableAnalyzedReviews = DeltaTable.forPath(spark, delta_table_path)
#     deltaTableAnalyzedReviews.alias('old') \
#         .merge(
#         result.alias('updates'),
#         'old.review_id = updates.review_id'
#   ) \
#   .whenMatchedUpdate(set =
#     {
#       "review_id": "updates.review_id",
#       "business_id": "updates.business_id",
#       "text": "updates.text",
#       "date": "updates.date",
#       "categories": "updates.categories",
#       "final_sentiment": "updates.final_sentiment",
#       "right_prediction": "updates.right_prediction"
#     }
#   ) \
#   .whenNotMatchedInsert(values =
#     {
#       "review_id": "updates.review_id",
#       "business_id": "updates.business_id",
#       "text": "updates.text",
#       "date": "updates.date",
#       "categories": "updates.categories",
#       "final_sentiment": "updates.final_sentiment",
#       "right_prediction": "updates.right_prediction"
#     }
#   ) \
#   .execute()
# else:
#     # If the Delta table does not exist, create it
#     print("Creating delta")
#     deltaTableAnalyzedReviews = result.write.format('delta').mode('overwrite').save(delta_table_path)
#     deltaTableAnalyzedReviews = DeltaTable.forPath(spark, delta_table_path)
# print("Finished!")
spark.sparkContext.setJobDescription('write to disk')
# TODO: Change it in a way similar to below so only new data will be inserted
result.write.format("delta").mode("overwrite").save("/temp/sentiment_predicted_restaurant_reviews")
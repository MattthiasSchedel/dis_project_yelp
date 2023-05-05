import findspark
findspark.init("/usr/local/spark")

from pyspark.sql import SparkSession 
import pyspark.sql.functions as F 
from pyspark.ml import Pipeline
from pyspark.sql.types import *

                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
from delta import *

# Import Spark NLP
import sparknlp
from sparknlp.base import *
from sparknlp.annotator import *
from sparknlp.pretrained import PretrainedPipeline

builder = SparkSession.builder.appName("Sentiment Analysis") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.executor.memory", "6g") \
    .config("spark.jars.packages", "com.johnsnowlabs.nlp:spark-nlp_2.12:4.4.0")\
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
    .master("spark://namenode:7077")\
    
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
# review_df = spark.read.csv("hdfs://namenode:9000/project_data/review.csv", sep = '|', header = False, schema = review_schema)
review_df = spark.read.csv("hdfs://namenode:9000/project_data/review_small_b.csv", sep = '|', header = False, schema = review_schema)
review_df.createOrReplaceTempView("reviews")

#review_df.show()

# Business data
spark.sparkContext.setJobDescription('load review dataset')
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
business_df.createOrReplaceTempView("business")

# Combine data on common restaurant id
spark.sparkContext.setJobDescription('combine datasets')
reviews_with_category = spark.sql("SELECT r.review_id, b.business_id, r.text, r.date, b.categories, r.stars FROM reviews AS r LEFT JOIN business AS b ON b.business_id = r.business_id ")

# Filter for only restaurant reviews
spark.sparkContext.setJobDescription('filter reviews')
restaurant_reviews = reviews_with_category.where(F.col('categories').contains("Restaurants"))
restaurant_reviews.show()
# Build a pipeline
# Document Assembler
documentAssembler = DocumentAssembler() \
    .setInputCol("text") \
    .setOutputCol("document")

tokenizer = Tokenizer() \
    .setInputCols(["document"]) \
    .setOutputCol("token")#\
    #.fit(restaurant_reviews) # apperantly works without this

normalizer = Normalizer() \
    .setInputCols(["token"]) \
    .setOutputCol("normal")

vivekn = ViveknSentimentModel.pretrained() \
    .setInputCols(["document", "normal"]) \
    .setOutputCol("result_sentiment")

finisher = Finisher() \
    .setInputCols(["result_sentiment"]) \
    .setOutputCols("final_sentiment")

pipeline = Pipeline().setStages([documentAssembler, tokenizer, normalizer, vivekn, finisher])

pipelineModel = pipeline.fit(restaurant_reviews)

# Calculate sentiment for all restaurant reviews
result = pipelineModel.transform(restaurant_reviews)

# Check accuracy
result = result.withColumn("right_prediction", 
                   F.when(((F.array_contains(F.col("final_sentiment"),"positive")) & (F.col("stars").isin(["5.0", "4.0", "3.0"]))) |
                        ((F.array_contains(F.col("final_sentiment"),"negative")) & (F.col("stars").isin(["3.0", "2.0", "1.0"]))), 
                        1).otherwise(0))

count_ones = result.agg(F.sum("right_prediction")).collect()[0][0]

total_count = result.count()

print(f"Prediction accuracy for sentiment: {count_ones/total_count}")

#result.write.format("delta").mode("overwrite").save("/temp/sentiment_predicted_restaurant_reviews")

# delta_table_path = "/temp/sentiment_predicted_restaurant_reviews"
delta_table_path = "/temp/sentiment_predicted_restaurant_reviews_presentation"
# Check if the Delta table exists
if DeltaTable.isDeltaTable(spark, delta_table_path):
    print("Updating delta")
    # If the Delta table exists, load it into a DataFrame
    deltaTableAnalyzedReviews = DeltaTable.forPath(spark, delta_table_path)
    deltaTableAnalyzedReviews.alias('old') \
        .merge(
        result.alias('updates'),
        'old.review_id = updates.review_id'
  ) \
  .whenMatchedUpdate(set =
    {
      "review_id": "updates.review_id",
      "business_id": "updates.business_id",
      "text": "updates.text",
      "date": "updates.date",
      "categories": "updates.categories",
      "final_sentiment": "updates.final_sentiment",
      "right_prediction": "updates.right_prediction"
    }
  ) \
  .whenNotMatchedInsert(values =
    {
      "review_id": "updates.review_id",
      "business_id": "updates.business_id",
      "text": "updates.text",
      "date": "updates.date",
      "categories": "updates.categories",
      "final_sentiment": "updates.final_sentiment",
      "right_prediction": "updates.right_prediction"
    }
  ) \
  .execute()
else:
    # If the Delta table does not exist, create it
    print("Creating delta")
    deltaTableAnalyzedReviews = result.write.format('delta').mode('overwrite').save(delta_table_path)
    deltaTableAnalyzedReviews = DeltaTable.forPath(spark, delta_table_path)
print("Finished!")
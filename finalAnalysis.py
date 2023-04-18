import findspark
findspark.init("/usr/local/spark")

from pyspark.sql import SparkSession 
import pyspark.sql.functions as F 
from pyspark.ml import Pipeline
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
from delta import *

# Import Spark NLP
import sparknlp
from sparknlp.base import *
from sparknlp.annotator import *
from sparknlp.pretrained import PretrainedPipeline

builder = SparkSession.builder.appName("Sentiment Analysis") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.jars.packages", "com.johnsnowlabs.nlp:spark-nlp_2.12:4.4.0")\
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
    #.master("spark://namenode:7077")\
    
spark = configure_spark_with_delta_pip(builder).getOrCreate()


df = spark.read.format("delta").load("/temp/filtered_reviews")
document = DocumentAssembler() \
    .setInputCol("text") \
    .setOutputCol("document")

token = Tokenizer() \
    .setInputCols(["document"]) \
    .setOutputCol("token")\
    .fit(df)

normalizer = Normalizer() \
    .setInputCols(["token"]) \
    .setOutputCol("normal")

vivekn = ViveknSentimentModel.pretrained() \
    .setInputCols(["document", "normal"]) \
    .setOutputCol("result_sentiment")

finisher = Finisher() \
    .setInputCols(["result_sentiment"]) \
    .setOutputCols("final_sentiment")

pipeline = Pipeline().setStages([document, token, normalizer, vivekn, finisher])

pipelineModel = pipeline.fit(df)

result = pipelineModel.transform(df)

result.write.format("delta").mode("overwrite").save("/temp/sentiment_predicted_reviews")
result.show(5)


# %%
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, FloatType, IntegerType
from pyspark.ml.feature import CountVectorizer
from pyspark.ml import Pipeline
import pyspark.sql.functions as F

from sparknlp.annotator import LemmatizerModel, Tokenizer, Normalizer, StopWordsCleaner, NGramGenerator
from sparknlp.base import Finisher, DocumentAssembler

import argparse
parser = argparse.ArgumentParser()
parser.add_argument('--input', required=True)
parser.add_argument('--dataset', required=True)
parser.add_argument('--subreddit', required=True)
parser.add_argument('--mode', required=True)
args = parser.parse_args()

input = args.input
dataset = args.dataset
subreddit = args.subreddit
mode = args.mode

# change this to your bucket
# bucket is used as temporary storage while writing data from Spark to BigQuery
BUCKET = 'datalake_de-r-stocks'

# Start Spark session
spark = SparkSession.builder \
    .appName('preprocessing_wordcount') \
    .config('"spark.jars.packages", "com.johnsnowlabs.nlp:spark-nlp_2.12:3.4.3"') \
    .getOrCreate()

# %%
# Access data from GCS
df = spark.read.parquet(input)

# 1. Remove posts by AutoModerator
# 2. Remove duplicate titles
# 3. Convert unix timestamp to date
# 4. Keep title and date columns
df_filter = df.filter(~F.col('author').contains('AutoModerator')) \
    .dropDuplicates(['title']) \
        .withColumn('date', F.from_unixtime(F.col('created_utc'), 'yyyy-MM-dd')) \
            .select('title', 'date')
            
documentAssembler = DocumentAssembler() \
    .setInputCol('title') \
    .setOutputCol('title_document')

tokenizer = Tokenizer() \
    .setInputCols(['title_document']) \
    .setOutputCol('title_token')

normalizer = Normalizer() \
    .setInputCols(['title_token']) \
    .setOutputCol('title_normalized') \
    .setLowercase(True)

lemmatizer = LemmatizerModel.pretrained() \
            .setInputCols(['title_normalized']) \
            .setOutputCol('title_lemma')

stopwords_cleaner = StopWordsCleaner() \
    .setInputCols(['title_lemma']) \
    .setOutputCol('title_cleaned') \
    .setCaseSensitive(False)

ngrams_cum = NGramGenerator() \
            .setInputCols(["title_cleaned"]) \
            .setOutputCol("title_ngrams") \
            .setN(2) \
            .setEnableCumulative(True)\
            .setDelimiter("_") # Default is space

finisher = Finisher() \
    .setInputCols(['title_ngrams']) \
    .setOutputCols(['title_finished']) \
    .setCleanAnnotations(False)

nlpPipeline = Pipeline(stages=[
              documentAssembler, 
              tokenizer,
              normalizer,
              lemmatizer,
              stopwords_cleaner,
              ngrams_cum,
              finisher
 ])

df_result = nlpPipeline.fit(df_filter).transform(df_filter).select('title_finished', 'date')

# CountVectorizer model
cv = CountVectorizer(inputCol='title_finished', outputCol='features', minDF=3.0)

# Train on all submissions
model = cv.fit(df_result)

df_tokensbydate = df_result.groupBy('date').agg(F.flatten(F.collect_list('title_finished')).alias('title_finished'))

# Get counts for each date
counts = model.transform(df_tokensbydate).select('date','features').collect()

# Create empty dataframe
df_wordcountbydate = spark.createDataFrame(spark.sparkContext.emptyRDD(), 
                        schema=StructType(fields=[
                            StructField("word", StringType()), 
                            StructField("count", FloatType()),
                            StructField("date", StringType())]))

# Append count for each day to dataframe
for row in range(len(counts)):
    test_dict = dict(zip(model.vocabulary, (float(x) for x in counts[row]['features'].values)))
    df_temp = spark.createDataFrame(test_dict.items(), 
                        schema=StructType(fields=[
                            StructField("word", StringType()), 
                            StructField("count", FloatType())]))
    df_temp = df_temp.withColumn('date', F.lit(counts[row]['date']))
    df_wordcountbydate = df_wordcountbydate.unionAll(df_temp)

# %%

df_wordcountbydate = df_wordcountbydate.withColumn('count', F.col('count').cast(IntegerType())) \
                        .withColumn(f'{mode}_date', F.to_date(F.col('date'), 'yyyy-MM-dd')) \
                        .withColumnRenamed('count', 'wordcount') \
                        .drop('date')

# upload dataframe to BigQuery
df_wordcountbydate.write.format('bigquery') \
    .option('table', f'{dataset}.{subreddit}_{mode}_wordcount') \
    .option('temporaryGcsBucket', BUCKET) \
    .option('partitionField', f'{mode}_date') \
    .option('partitionType', 'DAY') \
    .mode('append') \
    .save()
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from schemas import enriched_schema
from spark_sessions_config import spark

spark = SparkSession \
    .builder \
    .master("local") \
      .appName("AlertCounter")\
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
    .config("spark.hadoop.fs.s3a.access.key", 'minioadmin')\
    .config("spark.hadoop.fs.s3a.secret.key", 'minioadmin')\
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")\
    .config("spark.hadoop.fs.s3a.path.style.access", "true")\
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2') \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")\
    .getOrCreate()


stream_df = spark\
    .readStream\
    .format("kafka")\
    .option("startingOffsets", "earliest")\
    .option("kafka.bootstrap.servers", 'course-kafka:9092')\
    .option("subscribe", "alert-data")\
    .load()\
    .select(F.col('value').cast(T.StringType()))

parse_df = stream_df \
    .withColumn('parsed_json', F.from_json(F.col('value'), enriched_schema)) \
    .select(F.col('parsed_json.*')) \
    .withColumn('event_time', F.to_timestamp(F.col('event_time')))

agg_df = parse_df.withWatermark("event_time", "15 minutes") \
       .groupBy() \
       .agg(
       F.count("*").alias("num_of_rows"),
       F.sum(F.when(F.lower(F.col("color_name")) == "black", 1).otherwise(0)).alias("num_of_black"),
       F.sum(F.when(F.lower(F.col("color_name"))=="white",1).otherwise(0)).alias("num_of_white"),
       F.sum(F.when(F.lower(F.col("color_name"))=="silver",1).otherwise(0)).alias("num_of_silver"),
       F.max("speed").alias("maximum_speed"),
       F.max("gear").alias("maximum_gear"),
       F.max("rpm").alias("maximum_rpm")
       )

agg_df \
    .writeStream \
    .trigger(processingTime='1 minute') \
    .format("console") \
    .outputMode('complete') \
    .start()\
    .awaitTermination()


spark.stop()
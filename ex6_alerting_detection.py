from pyspark.sql import SparkSession, functions as F
from spark_sessions_config import spark
from schemas import enriched_schema

# 1. Changed .read to .readStream
stream_df = spark\
    .readStream\
    .format("kafka")\
    .option("startingOffsets", "earliest")\
    .option("kafka.bootstrap.servers", 'course-kafka:9092')\
    .option("subscribe", "samples-enriched")\
    .load()\
    .select(F.col("value").cast("string"))

parse_df = stream_df\
         .select(F.from_json(F.col("value"), enriched_schema).alias("value"))\
         .select("value.*")

alerts_df = parse_df.filter(
    (F.col('speed') > 120) &
    (F.col('expected_gear') != F.col('gear')) &
    (F.col('rpm') > 6000)
)

# alerts_df.show(100)


alerts_df.selectExpr("to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "course-kafka:9092") \
    .option("topic", "alert-data") \
    .option("checkpointLocation", "s3a://data/checkpoints/alerts") \
    .start() \
    .awaitTermination()

spark.stop()
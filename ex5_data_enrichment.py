from pyspark.sql import functions as F
from spark_sessions_config import spark
from schemas import data_enrichment_schema


df_cars = spark.read.parquet('s3a://data/dims/cars')
df_models = spark.read.parquet('s3a://data/dims/car_models')
df_colors = spark.read.parquet('s3a://data/dims/car_colors')

df_cars.cache()
df_models.cache()
df_colors.cache()

stream_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "course-kafka:9092") \
    .option("subscribe", "sensors-sample") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()\
    .select (F.col("value").cast("string"))

parse_df=stream_df\
         .select(F.from_json(F.col("value"),data_enrichment_schema).alias("value"))\
         .select("value.*")
    


enriched_df = parse_df \
    .join(df_cars, "car_id") \
    .join(df_models, "model_id") \
    .join(df_colors, "color_id") \
    .select(     
        F.col("event_id"),   
        F.col("event_time"),   
        F.col("car_id"),   
        F.col("speed"),   
        F.col("rpm"),  
        F.col("gear"),
        F.col("driver_id"),            
        F.col("car_brand").alias("brand_name"),
        F.col("car_model").alias("model_name"),
        F.col("color_name"),
       (F.round(F.col("speed") / F.lit(30))).cast("int").alias("expected_gear")
    )


enriched_df.selectExpr("to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "course-kafka:9092") \
    .option("topic", "samples-enriched") \
    .option("checkpointLocation", "s3a://data/checkpoints/enrichment") \
    .outputMode("append") \
    .start() \
    .awaitTermination()




    
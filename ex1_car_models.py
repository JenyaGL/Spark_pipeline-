from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from schemas import car_model_schema
from spark_sessions_config import spark

data = [
    (1, "Mazda", "3"),
    (2, "Mazda", "6"),
    (3, "Toyota", "Corolla"),
    (4, "Hyundai", "i20"),
    (5, "Kia", "Sportage"),
    (6, "Kia", "Rio"),
    (7, "Kia", "Picanto")
       ]

car_models_df = spark.createDataFrame(data, car_model_schema)

car_models_df.printSchema()
car_models_df.show()

car_models_df.write.parquet('s3a://data/dims/car_models', mode='overwrite')


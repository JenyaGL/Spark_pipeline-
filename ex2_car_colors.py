from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from schemas import car_color_schema
from spark_sessions_config import spark

data = [
    (1, "Black"),
    (2, "Red"),
    (3, "Gray"),
    (4, "White"),
    (5, "Green"),
    (6, "Blue"),
    (7, "Pink")
       ]

car_colors_df = spark.createDataFrame(data, car_color_schema)

car_colors_df.printSchema()
car_colors_df.show()

car_colors_df.write.parquet('s3a://data/dims/car_colors', mode='overwrite')


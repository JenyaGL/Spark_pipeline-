import random
from pyspark.sql import SparkSession
from pyspark.sql import types as T
from schemas import car_color_schema
from spark_sessions_config import spark
from schemas import cars_schema



unique_car_id = random.sample(range(1000000, 2000000), 20)

data = []

for car_id in unique_car_id:

    car_id = random.randint(1000000, 2000000)
    driver_id = random.randint(100000000, 200000000)
    model_id = random.randint(1, 7)
    car_color = random.randint(1, 7)

    data.append((car_id, driver_id, model_id, car_color))

cars_df = spark.createDataFrame(data, cars_schema)

cars_df.printSchema()
cars_df.show()

cars_df.write.parquet('s3a://data/dims/cars', mode='overwrite')

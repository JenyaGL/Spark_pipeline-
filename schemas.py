# schemas Configuration
from pyspark.sql import types as T
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StructType, StructField, StringType, IntegerType


# ex 1 - Car Models 
car_model_schema = T.StructType([
    T.StructField('model_id', T.StringType(), True),
    T.StructField('car_brand', T.StringType(), True),
    T.StructField('car_model', T.StringType(), True)
    ])

# ex 2 - Car Colors
car_color_schema = T.StructType([
    T.StructField('color_id', T.StringType(), True),
    T.StructField('color_name', T.StringType(), True)
    ])

# ex 3 - Cars
cars_schema = T.StructType([
    T.StructField('car_id', T.StringType(), True),
    T.StructField('driver_id', T.StringType(), True),
    T.StructField('model_id', T.StringType(), True),
    T.StructField('color_id', T.StringType(), True)
                     ])

data_enrichment_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("car_id", StringType(), True),
    StructField("speed", IntegerType(), True),
    StructField("rpm", IntegerType(), True),
    StructField("gear", IntegerType(), True)
])


enriched_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("car_id", StringType(), True),
    StructField("speed", IntegerType(), True),
    StructField("rpm", IntegerType(), True),
    StructField("gear", IntegerType(), True),
    StructField("expected_gear", DoubleType(), True),
    StructField("driver_id", StringType(), True),
    StructField("brand_name", StringType(), True),
    StructField("model_name", StringType(), True),
    StructField("color_name", StringType(), True)
])
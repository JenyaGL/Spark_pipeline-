# spark sesiosn configuration

from pyspark.sql import SparkSession


# # ex 1 - Car Models
# spark_car_models = SparkSession.builder.master("local").appName('CarsGenerator').getOrCreate()

# # ex 2 - Car Colors
# spark_car_colors = SparkSession.builder.master("local").appName('CarsGenerator').getOrCreate()

# # ex 3 - Cars
# spark_cars = SparkSession.builder.master("local").appName('CarsGenerator').getOrCreate()

# # ex 4 - Data Generator
# spark_data_generator = SparkSession.builder.master("local").appName('CarsGenerator')\
#     .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
#     .config("spark.hadoop.fs.s3a.access.key", 'minioadmin')\
#     .config("spark.hadoop.fs.s3a.secret.key", 'minioadmin')\
#     .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9002/")\
#     .config("spark.hadoop.fs.s3a.path.style.access", "true")\
#     .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false").getOrCreate()


# ------------------

# One unified session for the entire project
spark = SparkSession \
    .builder \
.appName("ModelCreation1") \
    .master("local") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
    .config("spark.hadoop.fs.s3a.access.key", 'minioadmin')\
    .config("spark.hadoop.fs.s3a.secret.key", 'minioadmin')\
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")\
    .config("spark.hadoop.fs.s3a.path.style.access", "true")\
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2') \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false").getOrCreate()

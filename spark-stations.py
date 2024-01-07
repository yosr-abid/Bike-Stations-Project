from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

from elasticsearch import Elasticsearch
es = Elasticsearch("http://localhost:9200")

spark = SparkSession.builder.appName("consumer").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


schema = StructType([
    StructField("numbers", IntegerType(), True),
    StructField("contract_name", StringType(), True),
    StructField("banking", StringType(), True),
    StructField("bike_stands", IntegerType(), True),
    StructField("available_bike_stands", IntegerType(), True),
    StructField("available_bikes", IntegerType(), True),
    StructField("address", StringType(), True),
    StructField("status", StringType(), True),
    StructField("position", StructType([
        StructField("lat", DoubleType(), True),
        StructField("lng", DoubleType(), True)
    ]), True),
    StructField("timestamps", StringType(), True),
])

# Subscribe to 1 topic
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "stations") \
    .option("startingOffsets", "latest")\
    .load()

df = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", schema).alias("data")) \
    .select("data.*")
df = df.withColumn("position", col("position").alias("position").cast("struct<lat:double, lon:double>"))


query = df.writeStream \
         .format("org.elasticsearch.spark.sql") \
         .outputMode("append")\
         .option("es.nodes", "127.0.0.1")\
         .option("es.port", "9200")\
         .option("es.index.auto.create", "true") \
         .option("es.resource", "stations")\
         .option("es.nodes.wan.only", "true") \
         .option("checkpointLocation", "C:/Users/MSI/Documents/YOSR/AIM/BigData/projects/checkpoints/new") \
         .start()
query.awaitTermination()
spark.stop()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType
import os

spark = SparkSession.builder \
    .appName("Kafka Integration") \
    .getOrCreate()

schema = StructType([
    StructField("lineID", IntegerType()),
    StructField("day", IntegerType()),
    StructField("pid", IntegerType()),
    StructField("adFlag", IntegerType()),
    StructField("availability", IntegerType()),
    StructField("competitorPrice", DoubleType()),
    StructField("click", IntegerType()),
    StructField("basket", IntegerType()),
    StructField("order", IntegerType()),
    StructField("price", DoubleType()),
    StructField("revenue", DoubleType())
])

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "market_data") \
    .load()

parsed_df = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

output_path = "/home/kafka/BDA-MiniProject/output_data"
checkpoint_path = "/home/kafka/BDA-MiniProject/checkpoint"

os.makedirs(output_path, exist_ok=True)

query = parsed_df.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", output_path) \
    .option("checkpointLocation", checkpoint_path) \
    .start()

query.awaitTermination()

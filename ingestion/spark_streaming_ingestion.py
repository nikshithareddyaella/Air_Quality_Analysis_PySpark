from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, to_timestamp, when, max as spark_max

# Start Spark session
spark = SparkSession.builder.appName("AirQualityStreaming").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# TCP configuration
tcp_host = "localhost"
tcp_port = 9999

# Read raw lines from TCP stream
raw_df = spark.readStream \
    .format("socket") \
    .option("host", tcp_host) \
    .option("port", tcp_port) \
    .load()

# Parse and convert columns
parsed_df = raw_df.select(
    split(raw_df.value, ",").getItem(0).alias("timestamp"),
    split(raw_df.value, ",").getItem(1).alias("region"),
    split(raw_df.value, ",").getItem(2).alias("metric"),
    split(raw_df.value, ",").getItem(3).cast("double").alias("value")
).withColumn("timestamp", to_timestamp("timestamp"))

# Add watermark before aggregation
structured_df = parsed_df \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy("timestamp", "region").agg(
        spark_max(when(col("metric") == "PM2.5", col("value"))).alias("PM2_5"),
        spark_max(when(col("metric") == "temperature", col("value"))).alias("temperature"),
        spark_max(when(col("metric") == "humidity", col("value"))).alias("humidity")
    )

# Write output to CSV files under section1/
query = structured_df.writeStream \
    .format("csv") \
    .option("path", "section1/output/clean_data_csv/") \
    .option("checkpointLocation", "section1/output/checkpoint_dir_csv/") \
    .option("header", "true") \
    .outputMode("append") \
    .start()

query.awaitTermination()

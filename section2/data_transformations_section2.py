from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, stddev, when, lag, mean, to_date, hour, round as spark_round
)
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.appName("Section2_FeatureEngineering").getOrCreate()

# Load input data
input_path = "/workspaces/air_quality_analysis_spark/section1/output/clean_data_csv/*.csv"
df = spark.read.option("header", True).option("inferSchema", True).csv(input_path)

# Type Casting
df = df.withColumn("PM2_5", col("PM2_5").cast("double")) \
       .withColumn("temperature", col("temperature").cast("double")) \
       .withColumn("humidity", col("humidity").cast("double")) \
       .withColumn("timestamp", col("timestamp").cast("timestamp"))

# -------------------------------
# 1. Handle Missing Values (Mean Imputation)
# -------------------------------
for col_name in ["PM2_5", "temperature", "humidity"]:
    mean_val = df.select(mean(col(col_name))).first()[0]
    df = df.fillna({col_name: mean_val})

# -------------------------------
# 2. Handle Outliers (Cap using mean ± 3*stddev)
# -------------------------------
for col_name in ["PM2_5", "temperature", "humidity"]:
    stats = df.select(mean(col(col_name)), stddev(col(col_name))).first()
    mean_val, std_val = stats
    lower = mean_val - (3 * std_val)
    upper = mean_val + (3 * std_val)
    df = df.withColumn(col_name,
        when((col(col_name) < lower) | (col(col_name) > upper), mean_val).otherwise(col(col_name))
    )

# -------------------------------
# 3. Normalize (Z-Score Normalization)
# -------------------------------
for col_name in ["PM2_5", "temperature", "humidity"]:
    stats = df.select(mean(col(col_name)), stddev(col(col_name))).first()
    mean_val, std_val = stats
    df = df.withColumn(f"{col_name}_zscore", (col(col_name) - mean_val) / std_val)

# -------------------------------
# 4. Daily and Hourly Aggregations
# -------------------------------
df = df.withColumn("date", to_date(col("timestamp")))
df = df.withColumn("hour", hour(col("timestamp")))

daily_avg_df = df.groupBy("date").agg(
    spark_round(avg("PM2_5"), 2).alias("avg_PM2_5"),
    spark_round(avg("temperature"), 2).alias("avg_temperature"),
    spark_round(avg("humidity"), 2).alias("avg_humidity")
)

hourly_avg_df = df.groupBy("date", "hour").agg(
    spark_round(avg("PM2_5"), 2).alias("hourly_PM2_5"),
    spark_round(avg("temperature"), 2).alias("hourly_temperature"),
    spark_round(avg("humidity"), 2).alias("hourly_humidity")
)

# -------------------------------
# 5. Lag Features and Rate-of-Change
# -------------------------------
windowSpec = Window.orderBy("timestamp")

df = df.withColumn("PM2_5_lag1", lag("PM2_5", 1).over(windowSpec))
df = df.withColumn("PM2_5_rate_change",
    ((col("PM2_5") - col("PM2_5_lag1")) / col("PM2_5_lag1"))
)

# Fill nulls created by lag
df = df.fillna({"PM2_5_lag1": 0.0, "PM2_5_rate_change": 0.0})

# -------------------------------
# Save Outputs
# -------------------------------
df.write.mode("overwrite").option("header", True).csv("/workspaces/air_quality_analysis_spark/section2/output/feature_engineered_data/")
daily_avg_df.write.mode("overwrite").option("header", True).csv("/workspaces/air_quality_analysis_spark/section2/output/daily_aggregations/")
hourly_avg_df.write.mode("overwrite").option("header", True).csv("/workspaces/air_quality_analysis_spark/section2/output/hourly_aggregations/")

print("✅ Section 2 processing complete: Outliers handled, features normalized, trends analyzed.")

spark.stop()

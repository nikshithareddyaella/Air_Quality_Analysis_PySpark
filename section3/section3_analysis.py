from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, udf, lag, lead
from pyspark.sql.window import Window
from pyspark.sql.types import StringType

# Start Spark session
spark = SparkSession.builder.appName("Section3_SQL_Analysis").getOrCreate()

# Load the input dataset from Section 2
input_path = "/workspaces/air_quality_analysis_spark/section2/output/feature_engineered_data/"
df = spark.read.option("header", True).option("inferSchema", True).csv(input_path)

# Register temporary SQL view
df.createOrReplaceTempView("air_quality")

# 1. Regions with Highest Average PM2.5 Over Last 24 Hours (assuming hourly data)
windowSpec = Window.partitionBy("region").orderBy(col("timestamp").cast("timestamp")).rowsBetween(-23, 0)
df_24hr_avg = df.withColumn("avg_pm25_24h", avg("PM2_5").over(windowSpec))
df_24hr_avg.createOrReplaceTempView("avg_pm25_windowed")

highest_pm25 = spark.sql("""
    SELECT region, MAX(avg_pm25_24h) as max_avg_pm25_24h
    FROM avg_pm25_windowed
    GROUP BY region
    ORDER BY max_avg_pm25_24h DESC
""")

highest_pm25.write.mode("overwrite").option("header", True).csv(
    "/workspaces/air_quality_analysis_spark/section3/output/highest_avg_pm25_24h"
)

# 2. Peak Pollution Intervals (Top 10 PM2.5 timestamps)
peak_pollution = df.orderBy(col("PM2_5").desc()).select("timestamp", "region", "PM2_5").limit(10)
peak_pollution.write.mode("overwrite").option("header", True).csv(
    "/workspaces/air_quality_analysis_spark/section3/output/peak_pollution_intervals"
)

# 3. Trend Analysis Using LAG and LEAD
trend_window = Window.partitionBy("region").orderBy(col("timestamp").cast("timestamp"))
df_trend = df.withColumn("PM2_5_prev", lag("PM2_5", 1).over(trend_window)) \
             .withColumn("PM2_5_next", lead("PM2_5", 1).over(trend_window))

df_trend = df_trend.withColumn("increase_flag",
    ((col("PM2_5") > col("PM2_5_prev")) & (col("PM2_5_next") > col("PM2_5"))).cast("int")
)

df_trend.select("timestamp", "region", "PM2_5_prev", "PM2_5", "PM2_5_next", "increase_flag") \
    .write.mode("overwrite").option("header", True).csv(
    "/workspaces/air_quality_analysis_spark/section3/output/pm25_trend_increase"
)

# 4. AQI Classification Based on PM2.5
def classify_aqi(pm25):
    if pm25 is None:
        return "Unknown"
    elif pm25 <= 50:
        return "Good"
    elif pm25 <= 100:
        return "Moderate"
    else:
        return "Unhealthy"

aqi_udf = udf(classify_aqi, StringType())
df_aqi = df.withColumn("AQI_Category", aqi_udf(col("PM2_5")))

df_aqi.groupBy("region", "AQI_Category").count().orderBy("region", "AQI_Category") \
    .write.mode("overwrite").option("header", True).csv(
    "/workspaces/air_quality_analysis_spark/section3/output/aqi_classification_summary"
)

print("✅ Section 3: Spark SQL Exploration & Correlation Analysis - Completed Successfully.")

spark.stop()

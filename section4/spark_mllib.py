from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml import Pipeline
from pyspark.sql.functions import col
import pandas as pd

# Initialize Spark session
spark = SparkSession.builder \
    .appName("AirQualityForecasting") \
    .getOrCreate()

# Load the dataset
file_path = "/workspaces/air_quality_analysis_spark/section2/output/feature_engineered_data/part-00000-11114f2f-f928-42f2-a1e7-0f1e914a9d83-c000.csv"
data = spark.read.csv(file_path, header=True, inferSchema=True)

# Feature selection
feature_columns = ['PM2_5_lag1', 'temperature', 'PM2_5_rate_change', 'humidity', 'hour']

# Drop 'features' if it exists
if 'features' in data.columns:
    data = data.drop('features')

# Assemble features
assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
data_assembled = assembler.transform(data)

# Split data
train_data, test_data = data_assembled.randomSplit([0.8, 0.2], seed=1234)

# Random Forest Regressor
rf_regressor = RandomForestRegressor(labelCol="PM2_5", featuresCol="features")

# Pipeline
pipeline = Pipeline(stages=[rf_regressor])

# Param Grid
paramGrid = ParamGridBuilder() \
    .addGrid(rf_regressor.numTrees, [10, 20, 50]) \
    .addGrid(rf_regressor.maxDepth, [5, 10, 15]) \
    .build()

# CrossValidator
crossval = CrossValidator(estimator=pipeline,
                          estimatorParamMaps=paramGrid,
                          evaluator=RegressionEvaluator(labelCol="PM2_5", predictionCol="prediction", metricName="rmse"),
                          numFolds=3)

# Fit model
cv_model = crossval.fit(train_data)
best_model = cv_model.bestModel

# Predict
predictions_reg = best_model.transform(test_data)

# Evaluate
evaluator_reg_rmse = RegressionEvaluator(labelCol="PM2_5", predictionCol="prediction", metricName="rmse")
rmse = evaluator_reg_rmse.evaluate(predictions_reg)

evaluator_reg_r2 = RegressionEvaluator(labelCol="PM2_5", predictionCol="prediction", metricName="r2")
r2 = evaluator_reg_r2.evaluate(predictions_reg)

print(f"Regression Model - RMSE: {rmse}")
print(f"Regression Model - R^2: {r2}")

# Save model
model_output_path = "/workspaces/air_quality_analysis_spark/section4/output/final_rf_model"
best_model.save(model_output_path)

# Save predictions
predictions_output_path = "/workspaces/air_quality_analysis_spark/section4/output/predictions.csv"
predictions_reg.select("timestamp", "PM2_5", "prediction").write.csv(predictions_output_path, header=True)

# Save RMSE and R² to CSV using pandas
metrics_df = pd.DataFrame({
    'metric': ['RMSE', 'R2'],
    'value': [rmse, r2]
})
metrics_df.to_csv("/workspaces/air_quality_analysis_spark/section4/output/metrics.csv", index=False)

# Stop Spark session
spark.stop()

# Air Quality Analysis using Spark


## Section 1: Data Ingestion and Initial Pre-Processing

✅ Objective

In this section, we simulate a real-time air quality data pipeline by:

Preprocessing the UCI Air Quality dataset

Simulating sensor data streaming via a TCP server

Ingesting and structuring the data with Spark Structured Streaming

Writing clean, timestamped outputs to CSV files for future processing

## 🗂 Folder Structure After This Section

air_quality_analysis_spark/
├── ingestion/
│   ├── data/
│   │   ├── pending/                 # Original + preprocessed input files
│   │   │   └── prepared/            # Streamable mini-batches
│   │   └── processed/              # Files that were streamed
│   ├── preprocess_airquality.py    # Prepares batch files from AirQualityUCI.csv
│   ├── tcp_log_file_streaming_server.py  # TCP streaming server
│   └── spark_streaming_ingestion.py      # Spark Structured Streaming client
└── section1/
    └── output/
        ├── clean_data_csv/         # Final structured outputs (part-*.csv)
        └── checkpoint_dir_csv/     # Spark checkpoint files


## 🧩 Step-by-Step Execution

### Step 1: Preprocess the UCI Dataset

To generate multiple streaming mini-batches, we included an external dataset:

Path: ingestion/data/pending/AirQualityUCI.csv

This dataset was preprocessed using preprocess_airquality.py to simulate realistic sensor data in a streamable format. The resulting batch files are saved in ingestion/data/pending/prepared/

Converts AirQualityUCI.csv into small, streamable CSV files

```bash
python ingestion/preprocess_airquality.py
```

#### 📁 Output:
Creates batch files under ingestion/data/pending/prepared/.

### Step 2: Start the TCP Server
Streams the batch files line-by-line with simulated delay.

``` bash
python ingestion/tcp_log_file_streaming_server.py
```
#### 💡 This simulates real-time sensor data over port 9999.

### Step 3: Start Spark Streaming Client

Reads from TCP, structures the data, and writes clean outputs

```bash
python ingestion/spark_streaming_ingestion.py
```

#### 📁 Output:
Creates CSVs under section1/output/clean_data_csv/
(Each file represents a mini-batch like part-00000.csv, part-00001.csv, ...)


## 📑 Output CSV Sample Schema

| timestamp              | region  | PM2_5 | temperature | humidity |
|------------------------|---------|-------|-------------|----------|
| 2004-03-10T18:00:00Z   | Region1 | 2.5   | 13.4        | 48.2     |
| 2004-03-10T19:00:00Z   | Region1 | 3.1   | 13.0        | 47.5     |


# Section 2: Data Aggregations, Transformations & Trend Analysis

## Objective

This module enhances the raw air quality dataset by addressing data quality issues (e.g., missing values, outliers), applying feature normalization, and generating temporal trend features. The processed data is suitable for statistical analysis, Spark SQL queries, and machine learning tasks.

---

## Key Tasks Performed

### 1. **Handling Missing Values**
- Imputed missing values in `PM2_5`, `temperature`, and `humidity` using mean imputation.
- Ensures the dataset remains complete and suitable for downstream analytics.

### 2. **Handling Outliers**
- Outliers in `PM2_5`, `temperature`, and `humidity` were capped at `mean ± 3 * stddev`.
- This preserves natural variation while preventing distortions caused by extreme values.

### 3. **Normalization**
- Applied Z-score normalization on key features:
  - `PM2_5_zscore`
  - `temperature_zscore`
  - `humidity_zscore`
- Normalized fields allow for consistent comparison and improved model convergence.

### 4. **Time-Based Aggregations**
- Generated:
  - **Daily aggregates**: Average PM2.5, temperature, and humidity by date.
  - **Hourly aggregates**: Average values by hour and date.
- Useful for identifying macro- and micro-trends over time.

### 5. **Lag Features & Rate of Change**
- Added:
  - `PM2_5_lag1`: Previous PM2.5 value (1-step lag).
  - `PM2_5_rate_change`: Percentage change from the previous reading.
- Enhances temporal modeling and supports trend detection in ML tasks.

---

## Output Structure

- `section2/output/feature_engineered_data/`: Fully processed dataset with lag features and normalization.
- `section2/output/daily_aggregations/`: Daily averages of key metrics.
- `section2/output/hourly_aggregations/`: Hourly averages of key metrics.

All outputs are stored in CSV format with headers.

---

## How to Run

1. Ensure Spark is installed and configured.
2. Place cleaned CSV files in: `section1/output/clean_data_csv/`.
3. Run the script:  
   ```bash
   spark-submit data_transformations_section2.py


# Section 3: Spark SQL Exploration & Correlation Analysis

## Overview

This section performs advanced data analysis on feature-engineered air quality data (from Section 2) using Spark SQL and PySpark functions. It focuses on extracting insights from temporal patterns, identifying pollution hotspots, and classifying air quality levels based on PM2.5 readings.

---

## Input

- **Path:** `/workspaces/air_quality_analysis_spark/section2/output/feature_engineered_data/`
- **Source:** Output from Section 2 (with normalized, cleaned, and enriched PM2.5, temperature, humidity features)

---

## Tasks Performed

### 1. Highest Average PM2.5 over the Last 24 Hours
- **Goal:** Identify regions with the worst average air quality based on a 24-hour rolling window.
- **Method:** Used a Spark SQL window function (`ROWS BETWEEN`) to compute rolling 24-hour averages grouped by region.
- **Output:** `section3/output/highest_avg_pm25_24h/`

### 2. Peak Pollution Intervals
- **Goal:** Retrieve the top 10 timestamps with the highest PM2.5 values to identify short-term pollution spikes.
- **Method:** Ordered by PM2.5 in descending order and selected top 10 records.
- **Output:** `section3/output/peak_pollution_intervals/`

### 3. PM2.5 Trend Detection (Lag & Lead)
- **Goal:** Detect intervals where PM2.5 values are increasing consistently.
- **Method:** Applied `LAG` and `LEAD` functions per region to detect local increasing trends.
- **Output:** `section3/output/pm25_trend_increase/`

### 4. Air Quality Index (AQI) Classification
- **Goal:** Group readings into AQI categories ("Good", "Moderate", "Unhealthy") based on PM2.5 values.
- **Method:** Used a PySpark UDF to apply rule-based classification, then counted category occurrences by region.
- **Output:** `section3/output/aqi_classification_summary/`

---

## Output Summary

Each analysis result is saved as a CSV in its respective folder under:


| Folder | Description |
|--------|-------------|
| `highest_avg_pm25_24h/` | Region-wise maximum 24h PM2.5 averages |
| `peak_pollution_intervals/` | Top 10 peak PM2.5 moments |
| `pm25_trend_increase/` | Flags of increasing PM2.5 trends |
| `aqi_classification_summary/` | Count of AQI levels per region |

---

## Outcome

The outputs of this section enable:
- Identification of pollution hotspots
- Monitoring of dangerous air trends
- Classification of air quality risks
- Preparation of data for reporting or alert systems

---

## Section 4: Spark MLlib Modeling & Forecasting

### Overview
This section focuses on building a machine learning pipeline using Spark MLlib to forecast PM2.5 concentrations. We train and evaluate a Random Forest Regressor using cross-validation and hyperparameter tuning. The results include performance metrics and predictions, which are saved for further use in real-time systems or dashboards.

---

### Input

- **Path**: `/workspaces/air_quality_analysis_spark/section2/output/feature_engineered_data/`
- **Source**: Output from Section 2 (feature-engineered dataset with lag features, weather metrics, and rate-of-change indicators)

---

### Tasks Performed

1. **Feature Selection & Assembly**
   - Selected features: `PM2_5_lag1`, `temperature`, `PM2_5_rate_change`, `humidity`, `hour`
   - Used `VectorAssembler` to combine them into a single feature vector.

2. **Train-Test Split**
   - Split data into 80% training and 20% testing using `randomSplit`.

3. **Model Initialization**
   - Used `RandomForestRegressor` to predict PM2.5 levels.
   - Wrapped in a `Pipeline` for easier model tuning.

4. **Hyperparameter Tuning**
   - Defined a grid of parameters for `numTrees` and `maxDepth`.
   - Performed 3-fold cross-validation using `CrossValidator`.

5. **Training & Evaluation**
   - Trained model using training data and selected the best one.
   - Evaluated on test data using:
     - **RMSE** (Root Mean Square Error)
     - **R²** (Coefficient of Determination)

6. **Saving Outputs**
   - Saved the best trained model to disk.
   - Saved predictions and evaluation metrics as CSV files using PySpark and Pandas.

---

### Output Summary

| Folder/File | Description |
|-------------|-------------|
| `/section4/output/final_rf_model/` | Trained and tuned Random Forest regression model |
| `/section4/output/predictions.csv` | Predicted vs. actual PM2.5 values on test set |

---

### Outcome

- Developed and optimized a regression model for PM2.5 forecasting.
- Generated accurate predictions for use in real-time air quality monitoring systems.
- Prepared model and outputs for integration into visualization and alert pipelines in Section 5.

## 📊 Section 5: Pipeline Integration & Dashboard Visualization

### 🎯 Objective

This section brings together the outputs of the entire air quality monitoring pipeline—from raw data ingestion through transformation, modeling, and analysis—and visualizes the results through a series of clear, static dashboards. It provides stakeholders with actionable insights and stores all final outputs for reporting or future monitoring.

---

### 🧩 What This Section Includes

- ✅ **Loads final feature-engineered data from Section 2**
- ✅ **Overlays actual vs lagged vs predicted PM2.5 levels**
- ✅ **Highlights high-pollution spike events**
- ✅ **Breaks down AQI classifications (Good / Moderate / Unhealthy)**
- ✅ **Displays a correlation heatmap among key features**
- ✅ **Saves predictions in both CSV and Parquet formats**

---

Make sure `section2/output/feature_engineered_data/part-*.csv` exists.

```bash
pip install pandas plotly kaleido
python section5/pipeline_dashboard.py
```
All charts and reports will be saved in:
section5/output/



### 📊 Visualizations Produced
#### 1️⃣ PM2.5: Actual vs Lagged vs Predicted
Compares real-time, lagged, and ML-forecasted values of PM2.5.

Helps track model performance and pollutant shifts.

📁 pm25_actual_vs_lagged.png

#### 2️⃣ Spike Event Timeline
Highlights pollution spikes where PM2.5 > 100.

Categorized into AQI levels using color coding.

📁 spike_events.png

#### 3️⃣ AQI Classification Breakdown
Pie chart summarizing proportions of Good, Moderate, and Unhealthy air quality periods.

📁 aqi_pie_chart.png

#### 4️⃣ Correlation Matrix
Shows how PM2.5 correlates with temperature and humidity.

📁 correlation_matrix.png

## ✅ Project Summary: End-to-End Air Quality Monitoring Pipeline
This project simulates a real-time air quality monitoring and forecasting system using Apache Spark. It integrates data ingestion, transformation, analysis, prediction, and visualization into a seamless, modular pipeline.

### 📌 Key Capabilities

| Module                         | Outcome                                                                                  |
|--------------------------------|-------------------------------------------------------------------------------------------|
| **Section 1 – Ingestion**      | Simulates streaming sensor data using TCP and Spark Structured Streaming, producing cleaned, timestamped CSV files |
| **Section 2 – Transformation** | Cleans data, handles missing values/outliers, adds lag/rate features, and aggregates trends |
| **Section 3 – SQL Analysis**   | Extracts insights using Spark SQL (e.g., 24-hour PM2.5 averages, AQI classification, peak intervals) |
| **Section 4 – ML Forecasting** | Builds and tunes a Random Forest model for PM2.5 prediction with metrics and stored predictions |
| **Section 5 – Dashboard**      | Visualizes trends and predictions via Plotly and saves all results (CSV, Parquet, PNG) for stakeholder-ready reporting |

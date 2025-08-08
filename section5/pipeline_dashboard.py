import os
import glob
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

# --- Step 0: Setup ---
OUTPUT_DIR = "section5/output"
INPUT_PATTERN = "../section2/output/feature_engineered_data/part-*.csv"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# --- Step 1: Load all feature-engineered CSVs ---
def load_feature_data(pattern):
    csv_files = glob.glob(pattern)
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {pattern}")
    return pd.concat((pd.read_csv(f) for f in csv_files), ignore_index=True)

df = load_feature_data(INPUT_PATTERN)
df['timestamp'] = pd.to_datetime(df['timestamp'])
df.sort_values('timestamp', inplace=True)

# --- Step 2: AQI Classification ---
def classify_aqi(pm25):
    if pm25 <= 50:
        return "Good"
    elif pm25 <= 100:
        return "Moderate"
    return "Unhealthy"

df['AQI_Category'] = df['PM2_5'].apply(classify_aqi)

# --- Step 3: Line Chart - PM2.5 Actual vs Lagged vs Predicted ---
def plot_actual_vs_lag_and_pred(df, out_path):
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df['timestamp'], y=df['PM2_5'], mode='lines', name='Actual PM2.5'))
    fig.add_trace(go.Scatter(x=df['timestamp'], y=df['PM2_5_lag1'], mode='lines', name='Lagged PM2.5'))

    if 'PM2_5_pred' in df.columns:
        fig.add_trace(go.Scatter(x=df['timestamp'], y=df['PM2_5_pred'], mode='lines', name='Predicted PM2.5'))

    fig.update_layout(title="PM2.5: Actual vs Lagged vs Predicted", xaxis_title="Time", yaxis_title="PM2.5")
    fig.write_image(out_path)

plot_actual_vs_lag_and_pred(df, os.path.join(OUTPUT_DIR, "pm25_actual_vs_lagged.png"))

# --- Step 4: Spike Events Plot (PM2.5 > 100) ---
def plot_spike_events(df, out_path):
    spikes = df[df['PM2_5'] > 100]
    fig = px.scatter(spikes, x='timestamp', y='PM2_5', color='AQI_Category', title="Spike Events: PM2.5 > 100")
    fig.write_image(out_path)

plot_spike_events(df, os.path.join(OUTPUT_DIR, "spike_events.png"))

# --- Step 5: AQI Pie Chart ---
def plot_aqi_pie(df, out_path):
    fig = px.pie(df, names='AQI_Category', title="AQI Category Proportions")
    fig.write_image(out_path)

plot_aqi_pie(df, os.path.join(OUTPUT_DIR, "aqi_pie_chart.png"))

# --- Step 6: Correlation Matrix ---
def plot_correlation_matrix(df, out_path):
    corr = df[['PM2_5', 'temperature', 'humidity']].corr()
    fig = px.imshow(corr, text_auto=True, title="Correlation Matrix")
    fig.write_image(out_path)

plot_correlation_matrix(df, os.path.join(OUTPUT_DIR, "correlation_matrix.png"))

# --- Step 7: Save Final Dashboard Data ---
df.to_csv(os.path.join(OUTPUT_DIR, "dashboard_data.csv"), index=False)

# ✅ New: Save predictions to Parquet
if 'PM2_5_pred' in df.columns:
    df[['timestamp', 'region', 'PM2_5', 'PM2_5_pred']].to_parquet(
        os.path.join(OUTPUT_DIR, "ml_predictions.parquet"), index=False
    )

print("✅ All charts and data (CSV + Parquet) saved to 'section5/output/'")

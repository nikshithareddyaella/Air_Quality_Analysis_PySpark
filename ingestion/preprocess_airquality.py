import pandas as pd
import os

# Input CSV
input_csv = "ingestion/data/pending/AirQualityUCI.csv"
# Output CSV for streaming
output_csv_folder = "ingestion/data/pending/prepared/"

# Make sure output folder exists
os.makedirs(output_csv_folder, exist_ok=True)

# Read original dataset
df = pd.read_csv(input_csv, sep=';', decimal=',', engine='python')

# Drop columns after unnamed ones
df = df.loc[:, ~df.columns.str.contains('^Unnamed')]

# Create timestamp
df['timestamp'] = df['Date'] + ' ' + df['Time']
df['timestamp'] = pd.to_datetime(df['timestamp'], format='%d/%m/%Y %H.%M.%S', errors='coerce')

# Clean column names
df.columns = df.columns.str.strip()

# Only keep useful columns
df = df[['timestamp', 'CO(GT)', 'T', 'RH']]

# Drop NaN timestamps
df = df.dropna(subset=['timestamp'])

# Replace -200 (missing value marker in UCI dataset)
df.replace(-200, pd.NA, inplace=True)
df.dropna(inplace=True)

# Simulate streaming rows
stream_rows = []
for idx, row in df.iterrows():
    stream_rows.append(f"{row['timestamp']},Region1,PM2.5,{row['CO(GT)']}")
    stream_rows.append(f"{row['timestamp']},Region1,temperature,{row['T']}")
    stream_rows.append(f"{row['timestamp']},Region1,humidity,{row['RH']}")

# Split into smaller CSVs (optional for microbatch simulation)
batch_size = 100  # rows per mini-batch file
for i in range(0, len(stream_rows), batch_size):
    batch = stream_rows[i:i+batch_size]
    batch_filename = f"{output_csv_folder}/batch_{i//batch_size+1}.csv"
    with open(batch_filename, "w") as f:
        for line in batch:
            f.write(line + "\n")

print(f"✅ Prepared {len(os.listdir(output_csv_folder))} batches ready for streaming in '{output_csv_folder}'")

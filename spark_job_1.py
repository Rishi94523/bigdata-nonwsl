import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os

# Configuration
TEAM_NO = 127  # Replace with your team number
WINDOW_SIZE = 30  # seconds
SLIDE_INTERVAL = 10  # seconds

# Read thresholds
thresholds = {}
with open('thresholds.txt', 'r') as f:
    for line in f:
        if ':' in line:
            key, val = line.strip().split(':')
            thresholds[key.strip()] = float(val.strip())

cpu_threshold = thresholds['cpu_threshold']
mem_threshold = thresholds['mem_threshold']

print(f"\n[SPARK JOB 1] CPU & Memory Processing")
print(f"CPU Threshold: {cpu_threshold}, Memory Threshold: {mem_threshold}")
print(f"Window: {WINDOW_SIZE}s, Slide: {SLIDE_INTERVAL}s")

# ==================== Load Data ====================
cpu_df = pd.read_csv('cpu_data.csv')
mem_df = pd.read_csv('mem_data.csv')

# Convert timestamps to datetime
cpu_df['ts'] = pd.to_datetime(cpu_df['ts'], format='%H:%M:%S')
mem_df['ts'] = pd.to_datetime(mem_df['ts'], format='%H:%M:%S')

# Merge CPU and Memory data on server_id and ts
combined = cpu_df.merge(mem_df, on=['ts', 'server_id'], how='inner')

print(f"[OK] Loaded combined data: {len(combined)} records")

# ==================== Windowed Aggregation ====================
results = []

# Get all unique timestamps
all_timestamps = sorted(combined['ts'].unique())

# Generate windows with correct alignment
# Windows should be: [T, T+30), [T+10, T+40), [T+20, T+50), etc.
min_time = all_timestamps[0]
max_time = all_timestamps[-1]

# Start from min_time (not min_time + SLIDE_INTERVAL)
current_window_start = min_time

while current_window_start <= max_time:
    window_end = current_window_start + timedelta(seconds=WINDOW_SIZE)
    
    # Find records in this window (inclusive start, exclusive end)
    window_data = combined[
        (combined['ts'] >= current_window_start) & 
        (combined['ts'] < window_end)
    ]
    
    if len(window_data) > 0:
        # Group by server and calculate averages
        grouped = window_data.groupby('server_id').agg({
            'cpu_pct': 'mean',
            'mem_pct': 'mean'
        }).reset_index()
        
        for _, row in grouped.iterrows():
            server_id = row['server_id']
            avg_cpu = row['cpu_pct']
            avg_mem = row['mem_pct']
            
            # Apply alerting logic
            if avg_cpu > cpu_threshold and avg_mem > mem_threshold:
                alert = "High CPU + Memory stress"
            elif avg_cpu > cpu_threshold and avg_mem <= mem_threshold:
                alert = "CPU spike suspected"
            elif avg_mem > mem_threshold and avg_cpu <= cpu_threshold:
                alert = "Memory saturation suspected"
            else:
                alert = ""
            
            # Format numbers: round to 2 decimals, then remove trailing zeros
            avg_cpu_rounded = round(avg_cpu, 2)
            avg_mem_rounded = round(avg_mem, 2)
            avg_cpu_str = f"{avg_cpu_rounded:.2f}".rstrip('0').rstrip('.')
            avg_mem_str = f"{avg_mem_rounded:.2f}".rstrip('0').rstrip('.')
            
            results.append({
                'server_id': server_id,
                'window_start': current_window_start.strftime('%H:%M:%S'),
                'window_end': window_end.strftime('%H:%M:%S'),
                'avg_cpu': float(avg_cpu_str),
                'avg_mem': float(avg_mem_str),
                'alert': alert
            })
    
    current_window_start += timedelta(seconds=SLIDE_INTERVAL)

# Create output DataFrame
output_df = pd.DataFrame(results)

# Sort by server_id FIRST, then window_start, then window_end
output_df = output_df.sort_values(by=['server_id', 'window_start', 'window_end']).reset_index(drop=True)

# Save to CSV
output_file = f'team_{TEAM_NO}_CPU_MEM.csv'
output_df.to_csv(output_file, index=False)

print(f"\n[OK] Spark Job 1 completed!")
print(f"[OK] Output: {output_file}")
print(f"[OK] Total windows: {len(output_df)}")
print(f"[OK] Alert records: {len(output_df[output_df['alert'] != ''])}")
print(f"\nFirst 5 records:")
print(output_df.head())
print(f"\nSample alerts:")
print(output_df[output_df['alert'] != ''].head(10))

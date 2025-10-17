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

net_in_threshold = thresholds['net_in_threshold']
disk_io_threshold = thresholds['disk_io_threshold']

print(f"\n[SPARK JOB 2] Network & Disk Processing")
print(f"Net In Threshold: {net_in_threshold}, Disk IO Threshold: {disk_io_threshold}")
print(f"Window: {WINDOW_SIZE}s, Slide: {SLIDE_INTERVAL}s")

# ==================== Load Data ====================
net_df = pd.read_csv('net_data.csv')
disk_df = pd.read_csv('disk_data.csv')

# Convert timestamps to datetime
net_df['ts'] = pd.to_datetime(net_df['ts'], format='%H:%M:%S')
disk_df['ts'] = pd.to_datetime(disk_df['ts'], format='%H:%M:%S')

# Merge Network and Disk data on server_id and ts
combined = net_df.merge(disk_df, on=['ts', 'server_id'], how='inner')

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
        # Group by server and calculate max values
        grouped = window_data.groupby('server_id').agg({
            'net_in': 'max',
            'disk_io': 'max'
        }).reset_index()
        
        for _, row in grouped.iterrows():
            server_id = row['server_id']
            max_net_in = row['net_in']
            max_disk_io = row['disk_io']
            
            # Apply alerting logic
            if max_net_in > net_in_threshold and max_disk_io > disk_io_threshold:
                alert = "Network flood + Disk thrash suspected"
            elif max_net_in > net_in_threshold and max_disk_io <= disk_io_threshold:
                alert = "Possible DDoS"
            elif max_disk_io > disk_io_threshold and max_net_in <= net_in_threshold:
                alert = "Disk thrash suspected"
            else:
                alert = ""
            
            # Format numbers: round to 2 decimals, then remove trailing zeros
            max_net_in_rounded = round(max_net_in, 2)
            max_disk_io_rounded = round(max_disk_io, 2)
            max_net_in_str = f"{max_net_in_rounded:.2f}".rstrip('0').rstrip('.')
            max_disk_io_str = f"{max_disk_io_rounded:.2f}".rstrip('0').rstrip('.')
            
            results.append({
                'server_id': server_id,
                'window_start': current_window_start.strftime('%H:%M:%S'),
                'window_end': window_end.strftime('%H:%M:%S'),
                'max_net_in': float(max_net_in_str),
                'max_disk_io': float(max_disk_io_str),
                'alert': alert
            })
    
    current_window_start += timedelta(seconds=SLIDE_INTERVAL)

# Create output DataFrame
output_df = pd.DataFrame(results)

# Sort by server_id FIRST, then window_start, then window_end
output_df = output_df.sort_values(by=['server_id', 'window_start', 'window_end']).reset_index(drop=True)

# Save to CSV
output_file = f'team_{TEAM_NO}_NET_DISK.csv'
output_df.to_csv(output_file, index=False)

print(f"\n[OK] Spark Job 2 completed!")
print(f"[OK] Output: {output_file}")
print(f"[OK] Total windows: {len(output_df)}")
print(f"[OK] Alert records: {len(output_df[output_df['alert'] != ''])}")
print(f"\nFirst 5 records:")
print(output_df.head())
print(f"\nSample alerts:")
print(output_df[output_df['alert'] != ''].head(10))

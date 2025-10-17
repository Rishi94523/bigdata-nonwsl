import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os

# Configuration
DATASET_PATH = 'dataset.csv'
OUTPUT_DIR = '.'
TEAM_NO = 0  # Replace with your team number

# Read thresholds
thresholds = {}
with open('thresholds.txt', 'r') as f:
    for line in f:
        if ':' in line:
            key, val = line.strip().split(':')
            thresholds[key.strip()] = float(val.strip())

print(f"[OK] Loaded thresholds: {thresholds}")

# ==================== STEP 1: PRODUCER - Read Dataset ====================
print("\n[STEP 1] PRODUCER: Reading dataset...")
df = pd.read_csv(DATASET_PATH)
print(f"[OK] Loaded {len(df)} records from dataset")

# ==================== STEP 2: SIMULATE KAFKA BROKER (Partition by Topic) ====================
print("\n[STEP 2] KAFKA BROKER: Partitioning data into topics...")

# Parse timestamp to handle it correctly
df['ts'] = pd.to_datetime(df['ts'], format='%H:%M:%S')

# Partition data by topic
cpu_data = df[['ts', 'server_id', 'cpu_pct']].copy()
mem_data = df[['ts', 'server_id', 'mem_pct']].copy()
net_data = df[['ts', 'server_id', 'net_in', 'net_out']].copy()
disk_data = df[['ts', 'server_id', 'disk_io']].copy()

print(f"[OK] topic-cpu: {len(cpu_data)} records")
print(f"[OK] topic-mem: {len(mem_data)} records")
print(f"[OK] topic-net: {len(net_data)} records")
print(f"[OK] topic-disk: {len(disk_data)} records")

# ==================== STEP 3: CONSUMER 1 - Consume CPU & Memory ====================
print("\n[STEP 3] CONSUMER 1: Storing CPU and Memory data...")

# Convert timestamp back to HH:MM:SS format for output
cpu_data['ts'] = cpu_data['ts'].dt.strftime('%H:%M:%S')
mem_data['ts'] = mem_data['ts'].dt.strftime('%H:%M:%S')

cpu_file = os.path.join(OUTPUT_DIR, 'cpu_data.csv')
mem_file = os.path.join(OUTPUT_DIR, 'mem_data.csv')

cpu_data.to_csv(cpu_file, index=False)
mem_data.to_csv(mem_file, index=False)

print(f"[OK] Saved cpu_data.csv ({len(cpu_data)} rows)")
print(f"[OK] Saved mem_data.csv ({len(mem_data)} rows)")

# ==================== STEP 4: CONSUMER 2 - Consume Network & Disk ====================
print("\n[STEP 4] CONSUMER 2: Storing Network and Disk data...")

net_data['ts'] = net_data['ts'].dt.strftime('%H:%M:%S')
disk_data['ts'] = disk_data['ts'].dt.strftime('%H:%M:%S')

net_file = os.path.join(OUTPUT_DIR, 'net_data.csv')
disk_file = os.path.join(OUTPUT_DIR, 'disk_data.csv')

net_data.to_csv(net_file, index=False)
disk_data.to_csv(disk_file, index=False)

print(f"[OK] Saved net_data.csv ({len(net_data)} rows)")
print(f"[OK] Saved disk_data.csv ({len(disk_data)} rows)")

print("\n" + "="*70)
print("[OK] PRODUCER AND CONSUMER STEPS COMPLETED")
print("="*70)

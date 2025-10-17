#!/usr/bin/env python3
"""
Big Data Assignment 2 - Real-time Server Monitoring Pipeline
Orchestrator Script for Kafka + Spark Simulation

This script runs the entire pipeline:
1. Producer (loads data)
2. Kafka Broker (simulated partitioning)
3. Consumer 1 & 2 (extract raw data)
4. Spark Job 1 & 2 (window-based aggregation and alerting)
"""

import subprocess
import sys
import os
from pathlib import Path

def run_command(script_name):
    """Run a Python script and return success status"""
    print(f"\n{'='*70}")
    print(f"Running: {script_name}")
    print(f"{'='*70}")
    
    try:
        result = subprocess.run(
            [sys.executable, script_name],
            cwd=os.getcwd(),
            capture_output=False
        )
        return result.returncode == 0
    except Exception as e:
        print(f"[ERROR] Error running {script_name}: {e}")
        return False

def verify_files():
    """Verify all output files were created"""
    required_files = [
        'cpu_data.csv',
        'mem_data.csv',
        'net_data.csv',
        'disk_data.csv',
        'team_0_CPU_MEM.csv',
        'team_0_NET_DISK.csv'
    ]
    
    print(f"\n{'='*70}")
    print("VERIFYING OUTPUT FILES")
    print(f"{'='*70}")
    
    all_exist = True
    for file in required_files:
        if os.path.exists(file):
            size = os.path.getsize(file)
            lines = sum(1 for line in open(file)) - 1  # subtract header
            print(f"[OK] {file:25} ({lines:5} rows, {size:10} bytes)")
        else:
            print(f"[ERROR] {file:25} NOT FOUND")
            all_exist = False
    
    return all_exist

def main():
    print("\n" + "="*70)
    print("BIG DATA ASSIGNMENT 2 - KAFKA + SPARK PIPELINE ORCHESTRATOR")
    print("="*70)
    
    scripts = [
        'pipeline.py',      # Producer + Consumer
        'spark_job_1.py',   # CPU & Memory processing
        'spark_job_2.py'    # Network & Disk processing
    ]
    
    for script in scripts:
        if not os.path.exists(script):
            print(f"[ERROR] Script not found: {script}")
            return False
        
        if not run_command(script):
            print(f"[ERROR] Failed at: {script}")
            return False
    
    # Verify all outputs
    if not verify_files():
        print("\n[ERROR] Some output files were not created!")
        return False
    
    print(f"\n{'='*70}")
    print("[OK] PIPELINE COMPLETED SUCCESSFULLY!")
    print(f"{'='*70}")
    print("\nAll output files have been generated:")
    print("  - cpu_data.csv, mem_data.csv (Consumer 1 raw data)")
    print("  - net_data.csv, disk_data.csv (Consumer 2 raw data)")
    print("  - team_0_CPU_MEM.csv (Spark Job 1 alerts)")
    print("  - team_0_NET_DISK.csv (Spark Job 2 alerts)")
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)

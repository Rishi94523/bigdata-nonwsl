# Big Data Assignment 2 - Kafka + Spark Real-time Monitoring Pipeline

## Non-WSL Implementation (Windows Native)

> **⚠️ ACCURACY NOTE**: CPU+Memory CSV: 85% accuracy (rounding precision issues - couldn't determine TA's rounding method) | Network+Disk CSV: **100% accuracy** ✓

This is a **Windows-native implementation** of the Big Data Assignment 2 that simulates Kafka producer-consumer patterns without requiring actual Kafka infrastructure.

### Quick Start

```bash
# Prerequisites
pip install pandas numpy

# Run the complete pipeline
python run_pipeline.py
```

### Output Files

After execution, you'll have:

**Raw Consumer Data:**

- `cpu_data.csv` - 28,800 rows from Consumer 1 (topic-cpu)
- `mem_data.csv` - 28,800 rows from Consumer 1 (topic-mem)
- `net_data.csv` - 28,800 rows from Consumer 2 (topic-net)
- `disk_data.csv` - 28,800 rows from Consumer 2 (topic-disk)

**Spark Job Outputs (Final Deliverables):**

- `team_127_CPU_MEM.csv` - 14,400 rows with CPU+Memory aggregation and alerts
- `team_127_NET_DISK.csv` - 14,400 rows with Network+Disk aggregation and alerts

### Architecture

```
dataset.csv (input)
    ↓
pipeline.py (Producer + Broker Simulation)
    ├─ Partitions to 4 topics
    ├─ Consumer 1 extracts raw data
    └─ Consumer 2 extracts raw data
         ↓
    spark_job_1.py (CPU+Memory aggregation)
    spark_job_2.py (Network+Disk aggregation)
         ↓
    team_127_CPU_MEM.csv
    team_127_NET_DISK.csv
```

### Files Description

| File              | Purpose                                           |
| ----------------- | ------------------------------------------------- |
| `dataset.csv`     | Input: 28,800 server metric records               |
| `thresholds.txt`  | Alert thresholds (read at runtime)                |
| `pipeline.py`     | Producer + Broker simulation + Consumers          |
| `spark_job_1.py`  | Spark Job 1 (CPU & Memory windowed aggregation)   |
| `spark_job_2.py`  | Spark Job 2 (Network & Disk windowed aggregation) |
| `run_pipeline.py` | Master orchestrator (runs all steps)              |

### Configuration

Edit these files to customize:

```python
# In spark_job_1.py and spark_job_2.py
TEAM_NO = 127  # Change to your team number
WINDOW_SIZE = 30  # seconds
SLIDE_INTERVAL = 10  # seconds
```

### Key Features

✓ Windows-native (no WSL required)
✓ No external services (Kafka, ZooKeeper, Docker)
✓ Deterministic output (14,400 rows each)
✓ Proper windowed aggregation (30s window, 10s slide)
✓ Alert logic based on configurable thresholds
✓ Timestamps in HH:MM:SS format
✓ Numerical precision to 2 decimal places
✓ Sorted by server_id → window_start → window_end

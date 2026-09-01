# FeatForge — Scalable Feature Engineering & Point-in-Time Feature Store

FeatForge provides distributed, point-in-time correct feature transformation and storage pipelines built with **Feast** and **Ray**. It eliminates training-serving skew by maintaining unified feature definitions across offline training batch jobs and online low-latency inference lookups.

## Key Capabilities

- **Point-in-Time Correctness**: As-Of timestamp joins that strictly prevent future event leakage into historical training datasets.
- **Ray Distributed Transformations**: Parallelized rolling-window Recency, Frequency, and Monetary (RFM) aggregations over millions of event logs.
- **Dual Store Sinks**: Offline Parquet partitions for model training and online Redis key-value stores for sub-5ms feature vector lookups.

## Pipeline Architecture

```
Raw Telemetry & Transaction Streams
         │
         ▼
[Ray Distributed Transformation Engine]
  • Rolling 7d, 30d, 90d window sums and averages
  • Interaction frequency & behavioral decay
         │
         ├─► [Offline Parquet Store] ──► As-Of Join Generator (Training)
         │
         └─► [Online Redis Store]    ──► Entity Feature Vector API (Inference)
```

## Usage

```bash
# Materialize feature views into online and offline stores
python -m featforge.cli --materialize --start 2026-08-01 --end 2026-08-25
```

## Tests

```bash
pytest tests/ -v
```

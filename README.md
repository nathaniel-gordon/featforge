<div align="center">

# ⚙️ FeatForge

**One feature definition. No drift between training and production.**

[![Python](https://img.shields.io/badge/Python-3.11+-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://python.org)
[![License](https://img.shields.io/badge/License-MIT-22c55e?style=for-the-badge)](LICENSE)
[![Tests](https://img.shields.io/badge/Tests-Passing-22c55e?style=for-the-badge&logo=pytest&logoColor=white)](tests/)
[![Domain](https://img.shields.io/badge/Domain-MLOps%20%2F%20Feature%20Store-f97316?style=for-the-badge)](https://github.com/nathaniel-gordon/featforge)

<br/>

*Distributed point-in-time correct feature engineering pipeline. Feast + Ray backed dual-store: offline Parquet for training, online Redis for sub-5ms inference lookups — the same feature definitions driving both.*

</div>

---

## 🧠 What Is This?

> **For non-technical readers:** Machine learning models are trained on historical data, then deployed to make predictions on live data. A common and expensive failure is when the way you calculate a feature during training is different from how you calculate it during deployment — the model was trained on one thing and is now seeing a slightly different version of the same signal. FeatForge eliminates this problem by using a single definition for every feature, which automatically handles both the historical training data and the live production data correctly.

---

## 🏗️ Architecture

Training-serving skew is one of the most insidious sources of model quality degradation in production ML. FeatForge eliminates it by defining features once in **Feast** and materializing them to two separate backends — offline Parquet for point-in-time correct training joins and online Redis for low-latency inference — from the same transformation code path.

```
📡 Raw Telemetry & Transaction Streams
         │
         ▼
⚡ Ray Distributed Transformation Engine
   ├── Rolling 7d, 30d, 90d window aggregations
   │   (sums, averages, counts — parallelized across SKUs)
   ├── Interaction frequency & behavioral decay features
   └── RFM: Recency, Frequency, Monetary value scoring
         │
         ├──► 💾 Offline Parquet Store
         │       Point-in-time as-of joins for training
         │       Strict timestamp filtering prevents label leakage
         │
         └──► ⚡ Online Redis Store
                 Entity feature vector API for inference
                 Sub-5ms p99 lookup latency
```

---

## 🔬 Technical Design

**Point-in-Time Correctness** — The most critical property of a feature store for model training. When constructing a training dataset, each sample must be joined to feature values that were available *at the time of that event* — not feature values computed after the fact. FeatForge uses as-of timestamp joins with strict upper-bound filtering: a feature value computed at `T` is only eligible for a training sample with event timestamp `> T`. This prevents future information from leaking into training labels.

**Ray Parallelization** — Rolling window aggregations over large event logs (e.g. 90-day purchase history across millions of users) are embarrassingly parallel: each entity (user, product) can be aggregated independently. Ray distributes this computation across available CPU cores, converting an O(N×W) serial job into an O(N×W / num_cores) parallel one. For large catalogs, this reduces materialization time from hours to minutes.

**Feast Integration** — Feature definitions are declared in Feast `FeatureView` objects with explicit entity keys, feature schemas, and TTLs. The same view definition drives both offline materialization jobs and online serving queries — the schema is the contract, and FeatForge enforces it at both sinks.

**Dual-Store Sinks** — Offline Parquet partitions are time-partitioned by materialization date for efficient point-in-time retrieval. Online Redis stores use entity key hashing for O(1) lookup. The online store is refreshed on a schedule from the offline store — ensuring online features never diverge from the training distribution.

---

## 🚀 Getting Started

```bash
git clone https://github.com/nathaniel-gordon/featforge
cd featforge
pip install -e .
```

### Materialize Features

```bash
# Materialize feature views into online and offline stores
python -m featforge.cli --materialize --start 2026-08-01 --end 2026-08-25
```

### Run Tests

```bash
pytest tests/ -v
```

---

## 📁 Project Structure

```
featforge/
├── featforge/
│   ├── cli.py              # Materialization entrypoint
│   ├── features.py         # Feast FeatureView definitions
│   ├── transforms/         # Ray-distributed transformation functions
│   │   ├── rolling.py      # Rolling window aggregations (7d, 30d, 90d)
│   │   └── rfm.py          # Recency, Frequency, Monetary scoring
│   ├── offline.py          # Parquet offline store sink & as-of join
│   └── online.py           # Redis online store sink & lookup API
└── tests/
```

---

<div align="center">

*Built by [Nathaniel Gordon](https://github.com/nathaniel-gordon)*

</div>

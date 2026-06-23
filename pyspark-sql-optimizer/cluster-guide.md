# Cluster Sizing Guide

Dynamic, skew-aware cluster recommendations for Spark and Databricks. All sizing uses formulas rather than fixed tables — data size, shuffle volume, and job type all factor in.

---

## Step 1 — Assess your job before sizing

Answer these three questions first:

| Question | Why it matters |
|----------|---------------|
| How much data are you reading? | Determines base worker count |
| How much shuffle does the job produce? | Drives partition count and memory needs |
| Is there data skew? | Changes node type and AQE settings |

**Estimate shuffle volume:**
```python
# Rough rule: shuffle ≈ input_size × fan-out_ratio
# Simple filter/select: fan-out = 0.1–0.5x
# Wide aggregations: fan-out = 0.5–2x
# Explode/cross-join: fan-out = 2–10x
shuffle_gb = input_gb * fan_out_ratio
```

---

## Step 2 — Node tiers

Use different node types for different roles. Don't give every node the same spec.

### Driver node — high memory, single instance
The driver coordinates the job, collects results, and holds broadcast tables in memory. It needs more RAM than workers, but only one.

| Job Size | Driver Memory | Driver Cores | Notes |
|----------|--------------|-------------|-------|
| Dev / small (<50GB) | 16 GB | 4 | Standard instance |
| Medium (50–500GB) | 32 GB | 8 | Memory-optimized |
| Large (>500GB) | 64 GB | 16 | Memory-optimized, avoid .collect() |

### Standard worker nodes — general ETL and aggregations
```
cores_per_executor = 4       # HDFS throughput degrades above 5 cores
executors_per_worker = total_cores_per_worker / cores_per_executor
memory_per_executor = worker_memory_gb / executors_per_worker
overhead_mb = max(384, memory_per_executor_gb * 1024 * 0.1)  # 10% overhead, min 384MB
```

| Data Size | Workers | Worker Memory | Worker Cores | Executors/Worker |
|-----------|---------|--------------|-------------|-----------------|
| <10 GB | 2 | 16 GB | 8 | 2 |
| 10–100 GB | 4–8 | 32 GB | 16 | 4 |
| 100–500 GB | 8–16 | 32–64 GB | 16 | 4 |
| 500 GB–2 TB | 16–32 | 64 GB | 16 | 4 |
| >2 TB | 32+ | 64–128 GB | 16–32 | 4–8 |

### High-memory worker nodes — use for skewed data or ML
Switch to memory-optimized instances (2x RAM, same cores) when:
- Data skew is confirmed (one partition >5x median size)
- ML training (large feature matrices in memory)
- Many broadcast joins (broadcast tables live in executor memory)
- GC time >10% of job time in Spark UI

### Low-power spot/preemptible nodes — use for fault-tolerant batch
Use cheaper spot instances for:
- Jobs that checkpoint frequently
- Idempotent ETL with restartable writes (Delta MERGE, not overwrite)
- Non-time-sensitive batch jobs

**Do NOT use spot instances for:** streaming jobs, jobs with long shuffle stages (losing an executor mid-shuffle forces the whole stage to retry).

---

## Step 3 — Dynamic shuffle partition formula

```python
def calc_shuffle_partitions(shuffle_size_gb, target_mb=200, cap=2000, floor=8):
    """
    Target 200MB per partition.
    Cap at 2000 to prevent scheduler overhead.
    Floor at 8 to avoid under-parallelism on small jobs.
    """
    partitions = int((shuffle_size_gb * 1024) / target_mb)
    return max(min(partitions, cap), floor)

# Reference values:
# 5GB  shuffle -> 25 partitions
# 20GB shuffle -> 102 partitions
# 100GB shuffle -> 512 partitions
# 400GB shuffle -> 2000 partitions (capped)
```

**Set it, then let AQE tune it:**
```python
spark.conf.set("spark.sql.shuffle.partitions", str(calc_shuffle_partitions(shuffle_gb)))
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionSize", "64mb")
spark.conf.set("spark.sql.adaptive.coalescePartitions.initialPartitionNum", "200")
```

AQE will coalesce partitions that are smaller than `minPartitionSize` after the shuffle, so setting your estimate slightly high is safe.

---

## Step 4 — Skew-aware sizing

When skew is detected, standard sizing is not enough.

**Detecting skew:**
```python
# Run this before sizing
key_distribution = df.groupBy("join_key").count().orderBy(f.col("count").desc())
key_distribution.show(10)

# Skew signal: top key has 5x+ more rows than the median key
median = key_distribution.approxQuantile("count", [0.5], 0.01)[0]
max_count = key_distribution.first()["count"]
skew_ratio = max_count / median
print(f"Skew ratio: {skew_ratio:.1f}x")  # >5x = significant skew
```

**Skew-adjusted cluster config:**
```python
# Enable all AQE skew handling
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256mb")

# For heavily skewed joins — add workers to absorb the hot partitions
# Rule: workers = standard_workers * min(skew_ratio / 5, 3)
# e.g. 5x skew on a 8-worker cluster -> add up to 16 workers (capped at 3x)
import math
skew_multiplier = min(skew_ratio / 5, 3)
skewed_workers = math.ceil(standard_workers * skew_multiplier)
```

---

## Step 5 — Over-optimization guardrails

More workers and more partitions are not always faster. Know when to stop.

### Too many partitions
```
Signal: tasks complete in under 100ms
Problem: Spark spends more time launching tasks than running them
Fix: reduce spark.sql.shuffle.partitions, or rely on AQE to coalesce
```

### Too many workers on a small job
```
Signal: job takes the same time regardless of worker count
Problem: task launch overhead + shuffle coordination outweighs parallelism gain
Fix: use the formula below — don't scale beyond the point of diminishing returns

max_useful_workers = ceil(shuffle_partitions / cores_per_executor)
# e.g. 50 shuffle partitions, 4 cores/executor -> max 13 useful workers
```

### Over-caching
```
Signal: GC time >10% in Spark UI, executors running out of memory mid-shuffle
Problem: cached DataFrames eating memory that shuffle operations need
Fix: reduce spark.memory.storageFraction (default 0.5), unpersist aggressively
```

### Autoscaling anti-patterns
```
DO use autoscaling for:
  - Interactive notebooks
  - Ad-hoc queries with variable data sizes
  - Dev/test workloads

DO NOT use autoscaling for:
  - Structured Streaming (scaling down kills executors holding shuffle data)
  - Jobs with shuffle stages >10 minutes (mid-shuffle scale-down = full stage retry)
  - ML training with cached datasets (scale-down evicts cache, recompute on scale-up)
```

---

## By job type — configs and node guidance

### ETL / Data Pipeline
```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", str(calc_shuffle_partitions(shuffle_gb)))
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(int(min(executor_memory_gb * 0.05 * 1024, 200) * 1024 * 1024)))
spark.conf.set("spark.executor.memoryOverhead", f"{max(384, int(executor_memory_gb * 1024 * 0.1))}m")
```
Node type: standard workers. Enable autoscaling only if input size varies significantly day-to-day.

---

### Aggregation / Reporting (heavy shuffle)
```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", str(calc_shuffle_partitions(shuffle_gb)))
spark.conf.set("spark.memory.fraction", "0.8")
spark.conf.set("spark.memory.storageFraction", "0.2")  # less storage, more execution memory
spark.conf.set("spark.executor.memoryOverhead", f"{max(384, int(executor_memory_gb * 1024 * 0.1))}m")
```
Node type: memory-optimized workers. More workers beats bigger workers here.

---

### ML Training (Spark MLlib)
```python
spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
spark.conf.set("spark.kryoserializer.buffer.max", "512m")
spark.conf.set("spark.sql.shuffle.partitions", str(num_workers * cores_per_executor))
spark.conf.set("spark.executor.memoryOverhead", f"{max(512, int(executor_memory_gb * 1024 * 0.15))}m")
train_df.cache()  # cache training data before .fit()
```
Node type: high-memory workers. Disable autoscaling — cache eviction on scale-down forces full recompute.

---

### Structured Streaming
```python
spark.conf.set("spark.sql.streaming.metricsEnabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", str(num_workers * 2))  # keep low, latency > throughput
spark.conf.set("spark.speculation", "false")  # speculation causes duplicate processing in streaming
```
Node type: fixed cluster, no autoscaling. Use standard workers, size for peak load not average.
Trigger: `.trigger(processingTime="30 seconds")` or `.trigger(availableNow=True)` for one-shot batch.

---

### Ad-hoc / Interactive Notebooks
```
Driver:  memory-optimized, 32GB+
Workers: autoscaling enabled
  Min workers: 1
  Max workers: 8
  Scale-up threshold: 1 min
  Auto-terminate: 30 min idle
```
Keep `spark.sql.shuffle.partitions` at 200 for interactive work — AQE handles the rest.

---

## Adaptive Query Execution (AQE) — always on

AQE is the dynamic layer that corrects your static estimates at runtime. Enable all of these:

```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionSize", "64mb")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", "true")
spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", "-1")  # let AQE decide broadcast
```

AQE handles: partition coalescing, skew split, runtime join strategy switching. Your static configs are the starting point; AQE adjusts from there.

---

## Symptoms → diagnosis → fix

| Symptom | Likely Cause | Fix |
|---------|-------------|-----|
| One task takes 10x longer than others | Data skew | Enable AQE skew join + salting (see spark-patterns.md) |
| Tasks complete in <100ms | Too many partitions | Lower shuffle.partitions or reduce estimated shuffle size |
| OOM on executors | Memory too small, or over-caching | Increase memoryOverhead, reduce storageFraction, unpersist caches |
| OOM on driver | .collect() or .toPandas() on large DF | Aggregate before collecting |
| GC time >10% in Spark UI | Too much data cached, heap pressure | Use MEMORY_AND_DISK_SER, reduce storageFraction |
| Slow Delta writes | Small files accumulating | Enable autoCompact, use coalesce() before write |
| Job fast locally, slow on cluster | Missing broadcast join | Use f.broadcast() on small tables |
| Scale-up doesn't help | Already at max useful parallelism | max_useful_workers = shuffle_partitions / cores_per_executor |
| Stage retries on autoscaling cluster | Scale-down mid-shuffle | Disable autoscaling for shuffle-heavy jobs |

---

## Databricks-specific settings

| Feature | Setting | When |
|---------|---------|------|
| Photon engine | Enable in cluster config | SQL-heavy workloads — free speedup |
| Delta Auto Optimize | `delta.autoOptimize.optimizeWrite = true` | Append workloads with many small writes |
| Z-ordering | `OPTIMIZE table ZORDER BY (col)` | Weekly on large Delta tables, use join/filter keys |
| Runtime | Databricks Runtime 14.x LTS+ | Always use latest LTS |
| Cluster policies | Enforce via admin policies | Prevent runaway over-provisioning |

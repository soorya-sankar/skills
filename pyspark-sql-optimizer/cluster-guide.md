<!-- Cluster Sizing Guide. Cloud infrastructure (AWS, Azure, GCP), Spark 3.0+, Databricks Runtime 14.x+. Test recommendations in your environment. -->

# Cluster Sizing Guide

Use this reference to recommend Spark / Databricks cluster configurations based on job type and data volume.
All recommendations assume cloud infrastructure (AWS, Azure, or GCP). Adjust instance types to match your cloud provider.

---

## Quick Reference — By Data Volume

| Data Size | Workers | Cores/Worker | Memory/Worker | Driver Memory | Notes |
|-----------|---------|--------------|---------------|---------------|-------|
| < 10 GB | 2 | 4 | 16 GB | 8 GB | Single-node or dev cluster is fine |
| 10–100 GB | 4 | 8 | 32 GB | 16 GB | Standard ETL workload |
| 100 GB–1 TB | 8–16 | 8–16 | 32–64 GB | 32 GB | Enable autoscaling |
| 1–10 TB | 16–32 | 16 | 64 GB | 64 GB | Optimize partitioning carefully |
| > 10 TB | 32+ | 16–32 | 64–128 GB | 64 GB | Contact ops; test on 16–32 TB first |

---

## By Job Type

### ETL / Data Pipeline
- **Pattern:** Read → transform → write. Usually I/O bound.
- **Recommendation:** General-purpose instances, memory-optimized if many joins.
- **Key configs:**
```python
spark.conf.set("spark.sql.shuffle.partitions", "200")       # default, tune up for > 100GB
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(10 * 1024 * 1024))  # 10MB
spark.conf.set("spark.sql.adaptive.enabled", "true")        # AQE — always on
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
```

---

### Aggregation / Reporting
- **Pattern:** Large group-bys, window functions, many shuffles.
- **Recommendation:** Memory-optimized instances. More workers, not bigger workers.
- **Key configs:**
```python
spark.conf.set("spark.sql.shuffle.partitions", str(num_workers * cores_per_worker * 2))
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")  # handles data skew
spark.conf.set("spark.memory.fraction", "0.8")
spark.conf.set("spark.memory.storageFraction", "0.3")
```

---

### ML Training (Spark MLlib)
- **Pattern:** Iterative, CPU-heavy, moderate shuffle.
- **Recommendation:** Compute-optimized instances. Enable caching of training data.
- **Key configs:**
```python
spark.conf.set("spark.sql.shuffle.partitions", str(num_workers * cores_per_worker))
spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
spark.conf.set("spark.kryoserializer.buffer.max", "512m")
# Cache training DataFrame before .fit()
train_df.cache()
```

---

### Streaming (Structured Streaming)
- **Pattern:** Continuous micro-batches, low-latency.
- **Recommendation:** Fewer, larger workers. Stable cluster (no autoscaling).
- **Key configs:**
```python
spark.conf.set("spark.sql.streaming.metricsEnabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "10")  # keep low for streaming
# Trigger interval
.trigger(processingTime="30 seconds")  # or trigger(availableNow=True) for one-shot
```

---

### Ad-hoc / Interactive (Notebooks)
- **Pattern:** Exploratory, variable query size, dev/test.
- **Recommendation:** Use autoscaling cluster. Small default, scales on demand.
- **Databricks config:**
```
Min workers: 1
Max workers: 8
Enable autoscaling: true
Auto-terminate: 30 minutes
```

---

## Partition Tuning

### How many shuffle partitions?
```
Recommended = (total data size in GB after shuffle) / 0.128
```

Examples:
- 10 GB shuffle → 78 partitions → use `spark.sql.shuffle.partitions = 100`
- 500 GB shuffle → 3906 → use `4000`
- Default of 200 is only right for ~25 GB shuffles

### When to repartition before writing?
```
Target file size: 128–256 MB per file
Num output files = ceil(total_size_GB * 1024 / 200)  # target 200MB files
df.repartition(num_output_files).write.parquet(...)
```

---

## Adaptive Query Execution (AQE) — Always Enable

AQE (available since Spark 3.0) automatically tunes shuffle partitions, handles skew, and switches join strategies at runtime.

```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", "true")
```

---

## Databricks-Specific Recommendations

| Feature | Recommendation |
|---------|---------------|
| Runtime | Use latest LTS (Databricks Runtime 14.x+) |
| Photon engine | Enable for SQL-heavy workloads — free speedup on supported instance types |
| Delta Lake | Always use Delta format over raw Parquet for production |
| OPTIMIZE + ZORDER | Run weekly on large Delta tables, Z-order on join/filter keys |
| Auto Optimize | `delta.autoOptimize.optimizeWrite = true` for append workloads |
| Cluster policies | Use policies to enforce memory limits and prevent over-provisioning |

---

## Common Symptoms → Root Cause

| Symptom | Likely Cause | Fix |
|---------|-------------|-----|
| Job slow with lots of small tasks | Too many shuffle partitions | Lower `spark.sql.shuffle.partitions` |
| OOM on executors | Insufficient memory or data skew | Increase executor memory or enable skew join |
| OOM on driver | `.collect()` or `.toPandas()` on large DF | Aggregate first, then collect |
| One task takes 10x longer than others | Data skew | Enable AQE skew join, or salt the join key |
| Job fast locally, slow on cluster | Missing broadcast join | Use `f.broadcast()` for small lookup tables |
| Slow Delta writes | Too many small files | Use `coalesce()` before write, or enable Auto Optimize |

---
name: pyspark-sql-optimizer
description: >
  Provides a practical workflow for profiling Spark workloads, estimating
  compute requirements, and generating cluster recommendations for PySpark and
  SQL jobs in Databricks. Use this guide when choosing a cluster size, tuning
  shuffle behavior, reducing skew, or deciding when broadcast joins are
  appropriate.
---

# Cluster Guide

This guide helps turn workload characteristics into a sensible Spark cluster recommendation. It is designed for Databricks and Spark 3.x+ workloads and favors evidence-based sizing over hardcoded defaults.

## 1. Profiling workflow
Before recommending a cluster:
1. Identify the tables referenced by the notebook, SQL, or job
2. Collect metadata such as size, row count, schema, partitioning, and skew
3. Examine joins, aggregation fan-out, and expected shuffle volume
4. Flag whether a small lookup table should be broadcast
5. Build a profiling summary that captures the signals used for sizing

### Recommended profiling signals
- table size and row count
- number of partitions
- skew on join keys
- estimated shuffle volume
- whether the workload is scan-heavy, join-heavy, or aggregation-heavy
- whether the job is likely to benefit from adaptive query execution

If MCP is not available, provide manual SQL queries for the same signals.

## 2. Sizing heuristics
Use workload shape rather than guesswork.

### Small workload
- 1–2 workers
- small or medium instance family
- modest memory footprint
- keep shuffle partitions conservative

### Medium workload
- 2–6 workers
- balanced CPU and memory
- enable adaptive execution and shuffle tuning

### Large or skewed workload
- add more workers only when the job clearly benefits from greater parallelism
- increase memory for wide shuffles and large joins
- watch for scheduler overhead and task startup cost

## 3. Cluster recommendation checklist
A complete recommendation should include:
- driver instance type and size
- worker instance type and size
- number of worker nodes
- Spark configuration block
- Databricks-specific settings
- the profiling evidence that drove the decision

## 4. Spark configuration patterns
Use these as starting points, then refine them from the profiling data.

### Shuffle partitions
A practical heuristic is:

$$
partitions = \max\left(8, \min\left(2000, \left\lceil \frac{shuffle\_size\_gb \times 1024}{200} \right\rceil\right)\right)
$$

This targets roughly 200 MB per partition while limiting scheduler overhead.

### Broadcast join threshold
Set the threshold based on executor memory rather than a fixed value:

```python
executor_memory_gb = 16
broadcast_mb = min(executor_memory_gb * 0.05 * 1024, 200)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", int(broadcast_mb * 1024 * 1024))
```

### Adaptive query execution

```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
```

## 5. Databricks-specific guidance
- Use Photon when supported and the workload benefits from it
- Enable AQE for joins, skew handling, and partition coalescing
- Prefer Delta tables and selective partition pruning
- Match the cluster family to the workload: general purpose for mixed jobs, memory optimized for wide joins and large aggregations

## 6. Guardrails
Avoid over-optimizing:
- Do not add workers if the job is already limited by scheduling overhead
- Do not repartition balanced data unnecessarily
- Do not cache a DataFrame that is used only once
- Do not hardcode cluster settings without checking the actual workload shape

## 7. Output format
When responding, present the recommendation in a structured way:
1. Profiling summary
2. Why this cluster fits the workload
3. Spark configuration block
4. Databricks settings
5. Risks and trade-offs

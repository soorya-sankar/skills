# Cluster Sizing Guide

Profiling-driven, dynamic cluster recommendations for Spark and Databricks. Recommendations are generated automatically from actual workload data — not from user-entered size estimates.

> **Architecture note:** This guide assumes a future MCP connection to Databricks (see Phase 1). Until MCP access is configured, the profiling phase can be run manually using the SQL queries provided. Ask Satish about access and setup before enabling live queries. Be mindful of cloud costs — turn compute off when not in use.

---

## How it works — the 4-phase flow

```
Notebook → Extract tables → Profile tables → Build profiling summary → Generate recommendation
```

The user does not need to know their data size. The system figures it out automatically by inspecting the actual tables referenced in the notebook.

---

## Phase 1 — Data Discovery (MCP / Databricks connection)

### Architecture

```
VS Code → MCP → Databricks → query metadata → profile data → recommend cluster
```

### MCP setup (when access is available)

```python
# Configure MCP connection to Databricks
# In VS Code: connect via Agents → configure MCP → connect to Databricks workspace
# MCP should expose the following capabilities:
#   - execute SQL
#   - list tables
#   - inspect schemas
#   - retrieve sample records
#   - access metadata

# Once connected, the discovery phase runs automatically on notebook open
```

### Step 1 — Parse notebook and extract all referenced tables

```python
import re

def extract_tables_from_notebook(notebook_code: str) -> list:
    """
    Parse notebook cells and extract all table references.
    Looks for: spark.read, FROM clauses, JOIN clauses, table() calls.
    """
    tables = set()

    # Match spark.read.table("table_name") or spark.table("table_name")
    spark_read = re.findall(r'(?:spark\.read\.table|spark\.table)\(["\'](\w+(?:\.\w+)?)["\']', notebook_code)
    tables.update(spark_read)

    # Match SQL FROM and JOIN clauses
    sql_tables = re.findall(r'(?:FROM|JOIN)\s+(\w+(?:\.\w+)?)', notebook_code, re.IGNORECASE)
    tables.update(sql_tables)

    # Match read.format().load() with path hints
    delta_tables = re.findall(r'\.load\(["\']([^"\']+)["\']', notebook_code)
    tables.update(delta_tables)

    return list(tables)
```

### Step 2 — Query Databricks for each table's metadata

```python
def get_table_metadata(table_name: str, spark) -> dict:
    """
    Collect all profiling information for a single table.
    Run via MCP → Databricks SQL execution.
    """
    metadata = {}

    # Table size and row count
    try:
        detail = spark.sql(f"DESCRIBE DETAIL {table_name}").collect()[0]
        metadata["size_bytes"] = detail["sizeInBytes"]
        metadata["size_gb"] = round(detail["sizeInBytes"] / (1024**3), 2)
        metadata["num_files"] = detail["numFiles"]
        metadata["format"] = detail["format"]
    except Exception as e:
        metadata["size_error"] = str(e)

    # Row count
    try:
        metadata["row_count"] = spark.sql(f"SELECT COUNT(*) FROM {table_name}").collect()[0][0]
    except Exception as e:
        metadata["row_count_error"] = str(e)

    # Schema
    try:
        metadata["schema"] = spark.sql(f"DESCRIBE {table_name}").collect()
        metadata["column_count"] = len(metadata["schema"])
    except Exception as e:
        metadata["schema_error"] = str(e)

    # Partition information
    try:
        partitions = spark.sql(f"SHOW PARTITIONS {table_name}").collect()
        metadata["partition_count"] = len(partitions)
        metadata["is_partitioned"] = len(partitions) > 0
    except Exception:
        metadata["is_partitioned"] = False
        metadata["partition_count"] = 0

    return metadata
```

---

## Phase 2 — Profiling

Once tables are discovered and metadata is collected, run the full profiling suite.

### Data distribution and skew detection

```python
def profile_skew(table_name: str, join_key: str, spark) -> dict:
    """
    Detect data skew on the join key.
    A skew ratio > 5x is significant and affects cluster sizing.
    """
    dist = spark.sql(f"""
        SELECT
            {join_key},
            COUNT(*) as row_count
        FROM {table_name}
        GROUP BY {join_key}
        ORDER BY row_count DESC
    """)

    from pyspark.sql import functions as f
    stats = dist.agg(
        f.max("row_count").alias("max_count"),
        f.expr("percentile(row_count, 0.5)").alias("median_count"),
        f.stddev("row_count").alias("stddev")
    ).collect()[0]

    skew_ratio = stats["max_count"] / stats["median_count"] if stats["median_count"] > 0 else 1

    return {
        "join_key": join_key,
        "skew_ratio": round(skew_ratio, 1),
        "is_skewed": skew_ratio > 5,
        "max_key_count": stats["max_count"],
        "median_key_count": stats["median_count"],
    }
```

### Join and broadcast analysis

```python
def analyze_joins(notebook_code: str, table_metadata: dict) -> dict:
    """
    Identify all joins in the notebook and flag broadcast opportunities.
    A table is broadcastable if it is under 200MB.
    """
    broadcast_threshold_bytes = 200 * 1024 * 1024  # 200MB

    join_analysis = {
        "total_joins": 0,
        "broadcast_candidates": [],
        "large_joins": [],
        "join_types_used": []
    }

    # Find all join patterns
    join_patterns = re.findall(
        r'\.join\((\w+).*?how=["\'](\w+)["\']',
        notebook_code
    )

    for table, join_type in join_patterns:
        join_analysis["total_joins"] += 1
        join_analysis["join_types_used"].append(join_type)

        if table in table_metadata:
            size = table_metadata[table].get("size_bytes", float("inf"))
            if size < broadcast_threshold_bytes:
                join_analysis["broadcast_candidates"].append({
                    "table": table,
                    "size_mb": round(size / (1024**2), 1),
                    "join_type": join_type
                })
            else:
                join_analysis["large_joins"].append({
                    "table": table,
                    "size_gb": round(size / (1024**3), 1),
                    "join_type": join_type
                })

    return join_analysis
```

### Shuffle estimation

```python
def estimate_shuffle(table_metadata: dict, join_analysis: dict) -> dict:
    """
    Estimate shuffle volume based on table sizes, join types, and aggregation patterns.
    This replaces user-entered data size estimates.
    """
    total_input_gb = sum(
        t.get("size_gb", 0) for t in table_metadata.values()
    )

    # Fan-out ratio based on join complexity
    num_large_joins = len(join_analysis["large_joins"])
    if num_large_joins == 0:
        fan_out = 0.3   # simple filter/select
    elif num_large_joins <= 2:
        fan_out = 0.8   # moderate joins
    else:
        fan_out = 1.5   # many wide joins

    estimated_shuffle_gb = total_input_gb * fan_out

    return {
        "total_input_gb": round(total_input_gb, 2),
        "estimated_shuffle_gb": round(estimated_shuffle_gb, 2),
        "fan_out_ratio": fan_out,
        "num_large_joins": num_large_joins
    }
```

### Build the profiling summary

```python
def build_profiling_summary(notebook_code: str, spark) -> dict:
    """
    Master function — runs all profiling steps and returns a single
    summary dict that drives the recommendation engine.
    This is the central input to Phase 3.
    """
    # Step 1: discover tables
    tables = extract_tables_from_notebook(notebook_code)

    # Step 2: collect metadata for each table
    table_metadata = {t: get_table_metadata(t, spark) for t in tables}

    # Step 3: analyze joins
    join_analysis = analyze_joins(notebook_code, table_metadata)

    # Step 4: estimate shuffle
    shuffle = estimate_shuffle(table_metadata, join_analysis)

    # Step 5: detect skew on large tables
    skew_results = {}
    for table, meta in table_metadata.items():
        if meta.get("size_gb", 0) > 1:  # only profile tables > 1GB
            # use first column of schema as proxy join key if unknown
            key = meta.get("schema", [{}])[0].get("col_name", "id")
            skew_results[table] = profile_skew(table, key, spark)

    is_skewed = any(s["is_skewed"] for s in skew_results.values())
    max_skew_ratio = max((s["skew_ratio"] for s in skew_results.values()), default=1)

    return {
        "num_tables": len(tables),
        "tables": table_metadata,
        "total_input_gb": shuffle["total_input_gb"],
        "estimated_shuffle_gb": shuffle["estimated_shuffle_gb"],
        "skew": {
            "detected": is_skewed,
            "max_ratio": max_skew_ratio,
            "details": skew_results
        },
        "joins": join_analysis,
        "broadcast_candidates": join_analysis["broadcast_candidates"],
    }
```

---

## Phase 3 — Recommendation Engine

The profiling summary drives all recommendations. No fixed buckets — everything is calculated.

### Dynamic shuffle partition formula

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

### Worker count calculation

```python
import math

def calc_worker_count(profile: dict) -> int:
    """
    Calculate worker count from profiling summary.
    Based on total input size, shuffle volume, and skew ratio.
    Never asks user for data size — reads it from profiling.
    """
    total_gb = profile["total_input_gb"]
    shuffle_gb = profile["estimated_shuffle_gb"]
    skew_ratio = profile["skew"]["max_ratio"]
    num_large_joins = profile["joins"]["num_large_joins"]

    # Base worker count from data volume
    if total_gb < 10:
        base_workers = 2
    elif total_gb < 100:
        base_workers = 4
    elif total_gb < 500:
        base_workers = 8
    elif total_gb < 2000:
        base_workers = 16
    else:
        base_workers = 32

    # Adjust for shuffle intensity
    shuffle_ratio = shuffle_gb / max(total_gb, 1)
    if shuffle_ratio > 1.0:
        base_workers = math.ceil(base_workers * 1.5)

    # Adjust for skew
    if skew_ratio > 5:
        skew_multiplier = min(skew_ratio / 5, 3)
        base_workers = math.ceil(base_workers * skew_multiplier)

    # Adjust for join complexity
    if num_large_joins > 3:
        base_workers = math.ceil(base_workers * 1.25)

    # Over-optimization guardrail
    shuffle_partitions = calc_shuffle_partitions(shuffle_gb)
    cores_per_executor = 4
    max_useful = math.ceil(shuffle_partitions / cores_per_executor)
    return min(base_workers, max_useful)
```

### Node type selection

```python
def select_node_type(profile: dict) -> dict:
    """
    Select driver and worker node types from profiling summary.
    Returns Databricks cluster family and instance recommendations.
    """
    total_gb = profile["total_input_gb"]
    is_skewed = profile["skew"]["detected"]
    has_broadcasts = len(profile["broadcast_candidates"]) > 0
    num_joins = profile["joins"]["total_joins"]

    # Driver sizing — based on total data volume
    if total_gb < 50:
        driver = {"memory_gb": 16, "cores": 4, "type": "Standard", "databricks_type": "i3.xlarge"}
    elif total_gb < 500:
        driver = {"memory_gb": 32, "cores": 8, "type": "Memory-optimized", "databricks_type": "r5.2xlarge"}
    else:
        driver = {"memory_gb": 64, "cores": 16, "type": "Memory-optimized", "databricks_type": "r5.4xlarge"}

    # Worker sizing — based on skew, joins, and broadcasts
    if is_skewed or num_joins > 3:
        worker = {
            "memory_gb": 64,
            "cores": 16,
            "type": "Memory-optimized",
            "cluster_family": "Memory Optimized",
            "databricks_type": "r5.4xlarge",
            "reason": "Skew or heavy joins detected — memory-optimized handles hot partitions"
        }
    elif has_broadcasts and total_gb > 100:
        worker = {
            "memory_gb": 32,
            "cores": 16,
            "type": "Standard",
            "cluster_family": "General Purpose",
            "databricks_type": "i3.2xlarge",
            "reason": "Broadcast joins present — standard workers with adequate memory"
        }
    else:
        worker = {
            "memory_gb": 16 if total_gb < 50 else 32,
            "cores": 8 if total_gb < 50 else 16,
            "type": "Standard",
            "cluster_family": "General Purpose",
            "databricks_type": "i3.xlarge" if total_gb < 50 else "i3.2xlarge",
            "reason": "Standard ETL workload"
        }

    return {"driver": driver, "worker": worker}
```

### Executor configuration

```python
def calc_executor_config(worker_memory_gb: int, worker_cores: int) -> dict:
    """
    Calculate executor memory, cores, and overhead from worker specs.
    cores_per_executor capped at 4 — HDFS throughput degrades above 5.
    """
    cores_per_executor = 4
    executors_per_worker = worker_cores // cores_per_executor
    memory_per_executor_gb = worker_memory_gb // executors_per_worker
    overhead_mb = max(384, int(memory_per_executor_gb * 1024 * 0.1))

    return {
        "cores_per_executor": cores_per_executor,
        "executors_per_worker": executors_per_worker,
        "memory_per_executor_gb": memory_per_executor_gb,
        "overhead_mb": overhead_mb
    }
```

---

## Phase 4 — Output

The recommendation engine returns a complete cluster configuration — not just memory and cores.

### Full recommendation generator

```python
def generate_cluster_recommendation(profile: dict) -> dict:
    """
    Master recommendation function.
    Input: profiling summary from build_profiling_summary()
    Output: complete Databricks cluster configuration
    """
    shuffle_gb = profile["estimated_shuffle_gb"]
    nodes = select_node_type(profile)
    worker_count = calc_worker_count(profile)
    executor_config = calc_executor_config(
        nodes["worker"]["memory_gb"],
        nodes["worker"]["cores"]
    )
    shuffle_partitions = calc_shuffle_partitions(shuffle_gb)
    broadcast_mb = min(nodes["worker"]["memory_gb"] * 0.05 * 1024, 200)

    recommendation = {
        # Driver configuration
        "driver": {
            "instance_type": nodes["driver"]["databricks_type"],
            "memory_gb": nodes["driver"]["memory_gb"],
            "cores": nodes["driver"]["cores"],
            "node_type": nodes["driver"]["type"]
        },

        # Worker configuration
        "worker": {
            "instance_type": nodes["worker"]["databricks_type"],
            "memory_gb": nodes["worker"]["memory_gb"],
            "cores": nodes["worker"]["cores"],
            "node_type": nodes["worker"]["type"],
            "cluster_family": nodes["worker"]["cluster_family"],
            "selection_reason": nodes["worker"]["reason"]
        },

        # Worker count
        "num_workers": worker_count,

        # Executor config
        "executor": executor_config,

        # Spark configuration
        "spark_config": {
            "spark.sql.shuffle.partitions": str(shuffle_partitions),
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.minPartitionSize": "64mb",
            "spark.sql.adaptive.skewJoin.enabled": str(profile["skew"]["detected"]).lower(),
            "spark.sql.adaptive.skewJoin.skewedPartitionFactor": "5",
            "spark.sql.adaptive.localShuffleReader.enabled": "true",
            "spark.sql.adaptive.autoBroadcastJoinThreshold": "-1",
            "spark.sql.autoBroadcastJoinThreshold": str(int(broadcast_mb * 1024 * 1024)),
            "spark.executor.memoryOverhead": f"{executor_config['overhead_mb']}m",
            "spark.executor.cores": str(executor_config["cores_per_executor"]),
            "spark.executor.memory": f"{executor_config['memory_per_executor_gb']}g"
        },

        # Databricks-specific settings
        "databricks_config": {
            "runtime": "14.x LTS+",
            "photon_enabled": True,
            "autoscaling": False if profile["skew"]["detected"] else "consider",
            "auto_optimize_write": True,
            "auto_compact": True,
            "cluster_policies": "enforce via admin"
        },

        # Profiling summary that drove the recommendation
        "based_on": {
            "total_input_gb": profile["total_input_gb"],
            "estimated_shuffle_gb": profile["estimated_shuffle_gb"],
            "num_tables": profile["num_tables"],
            "skew_detected": profile["skew"]["detected"],
            "max_skew_ratio": profile["skew"]["max_ratio"],
            "total_joins": profile["joins"]["total_joins"],
            "broadcast_candidates": len(profile["broadcast_candidates"])
        }
    }

    return recommendation
```

### Example output

```python
# What the recommendation looks like when printed
{
  "driver": {
    "instance_type": "r5.2xlarge",
    "memory_gb": 32,
    "cores": 8,
    "node_type": "Memory-optimized"
  },
  "worker": {
    "instance_type": "r5.4xlarge",
    "memory_gb": 64,
    "cores": 16,
    "cluster_family": "Memory Optimized",
    "selection_reason": "Skew detected — memory-optimized handles hot partitions"
  },
  "num_workers": 12,
  "spark_config": {
    "spark.sql.shuffle.partitions": "512",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.skewJoin.enabled": "true",
    "spark.executor.memory": "16g",
    "spark.executor.cores": "4"
  },
  "databricks_config": {
    "runtime": "14.x LTS+",
    "photon_enabled": True,
    "autoscaling": False
  },
  "based_on": {
    "total_input_gb": 180,
    "estimated_shuffle_gb": 144,
    "num_tables": 4,
    "skew_detected": True,
    "max_skew_ratio": 8.3,
    "total_joins": 3,
    "broadcast_candidates": 1
  }
}
```

---

## Over-optimization guardrails

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

# PySpark Patterns — Anti-Patterns & Rewrites

Reference this file when optimizing PySpark code. All patterns are validated against Spark 3.x+ and the DataFrame API. Prioritize DataFrame API over RDD API throughout.

---

## 🔴 High Impact

### 1. .collect() inside a loop
**Problem:** Pulls the entire dataset to the driver on every loop iteration, triggering a full Spark job each time.
```python
# ❌ Bad — full Spark job runs N times
results = []
for date in date_list:
    df_day = df.filter(f.col("date") == date)
    results.append(df_day.collect())

# ✅ Good — one job, Spark handles the distribution
df_filtered = df.filter(f.col("date").isin(date_list))
result = df_filtered.groupBy("date").agg(f.count("*").alias("count"))
```
**Why:** `.collect()` is an action — it materializes ALL data on the driver. Inside a loop, that's N full jobs instead of one.

---

### 2. Join types — cost comparison and when to use each

Not all joins cost the same. Choose the right type before optimizing anything else.

| Join Type | Rows Kept | Shuffle Cost | Memory Cost | Use When |
|-----------|-----------|-------------|-------------|----------|
| `inner` | Matching only | Medium | Low | You only need rows that exist in both tables |
| `left` | All left + matching right | Medium | Medium | You need all left rows, even with no match |
| `full` / `full_outer` | All rows from both | High | High | You need every row from both sides |
| `left_semi` | Left rows that have a match (no right cols) | Low | Low | Filtering existence, no right-side data needed |
| `left_anti` | Left rows with NO match | Low | Low | Finding rows missing from the right side |

```python
# ❌ Bad — full outer join when you only need matching rows
result = orders.join(customers, on="customer_id", how="full")

# ✅ Good — inner join is cheaper and correct here
result = orders.join(customers, on="customer_id", how="inner")

# ✅ Even better — if you only need to check existence, use left_semi
orders_with_customers = orders.join(customers, on="customer_id", how="left_semi")
```
**Rule:** Start with the most restrictive join type. Move to `left` or `full` only when the business logic requires it.

---

### 3. Missing broadcast join for small tables
**Problem:** Spark defaults to sort-merge join (shuffles both sides) even when one table is tiny.
```python
# ❌ Bad — both tables shuffled across network
result = large_df.join(lookup_df, on="product_id", how="left")

# ✅ Good — small table sent to every executor, large table never moves
from pyspark.sql import functions as f
result = large_df.join(f.broadcast(lookup_df), on="product_id", how="left")
```
**Broadcast works with all join types** — inner, left, left_semi, left_anti.

**Dynamic threshold** — set based on executor memory, not a hardcoded value:
```python
# Auto-set to 5% of executor memory, capped at 200MB
executor_memory_gb = 16  # match your cluster config
broadcast_mb = min(executor_memory_gb * 0.05 * 1024, 200)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", int(broadcast_mb * 1024 * 1024))
```
**When NOT to broadcast:** If the "small" table is growing over time, don't hardcode broadcast — let AQE decide dynamically (see cluster-guide.md).

---

### 4. Data skew — detection and the salting fix
**Problem:** One partition has far more data than others. That task takes 10x longer while all other executors sit idle.

**How to detect skew:**
```python
# Check partition sizes — skew = one value >> others
df.groupBy("join_key").count().orderBy(f.col("count").desc()).show(20)

# Or check via Spark UI: Stages tab → look for one task with 10x longer duration
```

**Fix 1 — AQE (automatic, try this first):**
```python
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")   # partition is skewed if 5x median
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256mb")
```

**Fix 2 — Salting (manual, use when AQE isn't enough):**
```python
from pyspark.sql import functions as f

num_salts = 10  # increase for more skewed data

# Add random salt to the large (skewed) table
large_df_salted = large_df.withColumn(
    "salted_key",
    f.concat(
        f.col("skewed_key"),
        f.lit("_"),
        (f.rand() * num_salts).cast("int").cast("string")
    )
)

# Explode the small table to match all salt values
small_df_exploded = small_df.withColumn(
    "salt",
    f.explode(f.array([f.lit(i) for i in range(num_salts)]))
).withColumn(
    "salted_key",
    f.concat(f.col("key"), f.lit("_"), f.col("salt").cast("string"))
)

# Broadcast the (now larger) exploded small table
result = large_df_salted.join(
    f.broadcast(small_df_exploded), on="salted_key", how="inner"
)
```
**Why salting works:** Splits one hot key (e.g. `user_123`) into 10 keys (`user_123_0` through `user_123_9`), distributing the load across 10 partitions instead of 1.

---

### 5. Python UDFs — avoid entirely, three alternatives ranked

```python
# ❌ Bad — row-by-row Python execution, bypasses JVM + Catalyst
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

@udf(returnType=StringType())
def clean_name(name):
    return name.strip().upper() if name else None

df = df.withColumn("clean_name", clean_name(f.col("name")))

# ✅ Best — native Spark functions (JVM, Catalyst-optimized)
df = df.withColumn("clean_name", f.upper(f.trim(f.col("name"))))

# ✅ Good — Spark SQL (also JVM, readable for complex expressions)
df.createOrReplaceTempView("my_table")
df = spark.sql("SELECT *, UPPER(TRIM(name)) AS clean_name FROM my_table")

# ✅ Acceptable — Pandas UDF (vectorized, still crosses Python boundary but in batches)
from pyspark.sql.functions import pandas_udf
import pandas as pd

@pandas_udf(StringType())
def clean_name_vectorized(series: pd.Series) -> pd.Series:
    return series.str.strip().str.upper()
```
**Use Pandas UDF only** when the logic genuinely can't be expressed in native functions (e.g. calling an external ML model per batch).

---

### 6. Shuffle partition sizing — dynamic formula, not hardcoded 200
**Problem:** The default `spark.sql.shuffle.partitions = 200` is only correct for ~40GB of shuffle data. Too few = OOM. Too many = scheduler overhead kills performance.

```python
# ❌ Bad — hardcoded, wrong for most real jobs
spark.conf.set("spark.sql.shuffle.partitions", "200")

# ✅ Good — size dynamically based on shuffle data volume
# Formula: partitions = shuffle_size_gb * 1024 / 200  (targeting 200MB per partition)
# Cap at 2000 to prevent scheduler overhead

def calc_shuffle_partitions(shuffle_size_gb, target_mb=200, cap=2000, floor=8):
    partitions = int((shuffle_size_gb * 1024) / target_mb)
    return max(min(partitions, cap), floor)

# Examples:
# 10GB shuffle  -> 51 partitions
# 100GB shuffle -> 512 partitions
# 500GB shuffle -> 2000 partitions (capped)

estimated_shuffle_gb = 50  # estimate from data size * fan-out ratio
spark.conf.set(
    "spark.sql.shuffle.partitions",
    str(calc_shuffle_partitions(estimated_shuffle_gb))
)
```

**Let AQE handle the rest** — set your estimate slightly high, AQE coalesces small partitions down automatically:
```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionSize", "64mb")
```

**Warning — over-partitioning:** More than ~2000 partitions on a small dataset causes Spark to spend more time scheduling tasks than running them. If tasks complete in under 100ms, you have too many partitions.

---

### 7. Lazy evaluation — don't optimize what hasn't run yet
**Problem:** Spark is lazy — nothing executes until an action (`.count()`, `.write()`, `.collect()`) is called. Over-caching and over-repartitioning intermediate steps wastes memory and adds unnecessary shuffles.

```python
# ❌ Bad — caching and repartitioning a step that's only used once
df_filtered = df.filter(f.col("status") == "active").repartition(100).cache()
result = df_filtered.write.parquet("output/")  # cache never reused

# ✅ Good — only materialize when there's a real reason
df_filtered = df.filter(f.col("status") == "active")  # lazy, free
result = df_filtered.write.parquet("output/")  # one job, no wasted cache
```

**When to cache:** Only when the same DataFrame is used in 2+ actions AND recomputing it is expensive (involves reading from disk or heavy transformations).
```python
# ✅ Correct cache usage
df_expensive = (
    spark.read.parquet("s3://large-dataset/")
    .join(f.broadcast(lookup), on="id")
    .filter(f.col("status") == "active")
    .cache()  # justified: expensive to recompute, used twice below
)
count = df_expensive.count()         # action 1 — triggers cache
summary = df_expensive.groupBy("region").agg(f.sum("amount"))  # action 2 — uses cache
df_expensive.unpersist()             # always unpersist when done
```

---

### 8. cache() vs persist() — they are not the same
```python
# cache() always uses MEMORY_AND_DISK — no control over storage level
df.cache()

# persist() lets you choose the storage level
from pyspark import StorageLevel

df.persist(StorageLevel.MEMORY_ONLY)        # fastest, crashes if not enough RAM
df.persist(StorageLevel.MEMORY_AND_DISK)    # safe default (same as cache())
df.persist(StorageLevel.DISK_ONLY)          # slowest, use for very large DFs
df.persist(StorageLevel.MEMORY_AND_DISK_SER)  # serialized — less memory, slower access
```

**Decision rule:**
- Small DF, used many times → `MEMORY_ONLY`
- Medium DF, unsure about memory → `MEMORY_AND_DISK` (or just `.cache()`)
- Very large DF, memory is tight → `DISK_ONLY`
- If GC pressure is high → `MEMORY_AND_DISK_SER` (serialized uses less heap)

**Never cache** a DataFrame you use only once — it wastes executor memory that shuffle operations need.

---

### 9. repartition vs coalesce — and when to use neither
```python
# coalesce(n) — reduces partitions, NO shuffle. Use to shrink.
df.coalesce(4).write.parquet("output/")   # ✅ shrinking without shuffle

# repartition(n) — full shuffle, balanced output. Use to grow or rebalance.
df.repartition(200).groupBy("region").agg(...)  # ✅ even distribution before agg

# ❌ Bad — repartition just to shrink (wastes a full shuffle)
df.repartition(4).write.parquet("output/")  # use coalesce(4) instead

# ❌ Bad — repartitioning when data is already well-partitioned
df.repartition(200)  # pointless if df already has 200 balanced partitions
```

**When to use neither:** If your partitions are reasonably balanced (check via Spark UI → Stage → Task Duration spread), don't touch them. Every repartition is a shuffle.

---

### 10. partitionBy() on write — critical for downstream reads
```python
# ❌ Bad — flat output, every downstream query scans everything
df.write.mode("overwrite").parquet("s3://bucket/events/")

# ✅ Good — partition by date so downstream queries prune automatically
df.write \
    .mode("overwrite") \
    .partitionBy("event_date", "region") \
    .parquet("s3://bucket/events/")

# Downstream read — only reads 2024-01-15 data, ignores all other partitions
df_jan = spark.read.parquet("s3://bucket/events/") \
    .filter(f.col("event_date") == "2024-01-15")
```
**Rule of thumb:** Partition by columns that are almost always in your WHERE clause. Don't over-partition — if a column has 10,000+ unique values, it will create 10,000 directories and cause small-file problems.

---

## 🟡 Medium Impact

### 11. Small files problem — too many tiny output files
```python
# ❌ Bad — may write thousands of tiny files (one per partition)
df.write.parquet("output/")

# ✅ Good — coalesce to target ~200MB per file
target_file_size_mb = 200
estimated_output_gb = 10
num_files = max(1, int((estimated_output_gb * 1024) / target_file_size_mb))
df.coalesce(num_files).write.parquet("output/")
```

### 12. Schema inference — never on large datasets
```python
# ❌ Bad — scans all files just to figure out column types
df = spark.read.json("s3://bucket/large-dataset/")

# ✅ Good — provide schema explicitly
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType

schema = StructType([
    StructField("id", LongType(), True),
    StructField("name", StringType(), True),
    StructField("amount", LongType(), True),
    StructField("created_at", TimestampType(), True),
])
df = spark.read.schema(schema).json("s3://bucket/large-dataset/")
```

### 13. Avoid .toPandas() on large DataFrames
```python
# ❌ Bad — pulls entire dataset to driver
df.toPandas()

# ✅ Good — aggregate or sample first
df.groupBy("region").agg(f.sum("amount")).toPandas()  # small aggregated result
df.sample(fraction=0.01).toPandas()                   # 1% sample for exploration
```

### 14. Delta Lake optimizations (Databricks)
```python
# Z-ordering on frequently filtered columns (run weekly on large tables)
# OPTIMIZE events ZORDER BY (user_id, event_date)

# Auto-optimize on write (reduces small files automatically)
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

# MERGE instead of read-modify-write for upserts
# Avoids rewriting the entire table for small updates
```

---

## 🟢 Best Practices

### 15. Always specify .mode() when writing
```python
df.write.mode("overwrite").parquet(...)  # replace existing
df.write.mode("append").parquet(...)     # add to existing
# Never omit — default behavior varies by connector and can cause silent data loss
```

### 16. Use f.col() over string expressions in complex logic
```python
df.filter(f.col("amount") > 1000)      # type-checked at parse time
df.filter("amount > 1000")             # works but skips type validation
```

### 17. Speculative execution — turn off for skewed or shuffle-heavy jobs
```python
# Default: true (Spark re-runs slow tasks speculatively)
# On skewed data this wastes resources duplicating tasks that are slow for a reason
spark.conf.set("spark.speculation", "false")  # turn off for known-skewed jobs
```

### 18. DataFrame API over RDD API (Spark 3.x+)
```python
# ❌ Avoid RDD API for new code — bypasses Catalyst optimizer
rdd.groupByKey().mapValues(sum)
rdd.reduceByKey(lambda a, b: a + b)

# ✅ Use DataFrame API — Catalyst optimizes the execution plan
df.groupBy("key").agg(f.sum("value"))
```
The DataFrame API lets Spark optimize the execution plan. RDD API forces Spark to execute exactly what you wrote.

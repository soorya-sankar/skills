<!-- PySpark anti-patterns and optimizations. Spark 3.0+, DataFrame API. Test patterns in your environment before production use. -->

# PySpark Patterns — Anti-Patterns & Rewrites

Reference this file when optimizing PySpark code. Covers DataFrame API, actions vs transformations, shuffle reduction, caching, and schema handling.

---

## 🔴 High Impact

### 1. .collect() inside a loop
**Problem:** Pulls the entire dataset to the driver on every iteration.
```python
# ❌ Bad
results = []
for date in date_list:
    df_day = df.filter(f.col("date") == date)
    results.append(df_day.collect())  # triggers full job per iteration

# ✅ Good
# Filter once, partition the work in Spark
df_filtered = df.filter(f.col("date").isin(date_list))
result = df_filtered.groupBy("date").agg(...)
```
**Why:** `.collect()` pulls everything to the driver; in a loop, this runs a full Spark job N times.

---

### 2. Missing broadcast join for small tables
**Problem:** Spark defaults to a sort-merge join (shuffle both sides) even when one table is tiny.
```python
# ❌ Bad
result = large_df.join(lookup_df, on="product_id", how="left")

# ✅ Good
from pyspark.sql import functions as f
result = large_df.join(f.broadcast(lookup_df), on="product_id", how="left")
```
**Why:** Broadcast sends the small table to all executors, eliminating the shuffle. Use when small table < 10–100MB.

Spark config threshold (set in cluster-guide.md):
```python
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10 * 1024 * 1024)  # 10MB
```

---

### 3. Unnecessary .cache() or missing .cache()
```python
# ❌ Bad — caching a DataFrame used only once
df_clean = df.filter(...).cache()
result = df_clean.count()  # only used here

# ❌ Bad — not caching a DataFrame used multiple times
df_clean = df.filter(f.col("status") == "active")
count = df_clean.count()        # triggers job 1
summary = df_clean.groupBy(...) # triggers job 2 — recomputes df_clean

# ✅ Good — cache only when reused
df_clean = df.filter(f.col("status") == "active").cache()
count = df_clean.count()
summary = df_clean.groupBy("region").agg(f.sum("amount"))
df_clean.unpersist()  # always unpersist when done
```

---

### 4. Using Python UDFs instead of built-in Spark functions
**Problem:** Python UDFs run row-by-row in Python, breaking JVM optimization and serializing data back and forth.
```python
# ❌ Bad
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

@udf(returnType=StringType())
def clean_name(name):
    return name.strip().upper() if name else None

df = df.withColumn("clean_name", clean_name(f.col("name")))

# ✅ Good — use native Spark functions
df = df.withColumn("clean_name", f.upper(f.trim(f.col("name"))))
```
**Why:** Native functions run in the JVM with Catalyst optimization; Python UDFs bypass this for 10–100x slowdown.

If a UDF is truly unavoidable, use **Pandas UDFs (vectorized)** instead:
```python
from pyspark.sql.functions import pandas_udf
import pandas as pd

@pandas_udf(StringType())
def clean_name_vectorized(series: pd.Series) -> pd.Series:
    return series.str.strip().str.upper()
```

---

### 5. Too many small files (the small files problem)
**Problem:** Reading/writing thousands of tiny Parquet files is slow due to file listing overhead.
```python
# ❌ Bad — writes hundreds of tiny files
df.write.parquet("output/")

# ✅ Good — coalesce to reduce output files
df.coalesce(8).write.parquet("output/")

# Or repartition if you need balanced partitions
df.repartition(16).write.parquet("output/")
```
**Rule of thumb:** Target ~128MB–256MB per output file. Smaller jobs → fewer files.

---

### 6. Wide shuffles — wrong use of repartition vs coalesce
```python
# coalesce(n) — reduces partitions WITHOUT a shuffle. Use for decreasing.
df.coalesce(4).write.parquet(...)   # ✅ shrinking partition count

# repartition(n) — full shuffle, balanced partitions. Use when:
#   - Increasing partition count
#   - Need even distribution before a join or aggregation
df.repartition(200).groupBy("region").agg(...)  # ✅ even distribution for agg

# ❌ Bad — using repartition just to shrink (wastes a shuffle)
df.repartition(4).write.parquet(...)  # use coalesce(4) instead
```

---

### 7. Reading entire table when only a slice is needed
```python
# ❌ Bad — reads all partitions, then filters in Spark
df = spark.read.parquet("s3://bucket/events/")
df_jan = df.filter(f.col("event_date") >= "2024-01-01")

# ✅ Good — use partition pruning at read time
df_jan = spark.read.parquet("s3://bucket/events/") \
    .filter(f.col("event_date") >= "2024-01-01")  # Catalyst pushes this down to reader

# Even better with explicit partition path
df_jan = spark.read.parquet("s3://bucket/events/event_date=2024-01-*")
```

---

## 🟡 Medium Impact

### 8. Schema inference on large datasets
```python
# ❌ Bad — scans all files to infer schema
df = spark.read.json("s3://bucket/large-dataset/")

# ✅ Good — provide schema explicitly
from pyspark.sql.types import StructType, StructField, StringType, LongType

schema = StructType([
    StructField("id", LongType(), True),
    StructField("name", StringType(), True),
    StructField("amount", LongType(), True),
])
df = spark.read.schema(schema).json("s3://bucket/large-dataset/")
```

---

### 9. groupByKey instead of reduceByKey (RDD API)
```python
# ❌ Bad — shuffles all values to reducer before aggregating
rdd.groupByKey().mapValues(sum)

# ✅ Good — pre-aggregates on each partition before shuffle
rdd.reduceByKey(lambda a, b: a + b)
```

---

### 10. Not using Delta Lake optimizations (if on Databricks)
```python
# ✅ Use Z-ordering on commonly filtered columns
# Run in Databricks:
# OPTIMIZE events ZORDER BY (user_id, event_date)

# ✅ Use Delta's MERGE instead of read-modify-write
# Avoids rewriting entire tables for upserts
```

---

## 🟢 Best Practices

### 11. Always specify .mode() when writing
```python
df.write.mode("overwrite").parquet(...)   # overwrite existing
df.write.mode("append").parquet(...)      # add to existing
# Never omit — default behavior varies and can cause silent failures
```

### 12. Use f.col() not string column references in complex expressions
```python
# Prefer
df.filter(f.col("amount") > 1000)
# Over
df.filter("amount > 1000")  # works but skips type checking at parse time
```

### 13. Persist storage level
```python
from pyspark import StorageLevel

# For DataFrames that fit in memory
df.persist(StorageLevel.MEMORY_AND_DISK)

# Levels ranked by speed vs safety:
# MEMORY_ONLY → fastest, but spills crash if not enough RAM
# MEMORY_AND_DISK → safe default
# DISK_ONLY → slowest, use only for very large DFs
```

### 14. Avoid .toPandas() on large DataFrames
```python
# ❌ Bad — pulls everything to driver
df.toPandas()

# ✅ Good — sample first, or aggregate before collecting
df.sample(fraction=0.01).toPandas()
df.groupBy("region").agg(f.sum("amount")).toPandas()  # small result set
```

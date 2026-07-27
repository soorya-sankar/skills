# Examples — Databricks Knowledge Graph

This file shows a concrete end-to-end example of how source artifacts map into the graph schema and how the graph can answer common questions.

---

## Example input artifacts

### Notebook: `/Workspace/etl/ingest_orders`
```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

orders = spark.read.table("bronze.orders_raw")
clean_orders = orders.filter("status IS NOT NULL")
clean_orders.write.mode("overwrite").saveAsTable("silver.orders_clean")
```

### Notebook: `/Workspace/etl/aggregate_orders`
```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

orders = spark.sql("SELECT * FROM silver.orders_clean")
summary = orders.groupBy("customer_id").sum("amount")
summary.write.format("delta").saveAsTable("gold.customer_summary")
```

### Job JSON
```json
{
  "name": "order_pipeline",
  "tasks": [
    {"task_key": "ingest", "notebook_task": {"notebook_path": "/Workspace/etl/ingest_orders"}},
    {"task_key": "aggregate", "depends_on": [{"task_key": "ingest"}],
     "notebook_task": {"notebook_path": "/Workspace/etl/aggregate_orders"}}
  ]
}
```

---

## Extracted graph

```json
{
  "nodes": [
    {"id": "bronze.orders_raw", "type": "Table"},
    {"id": "silver.orders_clean", "type": "Table"},
    {"id": "gold.customer_summary", "type": "Table"},
    {"id": "/Workspace/etl/ingest_orders", "type": "Notebook"},
    {"id": "/Workspace/etl/aggregate_orders", "type": "Notebook"},
    {"id": "order_pipeline", "type": "Job"},
    {"id": "ingest", "type": "Task"},
    {"id": "aggregate", "type": "Task"}
  ],
  "edges": [
    {"from": "/Workspace/etl/ingest_orders", "rel": "reads_from", "to": "bronze.orders_raw"},
    {"from": "/Workspace/etl/ingest_orders", "rel": "writes_to", "to": "silver.orders_clean"},
    {"from": "/Workspace/etl/aggregate_orders", "rel": "reads_from", "to": "silver.orders_clean"},
    {"from": "/Workspace/etl/aggregate_orders", "rel": "writes_to", "to": "gold.customer_summary"},
    {"from": "silver.orders_clean", "rel": "depends_on", "to": "bronze.orders_raw"},
    {"from": "gold.customer_summary", "rel": "depends_on", "to": "silver.orders_clean"},
    {"from": "order_pipeline", "rel": "triggers", "to": "ingest"},
    {"from": "ingest", "rel": "calls", "to": "/Workspace/etl/ingest_orders"},
    {"from": "ingest", "rel": "triggers", "to": "aggregate"},
    {"from": "aggregate", "rel": "calls", "to": "/Workspace/etl/aggregate_orders"}
  ]
}
```

---

## Questions and expected traversal

### What breaks if I change `bronze.orders_raw`?
- direct downstream: `silver.orders_clean` (table)
- second hop: `gold.customer_summary` (table)
- direct notebook readers: `/Workspace/etl/ingest_orders`
- job impact: `order_pipeline` via tasks `ingest` and `aggregate`

### Where does `gold.customer_summary` come from?
- `gold.customer_summary` depends on `silver.orders_clean`
- `silver.orders_clean` depends on `bronze.orders_raw`
- chain: `gold.customer_summary ← silver.orders_clean ← bronze.orders_raw`

### Which job writes to `gold.customer_summary`?
- the notebook `/Workspace/etl/aggregate_orders` writes to it
- that notebook is invoked by task `aggregate`
- task `aggregate` is triggered by job `order_pipeline`

---

## Additional example: SQL view lineage

### SQL script
```sql
CREATE VIEW gold.customer_summary AS
SELECT c.customer_id, SUM(o.amount) AS total_amount
FROM silver.customers c
JOIN silver.orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id;
```

### Extracted graph fragment
```json
{
  "nodes": [
    {"id": "silver.customers", "type": "Table"},
    {"id": "silver.orders", "type": "Table"},
    {"id": "gold.customer_summary", "type": "View"}
  ],
  "edges": [
    {"from": "gold.customer_summary", "rel": "depends_on", "to": "silver.customers"},
    {"from": "gold.customer_summary", "rel": "depends_on", "to": "silver.orders"}
  ]
}
```

### Why it matters
- Shows views as lineage nodes and multi-source `depends_on` edges.

---

## Additional example: streaming write lineage

### Notebook: `/Workspace/etl/stream_events`
```python
stream = spark.readStream.format("kafka")
  .option("kafka.bootstrap.servers", "broker:9092")
  .option("subscribe", "events")
  .load()

stream.writeStream
  .format("delta")
  .outputMode("append")
  .option("checkpointLocation", "/tmp/checkpoints/events")
  .table("bronze.events_stream")
```

### Extracted graph fragment
```json
{
  "nodes": [
    {"id": "bronze.events_stream", "type": "Table"},
    {"id": "/Workspace/etl/stream_events", "type": "Notebook"}
  ],
  "edges": [
    {"from": "/Workspace/etl/stream_events", "rel": "writes_to", "to": "bronze.events_stream"}
  ]
}
```

### Why it matters
- Captures streaming output and continuous-update semantics.

---

## Why this is useful

- Validates the graph model and query patterns with concrete Databricks cases.

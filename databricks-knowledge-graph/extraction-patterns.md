# Extraction Patterns

How to pull nodes and edges out of common Databricks source material.
Process one source type at a time; don't try to pattern-match all of these
in a single unstructured pass.

---

## From PySpark / notebooks

**Table reads:**
```python
df = spark.read.table("bronze.events_raw")
df = spark.read.format("delta").load("s3://bucket/bronze/events")
df = spark.sql("SELECT * FROM bronze.events_raw")
```
→ edge: `{notebook} reads_from {bronze.events_raw}`

**Table writes:**
```python
df.write.mode("overwrite").saveAsTable("silver.events_clean")
df.write.format("delta").save("s3://bucket/silver/events")
```
→ edge: `{notebook} writes_to {silver.events_clean}`

**Notebook-to-notebook calls:**
```python
# Databricks magic command
%run /Workspace/etl/shared_functions
dbutils.notebook.run("/Workspace/etl/clean_events", timeout_seconds=600)
```
→ edge: `{calling_notebook} calls {called_notebook}`
**Gap to flag:** if the path is built dynamically (a variable, an f-string),
note it can't be resolved statically.

**Function calls within a notebook:**
```python
def clean_customer_name(name): ...
df = df.withColumn("clean_name", clean_customer_name(f.col("name")))
```
→ edge: `{notebook} uses {clean_customer_name}`

---

## From SQL

**Lineage from CREATE TABLE AS / INSERT:**
```sql
CREATE TABLE silver.orders_clean AS
SELECT * FROM bronze.orders_raw WHERE status IS NOT NULL;
```
→ edge: `{silver.orders_clean} depends_on {bronze.orders_raw}`

**Lineage from joins in a view definition:**
```sql
CREATE VIEW gold.customer_summary AS
SELECT c.customer_id, SUM(o.amount)
FROM silver.customers c JOIN silver.orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id;
```
→ edges: `{gold.customer_summary} depends_on {silver.customers}`,
`{gold.customer_summary} depends_on {silver.orders}`

**MERGE (upsert) lineage:**
```sql
MERGE INTO silver.customers t USING bronze.customers_raw s
ON t.id = s.id WHEN MATCHED THEN UPDATE SET *;
```
→ edge: `{silver.customers} depends_on {bronze.customers_raw}`

---

## From Job / Workflow configs

Databricks Jobs JSON (from the API or UI export) defines tasks and their
order explicitly — this is the most reliable source for orchestration edges.

```json
{
  "name": "nightly_orders_pipeline",
  "tasks": [
    {"task_key": "ingest", "notebook_task": {"notebook_path": "/etl/ingest_orders"}},
    {"task_key": "clean", "depends_on": [{"task_key": "ingest"}],
     "notebook_task": {"notebook_path": "/etl/clean_orders"}}
  ]
}
```
→ nodes: `Job(nightly_orders_pipeline)`, `Task(ingest)`, `Task(clean)`
→ edges: `{Job} triggers {ingest}`, `{ingest} triggers {clean}` (via `depends_on`),
`{ingest} calls {/etl/ingest_orders}`

Use `calls` for task-to-notebook invocation edges when a workflow task executes a notebook.

**Always prefer job config JSON over inferring order from notebook contents**
— explicit `depends_on` fields are ground truth; guessing order from file
names or comments is not.

---

## From Delta table metadata (when accessible)

If MCP or a live connection can query `DESCRIBE HISTORY` or `information_schema`:
- `DESCRIBE HISTORY table_name` — reveals which jobs/notebooks wrote to a
  table over time (operationMetrics, job IDs) — useful for confirming
  `writes_to` edges that static code analysis missed (e.g. dynamic paths)
- `information_schema.tables` / `information_schema.views` — confirms
  existence and full qualified names, reducing naming-mismatch errors

If no live connection is available, state that lineage is being inferred
from static code only, and may miss dynamically-constructed references.

---

## Common gaps to flag explicitly

- Table names built via string concatenation or f-strings (can't resolve statically)
- `%run` or `dbutils.notebook.run` with a variable path
- Jobs that trigger other jobs via API calls rather than config-defined dependencies
- Streaming jobs (`readStream`/`writeStream`) — note the table is continuously
  written rather than batch-written, since this changes impact analysis

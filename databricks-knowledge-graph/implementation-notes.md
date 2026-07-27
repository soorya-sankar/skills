# Implementation Notes — Databricks Knowledge Graph

This file captures practical extraction heuristics and normalization rules for building a Databricks knowledge graph.

---

## Supported input types

- Python notebooks and scripts containing Databricks-specific APIs:
  - `spark.read.table(...)`, `spark.read.format(...).load(...)`
  - `spark.sql(...)`
  - `df.write.saveAsTable(...)`, `df.write.format(...).save(...)`
  - `%run`, `dbutils.notebook.run(...)`
- SQL scripts and definitions:
  - `CREATE TABLE ... AS SELECT ...`
  - `CREATE VIEW ... AS SELECT ...`
  - `INSERT INTO ... SELECT ...`
  - `MERGE INTO ... USING ...`
- Databricks job configuration JSON exports:
  - `name`, `tasks`, `task_key`, `depends_on`, `notebook_task`, `python_task`, `spark_jar_task`, etc.
- Delta metadata when available:
  - `DESCRIBE HISTORY` for writer provenance
  - `information_schema.tables` / `information_schema.views`

---

## Naming and normalization rules

- Use fully qualified table names whenever available:
  - `catalog.schema.table`
  - `schema.table` is acceptable when catalog is absent
- Treat views as `View` nodes and tables as `Table` nodes; both may participate in lineage.
- Use full workspace notebook paths for `Notebook` nodes, e.g. `/Workspace/etl/ingest_orders`.
- Use the exact `task_key` value for `Task` nodes when the job JSON provides one.
- Prefer explicit names from configs over inferred or derived names.

---

## Graph construction heuristics

- Create `reads_from` / `writes_to` edges from notebooks/scripts to tables.
- Create `depends_on` edges between tables/views from SQL definitions.
- Create `calls` edges for notebook-to-notebook, task-to-notebook, and
  direct function-to-function invocation when resolvable.
- Create `triggers` edges for job orchestration: `Job → Task` and `Task → Task`.
- Add `runs_on` edges only when cluster context is relevant.

---

## Extraction heuristics and caveats

- Prefer explicit job config relationships over inferred notebook ordering.
- Flag but do not resolve:
  - notebook paths built dynamically with variables or string formatting
  - table names constructed with concatenation, f-strings, or template interpolation
  - `%run` / `dbutils.notebook.run` calls with non-literal arguments
- Recognize SQL lineage in common patterns but avoid overreaching on complex dynamic SQL.
- If a notebook reads from or writes to a table indirectly (e.g. via a helper function), attribute the edge to the notebook rather than trying to infer the helper’s runtime path.
- Treat `readStream` / `writeStream` as streaming semantics: preserve the same `writes_to` edge, but flag that the target table is continuously updated rather than batch-written.

---

## Recommended graph conventions

- Keep the graph at table/view granularity unless the question explicitly asks for column-level lineage.
- Do not create duplicate nodes for a single entity with different naming forms; normalize names first.
- When both job/task and notebook edges exist, preserve both so orchestration and data lineage can be answered separately.

---

## Common gaps to call out

- Dynamic SQL or table names that cannot be resolved statically.
- External job triggers or API-driven orchestration not present in the provided job JSON.
- Streaming jobs (`readStream` / `writeStream`) where output is continuously updated rather than batch written.
- Any graph nodes referenced in user queries that were not extracted from the available files.

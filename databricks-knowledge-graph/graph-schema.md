# Graph Schema — Node & Edge Types

This defines the vocabulary used across every extracted graph. Stick to
these types so graphs built at different times stay comparable and
mergeable.

---

## Node types

| Type | Examples | Notes |
|------|----------|-------|
| `Table` | `sales.orders`, `bronze.events_raw` | Include full catalog.schema.table path when available |
| `View` | `gold.customer_summary` | Treat materialized views same as Table |
| `Notebook` | `/Workspace/etl/ingest_orders` | Use workspace path as identifier |
| `Job` | `nightly_orders_pipeline` | Databricks Job or Workflow name/ID |
| `Task` | `task_ingest`, `task_transform` | A step within a Job (Databricks Workflows) |
| `Function` | `clean_customer_name()` | Python/SQL function or UDF |
| `Cluster` | `shared-etl-cluster` | Only include if cluster config is relevant to the question |

## Edge types

| Edge | Direction | Meaning |
|------|-----------|---------|
| `reads_from` | Notebook/Job → Table | Source data read |
| `writes_to` | Notebook/Job → Table | Output data written |
| `calls` | Notebook → Notebook | `%run` or explicit notebook invocation |
| `calls` | Task → Notebook | A workflow task invokes a notebook |
| `calls` | Function → Function | Direct function call |
| `triggers` | Job → Task, Task → Task | Job/task orchestration order |
| `depends_on` | Table → Table | Lineage: this table's data derives from that one |
| `runs_on` | Job/Notebook → Cluster | Execution environment |
| `uses` | Notebook → Function | Notebook calls a defined function |

---

## Output format

Default to JSON triples, grouped as nodes + edges:

```json
{
  "nodes": [
    {"id": "bronze.events_raw", "type": "Table"},
    {"id": "silver.events_clean", "type": "Table"},
    {"id": "nightly_events_job", "type": "Job"},
    {"id": "task_ingest", "type": "Task"},
    {"id": "/Workspace/etl/ingest_orders", "type": "Notebook"}
  ],
  "edges": [
    {"from": "/Workspace/etl/ingest_orders", "rel": "reads_from", "to": "bronze.events_raw"},
    {"from": "task_ingest", "rel": "calls", "to": "/Workspace/etl/ingest_orders"},
    {"from": "nightly_events_job", "rel": "triggers", "to": "task_ingest"},
    {"from": "silver.events_clean", "rel": "depends_on", "to": "bronze.events_raw"}
  ]
}
```

**Naming rule:** always use the fully qualified name where one exists
(`catalog.schema.table`, full workspace path). Ambiguous short names cause
false-merge errors when graphs from different files get combined.

**Granularity rule:** don't create a node for every column by default — only
add column-level nodes if the user's question is specifically about
column-level lineage (e.g. "where does this specific field come from").
Otherwise table-level granularity keeps the graph readable.

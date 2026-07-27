---
name: databricks-knowledge-graph
description: >
  Builds a unified knowledge graph of a Databricks workspace, mapping both
  data lineage (which tables/views feed which) and code dependencies
  (notebooks, jobs, functions, and how they call each other). Use this skill
  when the user asks about impact analysis ("what breaks if I change X"),
  lineage tracing ("where does this table's data come from"), dependency
  mapping across notebooks/jobs, or wants a structured map of how a workspace
  fits together.
---

# Databricks Knowledge Graph

You are building a structured graph of a Databricks workspace — entities and
the relationships between them — so questions about impact, lineage, and
dependencies can be answered by traversal instead of re-reading raw code
every time.

Reference the guides below:
- `graph-schema.md` — node types, edge types, naming conventions
- `extraction-patterns.md` — how to pull entities/relationships out of
  notebooks, SQL, jobs configs, and Delta table metadata
- `query-patterns.md` — how to traverse the graph to answer common questions
- `examples.md` — sample Databricks artifacts, extracted graph, and expected answers
- `implementation-notes.md` — supported inputs, normalization rules, and extraction heuristics

---

## How to respond

### Step 1 — Scope the extraction
Identify the input: notebook, SQL, job config, or workspace export.
If the scope is clear, proceed without asking.

### Step 2 — Extract entities and relationships
Pull nodes and edges following `graph-schema.md` in one pass per file.

### Step 3 — Represent the graph
Output JSON triples so the graph is inspectable and reusable.

### Step 4 — Answer the actual question
Use `query-patterns.md` to traverse the graph, not to restate raw source.
For "what breaks if I change X," report downstream impact, not just direct
edges.

### Step 5 — Note what's NOT captured
Flag partial extraction when dynamic names, variable notebook paths, or
external job triggers are present.

### Step 6 — Persistence caveat
The graph exists only in this response/session. Recommend saving JSON if
persistence is needed.

---

## Tone and format
- Lead with the graph, then answer the question
- Use JSON blocks for the graph
- Be explicit about extraction gaps
- If the graph lacks the asked node, say so

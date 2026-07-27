# Query Patterns

How to traverse the graph once it's built, mapped to the questions users
actually ask.

---

## "What breaks if I change/drop table X?"
Traverse `depends_on` edges **backward** (find all nodes where `to == X`),
then continue outward — this is transitive, not just direct dependents.
Also check `reads_from` edges to find notebooks/jobs that read `X` directly,
even if no downstream table formally depends on it.

**Report format:** list direct dependents first, then second-hop, etc. Don't
flatten it into one undifferentiated list — the hop distance matters for
risk assessment.

---

## "Where does this table's data actually come from?"
Traverse `depends_on` edges **forward** (find all nodes `X depends_on`),
recursively, until you hit a node with no further `depends_on` edges (a
root/source table, usually bronze layer).

**Report format:** show the full chain in order, e.g.
`gold.customer_summary ← silver.customers ← bronze.customers_raw`, not just
the immediate parent.

---

## "Which job writes to this table?"
Find edges `{X} writes_to {table}`, then trace which `Job`/`Task` triggers
`X`. Report the job name, not just the notebook — users usually want to
know what to pause/monitor, which is job-level.

---

## "If I change this notebook/function, what else is affected?"
Traverse `calls` and `uses` edges outward from the changed node. This is
different from table-lineage impact — it's code-dependency impact. Report
both if the changed notebook also writes to a table (combine with the
table-impact traversal above).

---

## "Give me the full pipeline for table X"
Combine both traversals: forward `depends_on` for data lineage, plus the
`Job`/`Task`/`Notebook` chain that produces each hop. This is the "both
together" view — most useful when someone is onboarding to a pipeline they
didn't build.

---

## General traversal rules
- Always state the hop count / distance when reporting impact — "breaks
  directly" vs. "breaks two hops downstream" changes how urgent it is
- If a cycle appears (rare, but possible with merge/self-referencing jobs),
  flag it explicitly rather than looping silently
- If the graph doesn't contain a node the user asks about, say so plainly —
  don't guess at a relationship that wasn't actually extracted

---
name: databricks-token-optimization
description: >
  Audits and reduces token cost in Databricks notebook chat, SQL help, PySpark debugging,
  Delta table context, Unity Catalog metadata, and other notebook-heavy workflows.
  Use this skill to trim prompt bloat, reduce latency, and keep answers compact without hurting correctness.
---

# Databricks Token Optimization

Use this skill when a Databricks request is wasting tokens through long history, large notebook context, repeated schema details, Delta or Unity Catalog context, Spark config noise, or verbose output.

## Core workflow
1. Identify the smallest relevant slice of context.
2. Remove redundant layers and stale turns.
3. Keep only the context needed for correctness.
4. Report the main cost drivers before rewriting.
5. Return a compact answer with the fix first.

## Quick decision tree
- If the problem is mostly context size, history, or layer overlap, start with context-window-guide.md.
- If the problem is redundant instructions, pasted schemas, or bloated notebook snippets, start with input-patterns.md.
- If the problem is verbose answers or unsafe compression, start with output-and-structure.md.
- If the problem is model choice, mode selection, or billing cost, start with tools-and-billing.md.

## Priority rules
- Prefer the smallest safe change.
- Remove duplicate instructions before rewriting content.
- Prefer summaries over full history or full notebook dumps.
- If a compression could affect correctness, flag it.

## Common waste types
- REDUNDANT — the same rule or context appears in multiple layers
- STALE_HISTORY — old turns still occupy context after the task changed
- DEAD_CONTEXT — unrelated notebook cells, schemas, or outputs are included
- OUTPUT_BLOAT — the response is longer than the task needs
- OVER_SPECIFIED — the prompt includes default behavior that adds no value

## Fast triage
- If the issue is mostly prompt history, notebook state, or context layering, start with context-window-guide.md.
- If the issue is redundant instructions, long pasted context, or repeated schema details, start with input-patterns.md.
- If the issue is overly long answers or unsafe compression, start with output-and-structure.md.
- If the issue is model choice, mode selection, or billing cost, start with tools-and-billing.md.

## Stop conditions
- If the prompt is already lean, say so and stop.
- If the current task only needs a short fix, do not expand into a broader refactor.
- If correctness could be affected, flag the risk rather than over-compressing.

## Reference files
Use only the files that match the current issue.
- input-patterns.md — input-side waste and rewrite patterns
- output-and-structure.md — output compression and guardrails
- context-window-guide.md — token budgets and layer hierarchy
- tools-and-billing.md — cost drivers and model selection

## Default reply shape
- One-line savings summary
- Top 3 issues
- Recommended fix
- Risk or guardrail if any

If the prompt is already lean, say so explicitly.

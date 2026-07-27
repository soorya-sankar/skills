# Output, Structure & Guardrails

Use this file when the issue is output verbosity, response structure, or whether a compression is safe. Keep the answer short unless the task truly needs more detail.

Reference this file for output waste patterns, structure choices, compression priority, lossless vs. lossy rules, and guardrails for Databricks-style work.

Input waste patterns live in `input-patterns.md`.
Token budgets and context strategy live in `context-window-guide.md`.
Billing and model-cost guidance live in `tools-and-billing.md`.

---

## 🔴 High Impact — Output

### 10. Output bloat — responses longer than the task requires
**Waste type:** `OUTPUT_BLOAT`
**Problem:** Preambles, question restatements, and transitional commentary pad responses beyond what the task needs.
```
# ❌ Bad — restates the question and adds filler
"Great question! You asked about why your join is slow. Let me walk you through
the reasons a join can be slow in Spark before we look at your specific case..."

# ✅ Good — leads with the answer
"Your join is full_outer on two large tables — switch to inner and add
f.broadcast() for the lookup side."
```

**How to prevent output bloat:**
- "Answer with the fix first, explanation second"
- "Respond in under 200 words unless a code block is required"
- "Return only the changed lines, not the full notebook cell"
- For structured tasks: prefer bullets or a compact table over prose

**When output is load-bearing:** Never compress outputs containing accepted SQL, notebook steps, or decisions the user will reference later.

---

## 🟢 Best Practices

### 11. Model selection as a cost lever
**Problem:** Defaulting to a frontier model for every task applies the highest billing multiplier to work that does not need it.

| Task type | Appropriate tier | Example |
|-----------|-----------------|---------|
| Simple questions, renames, quick lookups | Lower tier | "What does this function return?" |
| Standard coding tasks | Mid tier | Most day-to-day notebook and SQL work |
| Architecture, hard debugging, complex reasoning | Frontier | Multi-step optimization or design work |

Use the smallest model that can do the job correctly.

---

### 12. Efficient layer structure
Each layer should contain only what belongs there. Never duplicate content across layers.

```
workspace instructions → global constraints, team-wide defaults, shared Databricks conventions
SKILL.md              → role, skill-specific constraints, output format, reference list
reference files       → examples, decision trees, config tables, pattern notes
```

Never put reference content inline in SKILL.md when it can live in a reference file.

---

### 13. Compression priority order
When you cannot compress everything at once, cut in this order:

1. **Wrong mode** — switch Agent → Ask/Chat for simple questions
2. **Tool/schema overhead** — disable unused integrations
3. **`STALE_HISTORY`** — highest volume, lowest risk
4. **`OVER_SPECIFIED`** defaults and ceremonial filler
5. **`REDUNDANT`** cross-layer instructions
6. **`DEAD_CONTEXT`** notebook cells, tables, or outputs
7. **`OUTPUT_BLOAT`** — constrain response format and reasoning effort
8. **`BLOATED_EXAMPLE`** — compress last

---

### 14. Lossless vs. lossy compression

**Lossless** (safe to apply without user confirmation):
- Delete `OVER_SPECIFIED` filler and defaults
- Remove `REDUNDANT` instructions from lower-priority layers
- Trim `STALE_HISTORY` turns with no active-task relevance
- Shorten `VERBOSE` role definitions and constraint lists
- Strip comments and docstrings from submitted code
- Disable unused integrations
- Switch mode from Agent to Ask/Chat for non-agentic tasks

**Lossy** (flag before applying, get user confirmation):
- Removing examples that encode a non-obvious Databricks pattern
- Summarizing history containing decisions or accepted SQL
- Stripping constraint instructions that shape behavior
- Hard-capping response length on open-ended debugging work

---

## Guardrails

Apply before removing or compressing anything:

- **Do not remove examples** that demonstrate a non-obvious Databricks or Delta behavior
- **Do not collapse multi-step instructions** into one line if the model needs to execute them in sequence
- **Do not strip constraint instructions** that affect correctness
- **Do not summarize history** containing accepted code, decisions, or notebook steps the active task references
- **Do not hard-cap response length** on tasks where output length is unpredictable
- **If a compression changes the model's behavior**, flag it and get user confirmation before applying
- **When in doubt**, flag the section as potentially load-bearing rather than cutting silently

---

## Practical Databricks response patterns

Use these when the task is routine and the answer should stay compact:
- Start with the fix or recommendation
- Include one short code block only if it materially helps
- Prefer a 3-bullet summary over a long narrative
- For SQL tuning, include the key change and the reason
- For notebook debugging, include the likely root cause before the patch

# Input Patterns — Anti-Patterns & Rewrites

Use this file when the issue is on the input side: redundant instructions, stale notebook history, bloated SQL or PySpark context, or unnecessary tool overhead. Keep the fixes practical and low-risk.

Reference this file when reducing input token usage across workspace instructions, skill files, reference files, notebook state, chat history, and Databricks-related tool schemas.

Token counting and cost guidance live in `tools-and-billing.md`.
Layer hierarchy and priority rules live in `context-window-guide.md`.

---

## 🔴 High Impact

### 1. Ceremonial filler and over-specified defaults
**Waste type:** `OVER_SPECIFIED`
**Problem:** Phrases describing default model behavior add cost with no behavioral value.
```
# ❌ Bad — ~60 tokens, changes nothing
You are a helpful, knowledgeable, and experienced assistant. Your goal is to
provide accurate, thoughtful, and well-reasoned answers to the user's questions.
Always be polite and professional.

# ✅ Good — role + non-default constraints only
You are a senior Databricks data engineer.
- Prefer PySpark DataFrame API over RDD API.
- Use Delta or SQL guidance only when the user asks for it.
```
**Fix:** Delete entirely. Do not move to another layer.

---

### 2. Redundant instructions across layers
**Waste type:** `REDUNDANT`
**Problem:** The same rule is repeated in workspace instructions and the skill file.
```
# ❌ Bad — same rule in two layers
[workspace instructions]  All examples must use Python and the DataFrame API.
[SKILL.md]               Provide all examples in Python and the DataFrame API.

# ✅ Good — keep in workspace instructions, remove from SKILL.md
[workspace instructions]  All examples must use Python and the DataFrame API.
[SKILL.md]               (line removed)
```
**Fix:** Delete the lower-priority copy.

---

### 3. Stale chat history
**Waste type:** `STALE_HISTORY`
**Problem:** Old turns remain in context even after the current notebook task has changed.
```
# ❌ Bad — a closed topic still occupies context
Turn 1 [user]: What is AQE in Spark?
Turn 2 [model]: [explanation]
...
Turn 9 [user]: Now help me optimize this Delta join.

# ✅ Good — summarize closed threads, keep only active context
[Earlier: explained AQE and showed a simple config example — resolved]
Turn 9 [user]: Now help me optimize this Delta join.
```
**Compression tactics:**
- Summarize closed threads in 1–2 sentences
- Drop acknowledgments such as "ok" and "got it"
- Keep only turns that directly inform the current notebook, SQL, or PySpark task

---

### 4. Bloated notebook or code context
**Waste type:** `DEAD_CONTEXT`
**Problem:** Sending full notebooks, long cell outputs, or unrelated code when only a small slice matters.
```python
# ❌ Bad — whole notebook sent, model uses 20 lines
[Full notebook: imports, config, ETL, charts, unrelated analysis...]
User: Why is this join slow?

# ✅ Good — slice to the relevant block
# [Lines 1–44 omitted — unrelated imports and setup]
# [Lines 73–300 omitted — downstream writes and reporting]
# Lines 45–72 — join block (focus of question)
result = orders.join(customers, on="customer_id", how="full")
User: Why is this join slow?
```

**For background tables or schemas — describe, don't paste:**
```
# orders: 800M rows, partitioned by event_date, ~2TB uncompressed
# customers: 12M rows, no partitioning
```
A single line can replace a large schema dump.

---

### 5. Verbose role and constraint definitions
**Waste type:** `VERBOSE`
**Problem:** Multi-sentence descriptions can be rewritten more compactly.
```
# ❌ Bad — 35 tokens
You are an expert data engineer with deep experience in distributed systems,
Apache Spark, and cloud data platforms, particularly Databricks.

# ✅ Good — 9 tokens
You are a senior Databricks data engineer.
```

**Positive rewrites:**
```
# ❌ Bad
Do not use RDD API. Do not suggest deprecated methods. Do not include
import statements the user didn't ask for.

# ✅ Good
Use DataFrame API. Skip imports and Spark basics unless asked.
```

---

### 6. Repeated context in multi-turn sessions
**Waste type:** `VERBOSE`
**Problem:** The same setup is repeated every turn when a short reference would suffice.
```
# ❌ Bad — the full context is repeated
Turn 3: I'm working on a Databricks notebook that reads Delta tables and joins
        two large tables. The job is slow. Can you help with the join?
Turn 5: I'm still working on that same Databricks Delta join. Can you help with
        the shuffle partition count now?

# ✅ Good — brief references after the first setup
Turn 3: The join is slow — [paste join block]
Turn 5: Now fix the shuffle partition count — [paste config block]
```

---

### 7. Verbose examples — full code when a pattern reference suffices
**Waste type:** `BLOATED_EXAMPLE`
**Problem:** Long code examples can be replaced with a compact pattern note.
```
# ❌ Bad — a full 40-line example when the point is simple
# (model already knows broadcast joins)

# ✅ Good — pattern reference only
Prefer f.broadcast(small_df) for small lookup tables.
Do not hardcode the threshold if table size may grow.
```

---

### 8. Tool schema overhead
**Waste type:** `DEAD_CONTEXT`
**Problem:** Enabled integrations add context even when the task only needs one narrow capability.

**How to reduce:**
- Disable unused extensions or tool integrations for the current task
- Keep the task scoped to a single notebook, SQL problem, or small refactor
- Prefer built-in editor actions when possible instead of adding another layer of tooling

---

### 9. Wrong Copilot mode for the task
**Waste type:** `DEAD_CONTEXT`
**Problem:** Agent mode is often overkill for simple notebook questions or small SQL fixes.

| Task | Right mode |
|------|-----------|
| Question, explanation, quick lookup | Ask / Chat |
| Single-file or single-cell edit | Edit |
| Multi-file or multi-notebook refactor | Agent |

```
# ❌ Bad — Agent mode for a simple SQL question
[Agent mode] What does this join do?

# ✅ Good — Ask mode for the same question
[Ask mode] What does this join do?
```

**Key rule:** Clarify scope first, then escalate to Agent only when the task truly requires it.

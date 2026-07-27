# Tools & Billing Reference

Use this file when the issue is billing, mode selection, model routing, or measurement. Focus on the biggest savings first.

This reference is tailored to Databricks-style work: notebook chat, SQL editing, PySpark debugging, Delta/Unity Catalog context, and repeated notebook state.

---

## 1. Billing model (as of June 2026)

GitHub Copilot uses **usage-based billing** — every token costs AI credits drawn from a pooled monthly allowance per seat.

- 1 AI Credit = $0.01 USD
- Business plan: ~$30/seat/month credit pool
- Enterprise plan: ~$70/seat/month credit pool
- Code completions and Next Edit Suggestions: **unlimited** (not metered)
- Everything else (Chat, Agent, CLI, Code Review): **metered by token**

**What gets billed:**
- Input tokens (everything sent to the model per request)
- Output tokens (everything the model returns)
- Cached tokens (reduced rate — prompt caching helps here)

**Model billing multipliers** (approximate):

| Model tier | Multiplier vs. base | Examples |
|-----------|---------------------|---------|
| Lower tier | 1× | GPT-4o mini, Haiku |
| Mid tier | 1–3× | GPT-4o, Claude Sonnet |
| Frontier | 6–27× | Claude Opus, GPT-5 class |

**Databricks-specific note:** Notebook questions that only need a SQL fix or a short PySpark explanation usually do not need frontier-model pricing. Use a smaller model unless the task truly requires deep debugging or architecture reasoning.

---

## 2. Mode cost comparison

Choosing the right mode is the highest-leverage token decision.

| Mode | Internal LLM calls | Approximate input tokens | When to use |
|------|-------------------|------------------------|------------|
| Ask / Chat | 1 | ~1–3K | Questions, explanations, quick lookups |
| Edit | 1–2 | ~2–5K | Single-cell or single-file changes |
| Agent | 5–25 | ~10–100K+ | Multi-file or multi-notebook refactors with clear scope |
| Agent (vague prompt) | 10–40+ | ~50–200K+ | Never — clarify in Ask first |

**Practical rule:** Use Ask for a quick SQL or notebook question, Edit for a targeted cell rewrite, and Agent only when the task really needs autonomous exploration.

**Reasoning / thinking effort:**

| Effort level | Output token cost | When to use |
|-------------|-----------------|------------|
| Low | Minimal | Renames, formatting, simple lookups |
| Medium (default) | Moderate | Most standard notebook and SQL tasks |
| High | Tens of thousands of extra tokens | Hard debugging, architecture decisions |

---

## 3. Cost math for Databricks workflows

### Notebook context
A notebook with several pasted cells, outputs, and prior runs can easily exceed a compact task budget. The cheapest fix is usually to trim the pasted context rather than to change the model.

### SQL and PySpark examples
A full SQL rewrite or long PySpark example often costs more than the actual question. Keep examples short and targeted.

### Delta and schema context
If the same table shape is repeated across turns, summarize it once instead of pasting it repeatedly.

### Tool overhead
If a task only needs a notebook or file edit, avoid turning on extra integrations or toolsets that add context overhead without helping the current request.

---

## 4. Token measurement tools

### Heuristic (fast, no dependencies)
- 1 token ≈ 4 characters
- 1 token ≈ ¾ of an English word
- A typical paragraph ≈ 60–100 tokens
- A 50-line code block ≈ 300–500 tokens

**Python helper:**
```python
def approx_tokens(text: str) -> int:
    return max(1, len(text) // 4)

text = open('some_file.md', 'r', encoding='utf-8').read()
print('Approx tokens:', approx_tokens(text))
```

### VS Code — Chat Debug View
Use it to inspect the real prompt and response size for a Databricks-related session.

### Copilot CLI — `/context` command
Use `/context` to see whether the problem is prompt history, notebook state, or tool/schema overhead.

### Practical audit checklist
- Check whether the chat history is still carrying closed notebook topics
- Check whether the current prompt includes the full notebook instead of the relevant cells
- Check whether the same table schema or config is repeated across turns
- Check whether the task could be handled in Ask or Edit instead of Agent

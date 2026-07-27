# Context Window Guide

Use this file for context budgeting, history trimming, notebook state, and layer hierarchy. The goal is to keep the prompt within budget without losing correctness.

## Quick budget targets
- Workspace instructions: 200–500 tokens
- Skill instructions: 300–600 tokens
- Reference files: 1,000–3,000 tokens total
- Notebook cell context: 500–1,500 tokens
- Chat history: 1,000–2,000 tokens
- Table/schema context: 300–1,000 tokens
- Total input target: 3,000–8,000 tokens

Compress when a layer exceeds roughly 2x its target.

## Layer hierarchy
Use the highest-priority layer that can carry the rule:
1. System context
2. Workspace instructions
3. SKILL.md
4. Reference files
5. Notebook state
6. Chat history
7. Table/schema context
8. User message

Do not repeat the same constraint in multiple layers.

## History strategy
- Keep the last 2–4 turns that directly matter.
- Summarize resolved threads in one line.
- Drop acknowledgments and closed-side topics.
- Save accepted decisions to a notes file if the session will continue.

## Notebook and Databricks context
- Send only the relevant cells or SQL block.
- Replace large schema dumps with one-line summaries.
- Close unrelated notebooks or outputs.
- Prefer explicit context references over full notebook state.

## Structural rules
- Put global rules in workspace instructions.
- Put skill-specific rules in SKILL.md.
- Put examples and decision trees in reference files.
- Remove filler such as “be helpful” or “be accurate” unless it changes behavior.
